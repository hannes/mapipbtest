#include <iostream>
#include <fstream>
#include <string>
#include <snappy.h>
#include <ctime>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sstream>
#include <vector>

#include "lib/zmq.hpp"
#include "lib/log.h"

#include "Node.hpp"
#include "TpcFile.hpp"
#include "Utils.hpp"

#include "protobuf/messages.pb.h"
using namespace std;

class HelloHandler: public MessageHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id) {
		sbp0i::Hello *m = (sbp0i::Hello*) msg;
		FILE_LOG(logDEBUG) << node->getSocket() << " got hello from: "
				<< m->origin() << " id:" << id;
		sbp0i::Hello resp;
		resp.set_origin(node->getSocket());
		node->send(m->origin(), &resp, node->createMessageId(), id);
	}
};

class HelloRespHandler: public ResponseHandler {
public:
	void response(Node *node, google::protobuf::Message *msg,
			string inResponseTo) {
		FILE_LOG(logDEBUG) << node->getSocket() << " got response to "
				<< inResponseTo;
	}
	void timeout(Node *node, string inResponseTo) {
		FILE_LOG(logDEBUG) << node->getSocket() << " got timeout on "
				<< inResponseTo;
	}
};

class StoreColumnDataHandler: public MessageHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id) {
		sbp0i::StoreColumnData *m = (sbp0i::StoreColumnData*) msg;

		string prefix = getPrefix(m);
		string target = node->findNode(prefix);

		if (target == "") {
			// damn, nobody is responsible yet. we could: assign a lingering node, and change the tree
			FILE_LOG(logDEBUG) << node->getSocket()
					<< " nobody responsible for prefix " << prefix;
			return;
		}

		// am I responsible for storing?
		if (target == node->getSocket()) {
			FILE_LOG(logDEBUG) << node->getSocket()
					<< " storing data for prefix " << prefix;
			node->store(m);
			// TODO: store
			return;
		}

		// forward messaged to that node!
		FILE_LOG(logDEBUG) << node->getSocket()
				<< " forwarding data for prefix " << prefix << " to " << target;
		// OR: forward to next node in tree on the way to dest.
		node->send(target, m);
		return;

	}
};

class LoadColumnDataHandler: public MessageHandler, public ResponseHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id) {
		sbp0i::LoadColumnData *m = (sbp0i::LoadColumnData*) msg;

		string prefix = getPrefix(m);
		string target = node->findNode(prefix);

		if (target == "") {
			// no match, no data, return empty response
			FILE_LOG(logDEBUG) << node->getSocket()
					<< " no node found for prefix " << prefix;
			return;
		}

		// do I have this?
		if (target == node->getSocket()) {
			FILE_LOG(logDEBUG) << node->getSocket()
					<< " loading data for prefix " << prefix;

			sbp0i::StoreColumnData res = node->load(m);
			node->send(m->origin(), &res, node->createMessageId(), id);
			FILE_LOG(logDEBUG) << node->getSocket()
					<< " sending loaded data for prefix " << prefix << " to "
					<< m->origin();

			return;
		}

		// forward messaged to that node!
		FILE_LOG(logDEBUG) << node->getSocket()
				<< " forwarding load for prefix " << prefix << " to " << target;
		// OR: forward to next node in tree on the way to dest.
		node->send(target, m);
		return;
	}

	void response(Node *node, google::protobuf::Message *msg,
			string inResponseTo) {
		sbp0i::StoreColumnData *m = (sbp0i::StoreColumnData*) msg;
		FILE_LOG(logDEBUG) << node->getSocket() << " got response on "
				<< inResponseTo << ", " << m->entries_size() << " entries.";

		printColumn(m);
	}
	void timeout(Node *node, string inResponseTo) {
		FILE_LOG(logDEBUG) << node->getSocket() << " got timeout on "
				<< inResponseTo;
	}
};

void testResponse() {
	zmq::context_t context(1);

	Node n1(&context);
	n1.listen("inproc://A");

	Node n2(&context);
	n2.listen("inproc://B");
	sleep(1);

	HelloHandler *hh = new HelloHandler();
	n2.registerHandler("sbp0i.Hello", hh);

	HelloRespHandler *hrh = new HelloRespHandler();

	sbp0i::Hello h;
	h.set_origin(n1.getSocket());
	n1.sendR(n2.getSocket(), &h, hrh, 100);

	sleep(1);

	pthread_exit(0);
}

void testWrite() {

	zmq::context_t context(1);

	Node n1(&context);
	n1.listen("inproc://A");

	Node n2(&context);
	n2.listen("inproc://B");
	//sleep(1);

	StoreColumnDataHandler *scdhandler = new StoreColumnDataHandler();
	LoadColumnDataHandler *lcdhandler = new LoadColumnDataHandler();

	n1.registerHandler("sbp0i.StoreColumnData", scdhandler);
	n1.registerHandler("sbp0i.LoadColumnData", lcdhandler);

	n2.registerHandler("sbp0i.StoreColumnData", scdhandler);
	n2.registerHandler("sbp0i.LoadColumnData", lcdhandler);

	TpcFile data = TpcFile("tpc-h-0.01/region.tbl");
	data.parse();

	n1.addRoutingEntry("/region/r_regionkey/", n2.getSocket());
	n1.addRoutingEntry("/region/r_name/", n1.getSocket());
	n2.addRoutingEntry("/region/r_regionkey/", n2.getSocket());
	n2.addRoutingEntry("/region/r_name/", n1.getSocket());

	for (vector<int>::size_type i = 0; i != data.getData().size(); i++) {
		n1.send("inproc://A", &data.getData()[i]);
	}

	// now lets get sth back

	usleep(10);

	sbp0i::LoadColumnData load1;
	load1.set_relation("region");
	load1.set_column("r_regionkey");
	load1.set_value("1");
	load1.set_origin(n1.getSocket());

	n1.sendR("inproc://B", &load1, lcdhandler, 1000);

	sleep(1);

	pthread_exit(0);
}

int main(int argc, char* argv[]) {
	srand(time(NULL));

// Verify that the version of the library that we linked against is
// compatible with the version of the headers we compiled against.
	GOOGLE_PROTOBUF_VERIFY_VERSION;
	testWrite();

	exit(0);
}

