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
		const sbp0i::TreeNode *mtch = node->findNode(prefix);

		if (!mtch) {
			// damn, nobody is responsible yet. we could: assign a lingering node, and change the tree
			FILE_LOG(logDEBUG) << node->getSocket()
					<< " nobody responsible for prefix " << prefix;
			return;
		}

		// am I responsible for storing?
		if (mtch->node() == node->getSocket()) {
			FILE_LOG(logDEBUG) << node->getSocket()
					<< " storing data for prefix " << prefix << " locally";
			node->store(m);
			// TODO: store
			return;
		}

		if (mtch != NULL) {
			// forward messaged to that node!
			FILE_LOG(logDEBUG) << node->getSocket() << " found prefix "
					<< prefix << ", going to " << mtch->node();
			// OR: forward to next node in tree on the way to dest.
			node->send(mtch->node(), m);
			return;
		}
	}
};

class LoadColumnDataHandler: public MessageHandler, public ResponseHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id) {
		sbp0i::LoadColumnData *m = (sbp0i::LoadColumnData*) msg;

		string prefix = getPrefix(m);
		const sbp0i::TreeNode *mtch = node->findNode(prefix);

		if (mtch == NULL) {
			// no match, no data, return empty response
			FILE_LOG(logDEBUG) << node->getSocket()
					<< " no node found for prefix " << prefix;
			return;
		}

		// do I have this?
		if (mtch->node() == node->getSocket()) {
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
				<< " forwarding load for prefix " << prefix << " going to "
				<< mtch->node();
		// OR: forward to next node in tree on the way to dest.
		node->send(mtch->node(), m);
		return;
	}

	void response(Node *node, google::protobuf::Message *msg,
			string inResponseTo) {
		sbp0i::StoreColumnData *m = (sbp0i::StoreColumnData*) msg;
		FILE_LOG(logDEBUG) << node->getSocket() << " got response on "
				<< inResponseTo;
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

	// TODO: convert tree to map and flood updates to other nodes
	sbp0i::TreeNode* c1 = n1.getTree()->add_children();
	c1->set_node(n2.getSocket());
	c1->set_prefix("/region/r_regionkey/");

	sbp0i::TreeNode* c2 = n1.getTree()->add_children();
	c2->set_node(n1.getSocket());
	c2->set_prefix("/region/r_name/");

	sbp0i::TreeNode* c12 = n2.getTree()->add_children();
	c12->set_node(n2.getSocket());
	c12->set_prefix("/region/r_regionkey/");

	sbp0i::TreeNode* c22 = n2.getTree()->add_children();
	c22->set_node(n1.getSocket());
	c22->set_prefix("/region/r_name/");

	for (vector<int>::size_type i = 0; i != data.getData().size(); i++) {
		n1.send("inproc://A", &data.getData()[i]);
	}

	// now lets get sth back

	sleep(1);

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
// Verify that the version of the library that we linked against is
// compatible with the version of the headers we compiled against.
	GOOGLE_PROTOBUF_VERIFY_VERSION;
	testWrite();

	exit(0);
}

