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
		//node->send(m->origin(), &resp, node->createMessageId(), id);
	}
};

class StoreColumnDataHandler: public MessageHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id) {
		sbp0i::StoreColumnData *m = (sbp0i::StoreColumnData*) msg;

		FILE_LOG(logDEBUG) << node->getSocket() << " got scd: " << getPrefix(m)
				<< ", " << m->entries_size() << " entries";

		// am I responsible for storing?

		// // do I want to? do I have lingering nodes around?

		const sbp0i::TreeNode *mtch = node->findNode(getPrefix(m));
		if (mtch != NULL) {
			// forward message to that node!
			FILE_LOG(logDEBUG) << "found prefix on node, going to "
					<< mtch->node();
			// OR: forward to next node in tree on the way to dest.
			node->send(mtch->node(), m);
			return;
		}
		// damn, nobody is responsible yet. we could: assign a lingering node, and change the tree

		// or we could forward to the next best node
	}
};

class HelloRespHandler: public ResponseHandler {
public:
	void response(Node *node, google::protobuf::Message *msg,
			string inResponseTo) {
		FILE_LOG(logDEBUG) << "response";
	}
	void timeout(Node *node, string inResponseTo) {
		FILE_LOG(logDEBUG) << "timeout";
	}
};

void testsetup() {
	zmq::context_t context(1);

	Node n1(&context);
	n1.listen("inproc://foo1");

	Node n2(&context);
	n2.listen("inproc://foo2");
	sleep(1);

	HelloHandler *hh = new HelloHandler();
	n2.registerHandler("sbp0i.Hello", hh);

	HelloRespHandler *hrh = new HelloRespHandler();

	sbp0i::Hello h;
	h.set_origin(n1.getSocket());
	n1.sendR(n2.getSocket(), &h, hrh, 1000);


	/*
	StoreColumnDataHandler *scdhandler = new StoreColumnDataHandler();

	n1.registerHandler("sbp0i.StoreColumnData", scdhandler);
	n2.registerHandler("sbp0i.StoreColumnData", scdhandler);

	TpcFile data = TpcFile("tpc-h-0.01/region.tbl");
	data.parse();

	sbp0i::TreeNode* newChild = n1.getTree()->add_children();
	newChild->set_node(n2.getSocket());
	newChild->set_prefix("/region/r_regionkey/");

	for (vector<int>::size_type i = 0; i != data.getData().size(); i++) {
		n1.send("inproc://foo1", &data.getData()[i]);
	}
*/
	sleep(5);

	pthread_exit(0);
}

int main(int argc, char* argv[]) {
// Verify that the version of the library that we linked against is
// compatible with the version of the headers we compiled against.
	GOOGLE_PROTOBUF_VERIFY_VERSION;
	testsetup();

	exit(0);
}

