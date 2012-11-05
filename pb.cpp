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

class StoreColumnDataHandler: public MessageHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id) {
		sbp0i::StoreColumnData *m = (sbp0i::StoreColumnData*) msg;

		string prefix = getPrefix(m);
		string target = node->findNode(prefix);

		bool setForce = false;

		if (target == "" && !m->force()) {
			// damn, nobody is responsible yet. we could: assign a lingering node, and change the tree

			// go to the node that is closest in the prefix tree
			map<string, string> rt = node->getRoutingTable();
			int maxOverlap = 1; // since prefixes start with /
			string bestMatch = "";
			for (map<string, string>::iterator iterator = rt.begin();
					iterator != rt.end(); iterator++) {
				int spl = samePrefixLength(prefix, iterator->first);
				if (spl > maxOverlap) {
					bestMatch = iterator->second;
					maxOverlap = spl;
				}
			}
			// if that is still nothing, we are responsible
			if (bestMatch != "") {
				target = bestMatch;
				FILE_LOG(logDEBUG) << node->getSocket()
						<< " nobody responsible for prefix " << prefix
						<< ", going to next best match " << target;
			} else {
				target = node->getSocket();
				FILE_LOG(logDEBUG) << node->getSocket()
						<< " nobody responsible for prefix " << prefix
						<< ", storing here";
			}
		}

		if (target == node->getSocket() && node->isOverloaded()) {
			setForce = true;
			FILE_LOG(logDEBUG) << node->getSocket() << " overloaded";
			// if we have a lingering node, add it to rt, forward data there
			if (node->getLingeringNodes()->size() > 0) {
				target = node->getLingeringNodes()->back();
				// TODO: check if ln is still alive...
				node->getLingeringNodes()->pop_back();
				FILE_LOG(logDEBUG) << node->getSocket() << " forwarding "
						<< prefix << " to lingering node " << target;

			} else {
				// are there other nodes? forward there, they may be less loaded
				// TODO: BUG: this gets not called yet
				map<string, string> rt = node->getRoutingTable();
				for (map<string, string>::iterator iterator = rt.begin();
						iterator != rt.end(); iterator++) {
					if (iterator->second != node->getSocket()) {
						target = iterator->second;
						FILE_LOG(logDEBUG) << node->getSocket()
								<< " forwarding " << prefix << " to "
								<< iterator->second << " out of desperation";
					}
				}
			}
			// if not, store here anyway (no action required)
		}

		// am I responsible for storing?
		if (target == node->getSocket() || m->force()) {
			FILE_LOG(logDEBUG) << node->getSocket()
					<< " storing data for prefix " << prefix;
			node->store(m);

			// check if this decision is reflected in the current routing table
			string rt = node->findNode(prefix);
			if (rt == "") {
				node->addRoutingEntry(prefix, node->getSocket());
				FILE_LOG(logDEBUG) << node->getSocket()
						<< " now responsible for prefix " << prefix;
				// TODO: paxos for update, later
				map<string, string> rt = node->getRoutingTable();
				for (map<string, string>::iterator iterator = rt.begin();
						iterator != rt.end(); iterator++) {
					if (iterator->second != node->getSocket()) {
						node->send(iterator->second, node->getRoutingMessage());
					}
				}
			}
			return;

		}

		m->set_force(setForce);

		// forward messaged to that node!
		FILE_LOG(logDEBUG) << node->getSocket()
				<< " forwarding data for prefix " << prefix << " to " << target;
		// OR: forward to next node in tree on the way to dest.
		node->send(target, m);
		return;

	}
};

class RoutingTableHandler: public MessageHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id) {
		sbp0i::RoutingTable *m = (sbp0i::RoutingTable*) msg;

		FILE_LOG(logDEBUG) << node->getSocket()
				<< " installing new routing table";
		for (int j = 0; j < m->entries_size(); j++) {
			const sbp0i::RoutingTable_RoutingTableEntry& entry = m->entries(j);
			node->addRoutingEntry(entry.prefix(), entry.node());
		}

	}
};

class NewNodeHandler: public MessageHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id) {
		sbp0i::NewNode *m = (sbp0i::NewNode*) msg;
		// add node to local list
		node->addLingeringNode(m->node());
		FILE_LOG(logDEBUG) << node->getSocket() << " new lingering node "
				<< m->node();
		//
		if (m->forward()) {
			m->set_forward(false);
			map<string, string> rt = node->getRoutingTable();
			for (map<string, string>::iterator iterator = rt.begin();
					iterator != rt.end(); iterator++) {
				if (iterator->second != node->getSocket()) {
					node->send(iterator->second, m);
				}
			}
		}
		return;
	}
}
;

void testBootstrap() {
	zmq::context_t context(1);

	NewNodeHandler *nnhandler = new NewNodeHandler();
	StoreColumnDataHandler *scdhandler = new StoreColumnDataHandler();
	LoadColumnDataHandler *lcdhandler = new LoadColumnDataHandler();
	RoutingTableHandler *rthandler = new RoutingTableHandler();

	Node n1(&context);
	n1.listen("inproc://A");
	n1.registerHandler("sbp0i.NewNode", nnhandler);
	n1.registerHandler("sbp0i.StoreColumnData", scdhandler);
	n1.registerHandler("sbp0i.LoadColumnData", lcdhandler);
	n1.registerHandler("sbp0i.RoutingTable", rthandler);

	Node n2(&context);
	n2.listen("inproc://B");
	n2.registerHandler("sbp0i.NewNode", nnhandler);
	n2.registerHandler("sbp0i.StoreColumnData", scdhandler);
	n2.registerHandler("sbp0i.LoadColumnData", lcdhandler);
	n2.registerHandler("sbp0i.RoutingTable", rthandler);

	Node n3(&context);
	n3.listen("inproc://C");
	n3.registerHandler("sbp0i.NewNode", nnhandler);
	n3.registerHandler("sbp0i.StoreColumnData", scdhandler);
	n3.registerHandler("sbp0i.LoadColumnData", lcdhandler);
	n3.registerHandler("sbp0i.RoutingTable", rthandler);

	usleep(10);

	sbp0i::NewNode nn2;
	nn2.set_node(n2.getSocket());
	nn2.set_forward(true);
	n2.send(n1.getSocket(), &nn2);

	sbp0i::NewNode nn3;
	nn3.set_node(n3.getSocket());
	nn3.set_forward(true);
	n3.send(n1.getSocket(), &nn3);

	// now hammer n1 it with data
	TpcFile data = TpcFile("tpc-h-0.01/lineitem.tbl");
	data.parse();

	for (vector<int>::size_type i = 0; i != data.getData().size(); i++) {
		n1.send("inproc://A", &data.getData()[i]);
	}

	usleep(10);

	sbp0i::LoadColumnData load1;
	load1.set_relation("lineitem");
	load1.set_column("l_orderkey");
	load1.set_value("42");
	load1.set_origin(n3.getSocket());

	n3.sendR("inproc://A", &load1, lcdhandler, 1000);

	usleep(1000);

	n1.printRoutingTable();
	n2.printRoutingTable();
	n3.printRoutingTable();

	// until some limit is tripped, and n1 decides to extend the routing table, using lingering nodes

	// now continue to hammer, until n3 is also used

	pthread_exit(0);
}

class PaxosNode: public Node {
public:
	PaxosNode(zmq::context_t *aContext) :
			Node(aContext) {
	}
	vector<string> acceptors;

private:
	unsigned int proposalNr;
};

class PrepareHandler: public MessageHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id) {

	}
}
;

class PromiseHandler: public MessageHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id) {

	}
}
;

class AcceptRequestHandler: public MessageHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id) {

	}
}
;

class AcceptHandler: public MessageHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id) {

	}
}
;
void testPaxos() {
	zmq::context_t context(1);

	vector<PaxosNode*> nodes;

	for (int i = 0; i < 10; i++) {
		PaxosNode* pn = new PaxosNode(&context);
		pn->listen("inproc://node" + intToStr(i));
		nodes.push_back(pn);
	}

	for (vector<PaxosNode*>::iterator it = nodes.begin(); it != nodes.end();
			++it) {
		for (vector<PaxosNode*>::iterator it2 = nodes.begin();
				it2 != nodes.end(); ++it2) {
			if ((*it)->getSocket() != (*it2)->getSocket()) {
				(*it)->acceptors.push_back((*it2)->getSocket());
			}
		}
	}

	sleep(1);
	pthread_exit(0);
}

int main(int argc, char* argv[]) {
	srand(time(NULL));

// Verify that the version of the library that we linked against is
// compatible with the version of the headers we compiled against.
	GOOGLE_PROTOBUF_VERIFY_VERSION;
	testPaxos();

	exit(0);
}

