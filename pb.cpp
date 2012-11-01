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

static const int numMessages = 10000;
static const int messageSize = 1024;

void basic() {
	zmq::context_t context(1);

	Node n1(&context);

	Node n2(&context);
	n2.listen("tcp://*:5555");

	Node n3(&context);
	n3.listen("tcp://*:5556");

	clock_t start = clock();

	for (int i = 0; i != numMessages; i++) {
		sbp0i::DummyMessage msg;
		msg.set_name(genRndStr(messageSize));
		msg.set_id(i);
		msg.set_target("tcp://localhost:5556");
		n1.send("tcp://localhost:5555", &msg);
	}
	clock_t end = clock();
	double elapsed_secs = double(end - start) / CLOCKS_PER_SEC;

	FILE_LOG(logINFO) << "Serialization, Compression & Transmission of "
			<< numMessages << " Messages took " << elapsed_secs << "s" << endl;
	pthread_exit(0);
}

string convertInt(int number) {
	stringstream ss;
	ss << number;
	return ss.str();
}

static const int numNodes = 30;

void ring() {
	zmq::context_t context(1);

	string proto = "tcp://127.0.0.1:1000";

	vector<Node*> nodes;
	for (int i = 0; i != numNodes; i++) {
		Node *n = new Node(&context);
		n->listen(proto + convertInt(i));
		nodes.push_back(n);
	}
	sbp0i::StillePost msg;
	msg.set_text(genRndStr(10000000));
	msg.set_pos(0);
	msg.set_proto(proto);
	msg.set_max(numNodes);
	msg.set_hops(0);

	nodes[0]->send(nodes[1]->getSocket(), &msg);

	pthread_exit(0);
}

void fileread() {
	TpcFile customer = TpcFile("tpc-h-0.01/customer.tbl");
	customer.parse();
	TpcFile lineitem = TpcFile("tpc-h-0.01/lineitem.tbl");
	lineitem.parse();
	TpcFile nation = TpcFile("tpc-h-0.01/nation.tbl");
	nation.parse();
	TpcFile orders = TpcFile("tpc-h-0.01/orders.tbl");
	orders.parse();
	TpcFile part = TpcFile("tpc-h-0.01/part.tbl");
	part.parse();
	TpcFile partsupp = TpcFile("tpc-h-0.01/partsupp.tbl");
	partsupp.parse();
	TpcFile supplier = TpcFile("tpc-h-0.01/supplier.tbl");
	supplier.parse();
	TpcFile region = TpcFile("tpc-h-0.01/region.tbl");
	region.parse();
}

void testwrite() {
	zmq::context_t context(1);

	TpcFile data = TpcFile("tpc-h-0.01/region.tbl");
	data.parse();

	Node n1(&context);
	Node n2(&context);
	n2.listen("inproc://foo");
	sleep(1);

	for (vector<int>::size_type i = 0; i != data.getData().size(); i++) {
		n1.send("inproc://foo", &data.getData()[i]);
	}

	pthread_exit(0);
}

void testsetup() {
	zmq::context_t context(1);

	Node n1(&context);
	n1.listen("inproc://foo1");

	Node n2(&context);
	n2.listen("inproc://foo2");
	sleep(1);

	n2.sayHi(n1.getSocket());

	TpcFile data = TpcFile("tpc-h-0.01/region.tbl");
	data.parse();

	sbp0i::TreeNode* newChild = n1.getTree()->add_children();
	newChild->set_node(n2.getSocket());
	newChild->set_prefix("/region/r_regionkey/");

	for (vector<int>::size_type i = 0; i != data.getData().size(); i++) {
		n1.send("inproc://foo1", &data.getData()[i]);
	}
	sleep(1);

	pthread_exit(0);
}

int main(int argc, char* argv[]) {
// Verify that the version of the library that we linked against is
// compatible with the version of the headers we compiled against.
	GOOGLE_PROTOBUF_VERIFY_VERSION;
	testsetup();

	exit(0);
}

