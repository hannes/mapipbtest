#include <string>

#include <snappy.h>
#include <stdio.h>
#include <iostream>
#include <pthread.h>
#include <vector>
#include <map>

#include <google/protobuf/descriptor.h>
#include "protobuf/messages.pb.h"
#include "lib/log.h"
#include "lib/zmq.hpp"
#include "Utils.hpp"

using namespace std;

class Node {

public:
	Node(zmq::context_t *aContext) {
		context = aContext;
		// assign to prevent compiler warning
		pollert = -1;
	}
	void listen(string anAddress);
	void sayHi(string anAddress);

	void receive(string msg);
	bool send(string anAddress, google::protobuf::Message *msg);
	string getSocket();
	zmq::context_t* getContext();
	sbp0i::TreeNode* getTree();

	~Node();

private:
	void hStoreColumnData(sbp0i::StoreColumnData);
	void hStillePost(sbp0i::StillePost m);
	void hDummyMessage(sbp0i::DummyMessage m);
	void hHello(sbp0i::Hello m);
	string getPrefix(sbp0i::StoreColumnData m);
	const sbp0i::TreeNode* findNode(const sbp0i::TreeNode *n, string prefix);

	pthread_t pollert;
	zmq::context_t *context;
	string serverSocketName;
	map<string, zmq::socket_t*> sendSockets;
	vector<string> lingeringNodes;
	sbp0i::TreeNode prefixTree;
};
