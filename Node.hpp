#include <string>

#include <snappy.h>
#include <stdio.h>
#include <iostream>
#include <pthread.h>
#include <vector>
#include <map>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include "protobuf/messages.pb.h"
#include "lib/log.h"
#include "lib/zmq.hpp"
#include "Utils.hpp"

using namespace std;

// No comment on this
class Node;
typedef google::protobuf::Message Msg;

class MessageHandler {
public:
	virtual void handle(Node *node, google::protobuf::Message *msg,
			string id) = 0;
};

class ResponseHandler {
public:
	virtual void response(Node *node, google::protobuf::Message *msg,
			string inResponseTo) = 0;
	virtual void timeout(Node *node, string inResponseTo) = 0;
};

class Waiting {
public:
	long expireTime;
	ResponseHandler *handler;
};

class Node {

	// TODO: guard waiting map against concurrent access!

public:
	Node(zmq::context_t *aContext) {
		context = aContext;
		// assign to prevent compiler warning
		pollert = 0;
		timeoutt = 0;
	}
	void listen(string anAddress);
	void receive(string msg);

	// Fire & Forget
	bool send(string anAddress, Msg *msg);
	bool send(string anAddress, Msg *msg, string id);
	bool send(string anAddress, Msg *msg, string id, string inResponseTo);

	// Require a Response
	bool sendR(string anAddress, Msg *msg, ResponseHandler *respHandler,
			int timeoutMsecs);

	void registerHandler(string messageType, MessageHandler *handler);

	string getSocket();
	string createMessageId();

	zmq::context_t* getContext();
	sbp0i::TreeNode* getTree();
	map<string, Waiting>* getWaiting();
	const sbp0i::TreeNode* findNode(string prefix);

	void store(sbp0i::StoreColumnData *data);

	sbp0i::StoreColumnData load(sbp0i::LoadColumnData *data);

	~Node();

private:
	const sbp0i::TreeNode* findNode(const sbp0i::TreeNode *n, string prefix);

	pthread_t pollert;
	pthread_t timeoutt;

	zmq::context_t *context;
	string serverSocketName;
	map<string, zmq::socket_t*> sendSockets;
	vector<string> lingeringNodes;
	sbp0i::TreeNode prefixTree;
	map<string, MessageHandler*> handlers;
	map<string, Waiting> waiting;

	vector<sbp0i::StoreColumnData*> nodeData;
};

