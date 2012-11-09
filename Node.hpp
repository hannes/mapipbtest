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
	virtual void handle(Node *node, google::protobuf::Message *msg, string id,
			string origin) = 0;
};

class ResponseHandler {
public:
	virtual void response(Node *node, google::protobuf::Message *msg,
			string inResponseTo, string origin) = 0;
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
	void terminate();

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
	map<string, Waiting>* getWaiting();

	~Node();

private:
	pthread_t pollert;
	pthread_t timeoutt;

	zmq::context_t *context;
	string serverSocketName;
	map<string, zmq::socket_t*> sendSockets;

	map<string, MessageHandler*> handlers;
	map<string, Waiting> waiting;

	bool sendToSocket(string target, string message);

};

class CezanneNode: public Node {
public:
	string findNode(string prefix);
	void addRoutingEntry(string prefix, string node);
	map<string, string> getRoutingTable();
	sbp0i::RoutingTable* getRoutingMessage();
	bool isOverloaded();
	vector<string>* getLingeringNodes();
	void addLingeringNode(string node);
	void printRoutingTable();

	void store(sbp0i::StoreColumnData *data);
	sbp0i::StoreColumnData load(sbp0i::LoadColumnData *data);

private:
	vector<string> lingeringNodes;
	sbp0i::RoutingTable routingTable;
	vector<sbp0i::StoreColumnData*> nodeData;
};

