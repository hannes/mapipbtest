#include "Node.hpp"

void Node::registerHandler(string messageType, MessageHandler *handler) {
	handlers[messageType] = handler;
}

static void* timeout(void* ctx) {
	Node *t = (Node*) ctx;
	typedef map<string, Waiting>::iterator it_type;
	map<string, Waiting> m = t->getWaiting();

	while (m.size() > 0) {
		long time = getTimeMsec();

		map<string, Waiting>::iterator it = m.begin();

		while (it != m.end()) {
			if (it->second.expireTime < time) {
				FILE_LOG(logDEBUG) << "timeout: " << it->first;

				m.erase(it++);  // Use iterator.
								// Note the post increment.
								// Increments the iterator but returns the
								// original value for use by erase
			} else {
				++it;

			}
		}
		usleep(10000); // microseconds
	}
	return (NULL);
}

bool Node::sendR(string anAddress, google::protobuf::Message *msg,
		ResponseHandler *respHandler, int timeoutMsecs) {
	Waiting w;

	if (!timeoutt) {
		pthread_create(&timeoutt, NULL, timeout, &this[0]);
	}

	w.expireTime = getTimeMsec() + timeoutMsecs;
	w.handler = respHandler;
	string id = createMessageId();
	waiting[id] = w;

	return send(anAddress, msg, id);
}

static void* poll(void* ctx) {
	Node *t = (Node*) ctx;

	zmq::socket_t newSocket(*t->getContext(), ZMQ_PULL);
	newSocket.bind(t->getSocket().data());

	while (true) {
		zmq::message_t request;
		try {
			newSocket.recv(&request);

			string message = string(static_cast<char*>(request.data()),
					request.size());
			t->receive(message);
		} catch (zmq::error_t &e) {
			FILE_LOG(logERROR) << "poll: " << e.what();
			break;
		}

	}
	return (NULL);
}

void Node::listen(string anAddress) {
	FILE_LOG(logDEBUG) << "listening on: " << anAddress;

	serverSocketName = anAddress;
	pthread_create(&pollert, NULL, poll, &this[0]);
}

bool Node::send(string anAddress, google::protobuf::Message *msg) {
	return send(anAddress, msg, createMessageId(), "");
}

bool Node::send(string anAddress, google::protobuf::Message *msg, string id) {
	return send(anAddress, msg, id, "");
}

bool Node::send(string anAddress, google::protobuf::Message *msg, string id,
		string inResponseTo) {
	sbp0i::SelfDescribingMessage wrappedMsg;
	wrappedMsg.set_type(msg->GetDescriptor()->full_name());
	wrappedMsg.set_message_data(msg->SerializeAsString());
	wrappedMsg.set_inresponseto(inResponseTo);
	wrappedMsg.set_id(id);

	try {
		if (sendSockets.find(anAddress) == sendSockets.end()) {
			zmq::socket_t *socket = new zmq::socket_t(*context, ZMQ_PUSH);
			socket->connect(anAddress.data());
			sendSockets[anAddress] = socket;
		}

		string serialized = wrappedMsg.SerializeAsString();
		string compressed;
		snappy::Compress(serialized.data(), serialized.size(), &compressed);
		zmq::message_t messageS(compressed.size());
		memcpy(messageS.data(), compressed.data(), compressed.size());

		sendSockets[anAddress]->send(messageS);
		return true;
	} catch (zmq::error_t &e) {
		FILE_LOG(logERROR) << "send: " << e.what();
	}
	return false;
}

void Node::receive(string msg) {
	sbp0i::SelfDescribingMessage dmessage;
	string decompressed;
	snappy::Uncompress(msg.data(), msg.size(), &decompressed);
	dmessage.ParseFromString(decompressed);

// protobuf "magic", get inner class implementation

// first find the inner message's descriptor
	const google::protobuf::Descriptor *d =
			dmessage.descriptor()->file()->pool()->FindMessageTypeByName(
					dmessage.type());
	if (!d) {
		FILE_LOG(logERROR) << serverSocketName << " unknown message: "
				<< dmessage.type();
		return;
	}

// now find the inner message's prototype
	const google::protobuf::Message *innerMsgProto =
			::google::protobuf::MessageFactory::generated_factory()->GetPrototype(
					d);
	if (!innerMsgProto) {
		FILE_LOG(logERROR) << serverSocketName << " unknown message: "
				<< dmessage.type();
		return;
	}

// now construct new instance and parse inner message
	google::protobuf::Message *innerMsg = innerMsgProto->New();

// finally, parse the inner message
	innerMsg->ParseFromString(dmessage.message_data());

// check wether we have been waiting for this message
	if (waiting.find(dmessage.inresponseto()) != waiting.end()) {

		FILE_LOG(logDEBUG) << serverSocketName << " got response: "
				<< dmessage.type();
		waiting[dmessage.inresponseto()].handler->response(this, innerMsg,
				dmessage.inresponseto());
		return;
	}

// look into handlers map to find suitable handler
	if (handlers.find(dmessage.type()) != handlers.end()) {
		MessageHandler *handler = handlers[dmessage.type()];
		handler->handle(this, innerMsg, dmessage.id());
	} else {
		FILE_LOG(logERROR) << serverSocketName << " unhandled message: "
				<< dmessage.type();
	}
}

string Node::createMessageId() {
	return genRndStr(10);
}

const sbp0i::TreeNode* Node::findNode(string prefix) {
	return findNode(&prefixTree, prefix);
}

// recurse into tree
const sbp0i::TreeNode* Node::findNode(const sbp0i::TreeNode *root,
		string prefix) {
	if (root->prefix() == prefix) {
		return root;
	}

	if (root->children_size() == 0) {
		return NULL;
	}

	for (int j = 0; j < root->children_size(); j++) {
		const sbp0i::TreeNode *mtch = findNode(&root->children(j), prefix);
		if (mtch != NULL) {
			return mtch;
		}
	}
	return NULL;
}

string Node::getSocket() {
	return serverSocketName;
}

map<string, Waiting> Node::getWaiting() {
	return waiting;
}

sbp0i::TreeNode * Node::getTree() {
	return &prefixTree;
}

zmq::context_t * Node::getContext() {
	return context;
}

Node::~Node() {
	typedef map<string, zmq::socket_t*>::iterator it_type;
	for (it_type iterator = sendSockets.begin(); iterator != sendSockets.end();
			iterator++) {
		try {
			iterator->second->~socket_t();
		} catch (zmq::error_t &e) {
			FILE_LOG(logERROR) << "destruct: " << e.what();
		}
	}
}

