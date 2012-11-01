#include "Node.hpp"

void Node::listen(string anAddress) {
	FILE_LOG(logDEBUG) << "listening on: " << anAddress;

	serverSocketName = anAddress;
	pthread_create(&pollert, NULL, poll, &this[0]);
}
bool Node::send(string anAddress, google::protobuf::Message *msg) {
	sbp0i::SelfDescribingMessage wrappedMsg;
	wrappedMsg.set_type(msg->GetDescriptor()->name());
	wrappedMsg.set_message_data(msg->SerializeAsString());

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

void Node::sayHi(string anAddress) {
	sbp0i::Hello h;
	h.set_origin(getSocket());
	send(anAddress, &h);
}

void Node::receive(string msg) {
	sbp0i::SelfDescribingMessage dmessage;
	string decompressed;
	snappy::Uncompress(msg.data(), msg.size(), &decompressed);
	dmessage.ParseFromString(decompressed);

	bool handled = false;

	// setup
	if (dmessage.type() == "Hello") {
		sbp0i::Hello m;
		m.ParseFromString(dmessage.message_data());
		hHello(m);
		handled = true;
	}

	// store
	if (dmessage.type() == "StoreColumnData") {
		sbp0i::StoreColumnData m;
		m.ParseFromString(dmessage.message_data());
		hStoreColumnData(m);
		handled = true;
	}

	// testing stuff
	if (dmessage.type() == "DummyMessage") {
		sbp0i::DummyMessage m;
		m.ParseFromString(dmessage.message_data());
		hDummyMessage(m);
		handled = true;
	}
	if (dmessage.type() == "StillePost") {
		sbp0i::StillePost m;
		m.ParseFromString(dmessage.message_data());
		hStillePost(m);
		handled = true;
	}

	if (!handled) {
		FILE_LOG(logERROR) << serverSocketName << " unknown message: "
				<< dmessage.type();
	}
}

void Node::hHello(sbp0i::Hello m) {
	FILE_LOG(logDEBUG) << serverSocketName << " got hello from: " << m.origin();
	lingeringNodes.push_back(m.origin());\
	// what to do with lingering nodes?
}

void Node::hStoreColumnData(sbp0i::StoreColumnData m) {
	FILE_LOG(logDEBUG) << serverSocketName << " got scd: " << getPrefix(m)
			<< ", " << m.entries_size() << " entries";

	// am I responsible for storing?

	// // do I want to? do I have lingering nodes around?

	const sbp0i::TreeNode *mtch = findNode(&prefixTree, getPrefix(m));
	if (mtch != NULL) {
		// forward message to that node!
		FILE_LOG(logDEBUG) << "found prefix on node, going to " << mtch->node();
		// OR: forward to next node in tree on the way to dest.
		send(mtch->node(), &m);
		return;
	}
	// damn, nobody is responsible yet. we could: assign a lingering node, and change the tree

	// or we could forward to the next best node

}

string Node::getPrefix(sbp0i::StoreColumnData m) {
	return "/" + m.relation() + "/" + m.column() + "/";
}

void Node::hStillePost(sbp0i::StillePost m) {
	FILE_LOG(logDEBUG) << serverSocketName << " got sp: " << intToStr(m.hops());
	m.set_pos(m.pos() + 1);
	m.set_hops(m.hops() + 1);

	if (m.pos() == m.max()) {
		m.set_pos(0);
	}
	send(string(m.proto()) + intToStr(m.pos()), &m);
}

void Node::hDummyMessage(sbp0i::DummyMessage m) {
	FILE_LOG(logDEBUG) << serverSocketName << " got dm: " << m.id();
	if (m.has_target()) {
		cerr << serverSocketName << " forwarding to: " << m.target();

		string target = m.target();
		m.clear_target();
		send(target, &m);
	}
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

sbp0i::TreeNode* Node::getTree() {
	return &prefixTree;
}

zmq::context_t* Node::getContext() {
	return context;
}

Node::~Node() {
	typedef map<string, zmq::socket_t*>::iterator it_type;
	for (it_type iterator = sendSockets.begin(); iterator != sendSockets.end();
			iterator++) {
		try {
			iterator->second->~socket_t();
		} catch (zmq::error_t &e) {
			FILE_LOG(logERROR) << "destruct" << e.what();
		}
	}
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

