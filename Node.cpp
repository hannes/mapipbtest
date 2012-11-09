#include "Node.hpp"

void Node::registerHandler(string messageType, MessageHandler *handler) {
	handlers[messageType] = handler;
}

void CezanneNode::printRoutingTable() {
	cout << endl;
	cout << "Routing table for:  " << getSocket() << endl;

	map<string, string> rt = getRoutingTable();
	for (map<string, string>::iterator it = rt.begin(); it != rt.end(); it++) {
		cout << it->first << "\t\t: " << it->second << endl;
	}
	cout << endl;
}

static void* timeout(void* ctx) {
	Node *t = (Node*) ctx;
	typedef map<string, Waiting>::iterator it_type;
	map<string, Waiting> *m = t->getWaiting();

	while (m->size() > 0) {
		long time = getTimeMsec();

		map<string, Waiting>::iterator it = m->begin();

		while (it != m->end()) {
			if (it->second.expireTime < time) {
				string key = it->first;
				ResponseHandler *h = m->find(key)->second.handler;
				m->erase(it++);
				h->timeout(t, key);

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

	if (!timeoutt) {
		pthread_create(&timeoutt, NULL, timeout, &this[0]);
	}
	Waiting w;

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

	FILE_LOG(logDEBUG) << t->getSocket() << " listening";

	while (true) {
		zmq::message_t request;
		try {
			newSocket.recv(&request);

			string message = string(static_cast<char*>(request.data()),
					request.size());
			string decompressed;
			snappy::Uncompress(message.data(), message.size(), &decompressed);

			if (decompressed == "TERMINATE") {
				FILE_LOG(logERROR) << t->getSocket() << " terminating";
				break;
			}

			t->receive(decompressed);
		} catch (zmq::error_t &e) {
			FILE_LOG(logERROR) << t->getSocket() << " poll: " << e.what();
			break;
		}

	}
	return (NULL);
}

void Node::listen(string anAddress) {
	serverSocketName = anAddress;
	pthread_create(&pollert, NULL, poll, &this[0]);
}

void Node::terminate() {
	sendToSocket(getSocket(), "TERMINATE");
}

bool Node::send(string anAddress, google::protobuf::Message *msg) {
	return send(anAddress, msg, createMessageId(), "");
}

bool Node::send(string anAddress, google::protobuf::Message *msg, string id) {
	return send(anAddress, msg, id, "");
}

// TODO: local messages can be delivered directly
bool Node::send(string anAddress, google::protobuf::Message *msg, string id,
		string inResponseTo) {
	sbp0i::SelfDescribingMessage wrappedMsg;
	wrappedMsg.set_type(msg->GetDescriptor()->full_name());
	wrappedMsg.set_message_data(msg->SerializeAsString());
	wrappedMsg.set_inresponseto(inResponseTo);
	wrappedMsg.set_id(id);
	wrappedMsg.set_origin(getSocket());
	return sendToSocket(anAddress, wrappedMsg.SerializeAsString());
}

bool Node::sendToSocket(string target, string message) {
	try {
		if (sendSockets.find(target) == sendSockets.end()) {
			zmq::socket_t *socket = new zmq::socket_t(*context, ZMQ_PUSH);
			socket->connect(target.data());
			sendSockets[target] = socket;
		}

		string compressed;
		snappy::Compress(message.data(), message.size(), &compressed);
		zmq::message_t messageS(compressed.size());
		memcpy(messageS.data(), compressed.data(), compressed.size());

		sendSockets[target]->send(messageS);
		return true;
	} catch (zmq::error_t &e) {
		FILE_LOG(logERROR) << serverSocketName << " send: " << e.what();
	}
	return false;
}

void Node::receive(string msg) {
	sbp0i::SelfDescribingMessage dmessage;
	dmessage.ParseFromString(msg);

// protobuf "magic", get inner class implementation:

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
		waiting[dmessage.inresponseto()].handler->response(this, innerMsg,
				dmessage.inresponseto(), dmessage.origin());
		waiting.erase(dmessage.inresponseto());
		return;
	}

// look into handlers map to find suitable handler
	if (handlers.find(dmessage.type()) != handlers.end()) {
		MessageHandler *handler = handlers[dmessage.type()];
		handler->handle(this, innerMsg, dmessage.id(), dmessage.origin());
		return;
	}

// if we still did not do anything
	FILE_LOG(logERROR) << serverSocketName << " unhandled message: "
			<< dmessage.type();

}

string Node::createMessageId() {
	return genRndStr(10);
}

string Node::getSocket() {
	return serverSocketName;
}

map<string, Waiting>* Node::getWaiting() {
	return &waiting;
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

string CezanneNode::findNode(string prefix) {
	for (int j = 0; j < routingTable.entries_size(); j++) {
		const sbp0i::RoutingTable_RoutingTableEntry& entry =
				routingTable.entries(j);
		if (entry.prefix() == prefix) {
			return entry.node();
		}
	}
	return "";
}
void CezanneNode::addRoutingEntry(string prefix, string node) {
	sbp0i::RoutingTable_RoutingTableEntry* ne = routingTable.add_entries();
	ne->set_prefix(prefix);
	ne->set_node(node);
	routingTable.set_version(routingTable.version() + 1);
}

map<string, string> CezanneNode::getRoutingTable() {
	map<string, string> routingTableMap;
	for (int j = 0; j < routingTable.entries_size(); j++) {
		const sbp0i::RoutingTable_RoutingTableEntry& entry =
				routingTable.entries(j);
		routingTableMap[entry.prefix()] = entry.node();
	}
	return routingTableMap;
}

sbp0i::RoutingTable* CezanneNode::getRoutingMessage() {
	return &routingTable;
}

bool CezanneNode::isOverloaded() {
	long mappings = 0;
	for (vector<sbp0i::StoreColumnData*>::size_type i = 0; i < nodeData.size();
			i++) {
		sbp0i::StoreColumnData* c = nodeData[i];
		mappings = +c->entries_size();
	}
	return (mappings > 100);
// TODO: storage or cpu load determine this!
// TODO: propose tree change, vote with other nodes?

//return false;
}

void CezanneNode::addLingeringNode(string node) {
// check if we already have this one
	for (vector<string>::iterator it = lingeringNodes.begin();
			it != lingeringNodes.end(); ++it) {
		if (*it == node) {
			return;
		}
	}
	lingeringNodes.push_back(node);
}

vector<string>* CezanneNode::getLingeringNodes() {
	return &lingeringNodes;
}

void CezanneNode::store(sbp0i::StoreColumnData *data) {
	sbp0i::StoreColumnData *col = 0;
	for (vector<sbp0i::StoreColumnData*>::size_type i = 0; i != nodeData.size();
			i++) {
		sbp0i::StoreColumnData* c = nodeData[i];
		if (c->relation() == data->relation()
				&& c->column() == data->column()) {
			col = c;
		}
	}
	if (col == 0) {
		col = new sbp0i::StoreColumnData();
		col->set_relation(data->relation());
		col->set_column(data->column());
		nodeData.push_back(col);
	}
	for (int j = 0; j < data->entries_size(); j++) {
		const sbp0i::StoreColumnData::ColumnEntry& entry = data->entries(j);
		sbp0i::StoreColumnData::ColumnEntry* nentry = col->add_entries();

		nentry->set_rowid(entry.rowid());
		nentry->set_value(entry.value());
	}
}

sbp0i::StoreColumnData CezanneNode::load(sbp0i::LoadColumnData *req) {
	sbp0i::StoreColumnData *col = 0;
	for (vector<sbp0i::StoreColumnData*>::size_type i = 0; i != nodeData.size();
			i++) {
		sbp0i::StoreColumnData *c = nodeData[i];
		if (c->relation() == req->relation() && c->column() == req->column()) {
			col = c;
		}
	}
	sbp0i::StoreColumnData ret;
	ret.set_relation(req->relation());
	ret.set_column(req->column());
	if (col != 0) {

		for (int j = 0; j < col->entries_size(); j++) {
			const sbp0i::StoreColumnData::ColumnEntry& entry = col->entries(j);
			if (!req->has_value() || req->value() == entry.value()) {
				sbp0i::StoreColumnData::ColumnEntry* nentry = ret.add_entries();
				nentry->set_rowid(entry.rowid());
				nentry->set_value(entry.value());
			}
		}
	}
	return ret;
}

