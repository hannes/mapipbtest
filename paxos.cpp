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

class PromiseHandler;
class PrepareHandler;

class PaxosNode: public Node {
public:
	PaxosNode(zmq::context_t *aContext) :
			Node(aContext) {
		proposalNr = 0;
		acceptedPropNr = 0;
		acceptedPropVal = "";
	}

	void propose(string value) {
		proposalNr++;
		proposalValue = value;
		promiseQuorum = 0;
		acceptQuorum = 0;
		promiseQuorumReached = false;
		acceptQuorumReached = false;

		sbp0i::Prepare prep;
		prep.set_proposalnumber(proposalNr);

		for (vector<string>::iterator it = acceptors.begin();
				it != acceptors.end(); ++it) {

			sendR(*it, &prep, promiseh, 100);
		}
	}

	vector<string> acceptors;
	vector<string> promised;

	unsigned int proposalNr;
	string proposalValue;
	ResponseHandler *promiseh;
	MessageHandler *prepareh;
	ResponseHandler *accepth;
	MessageHandler *acceptrh;

	unsigned int acceptedPropNr;
	unsigned int promisedPropNr;
	string acceptedPropVal;

	bool promiseQuorumReached;
	bool acceptQuorumReached;

	bool timeoutQuorumReached;
	unsigned int timeoutQuorum;

	unsigned int promiseQuorum;
	unsigned int acceptQuorum;

};


class AcceptHandler: public ResponseHandler {
public:
	void response(Node *node, google::protobuf::Message *msg,
			string inResponseTo, string origin) {
		sbp0i::Accepted *m = (sbp0i::Accepted*) msg;

		PaxosNode* pn = (PaxosNode*) node;

		pn->acceptQuorum++;
		if (pn->acceptQuorum > pn->acceptors.size() * 0.7
				&& !pn->acceptQuorumReached) {
			pn->acceptQuorumReached = true;

			// the new value now stands
			FILE_LOG(logINFO) << node->getSocket() << " consensus on #"
					<< m->proposalnumber() << " (" << m->value() << ")";

		}

	}
	void timeout(Node *node, string inResponseTo) {
		FILE_LOG(logDEBUG) << node->getSocket() << " got timeout on "
				<< inResponseTo;
	}
};

class PromiseHandler: public ResponseHandler {
public:
	void response(Node *node, google::protobuf::Message *msg,
			string inResponseTo, string origin) {

		PaxosNode* pn = (PaxosNode*) node;

		// TODO: check the values sent back for newer information
		// start new round if something newer pops up

		pn->promiseQuorum++;

		if (pn->promiseQuorum > pn->acceptors.size() * 0.7
				&& !pn->promiseQuorumReached) {
			FILE_LOG(logDEBUG) << node->getSocket()
					<< " got enough promises for #" << pn->proposalNr;
			pn->promiseQuorumReached = true;
			sbp0i::AcceptRequest ar;
			ar.set_proposalnumber(pn->proposalNr);
			ar.set_value(pn->proposalValue);

			for (vector<string>::iterator it = pn->acceptors.begin();
					it != pn->acceptors.end(); ++it) {
				pn->sendR(*it, &ar, pn->accepth, 100);
			}
		}

	}
	void timeout(Node *node, string inResponseTo) {
		PaxosNode* pn = (PaxosNode*) node;

		pn->timeoutQuorum++;
		if (pn->timeoutQuorum > pn->acceptors.size() * 0.7
				&& !pn->timeoutQuorumReached) {
			FILE_LOG(logDEBUG) << node->getSocket() << " got timeout for #"
					<< pn->proposalNr;
			pn->timeoutQuorumReached = true;
			// TODO: do sth here
		}
	}
};

class PrepareHandler: public MessageHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id,
			string origin) {
		sbp0i::Prepare *m = (sbp0i::Prepare*) msg;
		PaxosNode* pn = (PaxosNode*) node;

		if (m->proposalnumber() > pn->promisedPropNr) {
			pn->promisedPropNr = m->proposalnumber();
			sbp0i::Promise p;
			if (pn->acceptedPropNr > 0) {
				p.set_proposalnumber(pn->acceptedPropNr);
				p.set_value(pn->acceptedPropVal);
			}
			node->send(origin, &p, node->createMessageId(), id);
		}
	}
}
;

class AcceptRequestHandler: public MessageHandler {
public:
	void handle(Node *node, google::protobuf::Message *msg, string id,
			string origin) {
		sbp0i::AcceptRequest *m = (sbp0i::AcceptRequest*) msg;
		PaxosNode* pn = (PaxosNode*) node;

		if (m->proposalnumber() > pn->acceptedPropNr) {

			sbp0i::Accepted a;
			a.set_proposalnumber(m->proposalnumber());
			a.set_value(m->value());
			node->send(origin, &a, node->createMessageId(), id);

			// TODO: flood accept, so everyone can update their data
		}
	}
}
;

/*
 Client    Servers
 |         |  |  | --- First Request ---
 X-------->|  |  |  Request
 |         X->|->|  Prepare(N)
 |         |<-X--X  Promise(N,I,{Va,Vb,Vc})
 |         X->|->|  Accept!(N,I,Vn)
 |         |<-X--X  Accepted(N,I)
 |<--------X  |  |  Response
 |         |  |  |
 */

void testPaxos() {
	zmq::context_t context(1);

	vector<PaxosNode*> nodes;

	for (int i = 0; i < 10; i++) {
		PaxosNode* pn = new PaxosNode(&context);

		pn->prepareh = new PrepareHandler();
		pn->promiseh = new PromiseHandler();
		pn->accepth = new AcceptHandler();
		pn->acceptrh = new AcceptRequestHandler();

		pn->registerHandler("sbp0i.Prepare", pn->prepareh);
		pn->registerHandler("sbp0i.AcceptRequest", pn->acceptrh);

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

	usleep(10);

	nodes[0]->propose("ONE");
	nodes[1]->propose("TWO");
	nodes[2]->propose("THREE");
	nodes[3]->propose("FOUR");
	nodes[4]->propose("FIVE");

	sleep(2);
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

