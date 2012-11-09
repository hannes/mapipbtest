#include <Node.hpp>
#include <Utils.hpp>
#include "lib/log.h"

#include "gtest/gtest.h"

class DummyHandler: public MessageHandler {
public:
	vector<Msg*> receivedMessages;

	void handle(Node *node, Msg *msg, string id, string origin) {
		receivedMessages.push_back(msg);
	}
};

TEST(NodeTest, RegistrationAndReceiveTest) {
	zmq::context_t c(1);

	Node n1(&c);
	n1.listen("inproc://foo1");
	Node n2(&c);
	n2.listen("inproc://foo2");

	DummyHandler dh;

	n1.registerHandler("sbp0i.TestMessage", &dh);

	sbp0i::TestMessage msg;
	string testStr = "Das Pferd frisst keinen Gurkensalat!";
	msg.set_message(testStr);

	n2.send(n1.getSocket(), &msg);
	usleep(1000);

	ASSERT_EQ((int) dh.receivedMessages.size(), 1);
	ASSERT_STREQ(testStr.data(),
			( (sbp0i::TestMessage*) dh.receivedMessages[0])->message().data());
	FILE_LOG(logDEBUG) << "passed";

	n1.terminate();
	n2.terminate();
	usleep(1000);

	c.close();

}

