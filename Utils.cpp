#include "Utils.hpp"

std::string intToStr(int number) {
	std::stringstream ss;
	ss << number;
	return ss.str();
}


static const char alphanum[] = "0123456789"
		"!@#$%^&*"
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		"abcdefghijklmnopqrstuvwxyz";

static const int stringLength = sizeof(alphanum) - 1;

char genRandom() {
	return alphanum[rand() % stringLength];
}

std::string genRndStr(int size) {
	std::string ret;
	for (int i = 0; i < size; i++) {
		ret += genRandom();
	}
	return ret;
}
