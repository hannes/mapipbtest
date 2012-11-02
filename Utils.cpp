#include "Utils.hpp"

string intToStr(int number) {
	stringstream ss;
	ss << number;
	return ss.str();
}

static const char alphanum[] = "0123456789"
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		"abcdefghijklmnopqrstuvwxyz";

static const int stringLength = sizeof(alphanum) - 1;


char genRandom() {
	return alphanum[rand() % stringLength];
}

string genRndStr(int size) {
	string ret;
	for (int i = 0; i < size; i++) {
		ret += genRandom();
	}
	return ret;
}

string getPrefix(sbp0i::StoreColumnData *m) {
	return "/" + m->relation() + "/" + m->column() + "/";
}

string getPrefix(sbp0i::LoadColumnData *m) {
	return "/" + m->relation() + "/" + m->column() + "/";
}

long getTimeMsec() {
	timeval curTime;
	gettimeofday(&curTime, NULL);
	return curTime.tv_sec * 1000 + curTime.tv_usec / 1000;
}

void printColumn(sbp0i::StoreColumnData *col) {
	cout << "Relation:  " << col->relation() << " Column: " << col->column();
	cout << endl;

	for (int j = 0; j < col->entries_size(); j++) {
		const sbp0i::StoreColumnData::ColumnEntry& entry = col->entries(j);
		cout << entry.rowid() << "=" << entry.value() << endl;
	}
	cout << endl;
}
