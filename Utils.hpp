#include <string>
#include <sstream>
#include <cstdlib>
#include <ctime>
#include <iostream>

#include <sys/time.h>

#include "protobuf/messages.pb.h"

using namespace std;

string intToStr(int number);
char genRandom();
string genRndStr(int size);

string getPrefix(sbp0i::StoreColumnData *m);
string getPrefix(sbp0i::LoadColumnData *m);

long getTimeMsec();
void printColumn(sbp0i::StoreColumnData *col);
