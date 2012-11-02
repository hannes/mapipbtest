#include <string>
#include <sstream>
#include <cstdlib>
#include <ctime>
#include <sys/time.h>

#include "protobuf/messages.pb.h"

using namespace std;

string intToStr(int number);
char genRandom();
string genRndStr(int size);

string getPrefix(sbp0i::StoreColumnData *m);

long getTimeMsec();
