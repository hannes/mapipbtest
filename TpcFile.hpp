#include <map>
#include "protobuf/messages.pb.h"
using namespace std;

class TpcFile {

public:
	typedef map<string, vector<string> > schema_t;

	TpcFile(string aFilename) {
		filename = aFilename;
	}

	void parse();
	void print();
	long size();
	vector<sbp0i::StoreColumnData> getData();

private:
	string filename;
	vector<sbp0i::StoreColumnData> columndata;
}
;

static TpcFile::schema_t createSchema();
