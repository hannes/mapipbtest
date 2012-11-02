#include <stdio.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <libgen.h>

#include "TpcFile.hpp"
#include "Utils.hpp"

vector<sbp0i::StoreColumnData> TpcFile::getData() {
	return columndata;
}

// Highly boring...
static TpcFile::schema_t createSchema() {
	TpcFile::schema_t schema;

	vector<string> nationFields;
	nationFields.push_back("n_nationkey");
	nationFields.push_back("n_name");
	nationFields.push_back("n_regionkey");
	nationFields.push_back("n_comment");
	schema["nation"] = nationFields;

	vector<string> lineitemFields;
	lineitemFields.push_back("l_orderkey");
	lineitemFields.push_back("l_partkey");
	lineitemFields.push_back("l_suppkey");
	lineitemFields.push_back("l_linenumber");
	lineitemFields.push_back("l_quantity");
	lineitemFields.push_back("l_extendedprice");
	lineitemFields.push_back("l_discount");
	lineitemFields.push_back("l_tax");
	lineitemFields.push_back("l_returnflag");
	lineitemFields.push_back("l_linestatus");
	lineitemFields.push_back("l_shipdate");
	lineitemFields.push_back("l_commitdate");
	lineitemFields.push_back("l_receiptdate");
	lineitemFields.push_back("l_shipinstruct");
	lineitemFields.push_back("l_shipmode");
	lineitemFields.push_back("l_comment");
	schema["lineitem"] = lineitemFields;

	vector<string> ordersFields;
	ordersFields.push_back("o_orderkey");
	ordersFields.push_back("o_custkey");
	ordersFields.push_back("o_orderstatus");
	ordersFields.push_back("o_totalprice");
	ordersFields.push_back("o_orderdate");
	ordersFields.push_back("o_orderpriority");
	ordersFields.push_back("o_clerk");
	ordersFields.push_back("o_shippriority");
	ordersFields.push_back("o_comment");
	schema["orders"] = ordersFields;

	vector<string> partsuppFields;
	partsuppFields.push_back("ps_partkey");
	partsuppFields.push_back("ps_suppkey");
	partsuppFields.push_back("ps_availqty");
	partsuppFields.push_back("ps_supplycost");
	partsuppFields.push_back("ps_comment");
	schema["partsupp"] = partsuppFields;

	vector<string> partFields;
	partFields.push_back("p_partkey");
	partFields.push_back("p_name");
	partFields.push_back("p_mfgr");
	partFields.push_back("p_brand");
	partFields.push_back("p_type");
	partFields.push_back("p_size");
	partFields.push_back("p_container");
	partFields.push_back("p_retailprice");
	partFields.push_back("p_comment");
	schema["part"] = partFields;

	vector<string> custFields;
	custFields.push_back("c_custkey");
	custFields.push_back("c_name");
	custFields.push_back("c_address");
	custFields.push_back("c_nationkey");
	custFields.push_back("c_phone");
	custFields.push_back("c_acctbal");
	custFields.push_back("c_mktsegment");
	custFields.push_back("c_comment");
	custFields.push_back("c_phone");
	schema["customer"] = custFields;

	vector<string> regionFields;
	regionFields.push_back("r_regionkey");
	regionFields.push_back("r_name");
	regionFields.push_back("r_comment");
	schema["region"] = regionFields;

	vector<string> supplierFields;
	supplierFields.push_back("s_suppkey");
	supplierFields.push_back("s_name");
	supplierFields.push_back("s_address");
	supplierFields.push_back("s_nationkey");
	supplierFields.push_back("s_phone");
	supplierFields.push_back("s_acctbal");
	supplierFields.push_back("s_comment");
	schema["supplier"] = supplierFields;

	return schema;
}

void TpcFile::parse() {
	schema_t schema = createSchema();

	// find out which table we are dealing with
	string tpctablefile = string(basename((char*) filename.data()));
	string tpctable = tpctablefile.substr(0, tpctablefile.length() - 4);

	if (schema.find(tpctable) == schema.end()) {
		cerr << "No schema definition for " << filename << endl;
	}

	ifstream file(filename.data());

	string value;
	int row = 0;
	string prefix = genRndStr(10);

	for (vector<string>::size_type i = 0; i != schema[tpctable].size(); i++) {
		sbp0i::StoreColumnData col;
		col.set_column(schema[tpctable][i]);
		col.set_relation(tpctable);
		columndata.push_back(col);
	}

	while (file.good()) {
		getline(file, value, '\n');
		stringstream ss(value);
		string item;
		vector<string> elems;

		while (getline(ss, item, '|')) {
			elems.push_back(item);
		}

		if (elems.size() > 0) {
			string rowid = prefix + "." + intToStr(row);
			for (vector<int>::size_type i = 0; i != elems.size(); i++) {
				sbp0i::StoreColumnData::ColumnEntry* entry =
						columndata[i].add_entries();
				entry->set_rowid(rowid);
				entry->set_value(elems[i]);
			}
			row++;
		}
	}
}

void TpcFile::print() {
	for (vector<sbp0i::StoreColumnData>::size_type i = 0;
			i != columndata.size(); i++) {
		sbp0i::StoreColumnData col = columndata[i];
		printColumn(&col);
	}
}

long TpcFile::size() {
	return columndata[0].entries_size();
}

