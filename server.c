#include "mapipbtest.h"

static void create_result_from_csv(Mapipbtest__QueryResult *result, char* file,
		size_t ncols, size_t nrows, char* coltypes, bool print_summary) {

	mapipbtest__query_result__init(result);

	result->rows = nrows;
	result->n_columns = ncols;

	FILE *fr = fopen(DATA_FILE, "rt");
	char line[MAX_LINE_LEN];
	char * pch;

	size_t row = 0;
	size_t col = 0;
	Mapipbtest__QueryResult__Column **columns;
	columns = malloc(sizeof(Mapipbtest__QueryResult__Column*) * (DATA_COLS));
	for (col = 0; col < DATA_COLS; col++) {
		columns[col] = malloc(sizeof(Mapipbtest__QueryResult__Column));
		mapipbtest__query_result__column__init(columns[col]);
		if (DATA_TYPES[col] == 'i') {
			columns[col]->intvalues = malloc(sizeof(int64_t) * nrows);
			columns[col]->n_intvalues = nrows;
			columns[col]->type =
					MAPIPBTEST__QUERY_RESULT__COLUMN__TYPE__INTEGER;
		}
		if (DATA_TYPES[col] == 'f') {
			columns[col]->floatvalues = malloc(sizeof(double) * nrows);
			columns[col]->n_floatvalues = nrows;
			columns[col]->type = MAPIPBTEST__QUERY_RESULT__COLUMN__TYPE__FLOAT;
		}
		if (DATA_TYPES[col] == 's') {
			columns[col]->stringvalues = malloc(sizeof(char*) * nrows);
			columns[col]->n_stringvalues = nrows;
			columns[col]->type = MAPIPBTEST__QUERY_RESULT__COLUMN__TYPE__STRING;
		}
	}
	result->columns = columns;
	size_t datalen = 0;

	row = 0;
	while (fgets(line, MAX_LINE_LEN, fr) != NULL && row <= DATA_ROWS ) {
		assert(row <= nrows);
		col = 0;
		datalen += strlen(line);
		pch = strtok(line, "|");

		while (pch != NULL && col < ncols) {
			if (DATA_TYPES[col] == 'i') {
				result->columns[col]->intvalues[row] = atol(pch);
			}
			if (DATA_TYPES[col] == 'f') {
				result->columns[col]->floatvalues[row] = atof(pch);
			}
			if (DATA_TYPES[col] == 's') {
				result->columns[col]->stringvalues[row] = strdup(pch);
			}
			col++;
			pch = strtok(NULL, "|");
		}
		row++;
	}
	if (print_summary)
		printf("READ %s, %i cols, %i rows, %zu bytes.\n", DATA_FILE, DATA_COLS,
				DATA_ROWS, datalen);

	fclose(fr);
}

int main(void) {
	// read data (this is outside time measurements, since the data comes from anywhere and is assumed in (virtual) memory)
	Mapipbtest__QueryResult query_result;
	create_result_from_csv(&query_result, DATA_FILE, DATA_COLS, DATA_ROWS,
			DATA_TYPES, 1);

	struct snappy_env se;
	snappy_init_env(&se);

	// react on queries
	void *context = zmq_ctx_new();
	void *socket = zmq_socket(context, ZMQ_REP);
	int rc;
	rc = zmq_bind(socket, SERVER_SOCKET);
	if (rc != 0) {
		fprintf(stderr, "Failed to bind to socket %s\n", SERVER_SOCKET);
		perror("Socket error ");
		return -1;
	}

	long s_time, c_time, req_time;
	struct timeval start, req_start;

	while (1) {
		gettimeofday(&req_start, NULL );
		// receive request
		message query_msg;
		message_receive(socket, &query_msg);
		size_t query_msg_size = message_size(&query_msg);
		void * query_msg_data = message_data(&query_msg);
		if (COMPRESS) {
			query_msg_data = message_uncompress(query_msg_data,
					&query_msg_size);
		}
		Mapipbtest__ExecuteQuery * request = mapipbtest__execute_query__unpack(
				NULL, query_msg_size, query_msg_data);
		message_close(&query_msg);
		if (COMPRESS) {
			free(query_msg_data);
		}
		//printf("%s\n", request->sqlquery);
		//mapipbtest__execute_query__free_unpacked(request, NULL );

		// send response
		gettimeofday(&start, NULL );
		size_t query_response_msg_size =
				mapipbtest__query_result__get_packed_size(&query_result);
		void * query_response_msg_data = malloc(query_response_msg_size);
		mapipbtest__query_result__pack(&query_result, query_response_msg_data);

		s_time = end_timer_ms(&start);
		gettimeofday(&start, NULL );
		char * free_ptr;
		if (COMPRESS) {
			free_ptr = query_response_msg_data;
			query_response_msg_data = message_compress(&se,
					query_response_msg_data, &query_response_msg_size);
		}
		c_time = end_timer_ms(&start);

		message_send(socket, query_response_msg_data, query_response_msg_size);
		//free(query_response_msg_data);
		req_time = end_timer_ms(&req_start);

		//free(query_response_msg_data);
		if (COMPRESS) {
			free(free_ptr);
		}

	}
	zmq_close(socket);
	zmq_ctx_destroy(context);
	return 0;
}
