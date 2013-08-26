#include "mapipbtest.h"

int main(void) {
	int rc;
	size_t len;

	struct snappy_env se;
	snappy_init_env(&se);

	// setup socket
	void *context = zmq_ctx_new();
	void *socket = zmq_socket(context, ZMQ_REQ);
	rc = zmq_connect(socket, CLIENT_SOCKET);
	assert(rc == 0);

	long uc_time, us_time, req_time;
	struct timeval start, req_start;

	int i = 0;
	for (i = 0; i < 5; i++) {
		gettimeofday(&req_start, NULL );

		// send request
		Mapipbtest__ExecuteQuery query;
		mapipbtest__execute_query__init(&query);
		query.sqlquery = "SELECT * FROM lineitem;";
		len = mapipbtest__execute_query__get_packed_size(&query);
		void * send_buffer = malloc(len);
		mapipbtest__execute_query__pack(&query, send_buffer);
		if (COMPRESS) {
			send_buffer = message_compress(&se, send_buffer, &len);
		}
		message_send(socket, send_buffer, len);

		// right, now let's receive the result
		// read from socket, decompress if req., unpack

		Mapipbtest__QueryResult * query_result;
		message query_result_msg;
		message_receive(socket, &query_result_msg);
		void *query_result_msg_data = message_data(&query_result_msg);
		len = message_size(&query_result_msg);

		gettimeofday(&start, NULL );
		// save compressed size for stats
		size_t clen = len;
		if (COMPRESS) {
			query_result_msg_data = message_uncompress(query_result_msg_data,
					&len);
			message_close(&query_result_msg);
		}
		uc_time = end_timer_ms(&start);
		gettimeofday(&start, NULL );
		query_result = mapipbtest__query_result__unpack(NULL, len,
				query_result_msg_data);
		assert(query_result != NULL);
		us_time = end_timer_ms(&start);

		if (COMPRESS) {
			free(query_result_msg_data);
		}

		message_close(&query_result_msg);
		req_time = end_timer_ms(&req_start);

		//print_result_set(query_result, 10);

		printf("PROTO\t%i\t%li\t%zu\t%zu\t%lu\t%lu\t%lu\n", COMPRESS,
				query_result->rows, len, clen, us_time, uc_time, req_time);
		mapipbtest__query_result__free_unpacked(query_result,NULL);
	}
	zmq_close(socket);
	zmq_ctx_destroy(context);
	return 0;
}
