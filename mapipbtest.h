#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <zmq.h>
#include <errno.h>
#include <stddef.h>
#include <stdbool.h>

#include <sys/time.h>

#include "protobuf/messages.pb-c.h"
#include "snappy-c/snappy.h"

#define MAX_MSG_SIZE 1024
#define MAX_LINE_LEN 102400

#define COMPRESS 0

#define DATA_COLS 16
#define DATA_ROWS 6001215

//#define DATA_ROWS 6000000
//#define DATA_ROWS 10
#define DATA_FILE "/export/scratch2/hannes/tpc/SF-1/lineitem.tbl"
//#define DATA_FILE "lineitem.tiny"
#define DATA_TYPES "iiiifffsssssssss"

#define SERVER_SOCKET "tcp://*:60003"
#define CLIENT_SOCKET "tcp://localhost:60003"

static void print_result_set(Mapipbtest__QueryResult * result, int n) {
	size_t row, col = 0;
	for (row = 0; row < n; row++) {
		for (col = 0; col < result->n_columns; col++) {
			printf("[%zu,%zu] ", row, col);
			if (DATA_TYPES[col] == 'i') {
				printf("%lu", result->columns[col]->intvalues[row]);
			}
			if (DATA_TYPES[col] == 'f') {
				printf("%f", result->columns[col]->floatvalues[row]);
			}
			if (DATA_TYPES[col] == 's') {
				printf("%s", result->columns[col]->stringvalues[row]);
			}
			printf("\t");
		}
		printf("\n");
	}
}

typedef zmq_msg_t message;

static long end_timer_ms(struct timeval * start) {
	long elapsed;
	struct timeval end;
	gettimeofday(&end, NULL);
	elapsed = (end.tv_sec - start->tv_sec) * 1000;      // sec to ms
	elapsed += (end.tv_usec - start->tv_usec) / 1000;   // us to ms
	return elapsed;
}

static size_t message_size(message *msg) {
	return zmq_msg_size(msg);
}

static char* message_data(message *msg) {
	return zmq_msg_data(msg);
}

static void message_receive(void* zmq_socket, message *msg) {
	int rc;
	rc = zmq_msg_init(msg);
	assert(rc == 0);
	rc = zmq_msg_recv(msg, zmq_socket, 0);
	assert(rc != -1);
}

void message_free(void *data, void *hint) {
	free(data);
}

static int message_close(message *msg) {
	return zmq_msg_close(msg);
}

static void message_send(void* zmq_socket, char * buffer, size_t buffer_len) {
	int rc;
	zmq_msg_t msg;
	zmq_msg_init_data(&msg, buffer, buffer_len, message_free, NULL);
	// send it
	rc = zmq_msg_send(&msg, zmq_socket, 0);
	assert(rc == buffer_len);
	rc = zmq_msg_close(&msg);
	assert(rc == 0);
}

static char* message_compress(struct snappy_env *se, char *msg_data,
		size_t * msg_size) {
	int rc;
	size_t compression_buffer_size = snappy_max_compressed_length(*msg_size);
	char *msg_data_compressed = malloc(compression_buffer_size);
	assert(msg_data_compressed != NULL);

	size_t msg_size_compressed;
	rc = snappy_compress(se, msg_data, *msg_size, msg_data_compressed,
			&msg_size_compressed);
	assert(rc == 0);
	*msg_size = msg_size_compressed;
	return msg_data_compressed;
}

static char* message_uncompress(char *msg_data, size_t * msg_size) {
	int rc;
	size_t msg_size_uncompressed;
	rc = snappy_uncompressed_length(msg_data, *msg_size,
			&msg_size_uncompressed);
	assert(rc == true);

	char *msg_data_uncompressed = malloc(msg_size_uncompressed);
	assert(msg_data_uncompressed != NULL);
	rc = snappy_uncompress(msg_data, *msg_size, msg_data_uncompressed);
	assert(rc == 0);
	*msg_size = msg_size_uncompressed;
	return msg_data_uncompressed;

}
