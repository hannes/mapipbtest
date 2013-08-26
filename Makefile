CC      = clang
PROTOC  = protoc-c

CFLAGS  = -I. -I${CPLUS_INCLUDE_PATH} -Wall -g
LDFLAGS = -L/export/scratch1/hannes/fakefs/lib -lprotobuf-c -lzmq

GENDIR = ./protobuf

maybe-mkdir = $(if $(wildcard $1),,mkdir -p $1)

all: client server

server: server.o $(GENDIR)/messages.pb-c.o snappy-c/snappy.o
	$(CC) -o $@ $^ $(LDFLAGS)

server.o: server.c $(GENDIR)/messages.pb-c.h
	$(CC) -c $(CFLAGS) $<
	
client: client.o $(GENDIR)/messages.pb-c.o  snappy-c/snappy.o
	$(CC) -o $@ $^ $(LDFLAGS)

client.o: client.c $(GENDIR)/messages.pb-c.h
	$(CC) -c $(CFLAGS) $<
	
%.o: %.c
	$(CC) -c $(CFLAGS) $<
	
clean:
	rm -rf  client server  *.o $(GENDIR)

$(GENDIR)/%.pb-c.h $(GENDIR)/%.pb-c.c: %.proto
	$(call maybe-mkdir, $(dir $@))
	$(PROTOC) -I. --c_out=$(GENDIR) $<

$(GENDIR)/%.pb-c.o: $(GENDIR)/%.pb-c.c
	$(CC) -c $< -o $@
	
	