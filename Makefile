CC      = g++
PROTOC  = protoc

CFLAGS  = -I. -I${CPLUS_INCLUDE_PATH} -Wall
LDFLAGS = -L/export/scratch1/usr/lib -lprotobuf -lsnappy -lzmq -lpthread

GENDIR = ./protobuf


# Make a directory unless it already exists
maybe-mkdir = $(if $(wildcard $1),,mkdir -p $1)

all: pb

pb: pb.o protobuf/messages.pb.o Node.o TpcFile.o Utils.o
	$(CC) -o $@ $^ $(LDFLAGS)

pb.o: pb.cpp protobuf/messages.pb.h
	$(CC) -c $(CFLAGS) $<
	
%.o: %.cpp
	$(CC) -c $(CFLAGS) $<
	
clean:
	rm -r pb *.o protobuf

# C++ build rule.
$(GENDIR)/%.o: %.cc
	$(call maybe-mkdir, $(dir $@))
	$(CC) $(CFLAGS) -c $< -o $@

# Protobuffer code generation rules (note the first rule has multiple targets).
$(GENDIR)/%.pb.h $(GENDIR)/%.pb.cc: %.proto
	$(call maybe-mkdir, $(dir $@))
	$(PROTOC) -I. --cpp_out=$(GENDIR) $<

$(GENDIR)/%.pb.o: $(GENDIR)/%.pb.cc
	$(CC) -c $< -o $@
