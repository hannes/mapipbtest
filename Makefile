CC      = g++
PROTOC  = protoc

CFLAGS  = -I. -I${CPLUS_INCLUDE_PATH} -Wall
LDFLAGS = -L/export/scratch1/hannes/fakefs/lib -lprotobuf -lsnappy -lzmq -lpthread

GENDIR = ./protobuf

# Make a directory unless it already exists
maybe-mkdir = $(if $(wildcard $1),,mkdir -p $1)


# BEGIN GTEST STUFF

# All tests produced by this Makefile.  Remember to add new tests you
# created to the list.
TESTS = Node_unittest

# Points to the root of Google Test, relative to where this file is.
# Remember to tweak this if you move this file.
GTEST_DIR = /export/scratch1/hannes/libs/gtest-1.6.0

# Where to find user code.
USER_DIR = .

# Flags passed to the preprocessor.
CPPFLAGS += -I$(GTEST_DIR)/include

# Flags passed to the C++ compiler.
CXXFLAGS += -g -Wall

# All Google Test headers.  Usually you shouldn't change this
# definition.
GTEST_HEADERS = $(GTEST_DIR)/include/gtest/*.h \
                $(GTEST_DIR)/include/gtest/internal/*.h

# House-keeping build targets.

tests : $(TESTS)

# Builds gtest.a and gtest_main.a.
GTEST_SRCS_ = $(GTEST_DIR)/src/*.cc $(GTEST_DIR)/src/*.h $(GTEST_HEADERS)

gtest-all.o : $(GTEST_SRCS_)
	$(CC) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c $(GTEST_DIR)/src/gtest-all.cc

gtest_main.o : $(GTEST_SRCS_)
	$(CC) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c $(GTEST_DIR)/src/gtest_main.cc

gtest.a : gtest-all.o
	$(AR) $(ARFLAGS) $@ $^

gtest_main.a : gtest-all.o gtest_main.o
	$(AR) $(ARFLAGS) $@ $^
	
## END GTEST STUFF

all: pb paxos

pb: pb.o protobuf/messages.pb.o Node.o TpcFile.o Utils.o
	$(CC) -o $@ $^ $(LDFLAGS)
	

paxos: paxos.o protobuf/messages.pb.o Node.o TpcFile.o Utils.o
	$(CC) -o $@ $^ $(LDFLAGS)

pb.o: pb.cpp protobuf/messages.pb.h
	$(CC) -c $(CFLAGS) $<

paxos.o: paxos.cpp protobuf/messages.pb.h
	$(CC) -c $(CFLAGS) $<
	
	
%.o: %.cpp
	$(CC) -c $(CFLAGS) $<
	
clean:
	rm -rf pb paxos *.o protobuf $(TESTS) gtest.a gtest_main.a *.o

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
	
	
# tests

Node_unittest.o: $(USER_DIR)/Node_unittest.cpp protobuf/messages.pb.h $(GTEST_HEADERS) 
	$(CC) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/Node_unittest.cpp
 
Node_unittest: protobuf/messages.pb.o Node.o Utils.o Node_unittest.o gtest_main.a 
	$(CC) $(CPPFLAGS) $(CXXFLAGS) $(LDFLAGS) -lpthread $^ -o $@

