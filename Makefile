CXX=g++
CXXFLAGS=-Wall -Wextra -pedantic -std=c++2a -g -o3
#-fsanitize=address -fsanitize=leak
#-fsanitize=thread
EXEC = lock_free_eht

all: $(EXEC)

$(EXEC):  main.cpp include/eht.h \
                  include/coarse-eth.h \
                  src/coarse-eth.cpp \
                  include/hash_function.h \
                  lib/murmur3/MurmurHash3.cpp \
                  lib/comparator/int-comparator.h \
                  lib/node/node.cpp \
                  lib/node/node.h \
                  lib/node/inner-node.hpp \
                  lib/node/leaf-node.hpp \
                  include/eth_storage/htable_header.h \
                  include/eth_storage/htable_directory.h \
                  include/common.h \
                  include/eth_storage/htable_bucket.h
	$(CXX) $(CXXFLAGS) main.cpp -o $@ -lpthread
