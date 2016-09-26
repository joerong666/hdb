PF=$(shell getconf LONG_BIT)
ifeq ($(PF),32)
PF_CFLAGS = "-march=i686"
else
PF_CFLAGS = 
endif

BASE_DIR=../..

ALLOC_PATH=$(BASE_DIR)/depend/jemalloc
ALLOC_DEP=$(BASE_DIR)/depend/jemalloc/lib/libjemalloc.a
ALLOC_LINK=$(ALLOC_DEP) -ldl -lpthread
ALLOC_FLAGS=-I$(BASE_DIR)/depend/jemalloc/include

CH_PATH=$(BASE_DIR)/common/consisten_hash
CH_DEP=$(BASE_DIR)/common/consisten_hash/libch.a
CH_LINK=$(CH_DEP)
CH_FLAGS=-I$(BASE_DIR)/common/consisten_hash

INC_DEP=
LIB_DEP= -lpthread
LINK_DEP= -lpthread


INCLUDE = $(INC_DEP)
LIB = $(LINK_DEP)

#CCFLAG = -std=c99 -O2 -fprofile-generate -g -pg -Wall -W -D_XOPEN_SOURCE -D__USE_BSD
#CCFLAG =  -ggdb -fno-omit-frame-pointer -Wno-write-strings -Wall  -Wextra -Winline -Wunused -Wfloat-equal -Wmissing-include-dirs -Wformat=2 
CCFLAG = $(PF_CFLAGS) -DFOR_SINGLE_DS -g3 -Wno-write-strings -Wall  -Wextra -Winline -Wunused -Wfloat-equal -Wmissing-include-dirs -Wformat=2 
#CCFLAG = -g3 -Wno-write-strings -Wall  -Wextra -Winline -Wunused -Wfloat-equal -Wmissing-include-dirs -Wformat=2 
#CCFLAG = -O2 -Wno-write-strings -Wall  -Wextra -Winline -Wunused -Wfloat-equal -Wmissing-include-dirs -Wformat=2 

CCLINK = -lm -lpthread

PF_DIR=../../pf_lib
PF_PATH=$(PF_DIR)/src
PF_DEP=$(PF_PATH)/libpflib.a 
PF_LINK=$(PF_DEP) -lrt
PF_FLAGS=-I$(PF_DIR)/include

UTIL_PATH=../../util
UTIL_DEP=$(UTIL_PATH)/libutil.a
UTIL_LINK=$(UTIL_DEP)
UTIL_FLAGS=-I$(UTIL_PATH)

JE_DIR=../../depend/jemalloc
JE_DEP=$(JE_DIR)/lib/libjemalloc.a
JE_LINK=$(UTIL_DEP)
JE_FLAGS=-I$(JE_DIR)/include

BL_PATH=../../common/binlog/
BL_DEP=$(BL_PATH)/libbl.a
BL_LINK=$(BL_DEP)
BL_FLAGS=-I$(BL_PATH)

DBV1_DIR=../../hidb
DBV1_OPT=-I$(DBV1_DIR) $(DBV1_DIR)/db/src/libhidb.a \
		 $(UTIL_FLAGS) $(UTIL_LINK) $(UTIL_DEP) \
		 $(PF_FLAGS) $(PF_LINK) $(PF_DEP) \
		 $(BL_FLAGS) $(BL_LINK) $(BL_DEP) \
		 $(JE_FLAGS) $(JE_LINK) $(JE_DEP)

CC=gcc
BIN_TARGET=hdb_test
INC=-Icore -Idb $(INC_DEP) $(ALLOC_FLAGS) -I$(BASE_DIR)/util -I.
SRC=core/*.c db/*.c
OBJ=core/*.o db/*.o
BUILD=build
BUILD_INC=build/include
BUILD_OBJ=build/obj
BUILD_LIB=build/lib
LIBHIDB2=libhidb.a

TEST_CCFLAG = -DFOR_UNIT_TEST

mklib: 
	-mkdir -p $(BUILD_INC) $(BUILD_OBJ) $(BUILD_LIB)
	$(CC) -c -DUSE_ZMALLOC $(CCFLAG) $(INC) $(SRC)	\
	$(UTIL_FLAGS)   					\
	$(PF_FLAGS)   						\
	$(BL_FLAGS)   						\
	$(JE_FLAGS)  

	mv *.o $(BUILD_OBJ)
	ar rcs $(BUILD_LIB)/$(LIBHIDB2) $(BUILD_OBJ)/*.o
	cp db/db.h db/db_com_def.h $(BUILD_INC)

mkar: 
	-mkdir -p $(BUILD_INC) $(BUILD_OBJ) $(BUILD_LIB)
	$(CC) -c $(TEST_CCFLAG) $(CCFLAG) $(INC) $(SRC)
	mv *.o $(BUILD_OBJ)
	ar rcs $(BUILD_LIB)/$(LIBHIDB2) $(BUILD_OBJ)/*.o
	cp db/db.h db/db_com_def.h $(BUILD_INC)

mktest: mkar
	$(CC) -o $(BIN_TARGET) test/*.c $(BUILD_LIB)/$(LIBHIDB2) -Itest $(INC) \
		$(TEST_CCFLAG) $(CCFLAG) $(DBV1_OPT) $(LIB_DEP)

mkprod: mklib
	$(CC) -DUSE_ZMALLOC -o $(BIN_TARGET) test/*.c $(BUILD_LIB)/$(LIBHIDB2) -Itest $(INC) \
		$(CCFLAG) $(DBV1_OPT) $(LIB_DEP)

clean:
	-rm build -r

.PHONY: mktest clean 

