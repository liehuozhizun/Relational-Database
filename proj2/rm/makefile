include ../makefile.inc

all: librm.a rmtest_create_tables rmtest_delete_tables rmtest

# lib file dependencies
librm.a: librm.a(rm.o)  # and possibly other .o files

# c file dependencies
rm.o: rm.h

rmtest.o: rm.h rm_test_util.h
rmtest_create_tables.o: rm.h rm_test_util.h
rmtest_delete_tables.o: rm.h rm_test_util.h

# binary dependencies
rmtest_create_tables: rmtest_create_tables.o librm.a $(CODEROOT)/rbf/librbf.a
rmtest_delete_tables: rmtest_delete_tables.o librm.a $(CODEROOT)/rbf/librbf.a
rmtest: rmtest.o librm.a $(CODEROOT)/rbf/librbf.a 


# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a


.PHONY: clean
clean:
	-rm rmtest_create_tables rmtest_delete_tables rmtest *.a *.o *~ 
	$(MAKE) -C $(CODEROOT)/rbf clean
