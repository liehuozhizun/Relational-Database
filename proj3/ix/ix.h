#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <cstring>
#include <iostream>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"

#define SUCCESS                   0
#define IX_CREATE_FAILED          1
#define IX_DESTORY_FAILED         2
#define IX_OPEN_FAILED            3
#define IX_CLOSE_FAILED           4
#define IX_APPEND_FAILED          5
#define IX_WRITE_FAILED           6
#define IX_INSERT_NONLEAF_FAILED  7
#define IX_INSERT_LEAF_FAILED     8
#define IX_DELETE_NON_LEAF_FAILED 9
#define IX_DELETE_LEAF_FAILED     10
#define IX_READ_FAILED            11
#define IX_NO_ENOUGH_SPACE        12

# define IX_EOF (-1)  // end of the index scan
# define POSSIBILITY_NUMBER 77401

// leaf page type
#define LEAF_PAGE       false
#define NON_LEAF_PAGE   true

typedef struct NonLeafHeader {
  int32_t entriesSize;
  int32_t freeSpaceOffset;
  int32_t leftChild;
} NonLeafHeader;

typedef struct NonLeafEntry {
  uint8_t key[4];
  int32_t rightChild;
} NonLeafEntry;

typedef struct LeafHeader {
  int32_t prev;
  int32_t next;
  int32_t entriesSize;
  int32_t freeSpaceOffset;
} LeafHeader;

typedef struct LeafEntry {
  uint8_t key[4];
  RID rid;
} LeafEntry;

typedef struct ChildEntry {
  void * key;
  int32_t rightChild;
} ChildEntry;

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);
        RC insert(IXFileHandle &fd, const Attribute &attr, const void *key, const RID &rid, int32_t pageNum, ChildEntry &childEntry);
        RC insertNonLeafEntry(const Attribute attr, ChildEntry childEntry, void *data);
        RC insertLeafEntry(const Attribute attr, const void *key, const RID &rid, void *data);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);
        RC deleteNonLeafEntry(const Attribute attr, const void* key, void* data);
        RC deleteLeafEntry(IXFileHandle &fd, const Attribute attr, const void *key, const RID &rid, void *data, int32_t pageNum);
        RC deleteLeaf(Attribute attr, const void *key, const RID &rid, void *data);

        // Other functions
        int32_t compareLeafEntry(const Attribute attr, const void *key, void *data, int32_t entryNum) const;
        int32_t compareNonLeafEntry(const Attribute attr, const void *key, void *data, int32_t entryNum);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        void print(IXFileHandle &fd, const Attribute &attr, int32_t pageNum, string spaces) const;
        void printNonLeafEntry(IXFileHandle fd, const Attribute &attr, void *data, string spaces) const;
        void printLeafEntry(const Attribute &attr, void *data) const;
        void printRID(const RID rid) const;
        void printPage(const Attribute attr, const void *data);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        static PagedFileManager *_pf_manager;
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // initialize scan iterator.
        RC scanInit(IXFileHandle &, Attribute, const void*, const void*, bool, bool);

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

  friend class IndexManager;

  private:
      IXFileHandle* fileHandle;
      Attribute attribute;
      const void* lowKey;
      const void* highKey;
      bool lowKeyInclusive;
      bool highKeyInclusive;

      void* pageData;
      int entryNum;
      int scanKey;
};



class IXFileHandle {
public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
  RC readPage            (PageNum pageNum, void *data);
  RC writePage           (PageNum pageNum, void *data);
  RC appendPage          (const void *data);
  unsigned getNumberOfPages();

friend class IndexManager;

private:
    FileHandle fh;
};

#endif
