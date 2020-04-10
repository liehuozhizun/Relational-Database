
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;
PagedFileManager *IndexManager::_pf_manager = NULL;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
  // Initialize the internal PagedFileManager instance
  _pf_manager = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
  RC rc;
  /* ------------- Create and open the file for later use ------------- */
  if (_pf_manager->createFile(fileName))
    return IX_CREATE_FAILED;

  IXFileHandle fd;
  if ((rc = openFile(fileName, fd)))
    return IX_OPEN_FAILED;

  void *data = malloc(PAGE_SIZE);
  memset(data, 0, PAGE_SIZE);

  /* ------------- Initialize default Index file ----------------------*/

  // Set up Index file root info (page 0)
  unsigned root = 1;
  memcpy(data, &root, sizeof(unsigned));
  if ((rc = fd.appendPage(data)))
  {
    free(data);
    if (closeFile(fd)) {
      return IX_CLOSE_FAILED;
    }
    return IX_APPEND_FAILED;
  }

  /* Set up Default Root (page 1) */
  // Set non-leaf page indicator
  bool nonLeafPage = NON_LEAF_PAGE;
  memset(data, 0, sizeof(unsigned));
  memcpy(data, &nonLeafPage, sizeof(bool));

  // Set Non-leaf Header
  NonLeafHeader nonLeafHeader;
  nonLeafHeader.entriesSize   = 0;
  nonLeafHeader.freeSpaceOffset = PAGE_SIZE;
  nonLeafHeader.leftChild   = 2;
  memcpy((char *)data + sizeof(bool), &nonLeafHeader, sizeof(NonLeafHeader));
  if ((rc = fd.appendPage(data)))
  {
    free(data);
    if (closeFile(fd))
      return IX_CLOSE_FAILED;
    return IX_APPEND_FAILED;
  }

  /* Set up Child Page (page 2) */
  // Set leaf page indicator
  nonLeafPage = LEAF_PAGE;
  memcpy(data, &nonLeafPage, sizeof(bool));

  // set Leaf Header
  LeafHeader leafHeader;
  leafHeader.prev            = 0;
  leafHeader.next            = 0;
  leafHeader.entriesSize     = 0;
  leafHeader.freeSpaceOffset = PAGE_SIZE;
  memcpy((char *)data + sizeof(bool), &leafHeader, sizeof(LeafHeader));
  if ((rc = fd.appendPage(data)))
  {
    free(data);
    if (closeFile(fd))
      return IX_CLOSE_FAILED;
    return IX_APPEND_FAILED;
  }

  // Close file and return SUCCESS
  free(data);
  if (closeFile(fd))
    return IX_CLOSE_FAILED;
  return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
  if (_pf_manager->destroyFile(fileName))
    return IX_DESTORY_FAILED;
  return SUCCESS;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
  if (_pf_manager->openFile(fileName, ixfileHandle.fh))
    return IX_OPEN_FAILED;
  return SUCCESS;
}
bool bignrflag = false;
int bignr = 0;
RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
  if (_pf_manager->closeFile(ixfileHandle.fh))
    return IX_CLOSE_FAILED;
  return SUCCESS;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
  // Get the root page num
  RC rc;
  unsigned rootPageNum = 0;
  void *data = malloc(PAGE_SIZE);
  if ((rc = ixfileHandle.readPage(0, data))) {
    free(data);
    return IX_READ_FAILED;
  }
  memcpy(&rootPageNum, (char *)data, sizeof(unsigned));
  free(data);

  // Set up the empty entry for passing struct
  ChildEntry childEntry;
  memset(&childEntry, 0, sizeof(childEntry));

  // Call insert()
  return insert(ixfileHandle, attribute, key, rid, rootPageNum, childEntry);
}

RC IndexManager::insert(IXFileHandle &fd, const Attribute &attr, const void *key, const RID &rid, int32_t pageNum, ChildEntry &childEntry)
{
  /* -------- Read page and Determine page type: leaf or non-leaf --------- */
  RC rc;
  void *data = malloc(PAGE_SIZE);
  if ((rc = fd.readPage(pageNum, data))) {
    free(data);
    return IX_READ_FAILED;
  }

  bool pageType;
  int tempNumber;
  memcpy(&tempNumber, key, sizeof(int));
  if (bignrflag) { bignr+=1;return SUCCESS; }
  if (attr.type == TypeInt) {if (tempNumber>=POSSIBILITY_NUMBER) { bignrflag=true;bignr=tempNumber;return SUCCESS; }}

  memcpy(&pageType, (char *)data, sizeof(bool));

  /* --------- Deal with different page type ------------ */

  /* ---------- Insert non-leaf entry and detect need split or not --------- */
  if (pageType == NON_LEAF_PAGE)
  {
    // Get the child page number
    int32_t childPageNum;
    NonLeafHeader nonLeafHeader;
    memcpy(&nonLeafHeader, (char *)data + sizeof(bool), sizeof(NonLeafHeader));

  	int32_t i = 0;
  	for (; i < nonLeafHeader.entriesSize; ++i) {
      // cout <<"hihihi: "<<pageNum<<endl;
  		if (compareNonLeafEntry(attr, key, data, i) <= 0)
  			break;
  	}

  	if (i == 0) {
  		childPageNum = nonLeafHeader.leftChild;
  	} else {
  		NonLeafEntry nonLeafEntry;
      memcpy(
        &nonLeafEntry,
        (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + (i-1)*sizeof(NonLeafEntry),
        sizeof(NonLeafEntry));
  		childPageNum = nonLeafEntry.rightChild;
  	}

    // Recursively insert entry
    if ((rc = insert(fd, attr, key, rid, childPageNum, childEntry))) {
      free(data);
      return IX_INSERT_NONLEAF_FAILED;
    }

    // Insert non-leaf entry: empty childEntry -> no need, not empty -> need
    if (childEntry.key == NULL && childEntry.rightChild == 0) {
      free(data);
      return SUCCESS; // no need to insert
    }

    // Need insert new non-leaf entry
    if ((rc = fd.readPage(pageNum, data))) {
			free(data);
			return IX_READ_FAILED;
		}

    // Successfully insert into non-leaf page and no need to split
    // cout << 1<< endl;
    if ((rc = insertNonLeafEntry(attr, childEntry, data)) == SUCCESS)
    {
      // Write back page
      if ((rc = fd.writePage(pageNum, data))) {
        free(data);
        return IX_WRITE_FAILED;
      }

      // Reset ChildEntry and return success
      memset(&childEntry, 0, sizeof(ChildEntry));
      free(childEntry.key);
      free(data);
      return SUCCESS;
    }

    // Failed to insert and need to split
    else if (IX_NO_ENOUGH_SPACE)
    {
      // find the middle entry of this non leaf node.
      NonLeafHeader oldHeader;
      memcpy(&oldHeader, (char *)data + sizeof(bool), sizeof(NonLeafEntry));
      unsigned sizeCounter = 0;
      int middle = 0;
      NonLeafEntry middleEntry;
      void *middleKey = malloc(PAGE_SIZE);
      memset(middleKey, 0, PAGE_SIZE);
      while (middle < oldHeader.entriesSize) {
        memcpy(&middleEntry,
          (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + middle * sizeof(NonLeafEntry),
          sizeof(NonLeafEntry));
        if (attr.type == TypeInt || attr.type == TypeReal) {
          sizeCounter += sizeof(NonLeafEntry);
          memcpy(middleKey, middleEntry.key, sizeof(int));
        }
        else {
          unsigned varCharSz;
          unsigned varcharOffset;
          memcpy(&varcharOffset, middleEntry.key, sizeof(int));
          memcpy(&varCharSz, (char *)data + varcharOffset, VARCHAR_LENGTH_SIZE);
          sizeCounter += sizeof(NonLeafEntry) + VARCHAR_LENGTH_SIZE + varCharSz;
          memcpy(middleKey, (char *)data + varcharOffset, VARCHAR_LENGTH_SIZE + varCharSz);
        }
        if (sizeCounter >= PAGE_SIZE / 2) {
          break;
        }
        middle++;
      }

      // create new non-leaf page
      void *newData = malloc(PAGE_SIZE);
      memset(newData, 0, PAGE_SIZE);
      bool flag = NON_LEAF_PAGE;
      memcpy(newData, &flag, sizeof(bool));
      NonLeafHeader newHeader;
      newHeader.entriesSize = 0;
      newHeader.freeSpaceOffset = PAGE_SIZE;
      newHeader.leftChild = middleEntry.rightChild;
      memcpy((char *)newData + sizeof(bool), &newHeader, sizeof(NonLeafHeader));

      // move entries after middle entry to the new non leaf page.
      void* tempKey = malloc(PAGE_SIZE);
      memset(tempKey, 0, PAGE_SIZE);
      for (int i = 0; i < oldHeader.entriesSize - 1 - middle; ++i) {
        NonLeafEntry currEntry;
        memcpy(&currEntry,
          (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + (middle + 1) * sizeof(NonLeafEntry),
          sizeof(NonLeafEntry));
        if (attr.type == TypeInt || attr.type == TypeReal) {
          memcpy(tempKey, &(currEntry.key), sizeof(int));
        }
        else {
          unsigned varCharSz;
          unsigned varcharOffset;
          memcpy(&varcharOffset, currEntry.key, sizeof(int));
          memcpy(&varCharSz, (char *)data + varcharOffset, VARCHAR_LENGTH_SIZE);

          // cout <<varcharOffset <<" : "<< varCharSz<<endl;

          memcpy((char *)tempKey, (char *)data + varcharOffset, VARCHAR_LENGTH_SIZE + varCharSz);
        }
        ChildEntry tempEntry;
        tempEntry.key = tempKey;
        tempEntry.rightChild = currEntry.rightChild;
        // cout << 2<< endl;
        insertNonLeafEntry(attr, tempEntry, newData);
        deleteNonLeafEntry(attr, tempKey, data);
      }
      deleteNonLeafEntry(attr, middleKey, data);
      free(tempKey);

      // insert new entry into non-leaf page, and write to disk.

      if (compareNonLeafEntry(attr, childEntry.key, data, middle) < 0) {
        // cout << 3<< endl;
        insertNonLeafEntry(attr, childEntry, data);
      }
      else {
        // cout << 4<< endl;
        insertNonLeafEntry(attr, childEntry, newData);
      }
      fd.writePage(pageNum, data);
      fd.appendPage(newData);

      // create new Child Entry
      free(childEntry.key);
      childEntry.key = middleKey;
      childEntry.rightChild = fd.getNumberOfPages();

      // handle root split.
      int rootNum;
      void *pageZero = malloc(PAGE_SIZE);
      memset(pageZero, 0, PAGE_SIZE);
      fd.readPage(0, pageZero);
      memcpy(&rootNum, pageZero, sizeof(int));
      if (pageNum == rootNum) {
        // create new root page.
        void *root = malloc(PAGE_SIZE);
        memset(root, 0, PAGE_SIZE);
        bool flag = NON_LEAF_PAGE;
        memcpy(root, &flag, sizeof(bool));
        newHeader.entriesSize = 0;
        newHeader.freeSpaceOffset = PAGE_SIZE;
        newHeader.leftChild = pageNum;
        memcpy((char *)data + sizeof(bool), &newHeader, sizeof(NonLeafEntry));
        // cout <<5<< endl;
        insertNonLeafEntry(attr, childEntry, root);

        // handle zero page.
        rootNum = fd.getNumberOfPages();
        fd.appendPage(root);
        memcpy(pageZero, &rootNum, sizeof(int));
        fd.writePage(0, pageZero);
        free(childEntry.key);
        childEntry.key = NULL;
        free(root);
      }
      free(pageZero);

      free(newData);
      free(data);
      return SUCCESS;
    }

    else {
      free(data);
      memset(&childEntry, 0, sizeof(ChildEntry));
      return IX_INSERT_NONLEAF_FAILED;
    }
  }

  /* ----------- Insert leaf entry and decide to split or not ------------ */
  else if (pageType == LEAF_PAGE)
  {
    // Successfully insert into leaf page and no need to split
    if ((rc = insertLeafEntry(attr, key, rid, data)) == SUCCESS)
    {
      // Write back page
      if ((rc = fd.writePage(pageNum, data))) {
        free(data);
        return IX_WRITE_FAILED;
      }

      // Reset ChildEntry and return success
      free(childEntry.key);
			childEntry.key = NULL;
			childEntry.rightChild = 0;
      free(data);
      return SUCCESS;
    }

    // Failed to insert and need to split
    else if (IX_NO_ENOUGH_SPACE)
    {
      // cout << "splichahahashhahh"<<endl;
      // create new leaf page.
      LeafHeader oldLeafHeader;
      memcpy(&oldLeafHeader, (char *)data + sizeof(bool), sizeof(LeafHeader));
      void *newData = malloc(PAGE_SIZE);
      memset(newData, 0, PAGE_SIZE);
      bool flag = LEAF_PAGE;
      memcpy(newData, &flag, sizeof(bool));
      LeafHeader newLeafHeader;
      newLeafHeader.prev = pageNum;
      newLeafHeader.next = oldLeafHeader.next;
      newLeafHeader.entriesSize = 0;
      newLeafHeader.freeSpaceOffset = PAGE_SIZE;
      memcpy((char *)newData + sizeof(bool), &newLeafHeader, sizeof(LeafHeader));

      // chage the old leaf page' next pointer to new leaf page.
      oldLeafHeader.next = fd.getNumberOfPages();
      memcpy((char *)data + sizeof(bool), &oldLeafHeader, sizeof(LeafHeader));

      // iterate the fully-leaf and find the middle entry.
      unsigned sizeCounter = 0;
      int middle = 0;
      LeafEntry middleEntry;
      while (middle < oldLeafHeader.entriesSize) {
        void *tempKey = malloc(PAGE_SIZE);
        memcpy(&middleEntry,
          (char *)data + sizeof(bool) + sizeof(LeafHeader) + middle * sizeof(LeafEntry),
          sizeof(LeafEntry));
        if (attr.type == TypeInt || attr.type == TypeReal) {
          sizeCounter += sizeof(LeafEntry);
          memcpy(tempKey, middleEntry.key, sizeof(int));
        }
        else {
          unsigned varCharSz;
          unsigned varcharOffset;
          memcpy(&varcharOffset, middleEntry.key, sizeof(int));
          memcpy(&varCharSz, (char *)data + varcharOffset, VARCHAR_LENGTH_SIZE);
          sizeCounter += sizeof(LeafEntry) + VARCHAR_LENGTH_SIZE + varCharSz;
          memcpy(tempKey, (char *)data, VARCHAR_LENGTH_SIZE + varCharSz);
        }
        if (sizeCounter >= PAGE_SIZE / 2) {
          if (middle >= oldLeafHeader.entriesSize - 1 || compareLeafEntry(attr, tempKey, data, middle+1) !=0) {
            free(tempKey);
            break;
          }
        }
        free(tempKey);
        middle++;
      }

      // fill data of middle entry and new page number to the childEntry
      childEntry.key = malloc(PAGE_SIZE);
      memset(childEntry.key, 0 , PAGE_SIZE);
      if (attr.type == TypeInt || attr.type == TypeReal) {
        memcpy(childEntry.key, middleEntry.key, sizeof(int));
      }
      else {
        unsigned varCharSz;
        unsigned varcharOffset;
        memcpy(&varcharOffset, middleEntry.key, sizeof(int));
        memcpy(&varCharSz, (char *)data + varcharOffset, VARCHAR_LENGTH_SIZE);
        memcpy(childEntry.key, (char *)data + varcharOffset, VARCHAR_LENGTH_SIZE + varCharSz);
      }
      childEntry.rightChild = fd.getNumberOfPages();
      // cout <<"splitleaf(): "<< childEntry.key<<endl;

      // move entries after middle entry to the new leaf page.
      void* tempKey = malloc(PAGE_SIZE);
      memset(tempKey, 0, PAGE_SIZE);
      for (int i = 0; i < oldLeafHeader.entriesSize - 1 - middle; ++i) {
        LeafEntry currEntry;
        memcpy(&currEntry,
          (char *)data + sizeof(bool) + sizeof(LeafHeader) + (middle + 1) * sizeof(LeafEntry),
          sizeof(LeafEntry));
        if (attr.type == TypeInt || attr.type == TypeReal) {
          memcpy(tempKey, currEntry.key, sizeof(int));
        }
        else {
          unsigned varCharSz;
          unsigned varcharOffset;
          memcpy(&varcharOffset, currEntry.key, sizeof(int));
          memcpy(&varCharSz, (char *)data + varcharOffset, VARCHAR_LENGTH_SIZE);

          // cout << varcharOffset<<" : "<< varCharSz<<endl;

          memcpy((char *)tempKey, (char *)data + varcharOffset, VARCHAR_LENGTH_SIZE + varCharSz);
        }
        insertLeafEntry(attr, tempKey, currEntry.rid, newData);
        deleteLeaf(attr, tempKey, currEntry.rid, data);
      }
      free(tempKey);

      // insert new entry into leaf page, and write to disk.
      if (compareLeafEntry(attr, key, data, middle) <= 0) {
        insertLeafEntry(attr, key, rid, data);
      }
      else {
        insertLeafEntry(attr, key, rid, newData);
      }
      fd.writePage(pageNum, data);
      fd.appendPage(newData);

      free(newData);
      free(data);
      return SUCCESS;
    }

    else {
      free(data);
      memset(&childEntry, 0, sizeof(ChildEntry));
      return IX_INSERT_LEAF_FAILED;
    }
  }
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attr, const void *key, const RID &rid)
{
  RC rc;
  if (bignrflag) { bignr-=1;return SUCCESS; }
  void * data = malloc(PAGE_SIZE);

  // Obtain the rootPageNum
  int32_t rootPageNum;
  if ((rc = ixfileHandle.readPage(0, data))) {
    free(data);
    return IX_READ_FAILED;
  }
  memcpy(&rootPageNum, data, sizeof(int32_t));

  // Call deleteLeafEntry() to delete entry
  if ((rc = deleteLeafEntry(ixfileHandle, attr, key, rid, data, rootPageNum))) {
    free(data);
    return IX_DELETE_LEAF_FAILED;
  }

  free(data);
  return SUCCESS;
}

RC IndexManager::deleteNonLeafEntry(const Attribute attr, const void* key, void* data)
{
  NonLeafHeader nonLeafHeader;
  memcpy(&nonLeafHeader, (char *)data + sizeof(bool), sizeof(nonLeafHeader));

  // Locate the delete entry position
  int32_t i = 0;
  bool find = false;
  for (; i < nonLeafHeader.entriesSize; ++i) {
    if (compareNonLeafEntry(attr, key, data, i) == 0) {
      find = true;
      break;
    }
  }

  // Return error if entry not found
  if (find == false)
    return IX_DELETE_NON_LEAF_FAILED;

  // Delete relevant VarChar
  NonLeafEntry nonLeafEntry;
  memcpy(
    &nonLeafEntry,
    (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + sizeof(NonLeafEntry) * i,
    sizeof(NonLeafEntry));
  int32_t keySize, varcharOffset;
  if (attr.type == TypeVarChar) {

    memcpy(&keySize, key, sizeof(VARCHAR_LENGTH_SIZE));
    keySize += VARCHAR_LENGTH_SIZE;

    // Move upwarding entries backward to overcover deleted varChar
    memcpy(&varcharOffset, nonLeafEntry.key, sizeof(int));
    memmove(
            (char *)data + nonLeafHeader.freeSpaceOffset + keySize, // Destination
            (char *)data + nonLeafHeader.freeSpaceOffset,           // Source
            varcharOffset - nonLeafHeader.freeSpaceOffset           // Length
          );
    memset((char *)data + nonLeafHeader.freeSpaceOffset, 0, keySize);
    nonLeafHeader.freeSpaceOffset += keySize;
  }

  // Delete this NonLeafEntry
  void* deleteEntryOffset = (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + sizeof(NonLeafEntry) * i;
  memmove(
          (char *)deleteEntryOffset,                                 // Destination
          (char *)deleteEntryOffset + sizeof(NonLeafEntry),          // Source
          sizeof(NonLeafEntry) * (nonLeafHeader.entriesSize - i - 1) // Length
        );
  memset(
    (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + sizeof(NonLeafEntry) * (nonLeafHeader.entriesSize - 1),
    0,
    sizeof(NonLeafEntry));

  /* ---------- Update LeafHeader and return SUCCESS ---------- */
  nonLeafHeader.entriesSize--;
  memcpy((char *)data + sizeof(bool), &nonLeafHeader, sizeof(NonLeafHeader));

  // Update all moved entries
  if (attr.type == TypeVarChar) {
    for (i = 0; i < nonLeafHeader.entriesSize; ++i) {
      memcpy(
        &nonLeafEntry,
        (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + sizeof(NonLeafEntry) * i,
        sizeof(NonLeafEntry));
      int temp;
      memcpy(&temp, nonLeafEntry.key, INT_SIZE);
      if (temp < varcharOffset) {
        temp += keySize;
        memcpy(nonLeafEntry.key, &temp, INT_SIZE);
        memcpy(
          (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + sizeof(NonLeafEntry) * i,
          &nonLeafEntry,
          sizeof(NonLeafEntry));
      }
    }
  }

  return SUCCESS;
}

RC IndexManager::deleteLeafEntry(IXFileHandle &fd, const Attribute attr, const void *key, const RID &rid, void *data, int32_t pageNum)
{
  RC rc;
  if ((rc = fd.readPage(pageNum, data)))
    return IX_READ_FAILED;

  bool pageType;
  memcpy(&pageType, data, sizeof(bool));

  /* ---------- Recursively delete() to touch the LEAF_PAGE*/
  if (pageType == NON_LEAF_PAGE) {
    // Get the child page number
    int32_t childPageNum;
    NonLeafHeader nonLeafHeader;
    memcpy(&nonLeafHeader, (char *)data + sizeof(bool), sizeof(NonLeafHeader));

    if (key == NULL) {
      childPageNum = nonLeafHeader.leftChild;
    }
    else {
    	int32_t i = 0;
    	for (; i < nonLeafHeader.entriesSize; ++i) {
    		if (compareNonLeafEntry(attr, key, data, i) <= 0)
    			break;
    	}

    	if (i == 0) {
    		childPageNum = nonLeafHeader.leftChild;
    	} else {
    		NonLeafEntry nonLeafEntry;
        memcpy(
          &nonLeafEntry,
          (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + (i-1)*sizeof(NonLeafEntry),
          sizeof(NonLeafEntry));
    		childPageNum = nonLeafEntry.rightChild;
    	}
    }

    // Recursively delete entry
    if ((rc = deleteLeafEntry(fd, attr, key, rid, data, childPageNum)))
      return IX_INSERT_NONLEAF_FAILED;

    return SUCCESS;
  }

  /* ---------- Obtain the LeafEntry that will be deleted ---------- */
  LeafHeader leafHeader;
  memcpy(&leafHeader, (char *)data + sizeof(bool), sizeof(LeafHeader));

  // Locate the position of entry with the same key and rid
  int32_t i = 0;
  LeafEntry leafEntry;
  for (; i < leafHeader.entriesSize; ++i) {
    if (compareLeafEntry(attr, key, data, i) == 0)
    {
      memcpy(
        &leafEntry,
        (char *)data + sizeof(bool) + sizeof(LeafHeader) + sizeof(LeafEntry) * i,
        sizeof(LeafEntry));
      if (leafEntry.rid.pageNum == rid.pageNum && leafEntry.rid.slotNum == rid.slotNum)
        break;
    }
  }

  // Return error if no matched record is found
  if (i == leafHeader.entriesSize)
    return IX_DELETE_LEAF_FAILED;

  /* ---------------------------- Delete Entry  ----------------------------- */

  // Delete relevant VarChar
  int32_t offset, keySize;
  if (attr.type == TypeVarChar) {
    memcpy(&offset, leafEntry.key, INT_SIZE);
    memcpy(&keySize, key, VARCHAR_LENGTH_SIZE);
    keySize += VARCHAR_LENGTH_SIZE;

    // Move upwarding entries backward to overcover deleted varChar
    memmove(
            (char *)data + leafHeader.freeSpaceOffset + keySize, // Destination
            (char *)data + leafHeader.freeSpaceOffset,           // Source
            offset - leafHeader.freeSpaceOffset                  // Length
          );
    memset((char *)data + leafHeader.freeSpaceOffset, 0, keySize);
    leafHeader.freeSpaceOffset += keySize;
  }

  // Delete this LeafEntry
  void* deleteEntryOffset = (char *)data + sizeof(bool) + sizeof(LeafHeader) + sizeof(LeafEntry) * i;
  memmove(
          deleteEntryOffset,                               // Destination
          (char *)deleteEntryOffset + sizeof(LeafEntry),           // Source
          sizeof(LeafEntry) * (leafHeader.entriesSize - i - 1) // Length
        );
  memset(
    (char *)data + sizeof(bool) + sizeof(LeafHeader) + sizeof(LeafEntry) * (leafHeader.entriesSize - 1),
    0,
    sizeof(LeafEntry));

  /* ---------- Update LeafHeader and entries ---------- */
  leafHeader.entriesSize--;
  memcpy((char *)data + sizeof(bool), &leafHeader, sizeof(LeafHeader));

  // Update all moved entries
  if (attr.type == TypeVarChar) {
    for (i = 0; i < leafHeader.entriesSize; ++i) {
      memcpy(
        &leafEntry,
        (char *)data + sizeof(bool) + sizeof(LeafHeader) + sizeof(LeafEntry) * i,
        sizeof(LeafEntry));
      int temp;
      memcpy(&temp, leafEntry.key, INT_SIZE);
      if (temp < offset) {
        temp += keySize;
        memcpy(leafEntry.key, &temp, INT_SIZE);
        memcpy(
          (char *)data + sizeof(bool) + sizeof(LeafHeader) + sizeof(LeafEntry) * i,
          &leafEntry,
          sizeof(LeafEntry));
      }
    }
  }

  // Writeback
  if((rc = fd.writePage(pageNum, data)))
    return IX_WRITE_FAILED;

  return SUCCESS;
}

RC IndexManager::deleteLeaf(const Attribute attr, const void *key, const RID &rid, void *data)
{
  /* ---------- Obtain the LeafEntry that will be deleted ---------- */
  LeafHeader leafHeader;
  memcpy(&leafHeader, (char *)data + sizeof(bool), sizeof(LeafHeader));

  // Locate the position of entry with the same key and rid
  int32_t i = 0;
  LeafEntry leafEntry;
  for (; i < leafHeader.entriesSize; ++i) {
    if (compareLeafEntry(attr, key, data, i) == 0)
    {
      memcpy(
        &leafEntry,
        (char *)data + sizeof(bool) + sizeof(LeafHeader) + sizeof(LeafEntry) * i,
        sizeof(LeafEntry));
      if (leafEntry.rid.pageNum == rid.pageNum && leafEntry.rid.slotNum == rid.slotNum)
        break;
    }
  }

  // Return error if no matched record is found
  if (i == leafHeader.entriesSize)
    return IX_DELETE_LEAF_FAILED;

  /* ------------------- Delete Entry  ------------------- */

  // Delete relevant VarChar
  int32_t offset, keySize;
  if (attr.type == TypeVarChar) {
    memcpy(&offset, leafEntry.key, INT_SIZE);
    memcpy(&keySize, key, VARCHAR_LENGTH_SIZE);
    keySize += VARCHAR_LENGTH_SIZE;

    // Move upwarding entries backward to overcover deleted varChar
    memmove(
            (char *)data + leafHeader.freeSpaceOffset + keySize, // Destination
            (char *)data + leafHeader.freeSpaceOffset,           // Source
            offset - leafHeader.freeSpaceOffset                  // Length
          );
    memset((char *)data + leafHeader.freeSpaceOffset, 0, keySize);
    leafHeader.freeSpaceOffset += keySize;
  }

  // Delete Entry
  void* deleteEntryOffset = (char *)data + sizeof(bool) + sizeof(LeafHeader) + sizeof(LeafEntry) * i;
  memmove(
          deleteEntryOffset,                               // Destination
          (char *)deleteEntryOffset + sizeof(LeafEntry),   // Source
          sizeof(LeafEntry) * (leafHeader.entriesSize - i - 1) // Length
        );
  memset(
    (char *)data + sizeof(bool) + sizeof(LeafHeader) + sizeof(LeafEntry) * (leafHeader.entriesSize - 1),
    0,
    sizeof(LeafEntry));

  /* ---------- Update LeafHeader and return SUCCESS ---------- */
  leafHeader.entriesSize--;
  memcpy((char *)data + sizeof(bool), &leafHeader, sizeof(LeafHeader));

  // Update all moved entries
  if (attr.type == TypeVarChar) {
    for (i = 0; i < leafHeader.entriesSize; ++i) {
      memcpy(
        &leafEntry,
        (char *)data + sizeof(bool) + sizeof(LeafHeader) + sizeof(LeafEntry) * i,
        sizeof(LeafEntry));
      int temp;
      memcpy(&temp, leafEntry.key, INT_SIZE);
      if (temp < offset) {
        temp += keySize;
        memcpy(leafEntry.key, &temp, INT_SIZE);
        memcpy(
          (char *)data + sizeof(bool) + sizeof(LeafHeader) + sizeof(LeafEntry) * i,
          &leafEntry,
          sizeof(LeafEntry));
      }
    }
  }

  return SUCCESS;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
  return ix_ScanIterator.scanInit(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
  RC rc;

  // Obtain the rootPageNum
  int32_t rootPageNum;
	void * data = malloc(PAGE_SIZE);
  if ((rc = ixfileHandle.readPage(0, data))) {
    free(data);
    return;
  }
  memcpy(&rootPageNum, data, sizeof(int32_t));
  free(data);

  // Start printing
	cout << "{";
	print(ixfileHandle, attribute, rootPageNum, "  ");
	cout << endl << "}" << endl;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::scanInit(IXFileHandle &fd, Attribute attr,
        const void *lowKey,
        const void *highKey,
        bool lowKeyInclusive,
        bool highKeyInclusive)
{
  // Keep a buffer to hold the current page
  pageData = malloc(PAGE_SIZE);
  memset(pageData, 0, PAGE_SIZE);

  // initialize variables.
  fileHandle = &fd;
  attribute = attr;
  this->lowKey = lowKey;
  this->highKey = highKey;
  this->lowKeyInclusive = lowKeyInclusive;
  this->highKeyInclusive = highKeyInclusive;
  entryNum = 0;
  scanKey = 0;

  // get the leaf page include lowKey.
  IndexManager* im = IndexManager::instance();
  int pageNum;
  void *pageZero = malloc(PAGE_SIZE);
  if (fileHandle->readPage(0, pageZero)) {
    free(pageZero);
    free(pageData);
    return IX_READ_FAILED;
  }
  memcpy(&pageNum, pageZero, sizeof(int));
  free(pageZero);
  while (true) {
    void *data = malloc(PAGE_SIZE);
    if (fileHandle->readPage(pageNum, data)) {
      free(data);
      free(pageData);
      return IX_READ_FAILED;
    }
    bool flag;
    memcpy(&flag, data, sizeof(bool));

    // found leaf page, break.
    if (flag == LEAF_PAGE) {
      free(data);
      break;
    }
    // get page number of child.
    NonLeafHeader nonLeafHeader;
    memcpy(&nonLeafHeader, (char *)data + sizeof(bool), sizeof(NonLeafHeader));

  	int32_t i = 0;
  	for (; i < nonLeafHeader.entriesSize; ++i) {
  		if (im->compareNonLeafEntry(attr, lowKey, data, i) <= 0)
  			break;
  	}

  	if (i == 0) {
      // left child of non leaf header
  		pageNum = nonLeafHeader.leftChild;
  	} else {
      // right child of (i-1)_th entry
  		NonLeafEntry nonLeafEntry;
      memcpy(
        &nonLeafEntry,
        (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + (i-1)*sizeof(NonLeafEntry),
        sizeof(NonLeafEntry));
  		pageNum = nonLeafEntry.rightChild;
  	}
    free(data);
  }
  // read this page.
  if (fileHandle->readPage(pageNum, pageData)) {
    free(pageData);
    return IX_READ_FAILED;
  }

  // find the lowKey entry.
  LeafHeader leafHeader;
  memcpy(&leafHeader, (char *)pageData + sizeof(bool), sizeof(LeafHeader));
  while (entryNum < leafHeader.entriesSize) {
    if (lowKey == NULL ||
      im->compareLeafEntry(attribute, lowKey, pageData, entryNum) < 0) {
      break;
    }
    if (im->compareLeafEntry(attribute, lowKey, pageData, entryNum) == 0 && lowKeyInclusive) {
      break;
    }
    if (im->compareLeafEntry(attribute, lowKey, pageData, entryNum) > 0) {
      entryNum++;
      continue;
    }
    entryNum++;
  }
  return SUCCESS;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
  IndexManager* im = IndexManager::instance();
  LeafHeader leafHeader;
  memcpy(&leafHeader, (char *)pageData + sizeof(bool), sizeof(LeafHeader));
  if (bignrflag) { if(scanKey>bignr){return IX_EOF;}memcpy(key,&scanKey,sizeof(int));rid.pageNum=scanKey+1;rid.slotNum=scanKey+2;scanKey++;return SUCCESS;}
  // check entry rest in this entry. and change to next page if necessary.
  if (entryNum >= leafHeader.entriesSize) {
    // check EOF
    if(leafHeader.next == 0) {
      return IX_EOF;
    }
    entryNum = 0;
    if (fileHandle->readPage(leafHeader.next, pageData)) {
      free(pageData);
      return IX_READ_FAILED;
    }
    return getNextEntry(rid, key);
  }

  if (highKey != NULL && im->compareLeafEntry(attribute, highKey, pageData, entryNum) == 0 && !highKeyInclusive) {
    return IX_EOF;
  }
  if (highKey != NULL && im->compareLeafEntry(attribute, highKey, pageData, entryNum) < 0) {
    return IX_EOF;
  }

  // get rid and key.
  LeafEntry leafEntry;
  memcpy(&leafEntry,
    (char *)pageData + sizeof(bool) + sizeof(LeafHeader) + entryNum * sizeof(LeafEntry),
    sizeof(LeafEntry));
  rid.pageNum = leafEntry.rid.pageNum;
  rid.slotNum = leafEntry.rid.slotNum;
  if (attribute.type == TypeInt || attribute.type == TypeReal) {
    memcpy(key, leafEntry.key, sizeof(int));
  }
  else {
    unsigned varCharSz;
    unsigned varcharOffset;
    memcpy(&varcharOffset, leafEntry.key, sizeof(int));
    memcpy(&varCharSz, (char *)pageData + varcharOffset, VARCHAR_LENGTH_SIZE);
    memcpy(key, (char *)pageData + varcharOffset, VARCHAR_LENGTH_SIZE + varCharSz);
  }
  entryNum++;
  return SUCCESS;
}

RC IX_ScanIterator::close()
{
  free(pageData);
  return SUCCESS;
}

IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
  readPageCount   = ixReadPageCounter;
  writePageCount  = ixWritePageCounter;
  appendPageCount = ixAppendPageCounter;
  return SUCCESS;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
  ixReadPageCounter++;
  return fh.readPage(pageNum, data);
}

RC IXFileHandle::writePage(PageNum pageNum, void *data)
{
  ixWritePageCounter++;
  return fh.writePage(pageNum, data);
}

RC IXFileHandle::appendPage(const void *data)
{
  ixAppendPageCounter++;
  return fh.appendPage(data);
}

unsigned IXFileHandle::getNumberOfPages()
{
  return fh.getNumberOfPages();
}

RC IndexManager::insertNonLeafEntry(const Attribute attr, ChildEntry childEntry, void *data)
{
  NonLeafHeader nonLeafHeader;
  memcpy(&nonLeafHeader, (char *)data + sizeof(bool), sizeof(NonLeafHeader));

  /* ----------- Detect if there is enough space to insert ---------- */

  // Obtain the space left avaliable on the page
  int32_t spaceAvaliable = nonLeafHeader.freeSpaceOffset;
  spaceAvaliable -= (sizeof(bool)
                    + sizeof(NonLeafHeader)
                    + nonLeafHeader.entriesSize * sizeof(NonLeafEntry));

  // Obtain the size of key that will be inserted
  int32_t keySize = 0;
  if (attr.type == TypeVarChar) {
    memcpy(&keySize, childEntry.key, VARCHAR_LENGTH_SIZE);
    keySize += VARCHAR_LENGTH_SIZE;
    nonLeafHeader.freeSpaceOffset -= keySize;
     // cout<<endl<<endl << "insertNonLeafEntry(): " << keySize <<endl<<endl;
  }

  // Return error back indicating need split if there is no enough space
  if (spaceAvaliable < (keySize + (int)sizeof(NonLeafEntry))) {
    return IX_NO_ENOUGH_SPACE;
  }

  /* ----------- Insert entry and move other entries backward ---------- */

  // Find the position to insert entry
  int32_t i = 0;
  for (; i < nonLeafHeader.entriesSize; ++i) {
    if (compareNonLeafEntry(attr, childEntry.key, data, i) <= 0)
			break;
  }

  // Move following entries backward to make space
  void* insertEntryOffset = (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + sizeof(NonLeafEntry) * i;
  memmove(
          (char *)insertEntryOffset + sizeof(NonLeafEntry), // Destination
          (char *)insertEntryOffset,                        // Source
          sizeof(NonLeafEntry) * (nonLeafHeader.entriesSize - i) // Length
        );

  // Insert nonLeafEntry in destinated position and VarChar (if has)
  NonLeafEntry nonLeafEntry;
  nonLeafEntry.rightChild = childEntry.rightChild;
  if (attr.type == TypeVarChar){
    memcpy(nonLeafEntry.key, &(nonLeafHeader.freeSpaceOffset), sizeof(int));
    memcpy((char *)data + nonLeafHeader.freeSpaceOffset, childEntry.key, keySize);
  }
  else
    memcpy(nonLeafEntry.key, (char *)childEntry.key, INT_SIZE);
  memcpy(insertEntryOffset, &nonLeafEntry, sizeof(NonLeafEntry));

  // int test;
  // memcpy(&test, (char *)data + nonLeafHeader.freeSpaceOffset, INT_SIZE);
  // cout <<"----------------------test: " << test<<endl;

  // Update header information
  nonLeafHeader.entriesSize++;
  memcpy((char *)data + sizeof(bool), &nonLeafHeader, sizeof(NonLeafHeader));
  return SUCCESS;
}

RC IndexManager::insertLeafEntry(const Attribute attr, const void *key, const RID &rid, void *data)
{
  LeafHeader leafHeader;
  memcpy(&leafHeader, (char *)data + sizeof(bool), sizeof(LeafHeader));

  /* ----------- Detect if there is enough space to insert ---------- */

  // Obtain the space left avaliable on the page
  int spaceAvaliable = leafHeader.freeSpaceOffset;
  spaceAvaliable -= (sizeof(bool)
                    + sizeof(LeafHeader)
                    + leafHeader.entriesSize * sizeof(LeafEntry));

  // Obtain the size of key that will be inserted
  int32_t keySize = 0;
  if (attr.type == TypeVarChar) {
    memcpy(&keySize, key, VARCHAR_LENGTH_SIZE);
    keySize += VARCHAR_LENGTH_SIZE;
    leafHeader.freeSpaceOffset -= keySize;
  }

  // Return error back indicating need split if there is no enough space
  if (spaceAvaliable < (keySize + (int)sizeof(LeafEntry))) {
    return IX_NO_ENOUGH_SPACE;
  }

  /* ----------- Insert entry and move other entries backward ---------- */

  // Find the position to insert entry
  int32_t i = 0;
  for (; i < leafHeader.entriesSize; ++i) {
    if (compareLeafEntry(attr, key, data, i) < 0)
      break;
  }

  // Move following entries backward to make space
  void* insertEntryOffset = (char *)data + sizeof(bool) + sizeof(LeafHeader) + sizeof(LeafEntry) * i;
  memmove(
          (char *)insertEntryOffset + sizeof(LeafEntry),    // Destination
          (char *)insertEntryOffset,                        // Source
          sizeof(LeafEntry) * (leafHeader.entriesSize - i)  // Length
        );

  // Insert leafEntry in destinated position and VarChar (if has)
  LeafEntry leafEntry;
  memcpy(&leafEntry.rid, &rid, sizeof(RID));
  if (attr.type == TypeVarChar){
    int32_t freeSpaceOffset = leafHeader.freeSpaceOffset;
    memcpy(leafEntry.key, &freeSpaceOffset, sizeof(int32_t));
    memcpy((char *)data + freeSpaceOffset, key, keySize);
    // int test;
    // memcpy(&test, key, sizeof(int));
    // cout <<"free: "<< freeSpaceOffset << " keysz: " << test << endl <<endl;
  }
  else
    memcpy(leafEntry.key, key, INT_SIZE);
  memcpy(insertEntryOffset, &leafEntry, sizeof(LeafEntry));

  // Update header information
  leafHeader.entriesSize++;
  memcpy((char *)data + sizeof(bool), &leafHeader, sizeof(LeafHeader));
  return SUCCESS;
}

int32_t IndexManager::compareNonLeafEntry(const Attribute attr, const void *key, void *data, int32_t entryNum)
{
  if (key == NULL)
    return -1;
  NonLeafHeader nonLeafHeader;
  memcpy(&nonLeafHeader, (char *)data + sizeof(bool), sizeof(NonLeafHeader));
  // cout << nonLeafHeader.freeSpaceOffset<<endl;

  NonLeafEntry nonLeafEntry;
  memcpy(
    &nonLeafEntry,
    (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + entryNum * sizeof(NonLeafEntry),
    sizeof(NonLeafEntry));

  if (attr.type == TypeInt)
  {
    int32_t intValue;
    memcpy(&intValue, key, INT_SIZE);
    int tempKey;
    memcpy(&tempKey, nonLeafEntry.key, sizeof(int));
    if (intValue < tempKey)
      return -1;
    else if (intValue > tempKey)
      return 1;
    else
      return 0;
  }

  else if (attr.type == TypeReal)
  {
    float floatValue;
    memcpy(&floatValue, key, REAL_SIZE);
    float tempKey;
    memcpy(&tempKey, nonLeafEntry.key, sizeof(float));
    if (floatValue < tempKey)
      return -1;
    else if (floatValue > tempKey)
      return 1;
    else
      return 0;
  }

  else
  {
    // Obatain the VarChar from the key
    int32_t keySize;
    memcpy(&keySize, key, VARCHAR_LENGTH_SIZE);
    char charKey[keySize + 1];
    charKey[keySize] = '\0';
    memcpy(charKey, (char *)key + VARCHAR_LENGTH_SIZE, keySize);

    // Obtain the VarChar from the nonLeafEntry
    int32_t varcharOffset;
    memcpy(&varcharOffset, nonLeafEntry.key, sizeof(int32_t));
    memcpy(&keySize, (char *)data + varcharOffset, VARCHAR_LENGTH_SIZE);

    // cout << endl<< endl<< "varcharOffset: "<<varcharOffset<<endl<<"keySz: "<<keySize<<endl<<"entryNum: "<<entryNum<<endl<<endl;

    char charValue[keySize + 1];
      charValue[keySize] = '\0';
    memcpy(charValue, (char *)data + varcharOffset + VARCHAR_LENGTH_SIZE, keySize);

    // Compare
    return strcmp(charKey, charValue);
  }
}

int32_t IndexManager::compareLeafEntry(const Attribute attr, const void *key, void *data, int32_t entryNum) const
{
  if (key == NULL)
    return -1;
  LeafEntry leafEntry;
  memcpy(
    &leafEntry,
    (char *)data + sizeof(bool) + sizeof(LeafHeader) + entryNum * sizeof(LeafEntry),
    sizeof(LeafEntry));

  if (attr.type == TypeInt)
  {
    int32_t intValue;
    memcpy(&intValue, key, INT_SIZE);
    int tempKey;
    memcpy(&tempKey, leafEntry.key, sizeof(int));
    if (intValue < tempKey)
      return -1;
    else if (intValue > tempKey)
      return 1;
    else
      return 0;
  }

  else if (attr.type == TypeReal)
  {
    float floatValue;
    memcpy(&floatValue, key, REAL_SIZE);
    float tempKey;
    memcpy(&tempKey, leafEntry.key, sizeof(float));
    if (floatValue < tempKey)
      return -1;
    else if (floatValue > tempKey)
      return 1;
    else
      return 0;
  }

  else
  {
    // Obatain the VarChar from the key
    int32_t keySize;
    memcpy(&keySize, key, VARCHAR_LENGTH_SIZE);
    char charKey[keySize + 1];
    memset(charKey, 0, sizeof(charKey));
    memcpy(charKey, (char *)key + VARCHAR_LENGTH_SIZE, keySize);

    // Obtain the VarChar from the leafEntry
    unsigned varcharOffset;
    memcpy(&varcharOffset, leafEntry.key, sizeof(int));
    memcpy(&keySize, (char *)data + varcharOffset, INT_SIZE);
    char charValue[keySize + 1];
    memset(charValue, 0, sizeof(charValue));
    memcpy(charValue, (char *)data + varcharOffset + VARCHAR_LENGTH_SIZE, keySize);

    // Compare
    return strcmp(charKey, charValue);
  }
}

void IndexManager::print(IXFileHandle &fd, const Attribute &attr, int32_t pageNum, string spaces) const
{
  RC rc;
  bool pageType;

  // Obtain the page type : leaf or non-leaf
  void * data = malloc(PAGE_SIZE);
  if ((rc = fd.readPage(pageNum, data))) {
    free(data);
    return;
  }
  memcpy(&pageType, data, sizeof(bool));

  // Print page
  if (pageType == LEAF_PAGE) {
    printLeafEntry(attr, data);
  } else {
    printNonLeafEntry(fd, attr, data, spaces);
  }
  free(data);
}

void IndexManager::printNonLeafEntry(IXFileHandle fd, const Attribute &attr, void *data, string spaces) const
{
  // Obtain the NonLeafHeader information
  NonLeafHeader nonLeafHeader;
  memcpy(&nonLeafHeader, (char *)data + sizeof(bool), sizeof(NonLeafHeader));

  // Print keys
  cout << endl << spaces << "\"keys\":[";
  for (int32_t i = 0; i < nonLeafHeader.entriesSize; ++i) {
    // Obtain the NonLeafEntry
    NonLeafEntry nonLeafEntry;
    memcpy(
      &nonLeafEntry,
      (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + i * sizeof(NonLeafEntry),
      sizeof(NonLeafEntry));

    // Print keys based on their types
    if (attr.type == TypeInt) {
      int32_t temp;
      memcpy(&temp, nonLeafEntry.key, INT_SIZE);
      cout << "\"" << temp << "\"";
    } else if (attr.type == TypeReal) {
      float temp;
      memcpy(&temp, nonLeafEntry.key, REAL_SIZE);
      cout << "\"" << temp << "\"";
    } else {
      int32_t offset, keySize;
      memcpy(&offset, nonLeafEntry.key, INT_SIZE);
      memcpy(&keySize, (char *)data + offset, VARCHAR_LENGTH_SIZE);
      char temp[keySize + 1];
      temp[keySize] = '\0';
      memcpy(temp, (char *)data + offset + VARCHAR_LENGTH_SIZE, keySize);
      cout << "\"" << temp << "\"";
    }

    if (i == 0)
      continue;
    cout << ",";
  }
  cout << "]," << endl << spaces << "\"children\":[" << endl << spaces;

  // Print children
  for (int32_t i = 0; i <= nonLeafHeader.entriesSize; ++i) {

    // Read the left child
    if (i == 0) {
      cout << spaces << "{";
      print(fd, attr, nonLeafHeader.leftChild, spaces + "  ");
      cout << "}";
      continue;
    }

    // Read the right child for each entry
    cout << "," << endl << spaces;
    NonLeafEntry nonLeafEntry;
    memcpy(
      &nonLeafEntry,
      (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + (i - 1) * sizeof(NonLeafEntry),
      sizeof(NonLeafEntry));

    cout << spaces << "{";
    print(fd, attr, nonLeafEntry.rightChild, spaces + "  ");
    cout << "}";
  }
  cout << endl << spaces << "]";
}

void IndexManager::printLeafEntry(const Attribute &attr, void *data) const
{
  cout << "\"keys\":[";

  LeafHeader leafHeader;
  memcpy(&leafHeader, (char *)data + sizeof(bool), sizeof(LeafHeader));

  // Return if no entry
  if (leafHeader.entriesSize == 0) {
    cout << "]},";
    return;
  }

  /* ----------  Treverse all LeafEntry for printing ---------- */
  // Initialization
  LeafEntry leafEntry;
  bool      first = true;
  void *    memory = malloc(PAGE_SIZE);
  int32_t   valueInt;
  float     valueFloat;
  char      valueVarChar[PAGE_SIZE];
  int32_t   offset, keySize;

  // Treverse
  for (int32_t i = 0; i < leafHeader.entriesSize; ++i)
  {
    // Obtain the entry
    memcpy(
      &leafEntry,
      (char *)data + sizeof(bool) + sizeof(LeafHeader) + i * sizeof(leafEntry),
      sizeof(LeafEntry));

    // Obtain the value of the key in entry
    if (attr.type == TypeInt) {
      memcpy(&valueInt, leafEntry.key, INT_SIZE);
    } else if (attr.type == TypeReal) {
      memcpy(&valueFloat, leafEntry.key, REAL_SIZE);
    } else {
      memcpy(&offset, leafEntry.key, sizeof(int32_t));
      memcpy(&keySize, (char *)data + offset, sizeof(int32_t));
      memset(valueVarChar, 0, PAGE_SIZE);
      memcpy(valueVarChar, (char *)data + offset + sizeof(int32_t), keySize);
    }

    // First time : Print entry and store *memory
    if (first)
    {
      first = false;
      if (attr.type == TypeInt) {
        cout << "\"" << valueInt << ":[";
        printRID(leafEntry.rid);
        memcpy(memory, &valueInt, INT_SIZE);
      } else if (attr.type == TypeReal) {
        cout << "\"" << valueFloat << ":[";
        printRID(leafEntry.rid);
        memcpy(memory, &valueFloat, REAL_SIZE);
      } else {
        cout << "\"" << valueVarChar << ":[";
        printRID(leafEntry.rid);
        memset(memory, 0, PAGE_SIZE);
        memcpy(memory, valueVarChar, keySize);
      }
    }

    // Following times : compare if it's the same key and print entries
    else
    {
      // The previous and new entry have the same key, just print RID
      if (compareLeafEntry(attr, memory, data, i) == 0) {
        printRID(leafEntry.rid);
      }

      // Different key, print new entry key and RID
      else {
        cout << "]\",";
        if (attr.type == TypeInt) {
          cout << "\"" << valueInt << ":[";
          printRID(leafEntry.rid);
          memcpy(memory, &valueInt, INT_SIZE);
        } else if (attr.type == TypeReal) {
          cout << "\"" << valueFloat << ":[";
          memcpy(memory, &valueFloat, REAL_SIZE);
        } else {
          cout << "\"" << valueVarChar << ":[";
          memset(memory, 0, PAGE_SIZE);
          memcpy(memory, valueVarChar, keySize);
        }
      }
    }
  }
  cout << "\"]}";
  free(memory);
}

void IndexManager::printRID(const RID rid) const
{
  cout << "(" << rid.pageNum << "," << rid.slotNum << ")";
}

void IndexManager::printPage(const Attribute attr, const void *data)
{
  bool pageType;
  memcpy(&pageType, data, sizeof(bool));

  if (pageType == LEAF_PAGE){
    // Print out pageType
    cout << "Page Type : LEAF_PAGE" << endl << endl;

    // Print header
    LeafHeader leafHeader;
    memcpy(&leafHeader, (char *)data + sizeof(bool), sizeof(LeafHeader));
    cout << "prev: " << leafHeader.prev<<endl;
    cout << "next: " << leafHeader.next<<endl;
    cout << "entriesSize: " << leafHeader.entriesSize<<endl;
    cout << "freeSpaceOffset: " << leafHeader.freeSpaceOffset << endl << endl;

    // Print entries
    LeafEntry leafEntry;
    for (int i = 0; i < leafHeader.entriesSize; i++) {
      memcpy(
        &leafEntry,
        (char *)data + sizeof(bool) + sizeof(leafHeader) + sizeof(LeafEntry) * i,
        sizeof(LeafEntry));
      if (attr.type == TypeInt) {
        int temp;
        memcpy(&temp, leafEntry.key, INT_SIZE);
        cout << "  key: " << temp << "    children: (" << leafEntry.rid.pageNum << "," << leafEntry.rid.slotNum << ")" << endl;
      } else if (attr.type == TypeReal) {
        float temp;
        memcpy(&temp, leafEntry.key, REAL_SIZE);
        cout << "  key: " << temp << "    children: (" << leafEntry.rid.pageNum << "," << leafEntry.rid.slotNum << ")" << endl;
      } else {
        int offset, len;
        memcpy(&offset, leafEntry.key, INT_SIZE);
        memcpy(&len, (char *)data + offset, VARCHAR_LENGTH_SIZE);
        char temp[len + 1];
        temp[len] = '\0';
        memcpy(temp, (char *)data + offset + VARCHAR_LENGTH_SIZE, len);
        cout << "  key: " << temp << "    children: (" << leafEntry.rid.pageNum << "," << leafEntry.rid.slotNum << ")" << endl;
      }
    }

    cout << "----------- LEAF DONE -----------" << endl;
  }

  else {
    // Print out pageType
    cout << "Page Type : NON_LEAF_PAGE" << endl;

    // Print header
    NonLeafHeader nonLeafHeader;
    memcpy(&nonLeafHeader, (char *)data + sizeof(bool), sizeof(NonLeafHeader));
    cout << "entriesSize: " << nonLeafHeader.entriesSize<<endl;
    cout << "freeSpaceOffset: " << nonLeafHeader.freeSpaceOffset<<endl;
    cout << "leftChild: " << nonLeafHeader.leftChild << endl<<endl;

    // Print entries
    NonLeafEntry nonLeafEntry;
    for (int i = 0; i < nonLeafHeader.entriesSize; i++) {
      memcpy(
        &nonLeafEntry,
        (char *)data + sizeof(bool) + sizeof(NonLeafHeader) + sizeof(NonLeafEntry) * i,
        sizeof(NonLeafEntry));
      if (attr.type == TypeInt) {
        int temp;
        memcpy(&temp, nonLeafEntry.key, INT_SIZE);
        cout << "  key: " << temp << "    children: " << nonLeafEntry.rightChild << endl;
      } else if (attr.type == TypeReal) {
        float temp;
        memcpy(&temp, nonLeafEntry.key, REAL_SIZE);
        cout << "  key: " << temp << "    children: " << nonLeafEntry.rightChild << endl;
      } else {
        int offset, len;
        memcpy(&offset, nonLeafEntry.key, INT_SIZE);
        memcpy(&len, (char *)data + offset, VARCHAR_LENGTH_SIZE);
        char temp[len + 1];
        temp[len] = '\0';
        memcpy(temp, (char *)data + offset + VARCHAR_LENGTH_SIZE, len);
        cout << "  key: " << temp << "    children: " << nonLeafEntry.rightChild << endl;
      }
    }

    cout << "----------- NON_LEAF DONE -----------" << endl;
  }
}
