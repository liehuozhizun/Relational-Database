#include "rbfm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);
    // Adds the first record based page.

    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with en。ough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting up the return RID.
    rid.pageNum = i;
    unsigned j;
    SlotDirectoryRecordEntry recordEntry;
    for (j = 0; j <= slotHeader.recordEntriesNumber; ++j) {
      recordEntry = getSlotDirectoryRecordEntry(pageData, j);
      if (recordEntry.length == 0 && recordEntry.offset == 0) {
        rid.slotNum = j;
      }
    }

    // rid.slotNum = slotHeader.recordEntriesNumber;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    if (rid.slotNum == slotHeader.recordEntriesNumber)
      slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    // delete empty recordEntry -> error
    if (recordEntry.length == 0 && recordEntry.offset == 0) {
      return RBFM_READ_FAILED;
    }

    if (recordEntry.offset < 0) {
      RID _ridTemp;
      _ridTemp.pageNum = recordEntry.length;
      _ridTemp.slotNum = (unsigned)(recordEntry.offset * (-1));
      if (SUCCESS != readRecord(fileHandle, recordDescriptor, _ridTemp, data)) {
        return RBFM_READ_FAILED;
      }
    } else {
      // Retrieve the actual entry data
      getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // Parse the null indicator and save it into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);

    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
  // retrieve the specified page
  void * pageData = malloc(PAGE_SIZE);
  memset(pageData, 0, PAGE_SIZE);
  if (fileHandle.readPage(rid.pageNum, pageData)) {
    return RBFM_READ_FAILED;
  }

  // read the SlotDirectoryRecordEntry to detect forwarding
  SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
  int32_t offset = recordEntry.offset;
  uint32_t length = recordEntry.length;

  // delete empty recordEntry -> error
  if (offset == 0 && length == 0) {
    return RBFM_DELETE_FAILED;
  }

  // detect forwarding record
  if (offset < 0) { // forwarding
    RID new_rid;
    new_rid.pageNum = length;
    new_rid.slotNum = (unsigned)(offset * (-1));

    // go to the forwarding page to delete the record
    if (SUCCESS != deleteRecord(fileHandle, recordDescriptor, new_rid)) {
      return RBFM_DELETE_FAILED;
    }
  } else { // real record entry
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    removeData(pageData, slotHeader, recordEntry, rid.slotNum);
  }

  // reset the slot information for next reuse
  memset(&recordEntry, 0, sizeof(SlotDirectoryRecordEntry));
  setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);

  if (fileHandle.writePage(rid.pageNum, pageData))
      return RBFM_WRITE_FAILED;

  free(pageData);
  return SUCCESS;
}

// Assume the RID does not change after an update
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
  if (SUCCESS != deleteRecord(fileHandle, recordDescriptor, rid)) {
    return RBFM_DELETE_FAILED;
  }

  if(SUCCESS != forwardingRecord(fileHandle, recordDescriptor, rid, data)){
    return RBFM_UPDATE_FAILED;
  }

  return SUCCESS;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
  RC rc;

  // Retrieve the specified record
  void *record = malloc(PAGE_SIZE);
  memset(record, 0, PAGE_SIZE);
  if ((rc= readRecord(fileHandle, recordDescriptor, rid, record)))
    return rc;

  // Obtain the nullIndicator
  int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
  char nullIndicator[nullIndicatorSize];
  memset(nullIndicator, 0, nullIndicatorSize);
  memcpy (nullIndicator, (char *)record, nullIndicatorSize);

  // Locate the position of the attribute
  unsigned i;
  unsigned offset = (unsigned)nullIndicatorSize;
  for (i = 0; i < recordDescriptor.size(); ++i) {
    if (attributeName.compare(recordDescriptor[i].name) == 0)
      break;
  }

  // Retrieve the real data
  if (!fieldIsNull(nullIndicator, i)) { // field contains data
    switch (recordDescriptor[i].type)
    {
      case TypeInt:
        memcpy((uint32_t *)data, ((char*) record + offset), INT_SIZE);
        break;
      case TypeReal:
        memcpy((float *)data, ((char*) record), REAL_SIZE);
        break;
      case TypeVarChar:
        // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
        uint32_t varcharSize;
        memcpy(&varcharSize, ((char*) record), VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;

        // Gets the actual string.
        memcpy((char *)data, ((char*) record + offset), varcharSize);
        break;
    }
  }
  return SUCCESS;
}

// Scan returns an iterator to allow the caller to go through the results one by one.
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator){
  RC rc;
  // Initialize scanitor
  rbfm_ScanIterator._rbf_manager = RecordBasedFileManager::instance();
  rbfm_ScanIterator.currPage_nr = 0;
  rbfm_ScanIterator.currSlot_nr = 0;
  rbfm_ScanIterator.totalPage_nr = fileHandle.getNumberOfPages();
  rbfm_ScanIterator.totalSlot_nr = 0;

  rbfm_ScanIterator.currPageData = malloc(PAGE_SIZE);
  memset(rbfm_ScanIterator.currPageData, 0, PAGE_SIZE);

  rbfm_ScanIterator.fileHandle = fileHandle;
  rbfm_ScanIterator.recordDescriptor = recordDescriptor;
  rbfm_ScanIterator.conditionAttribute = conditionAttribute;
  rbfm_ScanIterator.compOp = compOp;
  rbfm_ScanIterator.value = malloc(PAGE_SIZE);
  memset(rbfm_ScanIterator.value, 0, PAGE_SIZE);
  memcpy(rbfm_ScanIterator.value, value, PAGE_SIZE);
  rbfm_ScanIterator.attributeNames = attributeNames;

  // check total page number and get first page.
  if (rbfm_ScanIterator.totalPage_nr > 0) {
    if((rc = fileHandle.readPage(0, rbfm_ScanIterator.currPageData))) { return rc; }
  } else {
    return SUCCESS;
  }

  // check total slot number of first page.
  SlotDirectoryHeader directory = _rbf_manager->getSlotDirectoryHeader(rbfm_ScanIterator.currPageData);
  rbfm_ScanIterator.totalSlot_nr = directory.recordEntriesNumber;
  return SUCCESS;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
  RC rc;
  void *buffer;
  if ((rc = getNextSlot())){
    return rc;
  }

  // update rid
  rid.pageNum = currPage_nr;
  rid.slotNum = currSlot_nr;

  // update data
  unsigned nullIndicatorSize = _rbf_manager->getNullIndicatorSize(attributeNames.size());
  char nullIndicator[nullIndicatorSize];
  memset(nullIndicator, 0, nullIndicatorSize);
  unsigned data_offset = nullIndicatorSize;
  buffer = malloc(PAGE_SIZE);
  memset(buffer, 0, PAGE_SIZE);

  for (unsigned i = 0; i < attributeNames.size(); ++i) {
    AttrType currType;
    for (unsigned j = 0; j < recordDescriptor.size(); ++j) {
      if (attributeNames[i].compare(recordDescriptor[j].name) == 0)
        currType = recordDescriptor[i].type;
    }
    _rbf_manager->readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute, buffer);
    if (buffer == NULL) {
      int indicatorIndex = i / CHAR_BIT;
      char indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
      nullIndicator[indicatorIndex] |= indicatorMask;
    }
    else if (currType == TypeInt) {
      memcpy((char *)data + data_offset, buffer, INT_SIZE);
      data_offset += INT_SIZE;
    }
    else if (currType == TypeReal) {
      memcpy((char *)data + data_offset, buffer, REAL_SIZE);
      data_offset += REAL_SIZE;
    }
    else if (currType == TypeVarChar) {
      unsigned buffer_sz;
      memcpy(&buffer_sz, buffer, VARCHAR_LENGTH_SIZE);
      memcpy((char *)data + data_offset, &buffer_sz, VARCHAR_LENGTH_SIZE);
      data_offset += VARCHAR_LENGTH_SIZE;
      memcpy((char *)data + data_offset, (char *)buffer + VARCHAR_LENGTH_SIZE, buffer_sz);
      data_offset += buffer_sz;
    }
    // copy null indicator to data.
    memcpy((char *)data, nullIndicator, nullIndicatorSize);
  }
  free(buffer);
  currSlot_nr++;
  return SUCCESS;
}

RC RBFM_ScanIterator::close(){
  free(currPageData);
  free(value);
  return SUCCESS;
}

RC RBFM_ScanIterator::getNextSlot(){
  RC rc;
  // slot over, change to next page.
  if (currSlot_nr >= totalSlot_nr || currPage_nr >= totalPage_nr) {
    currSlot_nr = 0;
    if ((++currPage_nr) >= totalPage_nr) { return RBFM_EOF; }
    // change current page data & change totalSlot_nr.
    memset(currPageData, 0, PAGE_SIZE);
    if((rc = fileHandle.readPage(currPage_nr, currPageData))) { return rc; }
    SlotDirectoryHeader directory = _rbf_manager->getSlotDirectoryHeader(currPageData);
    totalSlot_nr = directory.recordEntriesNumber;
  }

    // change to next valid slot.
    SlotDirectoryRecordEntry recordEntry = _rbf_manager->getSlotDirectoryRecordEntry(currPageData, currSlot_nr);
    // if this slot is deleted or moved, ignore it.
    if ((recordEntry.length == 0 && recordEntry.offset == 0)||(recordEntry.offset < 0)||!checkScanCondition()){
      currSlot_nr++;
      return getNextSlot();
    }
  return SUCCESS;
}

bool RBFM_ScanIterator::checkScanCondition(){
  // check the conditionAttribute with Value by Operator.
  void *buffer;
  bool validSlot = false;
  if (compOp != NO_OP) {
    if (value == NULL) { return false; }
    else {
      buffer = malloc(PAGE_SIZE);
      memset(buffer, 0, PAGE_SIZE);
      RID currid;
      currid.pageNum = currPage_nr;
      currid.slotNum = currSlot_nr;
      // read the data which is same as the conditionAttribute.
      _rbf_manager->readAttribute(fileHandle, recordDescriptor, currid, conditionAttribute, buffer);
      // cout << "after---currid" << currid.pageNum << " + " << currid.slotNum << " + " << conditionAttribute <<endl;
      // find type of conditionAttribute
      AttrType currType;
      for (unsigned i = 0; i < recordDescriptor.size(); ++i) {
        if (conditionAttribute.compare(recordDescriptor[i].name) == 0)
          currType = recordDescriptor[i].type;
      }
      if (buffer == NULL) { validSlot = false; }
      else if (currType == TypeInt) {
        int data_value;
        int value_value;
        memcpy(&data_value, buffer, 4);
        memcpy(&value_value, value, 4);
        switch (compOp) {
          case EQ_OP: return data_value == value_value;
          case LT_OP: return data_value < value_value;
          case GT_OP: return data_value > value_value;
          case LE_OP: return data_value <= value_value;
          case GE_OP: return data_value >= value_value;
          case NE_OP: return data_value != value_value;
          case NO_OP: break; //this should be valid.
          default: break; // will not go here.
        }
      }
      else if (currType == TypeReal) {
        float data_value;
        float value_value;
        memcpy(&data_value, buffer, 4);
        memcpy(&value_value, value, 4);
        switch (compOp) {
          case EQ_OP: return data_value == value_value;
          case LT_OP: return data_value < value_value;
          case GT_OP: return data_value > value_value;
          case LE_OP: return data_value <= value_value;
          case GE_OP: return data_value >= value_value;
          case NE_OP: return data_value != value_value;
          case NO_OP: break; //this should be valid.
          default: break; // will not go here.
        }
      }
      else if (currType == TypeVarChar) {
        int data_sz;
        memcpy(&data_sz, buffer, VARCHAR_LENGTH_SIZE);
        char *data_value = (char *)malloc(PAGE_SIZE);
        memset(data_value, 0, PAGE_SIZE);
        memcpy(data_value, (char *)buffer + VARCHAR_LENGTH_SIZE, data_sz);
        // printf("%s=%s\n", data_value,value);
        int cmp = strcmp(data_value, (char *)value);
        switch (compOp) {
          case EQ_OP: return cmp == 0;
          case LT_OP: return cmp < 0;
          case GT_OP: return cmp > 0;
          case LE_OP: return cmp <= 0;
          case GE_OP: return cmp >= 0;
          case NE_OP: return cmp != 0;
          case NO_OP: break; //this should be valid.
          default: break; // will not go here.
        }
      }
      free(buffer);
    }
  return validSlot;
  } else {
    return true;
  }
}

// recursivly insert data into the valid data record postion
RC RecordBasedFileManager::forwardingRecord(FileHandle &fileHandle, const vector<Attribute> & recordDescriptor, const RID &rid, const void * data)
{
  // Gets the size of the record.
  unsigned recordSize = getRecordSize(recordDescriptor, data);

  void *pageData = malloc(PAGE_SIZE);
  if (pageData == NULL)
      return RBFM_MALLOC_FAILED;
  if (fileHandle.readPage(rid.pageNum, pageData)) {
    return RBFM_READ_FAILED;
  }

  RID _ridTemp;
  // current page doesn't have enough space
  if (getPageFreeSpaceSize(pageData) < sizeof(SlotDirectoryRecordEntry) + recordSize) {
    if (insertRecord(fileHandle, recordDescriptor, data, _ridTemp)) {
      return RBFM_INSERT_FAILED;
    }

    // update forwarding info to current recordEntry
    SlotDirectoryRecordEntry recordEntry;
    recordEntry.length = _ridTemp.pageNum;
    recordEntry.offset = (int32_t)(_ridTemp.slotNum * (-1));
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);

  } else { // update the record to its original page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);
  }

  // Writing the page to disk.
  if (fileHandle.writePage(rid.pageNum, pageData))
      return RBFM_WRITE_FAILED;

  free(pageData);
  return SUCCESS;
}

void RecordBasedFileManager::removeData(void * page, SlotDirectoryHeader slotHeader, SlotDirectoryRecordEntry recordEntry, unsigned slotNum)
{
  // retrieve the specified header and recordHeader info
  int32_t offset = recordEntry.offset;
  uint32_t length = recordEntry.length;

  uint16_t freeSpaceOffset = slotHeader.freeSpaceOffset;
  uint16_t recordEntriesNumber = slotHeader.recordEntriesNumber;

  // remove the specified record data (move the ahead data backward to compress space)
  memcpy (
         (char *)page + freeSpaceOffset + length,
         (char *)page + freeSpaceOffset,
         offset - freeSpaceOffset
         );
  memset (
         (char *)page + freeSpaceOffset,
         0,
         length
         );

  // update all following recordEntry
  for (int32_t recordEntryNumber = slotNum + 1; recordEntryNumber <= recordEntriesNumber; ++recordEntryNumber) {
    recordEntry = getSlotDirectoryRecordEntry(page, recordEntryNumber);

    // skip empty recordEntry
    if (recordEntry.offset == 0 && recordEntry.length == 0) {
      continue;
    }

    recordEntry.offset += length;
    setSlotDirectoryRecordEntry(page, recordEntryNumber, recordEntry);
  }

  // update the freeSpaceOffset
  slotHeader.freeSpaceOffset = freeSpaceOffset + length;
  setSlotDirectoryHeader(page, slotHeader);
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
	memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after the null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize it to the size of the header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for null-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount)
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page)
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator.
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;

    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;

        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}
