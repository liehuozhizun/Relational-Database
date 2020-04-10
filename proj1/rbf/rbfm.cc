#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager* RecordBasedFileManager::_pf_manager = 0;

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
    return _pf_manager->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
  void * pageData = malloc(PAGE_SIZE);
  void * record = malloc(PAGE_SIZE);
  memset((char *)pageData, 0, PAGE_SIZE);
  memset((char *)record, 0, PAGE_SIZE);
  unsigned freespace_offset = 0; // offset to the free space
  unsigned slot_total_Number = 0; // number of slot
  unsigned record_length = 0; // length of record stored in disk
  unsigned latest_page_number = 0;
  // get the latest recent page.
  if ((latest_page_number = fileHandle.getNumberOfPages()) == 0) {
    memcpy((char *)pageData + PAGE_SIZE - 1 - sizeof(unsigned), &freespace_offset, sizeof(unsigned)); // freespace_offset
    memcpy((char *)pageData + PAGE_SIZE - 1 - 2 * sizeof(unsigned), &slot_total_Number, sizeof(unsigned)); // slot_total_Number
    fileHandle.appendPage(pageData);
    memset((char *)pageData, 0, PAGE_SIZE);
  } else {
    latest_page_number -= 1;
  }

  if (fileHandle.readPage(latest_page_number, pageData) != 0) {
    return -1;
  }
  // get offset to the start of free space of the lastest recent page.
  memcpy(&freespace_offset, (char *)pageData + ((PAGE_SIZE) - sizeof(unsigned)), sizeof(unsigned));

  // get number of slots of the lastest recent page.
  memcpy(&slot_total_Number, (char *)pageData + ((PAGE_SIZE) - 2 * sizeof(unsigned)), sizeof(unsigned));

  // get null field indicator.
  bool nullBit = false;
  int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
  unsigned char * nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize + 1);
  memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize + 1);
  memcpy(nullFieldsIndicator, (char *)data, nullFieldsIndicatorActualSize);
  // get not null field number.
  unsigned valid_field_sz = 0;
  int maskbit = 7;
  for (unsigned i = 0; i < recordDescriptor.size(); ++i) {
    // Is this field not-NULL?
    if (maskbit == -1) maskbit = 7;
    nullBit = nullFieldsIndicator[(int)ceil(((double)i + 1) / CHAR_BIT) - 1] & (1 << maskbit);
    maskbit -= 1;
    if (!nullBit) {
      valid_field_sz++;
    }
  }
  unsigned index_offset = 0; // offset of data field index
  index_offset += sizeof(unsigned); // index_offset + sizeof(valid_field_sz)
  index_offset += nullFieldsIndicatorActualSize; // index_offset + nullFieldsIndicatorActualSize
  record_length += index_offset + valid_field_sz * sizeof(unsigned);
  memcpy((char *)record, &valid_field_sz, sizeof(unsigned));
  memcpy((char *)record + sizeof(unsigned), nullFieldsIndicator, nullFieldsIndicatorActualSize);
  // store data field index info to record (which is stored in disk.);
  unsigned offset = nullFieldsIndicatorActualSize;
  maskbit = 7;
  for (unsigned i = 0; i < recordDescriptor.size(); ++i) {
    // Is this field not-NULL?
    if (maskbit == -1) maskbit = 7;
    nullBit = nullFieldsIndicator[(int)ceil(((double)i + 1) / CHAR_BIT) - 1] & (1 << maskbit);
    maskbit -= 1;
    switch (recordDescriptor[i].type) {
      case TypeInt:
        if (!nullBit) {
          offset += sizeof(int);
          record_length += sizeof(int);
          memcpy((char *)record + index_offset, &record_length, sizeof(unsigned));
          index_offset += sizeof(unsigned);
        }
        break;
      case TypeReal:
        if (!nullBit) {
          offset += sizeof(float);
          record_length += sizeof(float);
          memcpy((char *)record + index_offset, &record_length, sizeof(unsigned));
          index_offset += sizeof(unsigned);
        }
        break;
      case TypeVarChar:
        if (!nullBit) {
          unsigned varchar_sz = 0;
          memcpy(&varchar_sz, (char *)data + offset, sizeof(unsigned));
          offset += varchar_sz + sizeof(unsigned);
          record_length = record_length + varchar_sz + sizeof(unsigned);
          memcpy((char *)record + index_offset, &record_length, sizeof(unsigned));
          index_offset += sizeof(unsigned);
        }
        break;
    }
  }
  memcpy((char *)record + index_offset, (char *)data + nullFieldsIndicatorActualSize, record_length - index_offset);
  // store record to disk.
  // If free space is not enough, append a new page
  // and store the record to the new page.
  if ((freespace_offset + (slot_total_Number + 1) * (2 * sizeof(unsigned)) + 2 * sizeof(unsigned))
      + record_length > PAGE_SIZE) {
    slot_total_Number = 1;
    unsigned record_offset = 0;
    memcpy((char *)pageData, (char *)record, record_length); // write record to page memory
    memcpy((char *)pageData + PAGE_SIZE - sizeof(unsigned), &record_length, sizeof(unsigned)); // freespace_offset
    memcpy((char *)pageData + PAGE_SIZE - 2 * sizeof(unsigned), &slot_total_Number, sizeof(unsigned)); // slot_total_Number
    memcpy((char *)pageData + PAGE_SIZE - 3 * sizeof(unsigned), &record_length, sizeof(unsigned)); // slot length
    memcpy((char *)pageData + PAGE_SIZE - 4 * sizeof(unsigned), &record_offset, sizeof(unsigned)); // slot offset
    fileHandle.appendPage(pageData);
  } else {
    memcpy((char *)pageData + freespace_offset, (char *)record, record_length); // write record to page memory
    slot_total_Number += 1;
    memcpy((char *)pageData + PAGE_SIZE - 2 * sizeof(unsigned) - (slot_total_Number * 2 * sizeof(unsigned)) + sizeof(unsigned), &record_length, sizeof(unsigned)); // slot length
    memcpy((char *)pageData + PAGE_SIZE - 2 * sizeof(unsigned) - (slot_total_Number * 2 * sizeof(unsigned)), &freespace_offset, sizeof(unsigned)); // slot offset
    freespace_offset += record_length;
    memcpy((char *)pageData + PAGE_SIZE - sizeof(unsigned), &freespace_offset, sizeof(unsigned)); // freespace_offset
    memcpy((char *)pageData + PAGE_SIZE - 2 * sizeof(unsigned), &slot_total_Number, sizeof(unsigned)); // slot_total_Number
    fileHandle.writePage(latest_page_number, pageData);
  }
  // update rid
  rid.pageNum = fileHandle.getNumberOfPages() - 1;
  rid.slotNum = slot_total_Number;

  free(nullFieldsIndicator);
  free(pageData);
  free(record);
  return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
  void * pageData = malloc(PAGE_SIZE);
  memset(pageData, 0, PAGE_SIZE);
  int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
  fileHandle.readPage(rid.pageNum, pageData);
  unsigned valid_record_number = 0;
  unsigned record_offset = 0;
  unsigned record_length = 0;
  memcpy(&record_offset, (char *)pageData + PAGE_SIZE - 8 - (rid.slotNum * 8), sizeof(unsigned)); // record_offset
  memcpy(&record_length, (char *)pageData + PAGE_SIZE - 8 - (rid.slotNum * 8) + 4, sizeof(unsigned)); // record_length
  void * record = malloc(record_length);
  memset(record, 0, record_length);
  memcpy((char *)record, (char *)pageData + record_offset, record_length); // record data
  memcpy(&valid_record_number, (char *)record, sizeof(unsigned));
  memcpy((char *)data, (char *)record + sizeof(unsigned), nullFieldsIndicatorActualSize);
  unsigned header_length = sizeof(unsigned) + nullFieldsIndicatorActualSize + (valid_record_number * sizeof(unsigned));
  unsigned data_length = record_length - header_length;
  memcpy((char *)data + nullFieldsIndicatorActualSize, (char *)record + header_length, data_length);
  free(pageData);
  free(record);
  return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
  unsigned offset = 0;
  // get null filed indicator.
  bool nullBit = false;
  int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
  char * nullFieldsIndicator = (char *) malloc(nullFieldsIndicatorActualSize + 1);
  memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize + 1);
  memcpy(nullFieldsIndicator, (char *)data, nullFieldsIndicatorActualSize);
  offset += nullFieldsIndicatorActualSize;

  // print actual data.
  int maskbit = 7;
  for (unsigned i = 0; i < recordDescriptor.size(); ++i) {
    // Is this field not-NULL?
    if (maskbit == -1) maskbit = 7;
    nullBit = nullFieldsIndicator[(int)ceil(((double)i + 1) / CHAR_BIT) - 1] & (1 << maskbit);
    maskbit -= 1;
    switch (recordDescriptor[i].type) {
      case TypeInt:
        if (!nullBit) {
          int buffer_int = 0;
          memcpy(&buffer_int, (char *)data + offset, sizeof(int));
          offset += sizeof(int);
          cout << recordDescriptor[i].name << ": " << buffer_int << "      ";
        } else {
          cout << recordDescriptor[i].name << ": NULL    ";
        }
        break;
      case TypeReal:
        if (!nullBit) {
          float buffer_float = 0.0;
          memcpy(&buffer_float, (char *)data + offset, sizeof(float));
          offset += sizeof(float);
          cout << recordDescriptor[i].name << ": " << buffer_float << "      ";
        } else {
          cout << recordDescriptor[i].name << ": NULL    ";
        }
        break;
      case TypeVarChar:
        if (!nullBit) {
          unsigned varchar_sz = 0;
          memcpy(&varchar_sz, (char *)data + offset, sizeof(int));
          offset += sizeof(int);
          char *buffer = (char *) malloc(varchar_sz + 1);
          memset(buffer, 0, varchar_sz + 1);
          memcpy(buffer, (char *)data + offset, varchar_sz);
          offset += varchar_sz;
          cout << recordDescriptor[i].name << ": " << buffer << "      ";
          free(buffer);
        } else {
          cout << recordDescriptor[i].name << ": NULL    ";
        }
        break;
    }
  }
  cout << endl;
  free(nullFieldsIndicator);
    return 0;
}
