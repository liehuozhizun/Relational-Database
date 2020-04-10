
#include "rm.h"

#include <algorithm>
#include <cstring>
#include <math.h>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
: tableDescriptor(createTableDescriptor()), columnDescriptor(createColumnDescriptor()), indexDescriptor(createIndexDescriptor())
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Create both tables and columns tables, return error if either fails
    RC rc;
    rc = rbfm->createFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;
    rc = rbfm->createFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;
    rc = rbfm->createFile(getFileName(INDEXES_TABLE_NAME));
    if (rc)
      return rc;

    // Add table entries for both Tables and Columns
    rc = insertTable(TABLES_TABLE_ID, 1, TABLES_TABLE_NAME);
    if (rc)
        return rc;
    rc = insertTable(COLUMNS_TABLE_ID, 1, COLUMNS_TABLE_NAME);
    if (rc)
        return rc;
    rc = insertTable(INDEXES_TABLE_ID, 1, INDEXES_TABLE_NAME);
    if (rc)
      return rc;


    // Add entries for tables and columns to Columns table
    rc = insertColumns(TABLES_TABLE_ID, tableDescriptor);
    if (rc)
        return rc;
    rc = insertColumns(COLUMNS_TABLE_ID, columnDescriptor);
    if (rc)
        return rc;
    rc = insertColumns(INDEXES_TABLE_ID, indexDescriptor);
    if (rc)
      return rc;


    return SUCCESS;
}

// Just delete the the two catalog files
RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    RC rc;

    rc = rbfm->destroyFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;

    rc = rbfm->destroyFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;

    rc = rbfm->destroyFile(getFileName(INDEXES_TABLE_NAME));
    if (rc)
      return rc;

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    // Create the rbfm file to store the table
    if ((rc = rbfm->createFile(getFileName(tableName))))
        return rc;

    // Get the table's ID
    int32_t id;
    rc = getNextTableID(id);
    if (rc)
        return rc;

    // Insert the table into the Tables table (0 means this is not a system table)
    rc = insertTable(id, 0, tableName);
    if (rc)
        return rc;

    // Insert the table's columns into the Columns table
    rc = insertColumns(id, attrs);
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot delete it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Delete the rbfm file holding this table's entries
    rc = rbfm->destroyFile(getFileName(tableName));
    if (rc)
        return rc;

    // Grab the table ID
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    // Open tables file
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find entry with same table ID
    // Use empty projection because we only care about RID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection; // Empty
    void *value = &id;

    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    RID rid;
    rc = rbfm_si.getNextRecord(rid, NULL);
    if (rc)
        return rc;

    // Delete RID from table and close file
    rbfm->deleteRecord(fileHandle, tableDescriptor, rid);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    // Delete from Columns table
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find all of the entries whose table-id equal this table's ID
    rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    while((rc = rbfm_si.getNextRecord(rid, NULL)) == SUCCESS)
    {
        // Delete each result with the returned RID
        rc = rbfm->deleteRecord(fileHandle, columnDescriptor, rid);
        if (rc)
            return rc;
    }
    if (rc != RBFM_EOF)
        return rc;

    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    return SUCCESS;
}

// Fills the given attribute vector with the recordDescriptor of tableName
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Clear out any old values
    attrs.clear();
    RC rc;

    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    void *value = &id;

    // We need to get the three values that make up an Attribute: name, type, length
    // We also need the position of each attribute in the row
    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back(COLUMNS_COL_COLUMN_NAME);
    projection.push_back(COLUMNS_COL_COLUMN_TYPE);
    projection.push_back(COLUMNS_COL_COLUMN_LENGTH);
    projection.push_back(COLUMNS_COL_COLUMN_POSITION);

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Scan through the Column table for all entries whose table-id equals tableName's table id.
    rc = rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
    if (rc)
        return rc;

    RID rid;
    void *data = malloc(COLUMNS_RECORD_DATA_SIZE);

    // IndexedAttr is an attr with a position. The position will be used to sort the vector
    vector<IndexedAttr> iattrs;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // For each entry, create an IndexedAttr, and fill it with the 4 results
        IndexedAttr attr;
        unsigned offset = 0;

        // For the Columns table, there should never be a null column
        char null;
        memcpy(&null, data, 1);
        if (null)
            rc = RM_NULL_COLUMN;

        // Read in name
        offset = 1;
        int32_t nameLen;
        memcpy(&nameLen, (char*) data + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        char name[nameLen + 1];
        name[nameLen] = '\0';
        memcpy(name, (char*) data + offset, nameLen);
        offset += nameLen;
        attr.attr.name = string(name);

        // read in type
        int32_t type;
        memcpy(&type, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.type = (AttrType)type;

        // Read in length
        int32_t length;
        memcpy(&length, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.length = length;

        // Read in position
        int32_t pos;
        memcpy(&pos, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.pos = pos;

        iattrs.push_back(attr);
    }
    // Do cleanup
    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    free(data);
    // If we ended on an error, return that error
    if (rc != RBFM_EOF)
        return rc;

    // Sort attributes by position ascending
    auto comp = [](IndexedAttr first, IndexedAttr second)
        {return first.pos < second.pos;};
    sort(iattrs.begin(), iattrs.end(), comp);

    // Fill up our result with the Attributes in sorted order
    for (auto attr : iattrs)
    {
        attrs.push_back(attr.attr);
    }

    return SUCCESS;
}

RC RelationManager::getIndexAttributes(const string &tableName, vector<IndexedAttr> &iattrs)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  // Clear out any old values
  iattrs.clear();
  RC rc;

  int32_t id;
  rc = getTableID(tableName, id);
  if (rc)
      return rc;

  void *value = &id;

  // We need to get the one values to indicate which value is the index in index file.
  // We also need the position of each attribute in the row
  RBFM_ScanIterator rbfm_si;
  vector<string> projection;
  projection.push_back(INDEXES_COL_COLUMN_NAME);
  projection.push_back(INDEXES_COL_COLUMN_TYPE);
  projection.push_back(INDEXES_COL_COLUMN_LENGTH);
  projection.push_back(INDEXES_COL_COLUMN_POSITION);

  FileHandle fileHandle;
  rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fileHandle);
  if (rc)
      return rc;

  // Scan through the Index table for all entries whose table-id equals tableName's table id.
  rc = rbfm->scan(fileHandle, indexDescriptor, INDEXES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
  if (rc)
      return rc;

  RID rid;
  void *data = malloc(INDEXES_COL_COLUMN_NAME_SIZE);

  // IndexedAttr is an attr with a position. The position will be used to sort the vector
  while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
  {
      // For each entry, create an IndexedAttr, and fill it with the columnName's result
      IndexedAttr attr;
      unsigned offset = 0;

      // For the Columns table, there should never be a null column
      offset = 1;

      // Read in name
      int32_t nameLen;
      memcpy(&nameLen, (char*) data + offset, VARCHAR_LENGTH_SIZE);
      offset += VARCHAR_LENGTH_SIZE;
      char name[nameLen + 1];
      name[nameLen] = '\0';
      memcpy(name, (char*) data + offset, nameLen);
      offset += nameLen;
      attr.attr.name = string(name);

      // read in type
      int32_t type;
      memcpy(&type, (char*) data + offset, INT_SIZE);
      offset += INT_SIZE;
      attr.attr.type = (AttrType)type;

      // Read in length
      int32_t length;
      memcpy(&length, (char*) data + offset, INT_SIZE);
      offset += INT_SIZE;
      attr.attr.length = length;

      // Read in position
      int32_t pos;
      memcpy(&pos, (char*) data + offset, INT_SIZE);
      offset += INT_SIZE;
      attr.pos = pos;

      iattrs.push_back(attr);
  }
  // Do cleanup
  rbfm_si.close();
  rbfm->closeFile(fileHandle);
  free(data);

  return rc;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);
    if (rc)
      return rc;

    // insert this tuple into all index Manager file
    rc = insertIndexTuple(tableName, recordDescriptor, data, rid);

    return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    void* data = malloc(PAGE_SIZE);
    rc = readTuple(tableName, rid, data);
    if (rc)
      return rc;

    rc = deleteIndexTuple(tableName, recordDescriptor, data, rid);
    free(data);
    if (rc)
      return rc;

    // Let rbfm do all the work
    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
    rbfm->closeFile(fileHandle);

    return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // handle index files.
    void* oldData = malloc(PAGE_SIZE);
    rc = readTuple(tableName, rid, oldData);
    if (rc)
      return rc;

    rc = deleteIndexTuple(tableName, recordDescriptor, oldData, rid);
    free(oldData);
    if (rc)
      return rc;

    rc = insertIndexTuple(tableName, recordDescriptor, data, rid);
    if (rc)
      return rc;

    // Let rbfm do all the work
    rc = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);

    return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // Get record descriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

// Let rbfm do all the work
RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

string RelationManager::getFileName(const char *tableName)
{
    return string(tableName) + string(TABLE_FILE_EXTENSION);
}

string RelationManager::getFileName(const string &tableName)
{
    return tableName + string(TABLE_FILE_EXTENSION);
}

string RelationManager::getIndexFileName(const string &tableName, const string &indexName)
{
	return tableName + "." + indexName + string(INDEX_FILE_EXTENSION);
}

vector<Attribute> RelationManager::createTableDescriptor()
{
    vector<Attribute> td;

    Attribute attr;
    attr.name = TABLES_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_TABLE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_TABLE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_FILE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_FILE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_SYSTEM;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    return td;
}

vector<Attribute> RelationManager::createColumnDescriptor()
{
    vector<Attribute> cd;

    Attribute attr;
    attr.name = COLUMNS_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)COLUMNS_COL_COLUMN_NAME_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_TYPE;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_LENGTH;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_POSITION;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    return cd;
}

vector<Attribute> RelationManager::createIndexDescriptor()
{
	vector<Attribute> id;

	Attribute attr;
	attr.name = INDEXES_COL_TABLE_ID;
	attr.type = TypeInt;
	attr.length = (AttrLength)INT_SIZE;
	id.push_back(attr);

	attr.name = INDEXES_COL_COLUMN_NAME;
	attr.type = TypeVarChar;
	attr.length = (AttrLength)INDEXES_COL_COLUMN_NAME_SIZE;
	id.push_back(attr);

  attr.name = INDEXES_COL_COLUMN_TYPE;
  attr.type = TypeInt;
  attr.length = (AttrLength)INT_SIZE;
  id.push_back(attr);

  attr.name = INDEXES_COL_COLUMN_LENGTH;
  attr.type = TypeInt;
  attr.length = (AttrLength)INT_SIZE;
  id.push_back(attr);

  attr.name = INDEXES_COL_COLUMN_POSITION;
  attr.type = TypeInt;
  attr.length = (AttrLength)INT_SIZE;
  id.push_back(attr);

	return id;
}

// Creates the Tables table entry for the given id and tableName
// Assumes fileName is just tableName + file extension
void RelationManager::prepareTablesRecordData(int32_t id, bool system, const string &tableName, void *data)
{
    unsigned offset = 0;

    int32_t name_len = tableName.length();

    string table_file_name = getFileName(tableName);
    int32_t file_name_len = table_file_name.length();

    int32_t is_system = system ? 1 : 0;

    // All fields non-null
    char null = 0;
    // Copy in null indicator
    memcpy((char*) data + offset, &null, 1);
    offset += 1;
    // Copy in table id
    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;
    // Copy in varchar table name
    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, tableName.c_str(), name_len);
    offset += name_len;
    // Copy in varchar file name
    memcpy((char*) data + offset, &file_name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, table_file_name.c_str(), file_name_len);
    offset += file_name_len;
    // Copy in system indicator
    memcpy((char*) data + offset, &is_system, INT_SIZE);
    offset += INT_SIZE; // not necessary because we return here, but what if we didn't?
}

// Prepares the Columns table entry for the given id and attribute list
void RelationManager::prepareColumnsRecordData(int32_t id, int32_t pos, Attribute attr, void *data)
{
    unsigned offset = 0;
    int32_t name_len = attr.name.length();

    // None will ever be null
    char null = 0;

    memcpy((char*) data + offset, &null, 1);
    offset += 1;

    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, attr.name.c_str(), name_len);
    offset += name_len;

    int32_t type = attr.type;
    memcpy((char*) data + offset, &type, INT_SIZE);
    offset += INT_SIZE;

    int32_t len = attr.length;
    memcpy((char*) data + offset, &len, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &pos, INT_SIZE);
    offset += INT_SIZE;
}

// Insert the given columns into the Columns table
RC RelationManager::insertColumns(int32_t id, const vector<Attribute> &recordDescriptor)
{
    RC rc;

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *columnData = malloc(COLUMNS_RECORD_DATA_SIZE);
    RID rid;
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        int32_t pos = i+1;
        prepareColumnsRecordData(id, pos, recordDescriptor[i], columnData);
        rc = rbfm->insertRecord(fileHandle, columnDescriptor, columnData, rid);
        if (rc)
            return rc;
    }

    rbfm->closeFile(fileHandle);
    free(columnData);
    return SUCCESS;
}

RC RelationManager::insertTable(int32_t id, int32_t system, const string &tableName)
{
    FileHandle fileHandle;
    RID rid;
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *tableData = malloc (TABLES_RECORD_DATA_SIZE);
    prepareTablesRecordData(id, system, tableName, tableData);
    rc = rbfm->insertRecord(fileHandle, tableDescriptor, tableData, rid);

    rbfm->closeFile(fileHandle);
    free (tableData);
    return rc;
}

// Get the next table ID for creating a table
RC RelationManager::getNextTableID(int32_t &table_id)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Grab only the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Scan through all tables to get largest ID value
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, NO_OP, NULL, projection, rbfm_si);

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    int32_t max_table_id = 0;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == (SUCCESS))
    {
        // Parse out the table id, compare it with the current max
        int32_t tid;
        fromAPI(tid, data);
        if (tid > max_table_id)
            max_table_id = tid;
    }
    // If we ended on eof, then we were successful
    if (rc == RM_EOF)
        rc = SUCCESS;

    free(data);
    // Next table ID is 1 more than largest table id
    table_id = max_table_id + 1;
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return SUCCESS;
}

// Gets the table ID of the given tableName
RC RelationManager::getTableID(const string &tableName, int32_t &tableID)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Fill value with the string tablename in api format (without null indicator)
    void *value = malloc(4 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    // Find the table entries whose table-name field matches tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    // There will only be one such entry, so we use if rather than while
    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        int32_t tid;
        fromAPI(tid, data);
        tableID = tid;
    }

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;
}

// Determine if table tableName is a system table. Set the boolean argument as the result
RC RelationManager::isSystemTable(bool &system, const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about system column
    vector<string> projection;
    projection.push_back(TABLES_COL_SYSTEM);

    // Set up value to be tableName in API format (without null indicator)
    void *value = malloc(5 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    // Find table whose table-name is equal to tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // Parse the system field from that table entry
        int32_t tmp;
        fromAPI(tmp, data);
        system = tmp == 1;
    }
    if (rc == RBFM_EOF)
        rc = SUCCESS;

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;
}

RC RelationManager::insertIndexTuple(const string& tableName, const vector<Attribute> recordDescriptor, const void* data, const RID& rid)
{
  RC rc;
  // get attr of Index table(this is a subset of column table attrs).
  vector<IndexedAttr> iattrs;
  getIndexAttributes(tableName, iattrs);

  // insert this tuple into all index file.
  for (auto iattr : iattrs) {
    void* value = malloc(PAGE_SIZE);
    memset(value, 0, PAGE_SIZE);
    rc = getFieldFromRecord(iattr.attr.name, recordDescriptor, data, value);
    if (rc)
      continue;

    IXFileHandle ixfileHandle;
    IndexManager *im = IndexManager::instance();

    rc = im->openFile(getIndexFileName(tableName, iattr.attr.name), ixfileHandle);
    if (rc)
      return rc;
    rc = im->insertEntry(ixfileHandle, recordDescriptor[iattr.pos], value, rid);
    if (rc)
      return rc;
    rc = im->closeFile(ixfileHandle);
      return rc;
    free(value);
  }

  return SUCCESS;
}

RC RelationManager::deleteIndexTuple(const string& tableName, const vector<Attribute> recordDescriptor, const void* data, const RID& rid)
{
  RC rc;
  // get attr of Index table(this is a subset of column table attrs).
  vector<IndexedAttr> iattrs;
  getIndexAttributes(tableName, iattrs);

  // insert this tuple into all index file.
  for (auto iattr : iattrs) {
    void* value = malloc(PAGE_SIZE);
    memset(value, 0, PAGE_SIZE);
    rc = getFieldFromRecord(iattr.attr.name, recordDescriptor, data, value);
    if (rc)
      continue;

    IXFileHandle ixfileHandle;
    IndexManager *im = IndexManager::instance();

    rc = im->openFile(getIndexFileName(tableName, iattr.attr.name), ixfileHandle);
    if (rc)
      return rc;
    rc = im->deleteEntry(ixfileHandle, recordDescriptor[iattr.pos], value, rid);
    if (rc)
      return rc;
    rc = im->closeFile(ixfileHandle);
      return rc;
    free(value);
  }

  return SUCCESS;
}

RC RelationManager::getFieldFromRecord(const string attrName, const vector<Attribute> recordDescriptor, const void* data, void* value)
{
  // get corresponding attr from input data.
  int offset = ceil(recordDescriptor.size() / 8.0);
  for (unsigned i = 0; i < recordDescriptor.size(); ++i) {
    char currNullTerminator = *((char*)data + i/8);
    if (currNullTerminator & (1<<(7-i%8))) {
      // this name is null field, do nothing.
      if (attrName == recordDescriptor[i].name) {
        return -1;
      }
      else {
        continue;
      }
    }

    int attrSize = sizeof(int);
    if (recordDescriptor[i].type == TypeVarChar) {
      memcpy(&attrSize, (char*)data + offset, VARCHAR_LENGTH_SIZE);
      memcpy((char*)value, &attrSize, VARCHAR_LENGTH_SIZE);
      memcpy((char*)value + VARCHAR_LENGTH_SIZE, (char*)data + offset + VARCHAR_LENGTH_SIZE, attrSize);
      attrSize += VARCHAR_LENGTH_SIZE;
    } else {
      memcpy(value, (char*)data + offset, sizeof(int));
    }
    offset += attrSize;

    if (attrName == recordDescriptor[i].name) {
      return 0;
    }
  }
  return -1;
}

void RelationManager::toAPI(const string &str, void *data)
{
    int32_t len = str.length();
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &len, INT_SIZE);
    memcpy((char*) data + 1 + INT_SIZE, str.c_str(), len);
}

void RelationManager::toAPI(const int32_t integer, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &integer, INT_SIZE);
}

void RelationManager::toAPI(const float real, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &real, REAL_SIZE);
}

void RelationManager::fromAPI(string &str, void *data)
{
    char null = 0;
    int32_t len;

    memcpy(&null, data, 1);
    if (null)
        return;

    memcpy(&len, (char*) data + 1, INT_SIZE);

    char tmp[len + 1];
    tmp[len] = '\0';
    memcpy(tmp, (char*) data + 5, len);

    str = string(tmp);
}

void RelationManager::fromAPI(int32_t &integer, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    int32_t tmp;
    memcpy(&tmp, (char*) data + 1, INT_SIZE);

    integer = tmp;
}

void RelationManager::fromAPI(float &real, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    float tmp;
    memcpy(&tmp, (char*) data + 1, REAL_SIZE);

    real = tmp;
}

// RM_ScanIterator ///////////////

// Makes use of underlying rbfm_scaniterator
RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    // Open the file for the given tableName
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm->openFile(getFileName(tableName), rm_ScanIterator.fileHandle);
    if (rc)
        return rc;

    // grab the record descriptor for the given tableName
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Use the underlying rbfm_scaniterator to do all the work
    rc = rbfm->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute,
                     compOp, value, attributeNames, rm_ScanIterator.rbfm_iter);
    if (rc)
        return rc;

    return SUCCESS;
}

// Let rbfm do all the work
RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    return rbfm_iter.getNextRecord(rid, data);
}

// Close our file handle, rbfm_scaniterator
RC RM_ScanIterator::close()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    rbfm_iter.close();
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
  /* ------------------- Check availability of index ------------------*/
  // Check if the tableName is system table
  RC rc;
  bool isSystem;
  if ((rc = isSystemTable(isSystem, tableName)))
    return rc;
  if (isSystem)
    return RM_CANNOT_MOD_SYS_TBL;

  // Check if the index exists in attributes of the record: Expect SUCCESS -> Exists
  if (indexExistsInAttributes(tableName, attributeName) != SUCCESS)
    return RM_INDEX_EXISTENCE_ERR;

  // Check if the index table already exists: Expect FAIL -> Not Exists
  if (indexExistsInIndex(tableName, attributeName, false) != RM_INDEX_EXISTENCE_ERR)
    return RM_INDEX_EXISTENCE_ERR;

  /* --------------- Insert each record value into Index table ---------------*/
  // Prepare for obtain all record value
  IndexManager *im = IndexManager::instance();
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  FileHandle fh;
  IXFileHandle ixfh;
  RBFM_ScanIterator rbfm_si;
  RID rid;
  if ((rc = im->createFile(getIndexFileName(tableName, attributeName))))
    return rc;
	if ((rc = im->openFile(getIndexFileName(tableName, attributeName), ixfh)))
    return rc;
	if ((rc = rbfm->openFile(getFileName(tableName), fh)))
    return rc;

  vector<Attribute> recordDescriptor;
  getAttributes(tableName, recordDescriptor);
  vector<string> projection;
  projection.push_back(attributeName);

  // Scan all records in table and insert into index
  if ((rc = rbfm->scan(fh, recordDescriptor, attributeName, NO_OP, NULL, projection, rbfm_si)))
    return rc;

  vector<Attribute> attrs;
  if ((rc = getAttributes(tableName, attrs)))
    return rc;

  size_t i = 0;
  for (; i < attrs.size(); ++i) {
    if (attrs[i].name == attributeName)
      break;
  }
  void *data = calloc(1 + recordDescriptor[i].length, 1);

  while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS) {
    int8_t nullIndicator;
    memcpy(&nullIndicator, data, 1);
    if (nullIndicator)
      continue;

    if (im->insertEntry(ixfh, recordDescriptor[i], (char*) data + 1, rid))
      return RM_CREATE_INDEX_FAILED;
  }

  if (rc != RBFM_EOF)
    return rc;

  rbfm->closeFile(fh);
	im->closeFile(ixfh);
	rbfm_si.close();
	free(data);

	// Insert index into INDEXES table
  int tableID;
  getTableID(tableName, tableID);
	if ((rc = insertIndex(tableID, recordDescriptor[i], i)))
    return rc;

	return SUCCESS;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
  /* ------------------- Check availability of index ------------------*/
  // Check if the tableName is system table
  RC rc;
  bool isSystem;
  if ((rc = isSystemTable(isSystem, tableName)))
    return rc;
  if (isSystem)
    return RM_CANNOT_MOD_SYS_TBL;

  // Check if the index exists in attributes of the record: Expect SUCCESS -> Exists
  if (indexExistsInAttributes(tableName, attributeName) != SUCCESS)
    return RM_INDEX_EXISTENCE_ERR;

  // Check if the index table already exists: Expect SUCCESS -> Exists
  if (indexExistsInIndex(tableName, attributeName, true) != SUCCESS)
    return RM_INDEX_EXISTENCE_ERR;

  // Delete index file
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	if ((rc = rbfm->destroyFile(getIndexFileName(tableName, attributeName))))
    return rc;

  return SUCCESS;
}

RC RelationManager::insertIndex(int tableID, const Attribute &attr, int position)
{
	RC rc;
  FileHandle fh;
	RID uselessRID;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	if ((rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fh)))
    return rc;

	void *data = malloc(INDEXES_RECORD_DATA_SIZE);
	prepareColumnsRecordData(tableID, position, attr, data);
	if ((rc = rbfm->insertRecord(fh, indexDescriptor, data, uselessRID)))
    return rc;

	rbfm->closeFile(fh);
	free (data);
	return SUCCESS;
}

// Check if the index exists in attributes of the record : SUCCESS -> EXISTs
RC RelationManager::indexExistsInAttributes(const string &tableName, const string &attributeName)
{
  RC rc;
  vector<Attribute> attrs;
  if ((rc = getAttributes(tableName, attrs)))
    return rc;

  for (size_t i = 0; i < attrs.size(); ++i) {
    if (attrs[i].name == attributeName)
      return SUCCESS;
  }
  return RM_INDEX_EXISTENCE_ERR;
}

// Check if the index table already exists : SUCCESS -> EXISTS
RC RelationManager::indexExistsInIndex(const string &tableName, const string &attributeName, bool deleteFlag)
{
  RC rc;
  int tableID;
  if ((rc = getTableID(tableName, tableID)))
    return rc;

  // Scan Index.t table to check if the index with the same tableID already exists
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  FileHandle fh;
  if ((rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fh)))
    return rc;

  RBFM_ScanIterator rbfm_si;
  vector<string> projection;
  projection.push_back(INDEXES_COL_COLUMN_NAME);
  void *value = &tableID;
  if ((rc = rbfm->scan(fh, indexDescriptor, INDEXES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si)))
    return rc;

  RID rid;
  void *data = malloc(INDEXES_RECORD_DATA_SIZE);
  while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS) {
    int size;
    memcpy(&size, (char*)data + 1, VARCHAR_LENGTH_SIZE);
    char indexName[size + 1];
    indexName[size] = '\0';
    memcpy(indexName, (char*)data + 1 + VARCHAR_LENGTH_SIZE, size);

    // Found the same index Name
    if(strcmp(indexName, attributeName.c_str()) == 0) {
			rbfm_si.close();
			rbfm->closeFile(fh);
			free(data);
      if (deleteFlag) {
        if (rbfm->deleteRecord(fh, indexDescriptor, rid) != SUCCESS)
          return -1;
      }
			return SUCCESS;
		}
  }

  rbfm_si.close();
  rbfm->closeFile(fh);
  free(data);
  if (rc == RM_EOF)
    return RM_INDEX_EXISTENCE_ERR;
  else
    return rc;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
  // Open the file for the given tableName
  IndexManager *im = IndexManager::instance();
  rm_IndexScanIterator.ix_iter.fileHandle = new IXFileHandle();
  RC rc = im->openFile(getIndexFileName(tableName, attributeName), *rm_IndexScanIterator.ix_iter.fileHandle);
  if (rc)
    return rc;

  // grab the record descriptor for the given tableName
  vector<Attribute> recordDescriptor;
	rc = getAttributes(tableName, recordDescriptor);
	if (rc)
    return rc;

	int attrPos = -1;
	for(unsigned i = 0; i < recordDescriptor.size(); ++i) {
		if(attributeName == recordDescriptor[i].name)  {
			attrPos = i;
			break;
		}
		if (i == recordDescriptor.size() -1)
      return RM_COLUMN_NON_EXIST;
	}

  if (attrPos == -1)
    return RM_COLUMN_NON_EXIST;

	rc = im->scan(*rm_IndexScanIterator.ix_iter.fileHandle, recordDescriptor[attrPos], lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_iter);
	if(rc)
    return rc;

	return SUCCESS;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key)
{
  return ix_iter.getNextEntry(rid, key);
}

RC RM_IndexScanIterator::close()
{
  IndexManager *im = IndexManager::instance();
  ix_iter.close();
  im->closeFile(*ix_iter.fileHandle);
  return SUCCESS;
}
