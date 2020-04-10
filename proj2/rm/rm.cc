#include "rm.h"
#include <iostream>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
  // init tableDescriptor
  Attribute attr;
  attr.name = "table-id";
  attr.type = TypeInt;
  attr.length = (AttrLength)4;
  tableDescriptor.push_back(attr);

  attr.name = "table-name";
  attr.type = TypeVarChar;
  attr.length = (AttrLength)50;
  tableDescriptor.push_back(attr);

  attr.name = "file-name";
  attr.type = TypeVarChar;
  attr.length = (AttrLength)50;
  tableDescriptor.push_back(attr);

  attr.name = "access-flag";
  attr.type = TypeInt;
  attr.length = (AttrLength)4;
  tableDescriptor.push_back(attr);

  // init columnDescriptor
  Attribute attrColumns;
  attrColumns.name = "table-id";
  attrColumns.type = TypeInt;
  attrColumns.length = (AttrLength)4;
  columnDescriptor.push_back(attrColumns);

  attrColumns.name = "column-name";
  attrColumns.type = TypeVarChar;
  attrColumns.length = (AttrLength)50;
  columnDescriptor.push_back(attrColumns);

  attrColumns.name = "column-type";
  attrColumns.type = TypeInt;
  attrColumns.length = (AttrLength)4;
  columnDescriptor.push_back(attrColumns);

  attrColumns.name = "column-length";
  attrColumns.type = TypeInt;
  attrColumns.length = (AttrLength)4;
  columnDescriptor.push_back(attrColumns);

  attrColumns.name = "column-position";
  attrColumns.type = TypeInt;
  attrColumns.length = (AttrLength)4;
  columnDescriptor.push_back(attrColumns);
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  RC rc;
  RID useless;
  string str1, str2;
  str1 = "Tables";
  str2 = "Tables.tbl";
  // create table files
  if ((rc = rbfm->createFile(str2)))
    return rc;

  // insert tables into Tables.tbl
  FileHandle tables_fh;
  if ((rc = rbfm->openFile(str2, tables_fh)))
    return rc;
  void *tableData = malloc(TABLE_DATA_SZ);
  prepareTable(1, str1, str2, AccessFlagSystem, tableData);
  if ((rc = rbfm->insertRecord(tables_fh, tableDescriptor, tableData, useless)))
    return rc;

  // insert columns into Tables.tbl
  str1 = "Columns";
  str2 = "Columns.tbl";
  if ((rc = rbfm->createFile(str2)))
    return rc;
  memset(tableData, 0, TABLE_DATA_SZ);
  prepareTable(1, str1, str2, AccessFlagSystem, tableData);
  if ((rc = rbfm->insertRecord(tables_fh, tableDescriptor, tableData, useless)))
    return rc;
  free(tableData);

  // insert initial attributes into Columns.tbl
  FileHandle columns_fh;
  void *columnData = malloc(COLUMN_DATA_SZ);
  if ((rc = rbfm->openFile(str2, columns_fh)))
    return rc;
  for (int i = 1; i <= 9; ++i)
  {
    memset(columnData, 0, COLUMN_DATA_SZ);
    switch (i)
    {
      case 1: str1 = "table-id";        prepareColumn(1, str1, TypeInt, 4, 1, columnData);      break;
      case 2: str1 = "table-name";      prepareColumn(1, str1, TypeVarChar, 50, 2, columnData); break;
      case 3: str1 = "file-name";       prepareColumn(1, str1, TypeVarChar, 50, 3, columnData); break;
      case 4: str1 = "access-flag";     prepareColumn(1, str1, TypeInt, 4, 1, columnData);      break;
      case 5: str1 = "table-id";        prepareColumn(2, str1, TypeInt, 4, 1, columnData);      break;
      case 6: str1 = "column-name";     prepareColumn(2, str1, TypeVarChar, 50, 2, columnData); break;
      case 7: str1 = "column-type";     prepareColumn(2, str1, TypeInt, 4, 3, columnData);      break;
      case 8: str1 = "column-length";   prepareColumn(2, str1, TypeInt, 4, 4, columnData);      break;
      case 9: str1 = "column-position"; prepareColumn(2, str1, TypeInt, 4, 5, columnData);      break;
    }
    if ((rc = rbfm->insertRecord(columns_fh, columnDescriptor, columnData, useless)))
      return rc;
  }
  free(columnData);

  if ((rc = rbfm->closeFile(tables_fh)))
    return rc;
  if ((rc = rbfm->closeFile(columns_fh)))
    return rc;

  return SUCCESS;
}

RC RelationManager::deleteCatalog()
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  RC rc;
  string str = "Tables.tbl";
  if ((rc = rbfm->destroyFile(str)))
    return rc;
  str = "Columns.tbl";
  if ((rc = rbfm->destroyFile(str)))
    return rc;

  return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  RC rc;
  RID useless;
  vector<Attribute> attr = attrs;
  int tableID;

  // Create the rbfm file to store the table
  string fileName = tableName;
  string table = tableName;

  if ((rc = rbfm->createFile(fileName))){
    return rc;
  }


  // Get the next table's ID
  FileHandle table_fh;
  string tables = "Tables.tbl";
  if ((rc = rbfm->openFile(tables, table_fh)))
    return rc;

  int tableID_temp;
  vector<string> projection;
  projection.push_back("table-id");
  projection.push_back("table-name");
  RBFM_ScanIterator rbfm_si;
  void * null = malloc(PAGE_SIZE);
  memset(null, 0, PAGE_SIZE);
  if ((rc = rbfm->scan(table_fh, tableDescriptor, "table-id", NO_OP, null, projection, rbfm_si)))
    return rc;

  void * idData = malloc(1 + sizeof(int) + 1 + 50); // nullIndicator(1) + table-id(TypeInt) + length(4) + table-name(TypeVarChar)
  void * tableName_temp = malloc(50 + 1);
  int maxID = 0;
  while ((rc = rbfm_si.getNextRecord(useless, idData)) == SUCCESS)
  {
    // Retrieve the max table ID
    memcpy(&tableID_temp, (char*)idData + 1, sizeof(int));
    if (tableID_temp > maxID)
      maxID = tableID_temp;

    // Detect if the table name already exists
    memset(tableName_temp, 0, 51);
    int length;
    memcpy(&length, (char*)idData + 1 + 4, sizeof(int));
    memcpy(tableName_temp, (char*)idData + 1 + sizeof(int) + 4, length);
    if (tableName.compare((char *)tableName_temp) == 0)
      return RM_CREATE_TABLE_FAILED;
  }
  if ((rc = rbfm_si.close()))
    return rc;
  free(idData);
  free(tableName_temp);
  tableID = maxID++;
  // Insert the table into the Tables table (0 means this is not a system table)
  void * tableData = malloc(TABLE_DATA_SZ);
  if ((rc = prepareTable(tableID, table, fileName, AccessFlagUser, tableData)))
    return rc;
  if ((rc = rbfm->insertRecord(table_fh, tableDescriptor, tableData, useless)))
    return rc;
  free(tableData);
  if ((rc = rbfm->closeFile(table_fh)))
    return rc;
  // Insert the table's columns into the Columns table
  FileHandle column_fh;
  string columns = "Columns.tbl";
  void * columnData = malloc(COLUMN_DATA_SZ);
  if ((rc = rbfm->openFile(columns, column_fh)))
    return rc;
  for (unsigned i = 0; i < attrs.size(); ++i)
  {
    memset(columnData, 0, COLUMN_DATA_SZ);
    if ((rc = prepareColumn(tableID, attr[i].name, attr[i].type, attr[i].length, i + 1, columnData)))
      return rc;
    if ((rc = rbfm->insertRecord(column_fh, columnDescriptor, columnData, useless)))
      return rc;
 }
  if ((rc = rbfm->closeFile(column_fh)))
    return rc;

  free(columnData);
  return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  FileHandle tables_fh, columns_fh, tableName_fh;
  RC rc;
  RID rid;
  int tableID = 0;
  AccessFlag access;

  // Delete 'tableName' record in Tables.tbl
  string tables = "Tables.tbl";
  if ((rc = rbfm->openFile(tables, tables_fh)))
    return rc;
  vector<string> projection;
  projection.push_back("table-id");
  projection.push_back("access-flag");
  RBFM_ScanIterator rbfm_table_si;
  void * name = malloc(tableName.length() + 1);
  memset(name, 0, tableName.length() + 1);
  memcpy(name, tableName.c_str(), tableName.length());
  string tname = "table-name";
  if ((rc = rbfm->scan(tables_fh, tableDescriptor, tname, EQ_OP, name, projection, rbfm_table_si)))
    return rc;
  free(name);
  void * data = malloc(1 + sizeof(int) + sizeof(AccessFlag)); // nullIndicator(1) + table-id(TypeInt) + access-flag(AccessFlag)
  memset(data, 0, 1 + sizeof(int) + sizeof(AccessFlag));
  if((rc = rbfm_table_si.getNextRecord(rid, data)))
    return rc; // not valid data retrieved
  memcpy(&tableID, (char*)data + 1, sizeof(int));
  memcpy(&access, (char*)data + 1 + sizeof(int), sizeof(AccessFlag));

  // Detect if it's system table
  if (access == AccessFlagSystem)
    return RM_DELETE_TABLE_FAILED;

  // Delete 'tableName' info in Tables.tbl
  if ((rc = rbfm->deleteRecord(tables_fh, tableDescriptor, rid)))
    return rc;
  if ((rc = rbfm->closeFile(tables_fh)))
    return rc;
  if ((rc = rbfm_table_si.close()))
    return rc;

  // Delete tableName record in Columns.tbl
  string columns = "Columns.tbl";
  if ((rc = rbfm->openFile(columns, columns_fh)))
    return rc;
  vector<string> projection2;
  projection2.push_back("table-id");
  RBFM_ScanIterator rbfm_column_si;
  void * id = malloc(sizeof(int));
  memcpy(id, &tableID, sizeof(int));
  string tableId = "table-id";
  if ((rc = rbfm->scan(columns_fh, columnDescriptor, tableId, EQ_OP, id, projection2, rbfm_column_si)))
    return rc;
  free(id);
  while ((rc = rbfm_column_si.getNextRecord(rid, data)) == SUCCESS)
  {
    // Delete 'tableName' info in Columns.tbl
    if ((rc = rbfm->deleteRecord(columns_fh, columnDescriptor, rid)))
      return rc;
  }

  if ((rc = rbfm->closeFile(columns_fh)))
    return rc;
  if ((rc = rbfm_column_si.close()))
    return rc;
  free(data);

  // Delete the tableName file
  if ((rc = rbfm->destroyFile(tableName)))
    return rc;

  return SUCCESS;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
  RecordBasedFileManager *_rbf_manager = RecordBasedFileManager::instance();
  FileHandle tables_fh, columns_fh;
  RC rc;
  RID rid;
  int tableID;

  // Find the table-id of 'tableName' in Tables.tbl
  string tables = "Tables.tbl";
  if ((rc = _rbf_manager->openFile(tables, tables_fh)))
    return rc;
  vector<string> projection;
  projection.push_back("table-id");
  RBFM_ScanIterator rbfm_table_si;
  void * name = malloc(PAGE_SIZE);
  memset(name, 0, PAGE_SIZE);
  memcpy(name, tableName.c_str(), tableName.length());
  string tn = "table-name";
  if ((rc = _rbf_manager->scan(tables_fh, tableDescriptor, tn, EQ_OP, name, projection, rbfm_table_si)))
    return rc;
  free(name);
  void * data = malloc(1 + sizeof(int));
  if((rc = rbfm_table_si.getNextRecord(rid, data)))
    return rc; // not valid data retrieved
  memcpy(&tableID, (char*)data + 1, sizeof(int));

  // Close relevant file
  if ((rc = _rbf_manager->closeFile(tables_fh)))
    return rc;
  if ((rc = rbfm_table_si.close()))
    return rc;
  free(data);

  // Find attributes info in Columns table
  string columns = "Columns.tbl";
  if ((rc = _rbf_manager->openFile(columns, columns_fh)))
    return rc;
  vector<string> projection2;
  projection2.push_back("column-name");
  projection2.push_back("column-type");
  projection2.push_back("column-length");
  projection2.push_back("column-position");
  RBFM_ScanIterator rbfm_column_si;
  void * id = malloc(sizeof(int));
  memcpy(id, &tableID, sizeof(int));
  string ti = "table-id";
  if ((rc = _rbf_manager->scan(columns_fh, columnDescriptor, ti, EQ_OP, id, projection2, rbfm_column_si)))
    return rc;
  free(id);

  // Retrieve attribute information from each record
  void * temp = malloc(1 + sizeof(int) + 50 + sizeof(int) + sizeof(int) + sizeof(unsigned));
  string attrName;
  string st;
  AttrType at;
  AttrLength al;
  while ((rc = rbfm_column_si.getNextRecord(rid, temp)) == SUCCESS)
  {
    // cout << "in get NEXTR: rid: " << rid.pageNum << " : "<<rid.slotNum<<endl;
    // Generate attribute
    Attribute attr;
    for (int i = 1; i <= 3; ++i)
    {
      memset(temp, 0, 51);
      switch (i)
      {
        case 1: attrName = "column-name";     break;
        case 2: attrName = "column-type";     break;
        case 3: attrName = "column-length";   break;
      }

      if ((rc = _rbf_manager->readAttribute(columns_fh, columnDescriptor, rid, attrName, temp)))
       return rc;

      switch (i)
      {
        case 1: st.append((char*)temp); attr.name = st; break;
        case 2: memcpy(&at, temp, sizeof(int)); attr.type = at; break;
        case 3: memcpy(&al, temp, sizeof(int)); attr.length = al; break;
      }
    }
    attrs.push_back(attr);
  }
  free(temp);

  if ((rc = _rbf_manager->closeFile(columns_fh)))
    return rc;
  if ((rc = rbfm_column_si.close()))
    return rc;

  return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
  RecordBasedFileManager *_rbf_manager = RecordBasedFileManager::instance();

  FileHandle fh;
  RC rc;

  // Open file
  if ((rc = _rbf_manager->openFile(tableName, fh)))
    return rc;

  // Retrieve recordDescriptor
  vector<Attribute> recordDescriptor;
  if ((rc = getAttributes(tableName, recordDescriptor)))
    return rc;

  // Insert record
  if ((rc = _rbf_manager->insertRecord(fh, recordDescriptor, data, rid)))
    return rc;

  // Close file
  if ((rc = _rbf_manager->closeFile(fh)))
    return rc;

  return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
  RecordBasedFileManager *_rbf_manager = RecordBasedFileManager::instance();

  FileHandle fh;
  RC rc;

  // Open file
  if ((rc = _rbf_manager->openFile(tableName, fh)))
    return rc;

  // Retrieve recordDescriptor
  vector<Attribute> recordDescriptor;
  if ((rc = getAttributes(tableName, recordDescriptor)))
    return rc;

  // Delete record
  if ((rc = _rbf_manager->deleteRecord(fh, recordDescriptor, rid)))
    return rc;

  // Close file
  if ((rc = _rbf_manager->closeFile(fh)))
    return rc;

  return SUCCESS;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
  RecordBasedFileManager *_rbf_manager = RecordBasedFileManager::instance();

  FileHandle fh;
  RC rc;

  // Open file
  if ((rc = _rbf_manager->openFile(tableName, fh)))
    return rc;

  // Retrieve recordDescriptor
  vector<Attribute> recordDescriptor;
  if ((rc = getAttributes(tableName, recordDescriptor)))
    return rc;

  // Update record
  if ((rc = _rbf_manager->updateRecord(fh, recordDescriptor, data, rid)))
    return rc;

  // Close file
  if ((rc = _rbf_manager->closeFile(fh)))
    return rc;

  return SUCCESS;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
  RecordBasedFileManager *_rbf_manager = RecordBasedFileManager::instance();

  FileHandle fh;
  RC rc;

  // Open file
  if ((rc = _rbf_manager->openFile(tableName, fh)))
    return rc;

  // Retrieve recordDescriptor
  vector<Attribute> recordDescriptor;
  if ((rc = getAttributes(tableName, recordDescriptor)))
    return rc;

  // Read record
  if ((rc = _rbf_manager->readRecord(fh, recordDescriptor, rid, data)))
    return rc;

  // Close file
  if ((rc = _rbf_manager->closeFile(fh)))
    return rc;

  return SUCCESS;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
  RecordBasedFileManager *_rbf_manager = RecordBasedFileManager::instance();

  RC rc;

  // Print record
  if ((rc = _rbf_manager->printRecord(attrs, data)))
    return rc;

  return SUCCESS;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
  RecordBasedFileManager *_rbf_manager = RecordBasedFileManager::instance();

  FileHandle fh;
  RC rc = -1;

  // Open file
  if ((rc == _rbf_manager->openFile(tableName, fh)))
    return rc;

  // Retrieve recordDescriptor
  vector<Attribute> recordDescriptor;
  if ((rc = getAttributes(tableName, recordDescriptor)))
    return rc;

  // Retrieve data
  void * attrData = malloc(PAGE_SIZE);
  memset(attrData, 0, PAGE_SIZE);
  AttrType at;
  if ((rc = _rbf_manager->readAttribute(fh, recordDescriptor, rid, attributeName, attrData)))
    return rc;
  for (unsigned i = 0; i < (unsigned)recordDescriptor.size(); ++i) {
    if (recordDescriptor[i].name.compare(attributeName)) {
      at = recordDescriptor[i].type;
      break;
    }
  }

  // Check if it is null
  uint8_t nullIndicator = 0;
  if (attrData == NULL) {
    nullIndicator = 1 << 7;
    memcpy(data, &nullIndicator, sizeof(uint8_t));
    return SUCCESS;
  }
  memcpy(data, &nullIndicator, sizeof(uint8_t));

  // Add length if the data is a varchar
  unsigned length;
  if (at == TypeVarChar) {
    memcpy(&length, attrData, sizeof(VARCHAR_LENGTH_SIZE));
    memcpy((char*)data + sizeof(uint8_t), attrData, VARCHAR_LENGTH_SIZE + length);
  } else {
    memcpy((char*)data + sizeof(uint8_t), attrData, INT_SIZE);
  }

  free(attrData);
  return SUCCESS;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}

RC RelationManager::prepareTable(int tbl_id, string &tbl_name, string &file_name, int flag, void *data){
  int offset = 0;
  int length;
  memset(data, 0, TABLE_DATA_SZ);
  // null-indicator will be 1 byte and all 0.
  offset += 1;

  // table-id (int)
  memcpy((char *)data + offset, &tbl_id, sizeof(int));
  offset += sizeof(int);

  // table-name (varchar)
  if ((length = tbl_name.length()) > 50)
    return RM_PREPARE_FAILED;
  memcpy((char *)data + offset, &length, sizeof(int));
  offset += sizeof(int);
  memcpy((char *)data + offset, tbl_name.c_str(), length);
  offset += length;

  // file-name (varchar)
  if ((length = file_name.length()) > 50)
    return RM_PREPARE_FAILED;
  memcpy((char *)data + offset, &length, sizeof(int));
  offset += sizeof(int);
  memcpy((char *)data + offset, file_name.c_str(), length);
  offset += length;

  // access flag (int)
  memcpy((char *)data + offset, &flag, sizeof(int));
  offset += sizeof(int);

  return SUCCESS;
}

RC RelationManager::prepareColumn(int tbl_id, string &name, int type, int length, int position, void *data)
{
  int offset = 1; // include nullIndicator 8-bit ZEROes
  memset(data, 0, COLUMN_DATA_SZ);

  // tbl_id (int)
  memcpy((char *)data + offset, &tbl_id, sizeof(int));
  offset += sizeof(int);

  // name (string)
  if ((length = name.length()) > 50)
    return RM_PREPARE_FAILED;
  memcpy((char *)data + offset, &length, sizeof(int));
  offset += sizeof(int);
  memcpy((char *)data + offset, name.c_str(), length);
  offset += name.length();

  // type (int)
  memcpy((char *)data + offset, &type, sizeof(int));
  offset += sizeof(int);

  // length (int)
  memcpy((char *)data + offset, &length, sizeof(int));
  offset += sizeof(int);

  // position (int)
  memcpy((char *)data + offset, &position, sizeof(int));
  offset += sizeof(int);
  return SUCCESS;
}
