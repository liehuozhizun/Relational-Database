
#include "rm.h"

#include <algorithm>
#include <cstring>
#include <iostream>

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

	// Add table entries for Tables, Columns and Indexes
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

	////cout << "getattr phase 0" <<endl;

	int32_t id;
	rc = getTableID(tableName, id);
	if (rc) return rc;

	////cout << "getattr phase 1" <<endl;

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

	////cout << "getattr phase 2" <<endl;

	// Scan through the Column table for all entries whose table-id equals tableName's table id.
	rc = rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
	if (rc)
		return rc;

	////cout << "getattr phase 3" <<endl;

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

	////cout << "getattr phase 4" <<endl;

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

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	//cout << "insertTuple phase 0" <<endl;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;

	//cout << "insertTuple phase 0.3" <<endl;

	// If this is a system table, we cannot modify it
	bool isSystem;
	rc = isSystemTable(isSystem, tableName);
	//cout << "insertTuple phase 0.5" <<endl;
	if (rc)
		return rc;
	if (isSystem)
		return RM_CANNOT_MOD_SYS_TBL;
	//cout << "insertTuple phase 1" <<endl;

	// Get recordDescriptor
	vector<Attribute> recordDescriptor;
	rc = getAttributes(tableName, recordDescriptor);
	if (rc)
		return rc;
	//cout << "insertTuple phase 2" <<endl;

	// And get fileHandle
	FileHandle fileHandle;
	rc = rbfm->openFile(getFileName(tableName), fileHandle);
	if (rc)
		return rc;
	//cout << "insertTuple phase 3" <<endl;

	// Let rbfm do all the work
	rc = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
	rbfm->closeFile(fileHandle);
	if(rc) return rc;

	//cout << "inserted rid " << rid.pageNum <<"."<<rid.slotNum <<endl;

	rc = AlterIndexTree(tableName, recordDescriptor, data, rid, INSERT_INDEX);
	if(rc) return rc;
	//cout << "inserted index for " << rid.pageNum <<"."<<rid.slotNum <<endl;

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

	void* tmpData = malloc(PAGE_SIZE);
	rc = readTuple(tableName, rid, tmpData);
	if(rc) return rc;

	rc = AlterIndexTree(tableName, recordDescriptor, tmpData, rid, DELETE_INDEX);
	if(rc) return rc;

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

	void* tmpData = malloc(PAGE_SIZE);
	rc = readTuple(tableName, rid, tmpData);
	if(rc) return rc;

	rc = AlterIndexTree(tableName, recordDescriptor, tmpData, rid, DELETE_INDEX);
	if(rc) return rc;

	rc = AlterIndexTree(tableName, recordDescriptor, data, rid, INSERT_INDEX);
	if(rc) return rc;

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

string RelationManager::getIndexFileName(const string &tableName, const char *indexName)
{
	return tableName + "." + string(indexName) + string(INDEX_FILE_EXTENSION);
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

// Prepares the Indexes table entry for the given id and attribute name
void RelationManager::prepareIndexesRecordData(int32_t id, const string &attributeName, void *data)
{
	unsigned offset = 0;
	int32_t name_len = attributeName.length();

	// None will ever be null
	char null = 0;

	memcpy((char*) data, &null, 1);
	offset += 1;

	memcpy((char*) data + offset, &id, INT_SIZE);
	offset += INT_SIZE;

	memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
	offset += VARCHAR_LENGTH_SIZE;
	
	memcpy((char*) data + offset, attributeName.c_str(), name_len);
	offset += name_len;
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

//insert an index entry into INDEXES table
RC RelationManager::insertIndex(int32_t id, const string &attributeName)
{
	FileHandle fileHandle;
	RID rid;
	RC rc;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fileHandle);
	if (rc)
		return rc;

	void *indexData = malloc (INDEXES_RECORD_DATA_SIZE);
	prepareIndexesRecordData(id, attributeName, indexData);
	rc = rbfm->insertRecord(fileHandle, indexDescriptor, indexData, rid);

	rbfm->closeFile(fileHandle);
	free (indexData);
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

	////cout << "getTableID phase 0" <<endl;

	rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
	if (rc)
		return rc;

	////cout << "getTableID phase 1" <<endl;

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
	if(rc) return rc;

	////cout << "getTableID phase 2" <<endl;
	// There will only be one such entry, so we use if rather than while
	RID rid;
	void *data = malloc (1 + INT_SIZE);
	if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
	{
		////cout << "getNextRecord successful" <<endl;
		int32_t tid;
		fromAPI(tid, data);
		tableID = tid;
	}

	////cout << "getTableID phase 3" <<endl;

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
	//cout << "systable phase 0" <<endl;

	rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
	if (rc)
		return rc;
	//cout << "systable phase 1" <<endl;

	// We only care about system column
	vector<string> projection;
	projection.push_back(TABLES_COL_SYSTEM);

	// Set up value to be tableName in API format (without null indicator)
	void *value = malloc(5 + TABLES_COL_TABLE_NAME_SIZE);
	int32_t name_len = tableName.length();
	memcpy(value, &name_len, INT_SIZE);
	memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);
	//cout << "systable phase 2" <<endl;

	// Find table whose table-name is equal to tableName
	RBFM_ScanIterator rbfm_si;
	rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

	//cout << "systable phase 3" <<endl;

	RID rid;
	void *data = malloc (1 + INT_SIZE);
	if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
	{
		// Parse the system field from that table entry
		int32_t tmp;
		fromAPI(tmp, data);
		system = tmp == 1;
	}

	//cout << "systable phase 4" <<endl;
	if (rc == RBFM_EOF)
		rc = SUCCESS;

	free(data);
	free(value);
	rbfm->closeFile(fileHandle);
	rbfm_si.close();
	return rc;   
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

RC RelationManager::AlterIndexTree(const string& tableName, const vector<Attribute> recordDescriptor, const void* data, const RID& rid, int mode){

	FileHandle fileHandle;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    RC rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back(INDEXES_COL_COLUMN_NAME);

    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;
    
    void *value = &id;
    rc = rbfm->scan(fileHandle, indexDescriptor, INDEXES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    
    RID recordRID;
    void *recordData = malloc(1 + INT_SIZE + INDEXES_COL_COLUMN_NAME_SIZE);
    vector<IndexTuple> indexes;
    while ((rc = rbfm_si.getNextRecord(recordRID, recordData)) == SUCCESS)
    {
        unsigned offset = 1;

        int32_t colLen;
        memcpy(&colLen, (char*) recordData + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;

        char col[colLen + 1];
        col[colLen] = '\0';
        memcpy(col, (char*) recordData + offset, colLen);

        indexes.push_back(make_tuple(string {col}, -1));
    }

    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    free(recordData);

    for(unsigned i = 0; i < recordDescriptor.size(); ++i) {
        for(unsigned j = 0; j < indexes.size(); ++j) {
            if(recordDescriptor[i].name == get<TupleColumn>(indexes[j])) {
                indexes[j] = make_tuple(get<TupleColumn>(indexes[j]), i);
            }
        }
    }

    for(auto& index:  indexes) {
        rc = getValue(get<TupleColumn>(index), recordDescriptor, data, value); 
        if(rc <= 0) {
            continue;
        }

        IXFileHandle ixfileHandle;
        IndexManager *im = IndexManager::instance();

        rc = im->openFile(getIndexFileName(tableName, get<TupleColumn>(index)), ixfileHandle);
        if (rc)
            return rc;

        if(mode == DELETE_INDEX)
            rc = im->deleteEntry(ixfileHandle, recordDescriptor[get<TupleIndex>(index)], value, rid);
        if(mode == INSERT_INDEX)
            rc = im->insertEntry(ixfileHandle, recordDescriptor[get<TupleIndex>(index)], value, rid);

        if (rc)
            return rc;

        im->closeFile(ixfileHandle);
    }

    return SUCCESS;
}

RC RelationManager::getValue(const string name, const vector<Attribute> &attrs, const void* data, void* value) 
{
    int offset = ceil(attrs.size() / 8.0);
    for (size_t i = 0; i < attrs.size(); ++i) {
        char target = *((char*)data + i/8);
        if (target & (1<<(7-i%8))) {
            if (name == attrs[i].name) {
                return -1;
            }
            else  {
                continue;
            }
        }
        int size = sizeof(int);
        if (attrs[i].type == TypeVarChar) {
            memcpy(&size, (char*)data + offset, sizeof(int));
            memcpy((char*)value, &size, sizeof(int));
            memcpy((char*)value + sizeof(int), (char*)data + offset + sizeof(int), size);
            size += sizeof(int);
        } else 
            memcpy(value, (char*)data + offset, sizeof(int));                  
        if (name == attrs[i].name)
            return size;       
        offset += size;
    }
    return 0;
}


RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{	
	bool isSystem;
    RC rc = isSystemTable(isSystem, tableName);
    if (rc) return rc;
    if (isSystem) return RM_CANNOT_MOD_SYS_TBL;

	////cout << "phase 00" <<endl;
	RBFM_ScanIterator rbfm_si;
	FileHandle fh;
	IXFileHandle ixfileHandle;
	vector<Attribute> recordDescriptor;

	////cout << "phase 0" <<endl;
	
	rc = getAttributes(tableName, recordDescriptor);
	if (rc) return rc;

	////cout << "phase 02" <<endl;

	//check if attribute name exists in the table, return -1 if it doesn't
	int attriIndex = -1;
	for(int i = 0; i < recordDescriptor.size(); i++) {
		if(recordDescriptor[i].name == attributeName)  {
			attriIndex = i;
			break;
		}
	}
	if(attriIndex == -1) return -1;

	////cout << "phase 1" <<endl;

	//scan indexes table for existing index entry
	int32_t tableId;
	rc = getTableID(tableName, tableId);
	if (rc) return rc;

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fh);
	if (rc) return rc;

	////cout << "phase 2" <<endl;

	vector<string> targetAttr;
	targetAttr.push_back(INDEXES_COL_COLUMN_NAME);

	void *value = &tableId;
	rc = rbfm->scan(fh, indexDescriptor, INDEXES_COL_TABLE_ID, EQ_OP, value, targetAttr, rbfm_si);

	RID rid;
	void *data = malloc(INDEXES_RECORD_DATA_SIZE);
	while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
	{
		unsigned offset = 1;
		int32_t attributeSize;
		
		memcpy(&attributeSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
		offset += VARCHAR_LENGTH_SIZE;

		char fieldValue[attributeSize + 1];
		fieldValue[attributeSize] = '\0';
		memcpy(fieldValue, (char*) data + offset, attributeSize);

		//if the new index already exists return error
		if(strcmp(fieldValue, attributeName.c_str()) == 0) {
			rbfm_si.close();
			rbfm->closeFile(fh);
			free(data);
			return RM_INDEX_EXISTS;
		}
	}

	////cout << "phase 3" <<endl;

	rbfm_si.close();
	rbfm->closeFile(fh);
	free(data);
	if(rc != RM_EOF) return rc;
	
	//create new index file and generate indexes
	IndexManager *im = IndexManager::instance();
	rc = im->createFile(getIndexFileName(tableName, attributeName));
	if (rc) return rc;
	rc = im->openFile(getIndexFileName(tableName, attributeName), ixfileHandle);
	if (rc) return rc;

	rc = rbfm->openFile(getFileName(tableName), fh);
	if (rc) return rc;

	////cout << "phase 4" <<endl;

	targetAttr.clear();
	targetAttr.push_back(attributeName);

	//scan through all record entries with non-null target field, construct index for each
	rc = rbfm->scan(fh, recordDescriptor, attributeName, NO_OP, NULL, targetAttr, rbfm_si);

	//initiate data to contain 1 null byte and field value
	data = malloc(1 + recordDescriptor[attriIndex].length);

	while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
	{
		//null indicator should be all 0s when theres only one field
		char nullByte;
		memcpy(&nullByte, data, 1);
		if (nullByte) continue;

		rc = im->insertEntry(ixfileHandle, recordDescriptor[attriIndex], (char*) data + 1, rid);
		if (rc) return rc;
	}

		////cout << "phase 5" <<endl;
	
	if (rc != RBFM_EOF) return rc;
	rbfm->closeFile(fh);
	im->closeFile(ixfileHandle);
	rbfm_si.close();
	free(data);

	//insert index entry into INDEXES table
	rc = insertIndex(tableId, attributeName);
	if(rc) return rc;	
	
	return SUCCESS;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	RBFM_ScanIterator rbfm_si;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RID rid;
	RC rc;
	int32_t id;
	rc = getTableID(tableName, id);
	if (rc) return rc;
	void *value = &id;

	FileHandle fileHandle;
	rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fileHandle);
	if (rc) return rc;

	//scan through INDEXES table to find the entry matching both tablename and attrname
	//remove the entry and break upon the first match
	vector<string> targetAttr;
	targetAttr.push_back(INDEXES_COL_COLUMN_NAME);
	rc = rbfm->scan(fileHandle, indexDescriptor, INDEXES_COL_TABLE_ID, EQ_OP, value, targetAttr, rbfm_si);
	if (rc) return rc;

	void *data = malloc(INDEXES_RECORD_DATA_SIZE);
	while((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
	{
		int32_t attributeSize;
		unsigned offset = 1;
		memcpy(&attributeSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
		offset += VARCHAR_LENGTH_SIZE;

		char attrValue[attributeSize + 1];
		attrValue[attributeSize] = '\0';
		memcpy(attrValue, (char*) data + offset, attributeSize);

		if(strcmp(attrValue, attributeName.c_str()) == 0){
			rc = rbfm->deleteRecord(fileHandle, indexDescriptor, rid);
			if (rc) return rc;
			break;
		}
	}

	rbfm->closeFile(fileHandle);
	rbfm_si.close();
	free(data);
	
	//destroy its corresponding index file
	rc = rbfm->destroyFile(getIndexFileName(tableName, attributeName));
	if (rc) return rc;

	return SUCCESS;
}

RC RelationManager::indexScan(const string &tableName,
					  const string &attributeName,
					  const void *lowKey,
					  const void *highKey,
					  bool lowKeyInclusive,
					  bool highKeyInclusive,
					  RM_IndexScanIterator &rm_IndexScanIterator)
{
	vector<Attribute> recordDescriptor;
	RC rc = getAttributes(tableName, recordDescriptor);
	if (rc) return rc;

	int attriIndex = -1;
	for(unsigned int i = 0; i < recordDescriptor.size(); ++i) {
		if(recordDescriptor[i].name == attributeName)  {
			attriIndex = i;
			break;
		}
		if (i == recordDescriptor.size() -1) return RM_COLUMN_MISSING;
	}

	IndexManager *im = IndexManager::instance();
	rm_IndexScanIterator.ix_si.fileHandle = new IXFileHandle();
	rc = im->openFile(getIndexFileName(tableName, attributeName), *rm_IndexScanIterator.ix_si.fileHandle);
	if (rc) return rc;

	rc = im->scan(*rm_IndexScanIterator.ix_si.fileHandle, recordDescriptor[attriIndex], lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_si);
	if(rc) return rc;

	return SUCCESS;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
	return ix_si.getNextEntry(rid, key);
}  

RC RM_IndexScanIterator::close() {
	IndexManager *im = IndexManager::instance();

	RC rc = ix_si.close();
	if (rc) return rc;

	rc = im->closeFile(*ix_si.fileHandle);
	if (rc) return rc;

	return SUCCESS;
}
