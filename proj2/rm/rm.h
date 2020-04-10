
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <cstring>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

# define TABLE_DATA_SZ 117 // nullIndicator + 2Int + 2VarChar
# define COLUMN_DATA_SZ 71 // nullIndicator + 5Int + 1VarChar

# define RM_EOF (-1)  // end of a scan operator
# define RM_PREPARE_FAILED      1
# define RM_CREATE_TABLE_FAILED 2
# define RM_DELETE_TABLE_FAILED 3

// Access flag
typedef enum { AccessFlagSystem = 0, AccessFlagUser } AccessFlag;

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) { return RM_EOF; };
  RC close(){return -1;};
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  vector<Attribute> tableDescriptor; // tableDescriptor will not change after init
  vector<Attribute> columnDescriptor; // columnDescriptor will not change after init

  // helper function.
  RC prepareTable(int tbl_id, string &tbl_name, string &file_name, int flag, void *data);
  RC prepareColumn(int tbl_id, string &name, int type, int length, int position, void *data);
};

#endif
