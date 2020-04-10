
#include "qe.h"
#include <cstring>
#include <math.h>
#include <iostream>

// --------------------------------Filter--------------------------------
Filter::Filter(Iterator* input, const Condition &condition)
{
  iter = input;
  this->condition = condition;
  attrs.clear();
  input->getAttributes(attrs);
}

RC Filter::getNextTuple(void* data)
{
  RelationManager *rm = RelationManager::instance();
  RC rc;
  while ((rc = iter->getNextTuple(data)) == SUCCESS) {
    if (condition.op == NO_OP)
      break;
    void* value = malloc(PAGE_SIZE);
    if (rm->getFieldFromRecord(condition.lhsAttr, attrs, data, value) != SUCCESS)
      continue;
    if (checkScanCondition(condition.rhsValue.type, value, condition.op, condition.rhsValue.data))
      break;
  }
  return rc;
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	attrs = this->attrs;
}

bool Filter::checkScanCondition(AttrType type, void* data, CompOp compOp, void* value)
{
  bool result;
  if (type == TypeInt)
  {
      int32_t recordInt;
      memcpy(&recordInt, (char*)data, INT_SIZE);
      result = checkScanCondition(recordInt, compOp, value);
  }
  else if (type == TypeReal)
  {
      float recordReal;
      memcpy(&recordReal, (char*)data, REAL_SIZE);
      result = checkScanCondition(recordReal, compOp, value);
  }
  else if (type == TypeVarChar)
  {
      uint32_t varcharSize;
      memcpy(&varcharSize, (char*)data, VARCHAR_LENGTH_SIZE);
      char recordString[varcharSize + 1];
      memcpy(recordString, (char*)data + VARCHAR_LENGTH_SIZE, varcharSize);
      recordString[varcharSize] = '\0';

      result = checkScanCondition(recordString, compOp, value);
  }
  return result;
}

bool Filter::checkScanCondition(int recordInt, CompOp compOp, const void *value)
{
    int32_t intValue;
    memcpy (&intValue, value, INT_SIZE);

    switch (compOp)
    {
        case EQ_OP: return recordInt == intValue;
        case LT_OP: return recordInt < intValue;
        case GT_OP: return recordInt > intValue;
        case LE_OP: return recordInt <= intValue;
        case GE_OP: return recordInt >= intValue;
        case NE_OP: return recordInt != intValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool Filter::checkScanCondition(float recordReal, CompOp compOp, const void *value)
{
    float realValue;
    memcpy (&realValue, value, REAL_SIZE);

    switch (compOp)
    {
        case EQ_OP: return recordReal == realValue;
        case LT_OP: return recordReal < realValue;
        case GT_OP: return recordReal > realValue;
        case LE_OP: return recordReal <= realValue;
        case GE_OP: return recordReal >= realValue;
        case NE_OP: return recordReal != realValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool Filter::checkScanCondition(char *recordString, CompOp compOp, const void *value)
{
    if (compOp == NO_OP)
        return true;

    int32_t valueSize;
    memcpy(&valueSize, value, VARCHAR_LENGTH_SIZE);
    char valueStr[valueSize + 1];
    valueStr[valueSize] = '\0';
    memcpy(valueStr, (char*) value + VARCHAR_LENGTH_SIZE, valueSize);

    int cmp = strcmp(recordString, valueStr);
    switch (compOp)
    {
        case EQ_OP: return cmp == 0;
        case LT_OP: return cmp <  0;
        case GT_OP: return cmp >  0;
        case LE_OP: return cmp <= 0;
        case GE_OP: return cmp >= 0;
        case NE_OP: return cmp != 0;
        // Should never happen
        default: return false;
    }
}

// --------------------------------Project------------------------------
Project::Project(Iterator *input, const vector<string> &attrNames)
{
	this->attrNames = attrNames;
	iter = input;
  attrs.clear();
	input->getAttributes(attrs);
}

RC Project::getNextTuple(void *data)
{
  RC rc;
  RelationManager *rm = RelationManager::instance();

  void *page  = malloc(PAGE_SIZE);
  void *value = malloc(PAGE_SIZE);

  if ((rc = iter->getNextTuple(page)) == SUCCESS) {
    int offset = ceil(attrNames.size()/8.0);
    memset(data, 0, offset);
    int nullIndicatorOffset = 0;
    // Tranverse all attrNames to get their value
    for (size_t i = 0; i < attrNames.size(); ++i) {
      if ((rc = rm->getFieldFromRecord(attrNames[i], attrs, page, value)) == SUCCESS)
      // SUCCESS means there is data
      {
        // Update the nullIndicator
        nullIndicatorOffset++;

        // Copy the value into the data
        if (attrs[i].type == TypeVarChar) {
          int length = 0;
          memcpy(&length, value, VARCHAR_LENGTH_SIZE);
          memcpy((char *)data + offset, value, VARCHAR_LENGTH_SIZE + length);
          offset += VARCHAR_LENGTH_SIZE + length;
        } else {
          memcpy((char *)data + offset, value, INT_SIZE);
          offset += INT_SIZE;
        }
      }
      // FAIL means there is null
      else
      {
        // Update the nullIndicator
        *(char*)data |= (1 << nullIndicatorOffset);
        nullIndicatorOffset++;
      }
    }
  }

  free(page);
  free(value);
  return rc;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	for (auto &name : this->attrNames){
		for (auto &attr : this->attrs){
			if (attr.name == name)
				attrs.push_back(attr);
		}
	}
}


// --------------------------------INLJoin------------------------------
INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition)
{
	outer = leftIn;
	inner = rightIn;
	this->condition = condition;
  outerAttrs.clear();
  innerAttrs.clear();
	outer->getAttributes(outerAttrs);
	inner->getAttributes(innerAttrs);
}

RC INLJoin::getNextTuple(void *data)
{
  RC rc;
  RelationManager *rm = RelationManager::instance();

  void* outerRelation = calloc(PAGE_SIZE, 1);
  void* innerRelation = calloc(PAGE_SIZE, 1);
  void* value = calloc(PAGE_SIZE, 1);
  while ((rc = outer->getNextTuple(outerRelation)) == SUCCESS) {
    // if outer iter have value in this field, set inner iter
    // otherwise, continue to next loop
    if ((rm->getFieldFromRecord(condition.lhsAttr, outerAttrs, outerRelation, value)) == SUCCESS) {
      inner->setIterator(value, value, true, true);
    }
    else{
      continue;
    }

    // read inner tuple.
    if ((rc = inner->getNextTuple(innerRelation)) == SUCCESS) {
      break;
    }
  }

  // join two tuple.
  int outerNullIndicatorSize = ceil(outerAttrs.size() / 8.0);
  int innerNullIndicatorSize = ceil(innerAttrs.size() / 8.0);
  int sumNullIndicatorSize   = ceil((outerAttrs.size() + innerAttrs.size()) / 8.0);
  memcpy(data, outerRelation, outerNullIndicatorSize);

  // Calculate the sizes of outer and inner tuples
  int outerSize = 0, innerSize = 0;;
  for (size_t i = 0; i < outerAttrs.size(); ++i) {
    char nullIndicator = *((char*)outerRelation + i/8);
    if (nullIndicator & (1<<(7-i%8))) {
      continue;
    }

    if (outerAttrs[i].type == TypeInt) {
      outerSize += sizeof(int);
    } else if (outerAttrs[i].type == TypeReal) {
      outerSize += sizeof(int);
    }
    else if (outerAttrs[i].type == TypeVarChar) {
        int size;
        memcpy(&size, (char*)outerRelation + outerSize, sizeof(int));
        outerSize += size + sizeof(int);
    }
  }

  for (size_t i = 0; i < innerAttrs.size(); ++i) {
    char nullIndicator = *((char*)innerRelation + i/8);
    if (nullIndicator & (1<<(7-i%8))) {
      continue;
    }

    if (innerAttrs[i].type == TypeInt) {
      innerSize += sizeof(int);
    } else if (innerAttrs[i].type == TypeReal) {
      innerSize += sizeof(int);
    } else if (innerAttrs[i].type == TypeVarChar) {
        int size;
        memcpy(&size, (char*)innerRelation + innerSize, sizeof(int));
        innerSize += size + sizeof(int);
    }
  }

  // Join the two nullIndicators
  for (size_t i = 0, j = 0; i < innerAttrs.size(); i++) {
      j = i + outerAttrs.size();
      char* nullIndicator = (char*)data + (j/8);
      char origin = *((char*)innerRelation + i/8);
      if (origin & (1<<(7-i%8))) {
          *nullIndicator |= (1<<(7-j%8));
      }
      else {
          *nullIndicator &= ~(1<<(7-j%8));
      }
  }

  // Join data of two tuples
  memcpy((char*)data + sumNullIndicatorSize, (char*)outerRelation + outerNullIndicatorSize, outerSize);
  memcpy((char*)data + sumNullIndicatorSize + outerSize, (char*)innerRelation + innerNullIndicatorSize, innerSize);

  free(outerRelation);
  free(innerRelation);
  free(value);
  return rc;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
 attrs.clear();
 for (auto &attr : outerAttrs)
   attrs.push_back(attr);
 for (auto &attr : innerAttrs)
   attrs.push_back(attr);
}
