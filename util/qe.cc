
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
	iter = input;
	this->condition = condition;
	attrValue = malloc(PAGE_SIZE);

	attrs.clear();
	input->getAttributes(attrs);
}

int Iterator::getSize(vector<Attribute> attrs, void* data) {
    int offset = ceil(attrs.size() / 8.0);
    size_t i;
    for (i = 0; i < attrs.size(); i++) {
        char target = *((char*)data + i/8);
        if (target & (1<<(7-i%8))) continue;
        if (attrs[i].type == TypeVarChar) {
            int size;
            memcpy(&size, (char*)data + offset, sizeof(int));
            offset += size;
        }
        offset += sizeof(int);
    }
    return offset;
}

RC Iterator::getValue(const string attrName, vector<Attribute> attrs, void* data, void* attrValue, int& attrSize) {
    int offset = ceil(attrs.size() / 8.0);
    for (size_t i = 0; i < attrs.size(); i++) {
        char target = *((char*)data + i/8);
        if (target & (1<<(7 - i%8))) {
            if(attrName == attrs[i].name) return 1;
            else 
                continue;
        }
        attrSize = sizeof(int);
        if (attrs[i].type == TypeVarChar) {
            memcpy(&attrSize, (char*)data + offset, sizeof(int));
            memcpy((char*)attrValue, &attrSize, sizeof(int));
            memcpy((char*)attrValue + sizeof(int), (char*)data + offset + sizeof(int), attrSize);
            attrSize += sizeof(int);
        }
        else 
            memcpy(attrValue, (char*)data + offset, sizeof(int));
        if(attrName == attrs[i].name) 
            return SUCCESS;
        offset += attrSize;
        }
        return 1;
}

RC Filter::getNextTuple(void* data){
	RC rc;
	while((rc = iter->getNextTuple(data)) == SUCCESS){
		//if no condition is specified, return any tuple found
		if(condition.op == NO_OP) break;

		//otherwise retrieve the condition attribute value and verify if condition is met
		int attrSize;
		if(getValue(condition.lhsAttr, attrs, data, attrValue, attrSize))
			continue;
		if(checkCondition(condition.rhsValue.type, attrValue, condition.op, condition.rhsValue.data))
			break;
	}
	return rc;
}

void Filter::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	attrs = this->attrs;
}

bool Filter::checkCondition(AttrType type, void* left, CompOp op, void* right) {			
	switch (type) {

		case TypeVarChar: {
			int32_t valueSize;
			memcpy(&valueSize, left, VARCHAR_LENGTH_SIZE);
			char leftStr[valueSize + 1];
			leftStr[valueSize] = '\0';
			memcpy(leftStr, (char*) left + VARCHAR_LENGTH_SIZE, valueSize);
			return checkCondition(leftStr, op, right);
		}

		case TypeInt: return checkCondition(*(int*)left, op, right);
		case TypeReal: return checkCondition(*(float*)left, op, right);
		default: return false;
	}			
};


//---------------Helper functions for Filter::checkCondition()-----------------//

bool Filter::checkCondition(int recordInt, CompOp compOp, const void *value)
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

bool Filter::checkCondition(float recordReal, CompOp compOp, const void *value)
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

bool Filter::checkCondition(char *recordString, CompOp compOp, const void *value)
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

//-----------------------------------------------------------------------------------//

Project::Project(Iterator *input, const vector<string> &attrNames)
{
	this->attrNames = attrNames;
	iter = input;
	input->getAttributes(attrs);
};

RC Project::getNextTuple(void* data) {

	RC rc;
	void* tuple = malloc(PAGE_SIZE);
	void* value = malloc(PAGE_SIZE);
	int attrSize;

	if( (rc = iter->getNextTuple(tuple)) == SUCCESS) {

		int offset = ceil(attrNames.size()/8.0);
		for (size_t i = 0; i < attrNames.size(); i++) {

			char* null = (char*)data + (char)(i/8);

			//if the projected attribute is null, toggle null bit and move on to next attr
			//otherwise copy attr value into data and fetch next attr value
			if(getValue(attrNames[i], attrs, tuple, value, attrSize)) {
				*null |= (1 << (7-i%8));
			} else {
				*null &= ~(1 << (7-i%8));
				memcpy((char*)data + offset, value, attrSize);
				offset += attrSize;
			}
		}  

	}else{
		free(tuple);
		free(value);
	}
	return rc;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	//only return the attributes in attrs that overlap with the attrNames to be projected
	for (auto &name : this->attrNames){
		for (auto &attr : this->attrs){
			if (attr.name == name)
				attrs.push_back(attr);
		}
	}
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
	outer = leftIn;
	inner = rightIn;
	this->condition = condition;
	outerTuple = malloc(PAGE_SIZE);
	outerValue = malloc(PAGE_SIZE); 
	innerTuple = malloc(PAGE_SIZE);
	outer->getAttributes(outerAttrs);
	inner->getAttributes(innerAttrs);
	nextOuter = true;

	//not-equal matching has two scan phases: lesser than outerValue, and greater than outerValue
	//starts with phase 1 
	NEPhase = NE_LT;
}

RC INLJoin::getNextTuple(void *data){
	int tmp;
	RC rc;
	while (true) {

		//if running for the 1st time or done matching current outer tuple, fetch next outer tuple
		if (nextOuter) {
			if ((rc = outer->getNextTuple(outerTuple)) == SUCCESS){

				//Upon failure to fetch the next outer value, try again in the next cycle
				//otherwise initiate the inner iterator based on the outer value and condition
				if (getValue(condition.lhsAttr, outerAttrs, outerTuple, outerValue, tmp) and condition.op != NO_OP) 
					continue;
				else {
					switch (condition.op) {
						case EQ_OP: inner->setIterator(outerValue, outerValue, true,  true ); break;
						case LT_OP: inner->setIterator(NULL,	   outerValue, true,  false); break; 
						case GT_OP: inner->setIterator(outerValue, NULL,	   false, true ); break;
						case LE_OP: inner->setIterator(NULL	  ,    outerValue, true,  true ); break;
						case GE_OP: inner->setIterator(outerValue, NULL,	   true,  true ); break; 
						case NE_OP: inner->setIterator(NULL,	   outerValue, true,  false); NEPhase = NE_LT; break; 
						case NO_OP: inner->setIterator(NULL,	   NULL,	   true,  true ); break; 
						default: return -1;
					}  
					nextOuter = false;
				}
			}
			//terminate the joining if we run out of outer tuples
			else return rc;
		}

		//Upon running out of inner tuples, if it is in NE scan phase 1, proceed to phase 2 (greater than outerValue)
		//otherwise proceed to the next outer tuple
		if (inner->getNextTuple(innerTuple) == QE_EOF) {
			if (condition.op == NE_OP and NEPhase == NE_LT) {
				NEPhase = NE_GT;
				inner->setIterator(outerValue, NULL, false, true); 
			} else {
				nextOuter = true;
			}
			continue;
		} 

		//neither outer or inner reaches EOF, break and combine tuple results
		break;
	}

	return joinTuples(outerAttrs, outerTuple, innerAttrs, innerTuple, data);
}

RC INLJoin::joinTuples(vector<Attribute> outerAttrs, void* outerTuple, vector<Attribute> innerAttrs,
	void* innerTuple, void* output) {
        
    int outerNullSize = ceil(outerAttrs.size() / 8.0);
    int innerNullSize = ceil(innerAttrs.size() / 8.0);
    int totNullSize   = ceil((outerAttrs.size() + innerAttrs.size()) / 8.0);
    memcpy(output, outerTuple, outerNullSize);
    
    size_t i, j;
    for (i = 0; i < innerAttrs.size(); i++) {
        j = i + outerAttrs.size();
        char* target = (char*)output + (j/8);
        char origin = *((char*)innerTuple + i/8);
        if (origin & (1<<(7-i%8))) {
            *target |= (1<<(7-j%8));
        }
        else {
            *target &= ~(1<<(7-j%8));
        }
    } 
    
    int outerSize = getSize(outerAttrs, outerTuple) - outerNullSize;
    int innerSize = getSize(innerAttrs, innerTuple) - innerNullSize;
    memcpy((char*)output + totNullSize, (char*)outerTuple + outerNullSize, outerSize);
    memcpy((char*)output + totNullSize + outerSize, (char*)innerTuple + innerNullSize, innerSize);
	
    return SUCCESS;
}

 void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	for (auto &attr : outerAttrs)
		attrs.push_back(attr);
	for (auto &attr : innerAttrs)
		attrs.push_back(attr);
}
