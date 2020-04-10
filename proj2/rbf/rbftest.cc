#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <stdio.h>
#include <fstream>

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

// Check if a file exists
bool FileExists(string fileName)
{
    struct stat stFileInfo;

    if (stat(fileName.c_str(), &stFileInfo) == 0)
        return true;
    else
        return false;
}

// Function to prepare the data in the correct form to be inserted/read
void prepareRecord(const int nameLength, const string &name, const int age, const float height, const int salary, void *buffer, int *recordSize)
{
    int offset = 0;

    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;

    memcpy((char *)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);

    memcpy((char *)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);

    *recordSize = offset;
}

void prepareLargeRecord(const int index, void *buffer, int *size)
{
    int offset = 0;

    // compute the count
    int count = index % 50 + 1;

    // compute the letter
    char text = index % 26 + 97;

    for (int i = 0; i < 10; i++)
    {
        memcpy((char *)buffer + offset, &count, sizeof(int));
        offset += sizeof(int);

        for (int j = 0; j < count; j++)
        {
            memcpy((char *)buffer + offset, &text, 1);
            offset += 1;
        }

        // compute the integer
        memcpy((char *)buffer + offset, &index, sizeof(int));
        offset += sizeof(int);

        // compute the floating number
        float real = (float)(index + 1);
        memcpy((char *)buffer + offset, &real, sizeof(float));
        offset += sizeof(float);
    }
    *size = offset;
}

int RBFTest_1(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Create File
    cout << "****In RBF Test Case 1****" << endl;

    RC rc;
    string fileName = "test";

    // Create a file named "test"
    rc = pfm->createFile(fileName.c_str());
    assert(rc == success);

    if (FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl
             << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case 1 Failed!" << endl
             << endl;
        return -1;
    }

    // Create "test" again, should fail
    rc = pfm->createFile(fileName.c_str());
    assert(rc != success);

    cout << "Test Case 1 Passed!" << endl
         << endl;
    return 0;
}

int RBFTest_2(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Destroy File
    cout << "****In RBF Test Case 2****" << endl;

    RC rc;
    string fileName = "test";

    rc = pfm->destroyFile(fileName.c_str());
    assert(rc == success);

    if (!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl
             << endl;
        cout << "Test Case 2 Passed!" << endl
             << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        cout << "Test Case 2 Failed!" << endl
             << endl;
        return -1;
    }
}

int RBFTest_3(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Create File
    // 2. Open File
    // 3. Get Number Of Pages
    // 4. Close File
    cout << "****In RBF Test Case 3****" << endl;
    cout << "test1" << endl;
    RC rc;
    string fileName = "test_1";

    // Create a file named "test_1"
    rc = pfm->createFile(fileName.c_str());
    assert(rc == success);

    if (FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case 3 Failed!" << endl
             << endl;
        return -1;
    }

    // Open the file "test_1"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Get the number of pages in the test file
    unsigned count = fileHandle.getNumberOfPages();
    assert(count == (unsigned)0);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);

    cout << "Test Case 3 Passed!" << endl
         << endl;

    return 0;
}

int RBFTest_4(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Open File
    // 2. Append Page
    // 3. Get Number Of Pages
    // 3. Close File
    cout << "****In RBF Test Case 4****" << endl;

    RC rc;
    string fileName = "test_1";

    // Open the file "test_1"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Append the first page
    void *data = malloc(PAGE_SIZE);
    for (unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data + i) = i % 94 + 32;
    }
    rc = fileHandle.appendPage(data);
    assert(rc == success);

    // Get the number of pages
    unsigned count = fileHandle.getNumberOfPages();
    assert(count == (unsigned)1);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);

    free(data);

    cout << "Test Case 4 Passed!" << endl
         << endl;

    return 0;
}

int RBFTest_5(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Open File
    // 2. Read Page
    // 3. Close File
    cout << "****In RBF Test Case 5****" << endl;

    RC rc;
    string fileName = "test_1";

    // Open the file "test_1"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Read the first page
    void *buffer = malloc(PAGE_SIZE);
    rc = fileHandle.readPage(0, buffer);
    assert(rc == success);

    // Check the integrity of the page
    void *data = malloc(PAGE_SIZE);
    for (unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data + i) = i % 94 + 32;
    }
    rc = memcmp(data, buffer, PAGE_SIZE);
    assert(rc == success);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);

    free(data);
    free(buffer);

    cout << "Test Case 5 Passed!" << endl
         << endl;

    return 0;
}

int RBFTest_6(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Open File
    // 2. Write Page
    // 3. Read Page
    // 4. Close File
    // 5. Destroy File
    cout << "****In RBF Test Case 6****" << endl;

    RC rc;
    string fileName = "test_1";

    // Open the file "test_1"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Update the first page
    void *data = malloc(PAGE_SIZE);
    for (unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data + i) = i % 10 + 32;
    }
    rc = fileHandle.writePage(0, data);
    assert(rc == success);

    // Read the page
    void *buffer = malloc(PAGE_SIZE);
    rc = fileHandle.readPage(0, buffer);
    assert(rc == success);

    // Check the integrity
    rc = memcmp(data, buffer, PAGE_SIZE);
    assert(rc == success);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);

    free(data);
    free(buffer);

    // Destroy File
    rc = pfm->destroyFile(fileName.c_str());
    assert(rc == success);

    if (!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl;
        cout << "Test Case 6 Passed!" << endl
             << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        cout << "Test Case 6 Failed!" << endl
             << endl;
        return -1;
    }
}

int RBFTest_7(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Create File
    // 2. Open File
    // 3. Append Page
    // 4. Get Number Of Pages
    // 5. Read Page
    // 6. Write Page
    // 7. Close File
    // 8. Destroy File
    cout << "****In RBF Test Case 7****" << endl;

    RC rc;
    string fileName = "test_2";

    // Create the file named "test_2"
    rc = pfm->createFile(fileName.c_str());
    assert(rc == success);

    if (FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case 7 Failed!" << endl
             << endl;
        return -1;
    }

    // Open the file "test_2"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Append 50 pages
    void *data = malloc(PAGE_SIZE);
    for (unsigned j = 0; j < 50; j++)
    {
        for (unsigned i = 0; i < PAGE_SIZE; i++)
        {
            *((char *)data + i) = i % (j + 1) + 32;
        }
        rc = fileHandle.appendPage(data);
        assert(rc == success);
    }
    cout << "50 Pages have been successfully appended!" << endl;

    // Get the number of pages
    unsigned count = fileHandle.getNumberOfPages();
    assert(count == (unsigned)50);

    // Read the 25th page and check integrity
    void *buffer = malloc(PAGE_SIZE);
    rc = fileHandle.readPage(24, buffer);
    assert(rc == success);

    for (unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data + i) = i % 25 + 32;
    }
    rc = memcmp(buffer, data, PAGE_SIZE);
    assert(rc == success);
    cout << "The data in 25th page is correct!" << endl;

    // Update the 25th page
    for (unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data + i) = i % 60 + 32;
    }
    rc = fileHandle.writePage(24, data);
    assert(rc == success);

    // Read the 25th page and check integrity
    rc = fileHandle.readPage(24, buffer);
    assert(rc == success);

    rc = memcmp(buffer, data, PAGE_SIZE);
    assert(rc == success);

    // Close the file "test_2"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);

    // Destroy File
    rc = pfm->destroyFile(fileName.c_str());
    assert(rc == success);

    free(data);
    free(buffer);

    if (!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl;
        cout << "Test Case 7 Passed!" << endl
             << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        cout << "Test Case 7 Failed!" << endl
             << endl;
        return -1;
    }
}

int RBFTest_8(RecordBasedFileManager *rbfm) 
{
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 8 *****" << endl;
   
    RC rc;
    string fileName = "test8";

    // Create a file named "test8"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file "test8"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");
      
    RID rid; 
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);
    
    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert a record into a file and print the record
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 177.8, 6200, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rbfm->printRecord(recordDescriptor, record);
    
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");
    
    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert(rc == success && "Reading a record should not fail.");

    cout << endl << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor, returnedData);

    // Compare whether the two memory blocks are the same
    if(memcmp(record, returnedData, recordSize) != 0)
    {
        cout << "[FAIL] Test Case 8 Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return -1;
    }
    
    cout << endl;

    // Close the file "test8"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy the file
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");
    
    free(record);
    free(returnedData);

    cout << "RBF Test Case 8 Finished! The result will be examined." << endl << endl;
    
    return 0;
}

int RBFTest_9(RecordBasedFileManager *rbfm, vector<RID> &rids, vector<int> &sizes) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Multiple Records
    // 4. Close Record-Based File
    cout << endl << "***** In RBF Test Case 9 *****" << endl;
   
    RC rc;
    string fileName = "test9";

    // Create a file named "test9"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Open the file "test9"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid; 
    void *record = malloc(1000);
    int numRecords = 2000;

    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);

    for(unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        cout << "Attr Name: " << recordDescriptor[i].name << " Attr Type: " << (AttrType)recordDescriptor[i].type << " Attr Len: " << recordDescriptor[i].length << endl;
    }
    cout << endl;
    
    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert 2000 records into file
    for(int i = 0; i < numRecords; i++)
    {
        // Test insert Record
        int size = 0;
        memset(record, 0, 1000);
        prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, record, &size);

        rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
        assert(rc == success && "Inserting a record should not fail.");

        rids.push_back(rid);
        sizes.push_back(size);        
    }
    // Close the file "test9"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    free(record);
    
    
    // Write RIDs to the disk. Do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    ofstream ridsFile("test9rids", ios::out | ios::trunc | ios::binary);

    if (ridsFile.is_open()) {
        ridsFile.seekp(0, ios::beg);
        for (int i = 0; i < numRecords; i++) {
            ridsFile.write(reinterpret_cast<const char*>(&rids[i].pageNum),
                    sizeof(unsigned));
            ridsFile.write(reinterpret_cast<const char*>(&rids[i].slotNum),
                    sizeof(unsigned));
            if (i % 1000 == 0) {
                cout << "RID #" << i << ": " << rids[i].pageNum << ", "
                        << rids[i].slotNum << endl;
            }
        }
        ridsFile.close();
    }

    // Write sizes vector to the disk. Do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    ofstream sizesFile("test9sizes", ios::out | ios::trunc | ios::binary);

    if (sizesFile.is_open()) {
        sizesFile.seekp(0, ios::beg);
        for (int i = 0; i < numRecords; i++) {
            sizesFile.write(reinterpret_cast<const char*>(&sizes[i]),sizeof(int));
            if (i % 1000 == 0) {
                cout << "Sizes #" << i << ": " << sizes[i] << endl;
            }
        }
        sizesFile.close();
    }
        
    cout << "RBF Test Case 9 Finished! The result will be examined." << endl << endl;

    return 0;
}

int RBFTest_10(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Open Record-Based File
    // 2. Read Multiple Records
    // 3. Close Record-Based File
    // 4. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 10 *****" << endl;
   
    RC rc;
    string fileName = "test9";

    // Open the file "test9"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");
    
    int numRecords = 2000;
    void *record = malloc(1000);
    void *returnedData = malloc(1000);

    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);

    vector<RID> rids;
    vector<int> sizes;
    RID tempRID;

    // Read rids from the disk - do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    ifstream ridsFileRead("test9rids", ios::in | ios::binary);

    unsigned pageNum;
    unsigned slotNum;

    if (ridsFileRead.is_open()) {
        ridsFileRead.seekg(0,ios::beg);
        for (int i = 0; i < numRecords; i++) {
            ridsFileRead.read(reinterpret_cast<char*>(&pageNum), sizeof(unsigned));
            ridsFileRead.read(reinterpret_cast<char*>(&slotNum), sizeof(unsigned));
            if (i % 1000 == 0) {
                cout << "loaded RID #" << i << ": " << pageNum << ", " << slotNum << endl;
            }
            tempRID.pageNum = pageNum;
            tempRID.slotNum = slotNum;
            rids.push_back(tempRID);
        }
        ridsFileRead.close();
    }

    assert(rids.size() == (unsigned) numRecords && "Reading records should not fail.");

    // Read sizes vector from the disk - do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    ifstream sizesFileRead("test9sizes", ios::in | ios::binary);

    int tempSize;
    
    if (sizesFileRead.is_open()) {
        sizesFileRead.seekg(0,ios::beg);
        for (int i = 0; i < numRecords; i++) {
            sizesFileRead.read(reinterpret_cast<char*>(&tempSize), sizeof(int));
            if (i % 1000 == 0) {
                cout << "loaded Sizes #" << i << ": " << tempSize << endl;
            }
            sizes.push_back(tempSize);
        }
        sizesFileRead.close();
    }

    assert(sizes.size() == (unsigned) numRecords && "Reading records should not fail.");

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    for(int i = 0; i < numRecords; i++)
    {
        memset(record, 0, 1000);
        memset(returnedData, 0, 1000);
        rc = rbfm->readRecord(fileHandle, recordDescriptor, rids[i], returnedData);
        assert(rc == success && "Reading a record should not fail.");
        
        if (i % 1000 == 0) {
            cout << endl << "Returned Data:" << endl;
            rbfm->printRecord(recordDescriptor, returnedData);
        }

        int size = 0;
        prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, record, &size);
        if(memcmp(returnedData, record, sizes[i]) != 0)
        {
            cout << "[FAIL] Test Case 10 Failed!" << endl << endl;
            free(record);
            free(returnedData);
            return -1;
        }
    }
    
    cout << endl;

    // Close the file "test9"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");
    
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");

    free(record);
    free(returnedData);

    cout << "RBF Test Case 10 Finished! The result will be examined." << endl << endl;

    remove("test9sizes");
    remove("test9rids");
    
    return 0;
}

int main()
{
    // To test the functionality of the paged file manager
    PagedFileManager *pfm = PagedFileManager::instance();

    // To test the functionality of the record-based file manager
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    // Remove files that might be created by previous test run
    remove("test");
    remove("test_1");
    remove("test_2");
    remove("test_3");
    remove("test_4");

    remove("test8");
    remove("test9");
    remove("test9rids");
    remove("test9sizes");

    RBFTest_1(pfm);
    RBFTest_2(pfm);
    RBFTest_3(pfm);
    RBFTest_4(pfm);
    RBFTest_5(pfm);
    RBFTest_6(pfm);
    RBFTest_7(pfm);
  
    RBFTest_8(rbfm);

    vector<RID> rids;
    vector<int> sizes;
    RBFTest_9(rbfm, rids, sizes);
    RBFTest_10(rbfm);
    
    return 0;
}
