#include <iostream>

#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
  char file[fileName.size() + 1];
  memset(file, 0, fileName.size() + 1);
  fileName.copy(file, fileName.size() + 1);
  FILE * pf;
  // subspecifier (wx) forces the function to fail if the file exists.
  pf = fopen(file, "wbx");
  // error: file already exist, or cannot create file return -1
  if (pf == NULL) return -1;
  else { fclose(pf); return 0; }
    return -1; // default return
}


RC PagedFileManager::destroyFile(const string &fileName)
{
  char file[fileName.size() + 1];
  memset(file, 0, fileName.size() + 1);
  fileName.copy(file, fileName.size() + 1);
  if (remove(file) != 0) return -1;
  else return 0;
    return -1; // default return
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
  FILE * pf;
  char file[fileName.size() + 1];
  memset(file, 0, fileName.size() + 1);
  fileName.copy(file, fileName.size() + 1);
  if(fileHandle.pf_existence()){ // if fileHandle already exists, error occurs
    return -1;
  }
  if((pf = fopen(file, "r+b"))){
    fileHandle.setpf(pf);
    return 0; // open file successfully
  }else{
    return -1; // fail to open file
  }
    return -1; // default return
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
  if(fclose(fileHandle.getpf()) == 0){
    return 0; // close file successfully
  }else{
    return -1; // fail to close file
  }
    return -1; // default return
}


FileHandle::FileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
  pf = NULL;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
  if(!fseek(pf, pageNum * PAGE_SIZE, SEEK_SET) && (fread(data, 1, PAGE_SIZE, pf) == PAGE_SIZE)){
    readPageCounter++;
    return 0;
  }else{
    return -1;
  }
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
  if(!fseek(pf,  pageNum * PAGE_SIZE, SEEK_SET) && (fwrite(data, 1, PAGE_SIZE, pf) == PAGE_SIZE)){
    writePageCounter++;
    return 0;
  }else{
    return -1;
  }
}


RC FileHandle::appendPage(const void *data)
{
  if(!fseek(pf, 0, SEEK_END) && (fwrite(data, 1, PAGE_SIZE, pf) == PAGE_SIZE)){
    appendPageCounter++;
    return 0;
  }else{
    return -1;
  }
}


unsigned FileHandle::getNumberOfPages()
{
  fseek(pf, 0, SEEK_END);
  unsigned file_size = ftell(pf);
  return ((file_size + PAGE_SIZE - 1) / PAGE_SIZE); // return ceil(f_s/P_S)
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
  readPageCount = this->readPageCounter;
  writePageCount = this->writePageCounter;
  appendPageCount = this->appendPageCounter;
  return 0;
}
