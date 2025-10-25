
#ifndef TABLE_RW_C
#define TABLE_RW_C

#include <fstream>
#include <sstream>
#include <iostream>
#include <string>
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_TableRecIterator.h"

using namespace std;

MyDB_TableReaderWriter :: MyDB_TableReaderWriter (MyDB_TablePtr forMe, MyDB_BufferManagerPtr myBuffer) 
    : myTable(forMe), myBufferManager(myBuffer) {
}

MyDB_PageReaderWriter MyDB_TableReaderWriter :: operator [] (size_t i) {
    // Create pages if accessing beyond current range (users may access pages out of the range of lastPage)
    while (i > (size_t)getLastPageIndex())
    {
        myTable->setLastPage(myTable->lastPage() + 1);
        MyDB_PageHandle pageHandle = myBufferManager->getPage(myTable, myTable->lastPage());
        MyDB_PageReaderWriter newPage(pageHandle, myBufferManager->getPageSize(), true);
    }

    MyDB_PageHandle pageHandle = myBufferManager->getPage(myTable, i);
    // this is reading an existing page
    return MyDB_PageReaderWriter(pageHandle, myBufferManager->getPageSize());
}

MyDB_RecordPtr MyDB_TableReaderWriter :: getEmptyRecord () {
	if (myTable == nullptr)
		return nullptr;
	return make_shared<MyDB_Record>(myTable->getSchema());
}

MyDB_PageReaderWriter MyDB_TableReaderWriter :: last () {
    // Initialize first page if table is new
    if (myTable->lastPage() == -1) {
        myTable->setLastPage(0);
        MyDB_PageHandle pageHandle = myBufferManager->getPage(myTable, 0);
        MyDB_PageReaderWriter firstPage(pageHandle, myBufferManager->getPageSize(), true); // true = new page
    }
    
    int lastPageIndex = getLastPageIndex();
    MyDB_PageHandle pageHandle = myBufferManager->getPage(myTable, lastPageIndex);
    return MyDB_PageReaderWriter(pageHandle, myBufferManager->getPageSize());
}


void MyDB_TableReaderWriter :: append (MyDB_RecordPtr appendMe) {
    if (appendMe == nullptr) {
        cout << "append() called with nullptr record" << endl;
        return;
    }
    
    // Try to append to the last page
    MyDB_PageReaderWriter lastPage = last();
    
    bool success = lastPage.append(appendMe);
    // cout << "DEBUG: append() to page " << getLastPageIndex() << ", success: " << success << endl;
    
    if (!success) {
        // Last page is full, create a new page
        myTable->setLastPage(myTable->lastPage() + 1);
        // cout << "DEBUG: Creating new page " << myTable->lastPage() << endl;
        MyDB_PageHandle pageHandle = myBufferManager->getPage(myTable, myTable->lastPage());
        MyDB_PageReaderWriter newPage(pageHandle, myBufferManager->getPageSize(), true);
        bool newSuccess = newPage.append(appendMe);
        // cout << "DEBUG: append() to new page, success: " << newSuccess << endl;
    }
}

void MyDB_TableReaderWriter :: loadFromTextFile (string fromMe) {
    ifstream file(fromMe);
    if (!file.is_open()) {
        cout << "Could not open file: " << fromMe << endl;
        return;
    }
    
    int lineCount = 0;
    string line;
    while (getline(file, line)) {
        if (line.empty()) 
            continue;
        
        lineCount++;
        MyDB_RecordPtr record = getEmptyRecord();
        if (record != nullptr) {
            record->fromString(line);
            append(record);
        }
        
    }
    
    // cout << "DEBUG: Total lines loaded: " << lineCount << ", Last page index: " << getLastPageIndex() << endl;
    file.close();
}

MyDB_RecordIteratorPtr MyDB_TableReaderWriter :: getIterator (MyDB_RecordPtr iterateIntoMe) {
    return make_shared<MyDB_TableRecIterator>(this, iterateIntoMe);
}

void MyDB_TableReaderWriter :: writeIntoTextFile (string toMe) {
    ofstream file(toMe);
    if (!file.is_open()) {
        cerr << "Could not create file: " << toMe << endl;
        return;
    }
    
    MyDB_RecordPtr record = getEmptyRecord();
    MyDB_RecordIteratorPtr iterator = getIterator(record);
    
    while (iterator->hasNext()) {
        iterator->getNext();
        file << record << endl;
    }
    
    file.close();
}

int MyDB_TableReaderWriter::getLastPageIndex() {
    return myTable->lastPage();
}

#endif

