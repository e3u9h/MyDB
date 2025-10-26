
#ifndef TABLE_RW_C
#define TABLE_RW_C

#include <fstream>
#include <sstream>
#include <iostream>
#include <string>
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"

using namespace std;

MyDB_TableReaderWriter :: MyDB_TableReaderWriter (MyDB_TablePtr forMe, MyDB_BufferManagerPtr myBuffer) 
    : myTable(forMe), myBufferManager(myBuffer) {
    // Initialize first page if table is new
    if (myTable->lastPage() == -1)
    {
        // cout << "22DEBUG: Initializing first page" << endl;
        myTable->setLastPage(0);
        MyDB_PageHandle pageHandle = myBufferManager->getPage(myTable, 0);
        MyDB_PageReaderWriter firstPage(pageHandle, myBufferManager->getPageSize(), true); // true = new page
    }
}

MyDB_BufferManagerPtr MyDB_TableReaderWriter ::getBufferMgr()
{
    return myBufferManager;
}

MyDB_TablePtr MyDB_TableReaderWriter ::getTable()
{
    return myTable;
}

int MyDB_TableReaderWriter ::getNumPages()
{
    // cout << "DEBUG: getNumPages() called, lastPage = " << myTable->lastPage() << endl;
    return myTable->lastPage() + 1;
}

MyDB_PageReaderWriter MyDB_TableReaderWriter ::getPinned(size_t i)
{
    return MyDB_PageReaderWriter(true, *this, i);
}

MyDB_PageReaderWriter MyDB_TableReaderWriter :: operator [] (size_t i) {
    // Create pages if accessing beyond current range (users may access pages out of the range of lastPage)
    while (i > (size_t)getLastPageIndex())
    {
        // cout << "11DEBUG: Creating new page " << myTable->lastPage() + 1 << endl;
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
        // cout << "22DEBUG: Initializing first page" << endl;
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
        // cout << "33DEBUG: Last page full, creating new page" << endl;
        myTable->setLastPage(myTable->lastPage() + 1);
        // cout << "DEBUG: Creating new page " << myTable->lastPage() << endl;
        MyDB_PageHandle pageHandle = myBufferManager->getPage(myTable, myTable->lastPage());
        MyDB_PageReaderWriter newPage(pageHandle, myBufferManager->getPageSize(), true);
        bool newSuccess = newPage.append(appendMe);
        // cout << "DEBUG: append() to new page, success: " << newSuccess << endl;
    }
}

// void MyDB_TableReaderWriter :: loadFromTextFile (string fromMe) {
//     ifstream file(fromMe);
//     if (!file.is_open()) {
//         cout << "Could not open file: " << fromMe << endl;
//         return;
//     }

//     int lineCount = 0;
//     string line;
//     while (getline(file, line)) {
//         if (line.empty())
//             continue;

//         lineCount++;
//         MyDB_RecordPtr record = getEmptyRecord();
//         if (record != nullptr) {
//             record->fromString(line);
//             append(record);
//         }

//     }

//     // cout << "DEBUG: Total lines loaded: " << lineCount << ", Last page index: " << getLastPageIndex() << endl;
//     file.close();
// }
// instructor's version of loadFromTextFile, with distinct value counting
pair<vector<size_t>, size_t> MyDB_TableReaderWriter ::loadFromTextFile(string fName)
{

    // empty out the database file
    // cout << "44Loading data from text file: " << fName << endl;
    myTable->setLastPage(0);
    MyDB_PageReaderWriterPtr lastPage = make_shared<MyDB_PageReaderWriter>(*this, myTable->lastPage());
    lastPage->clear();

    // try to open the file
    string line;
    ifstream myfile(fName);

    MyDB_RecordPtr tempRec = getEmptyRecord();

    // this data structure is used for apporoximate counting of the number of distinct
    // values of each attribute
    vector<pair<set<size_t>, int>> allHashes;
    for (int i = 0; i < tempRec->getSchema()->getAtts().size(); i++)
    {
        set<size_t> temp1;
        allHashes.push_back(make_pair(temp1, 1));
    }

    // if we opened it, read the contents
    size_t counter = 0;
    if (myfile.is_open())
    {

        // loop through all of the lines
        while (getline(myfile, line))
        {
            tempRec->fromString(line);
            counter++;
            // hash all of the attributes... this is used for counting
            int i = 0;
            for (auto &a : allHashes)
            {

                // insert the hash
                size_t hash = tempRec->getAtt(i)->hash();
                i++;
                if (hash % a.second != 0)
                    continue;

                a.first.insert(hash);

// if we have too many items, compact them
#define MAX_SIZE 1000
                if (a.first.size() > MAX_SIZE)
                {
                    a.second *= 2;
                    set<size_t> newSet;
                    for (auto &num : a.first)
                    {
                        if (num % a.second == 0)
                            newSet.insert(num);
                    }
                    a.first = newSet;
                }
            }
            append(tempRec);
        }
        myfile.close();
    }
    cout << "Loaded " << counter << " records.\n";

    // finally, compute the vector of estimates
    vector<size_t> returnVal;
    for (auto &a : allHashes)
    {
        size_t est = ((size_t)a.first.size()) * a.second;
        returnVal.push_back(est);
    }
    return make_pair(returnVal, counter);
}

MyDB_RecordIteratorPtr MyDB_TableReaderWriter :: getIterator (MyDB_RecordPtr iterateIntoMe) {
    return make_shared<MyDB_TableRecIterator>(this, iterateIntoMe);
}

MyDB_RecordIteratorAltPtr MyDB_TableReaderWriter ::getIteratorAlt()
{
    return make_shared<MyDB_TableRecIteratorAlt>(*this, myTable);
}

MyDB_RecordIteratorAltPtr MyDB_TableReaderWriter ::getIteratorAlt(int lowPage, int highPage)
{
    return make_shared<MyDB_TableRecIteratorAlt>(*this, myTable, lowPage, highPage);
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

