#ifndef TABLE_REC_ITER_CC
#define TABLE_REC_ITER_CC

#include "MyDB_TableRecIterator.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_PageReaderWriter.h"

MyDB_TableRecIterator::MyDB_TableRecIterator(MyDB_TableReaderWriter* table, MyDB_RecordPtr iterateIntoMe) 
    : myTable(table), myRecord(iterateIntoMe), currentPage(0) {
    
    if (myTable == nullptr) {
        cout << "DEBUG: TableRecIterator - table is nullptr" << endl;
        return;
    }
    
    lastPage = myTable->getLastPageIndex();
    
    // Initialize iterator for the first page
    if (currentPage <= lastPage) {
        MyDB_PageReaderWriter pageRW = (*myTable)[currentPage];
        currentPageIterator = pageRW.getIterator(myRecord);
    } else {
        currentPageIterator = nullptr;
    }
}

bool MyDB_TableRecIterator::hasNext() {
    // Check if current page iterator has records
    if (currentPageIterator != nullptr && currentPageIterator->hasNext()) {
        return true;
    }

    // Check the following pages
    int checkPage = currentPage + 1;
    while (checkPage <= lastPage) { // use while because it is possible to have empty pages in between
        MyDB_PageReaderWriter tempPageRW = (*myTable)[checkPage];
        MyDB_RecordIteratorPtr tempIterator = tempPageRW.getIterator(myRecord);
        
        if (tempIterator != nullptr && tempIterator->hasNext()) {
            return true;
        }
        checkPage++;
    }
    
    return false;
}

void MyDB_TableRecIterator::getNext() {
    if (currentPageIterator != nullptr && currentPageIterator->hasNext()) {
        currentPageIterator->getNext();
        return;
    }
    
    // Current page is exhausted, move to next page with records
    while (currentPage <= lastPage) {
        currentPage++;
        if (currentPage <= lastPage) {
            MyDB_PageReaderWriter pageRW = (*myTable)[currentPage];
            currentPageIterator = pageRW.getIterator(myRecord);
            
            if (currentPageIterator != nullptr && currentPageIterator->hasNext()) {
                currentPageIterator->getNext();
                return;
            }
        }
    }
}

#endif