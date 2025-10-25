#ifndef PAGE_REC_ITER_CC
#define PAGE_REC_ITER_CC

#include "MyDB_PageRecIterator.h"
#include "MyDB_PageReaderWriter.h"

MyDB_PageRecIterator::MyDB_PageRecIterator(MyDB_PageHandle page, MyDB_RecordPtr iterateIntoMe) 
    : myPage(page), myRecord(iterateIntoMe), currentIndex(0) {
    
    if (myPage == nullptr) {
        totalRecords = 0;
        currentPos = nullptr;
        return;
    }
    
    // Read page header to get total records and data start position
    struct PageHeader {
        int pageType;
        int recordCount;
        size_t offset;
        char bytes[0];
    };

    PageHeader* header = (PageHeader*)myPage->getBytes();
    totalRecords = header->recordCount; 
    // Set current position to start of record area using bytes[0]
    currentPos = header->bytes;
}

bool MyDB_PageRecIterator::hasNext() {
    return currentIndex < totalRecords;
}

void MyDB_PageRecIterator::getNext() {
    if (!hasNext() || currentPos == nullptr) {
        return;
    }

    // This line did two things: 1. myRecord->fromBinary(currentPos) reads the record from current position into myRecord
    // 2. the return vallue of myRecord->fromBinary(currentPos) is the new unreached position after reading the record, so we update currentPos
    currentPos = (char*)myRecord->fromBinary(currentPos);
    currentIndex++;
}

#endif