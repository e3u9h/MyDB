
#ifndef PAGE_RW_C
#define PAGE_RW_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageRecIterator.h"
#include "MyDB_PageHandle.h"
#include "MyDB_Record.h"
#include "MyDB_RecordIterator.h"
#include <cstring>

// Constructor
MyDB_PageReaderWriter::MyDB_PageReaderWriter(MyDB_PageHandle page, size_t pageSize, bool isNewPage) 
    : myPage(page), pageSize(pageSize) {
    // Only clear if this is explicitly a new page
    if (isNewPage && myPage != nullptr) {
        clear();
    }
}

void MyDB_PageReaderWriter :: clear () {
    PageHeader *header = (PageHeader *)myPage->getBytes();
    header->pageType = RegularPage;
    header->recordCount = 0;
    header->offset = 0;

    myPage->wroteBytes();
}

MyDB_PageType MyDB_PageReaderWriter :: getType () {
    PageHeader *header = (PageHeader *)myPage->getBytes();
    return header->pageType;
}

MyDB_RecordIteratorPtr MyDB_PageReaderWriter :: getIterator (MyDB_RecordPtr iterateIntoMe) {
    return std::make_shared<MyDB_PageRecIterator>(myPage, iterateIntoMe);
}

void MyDB_PageReaderWriter :: setType (MyDB_PageType toMe) {
    PageHeader *header = (PageHeader *)myPage->getBytes();
    header->pageType = toMe;
    myPage->wroteBytes();
}

bool MyDB_PageReaderWriter :: append (MyDB_RecordPtr appendMe) {
    size_t recordSize = appendMe->getBinarySize();
    PageHeader *header = (PageHeader *)myPage->getBytes();

    // Check if we have enough space
    size_t availableSpace = pageSize - sizeof(PageHeader) - header->offset;
    if (availableSpace < recordSize)
    {
        return false;
    }

    // Write the record at the current offset
    char *writePos = header->bytes + header->offset;
    appendMe->toBinary(writePos);

    header->recordCount++;
    header->offset += recordSize;

    myPage->wroteBytes();
    return true;
}

#endif
