
#ifndef PAGE_RW_C
#define PAGE_RW_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageRecIterator.h"
#include "MyDB_PageHandle.h"
#include "MyDB_Record.h"
#include "MyDB_RecordIterator.h"
#include <cstring>
#include <algorithm>
#include "MyDB_PageRecIteratorAlt.h"
#include "MyDB_PageListIteratorAlt.h"
#include "RecordComparator.h"

// Constructor (my version)
MyDB_PageReaderWriter::MyDB_PageReaderWriter(MyDB_PageHandle page, size_t pageSize, bool isNewPage) 
    : myPage(page), pageSize(pageSize) {
    // Only clear if this is explicitly a new page
    if (isNewPage && myPage != nullptr) {
        clear();
    }
}
// consstructons in the model implementation.
MyDB_PageReaderWriter ::MyDB_PageReaderWriter(MyDB_TableReaderWriter &parent, int whichPage)
{

    // get the actual page
    myPage = parent.getBufferMgr()->getPage(parent.getTable(), whichPage);
    pageSize = parent.getBufferMgr()->getPageSize();
}

MyDB_PageReaderWriter ::MyDB_PageReaderWriter(bool pinned, MyDB_TableReaderWriter &parent, int whichPage)
{

    // get the actual page
    if (pinned)
    {
        myPage = parent.getBufferMgr()->getPinnedPage(parent.getTable(), whichPage);
    }
    else
    {
        myPage = parent.getBufferMgr()->getPage(parent.getTable(), whichPage);
    }
    pageSize = parent.getBufferMgr()->getPageSize();
}

MyDB_PageReaderWriter ::MyDB_PageReaderWriter(MyDB_BufferManager &parent)
{
    myPage = parent.getPage();
    pageSize = parent.getPageSize();
    clear();
}

MyDB_PageReaderWriter ::MyDB_PageReaderWriter(bool pinned, MyDB_BufferManager &parent)
{

    if (pinned)
    {
        myPage = parent.getPinnedPage();
    }
    else
    {
        myPage = parent.getPage();
    }
    pageSize = parent.getPageSize();
    clear();
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
// provided in A3
MyDB_RecordIteratorAltPtr getIteratorAlt(vector<MyDB_PageReaderWriter> &forUs)
{
    return make_shared<MyDB_PageListIteratorAlt>(forUs);
}

MyDB_RecordIteratorPtr MyDB_PageReaderWriter :: getIterator (MyDB_RecordPtr iterateIntoMe) {
    return std::make_shared<MyDB_PageRecIterator>(myPage, iterateIntoMe);
}

MyDB_RecordIteratorAltPtr MyDB_PageReaderWriter ::getIteratorAlt()
{
    return make_shared<MyDB_PageRecIteratorAlt>(myPage);
}

void MyDB_PageReaderWriter :: setType (MyDB_PageType toMe) {
    PageHeader *header = (PageHeader *)myPage->getBytes();
    header->pageType = toMe;
    myPage->wroteBytes();
}
// provided in A3
void *MyDB_PageReaderWriter ::appendAndReturnLocation(MyDB_RecordPtr appendMe)
{
    PageHeader *header = (PageHeader *)myPage->getBytes();
    size_t NUM_BYTES_USED = header->offset + sizeof(PageHeader);
    void *recLocation = NUM_BYTES_USED + (char *)myPage->getBytes();
    if (append(appendMe))
        return recLocation;
    else
        return nullptr;
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

void MyDB_PageReaderWriter ::
    sortInPlace(function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
    PageHeader *header = (PageHeader *)myPage->getBytes();
    size_t NUM_BYTES_USED = header->offset + sizeof(PageHeader);
    void *temp = malloc(pageSize);
    memcpy(temp, myPage->getBytes(), pageSize);

    // first, read in the positions of all of the records
    vector<void *> positions;

    // this basically iterates through all of the records on the page
    int bytesConsumed = sizeof(size_t) * 2;
    while (bytesConsumed != NUM_BYTES_USED)
    {
        void *pos = bytesConsumed + (char *)temp;
        positions.push_back(pos);
        void *nextPos = lhs->fromBinary(pos);
        bytesConsumed += ((char *)nextPos) - ((char *)pos);
    }

    // and now we sort the vector of positions, using the record contents to build a comparator
    RecordComparator myComparator(comparator, lhs, rhs);
    std::stable_sort(positions.begin(), positions.end(), myComparator);

    // and write the guys back
    NUM_BYTES_USED = 2 * sizeof(size_t);
    myPage->wroteBytes();
    for (void *pos : positions)
    {
        lhs->fromBinary(pos);
        append(lhs);
    }

    free(temp);
}

MyDB_PageReaderWriterPtr MyDB_PageReaderWriter ::
    sort(function<bool()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs)
{
    PageHeader *header = (PageHeader *)myPage->getBytes();
    size_t NUM_BYTES_USED = header->offset + sizeof(PageHeader);

    // first, read in the positions of all of the records
    vector<void *> positions;

    // this basically iterates through all of the records on the page
    int bytesConsumed = sizeof(size_t) * 2;
    while (bytesConsumed != NUM_BYTES_USED)
    {
        void *pos = bytesConsumed + (char *)myPage->getBytes();
        positions.push_back(pos);
        void *nextPos = lhs->fromBinary(pos);
        bytesConsumed += ((char *)nextPos) - ((char *)pos);
    }

    // and now we sort the vector of positions, using the record contents to build a comparator
    RecordComparator myComparator(comparator, lhs, rhs);
    std::stable_sort(positions.begin(), positions.end(), myComparator);

    // and now create the page to return
    MyDB_PageReaderWriterPtr returnVal = make_shared<MyDB_PageReaderWriter>(myPage->getParent());
    returnVal->clear();

    // loop through all of the sorted records and write them out
    for (void *pos : positions)
    {
        lhs->fromBinary(pos);
        returnVal->append(lhs);
    }

    return returnVal;
}

size_t MyDB_PageReaderWriter ::getPageSize()
{
    return pageSize;
}

void *MyDB_PageReaderWriter ::getBytes()
{
    return myPage->getBytes();
}

#endif
