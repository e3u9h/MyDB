#ifndef PAGE_REC_ITER_H
#define PAGE_REC_ITER_H

#include "MyDB_RecordIterator.h"
#include "MyDB_Record.h"
#include "MyDB_PageHandle.h"

class MyDB_PageRecIterator : public MyDB_RecordIterator {

public:
    MyDB_PageRecIterator(MyDB_PageHandle page, MyDB_RecordPtr iterateIntoMe);
    
    void getNext() override;
    bool hasNext() override;

private:
    MyDB_PageHandle myPage;
    MyDB_RecordPtr myRecord;
    char* currentPos;
    int currentIndex;
    int totalRecords;
};

#endif