#ifndef TABLE_REC_ITER_H
#define TABLE_REC_ITER_H

#include "MyDB_RecordIterator.h"
#include "MyDB_Record.h"
#include "MyDB_Table.h"
#include "MyDB_BufferManager.h"

// Forward declaration
class MyDB_TableReaderWriter;

class MyDB_TableRecIterator : public MyDB_RecordIterator {

public:
    MyDB_TableRecIterator(MyDB_TableReaderWriter* table, MyDB_RecordPtr iterateIntoMe);
    
    void getNext() override;
    bool hasNext() override;

private:
    MyDB_TableReaderWriter* myTable;
    MyDB_RecordPtr myRecord;
    MyDB_RecordIteratorPtr currentPageIterator;
    int currentPage;
    int lastPage;
};

#endif