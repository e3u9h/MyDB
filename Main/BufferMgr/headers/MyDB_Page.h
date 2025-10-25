
#ifndef MYDB_PAGE_H
#define MYDB_PAGE_H

#include <memory>
#include <vector>
#include "MyDB_Table.h"

class MyDB_BufferManager;
class MyDB_PageHandleBase;

class MyDB_Page : public std::enable_shared_from_this<MyDB_Page>
{
public:
	MyDB_Page() : lruPrev(nullptr), lruNext(nullptr), whichTable(nullptr), posInTable(-1), posInBuffer(-1),
				  isPinned(false), isDirty(false), referenceCnt(0), lruCounter(-1), bufferMgr(nullptr) {}

	MyDB_Page(MyDB_TablePtr whichTable, long posInTable)
		: lruPrev(nullptr), lruNext(nullptr), whichTable(whichTable), posInTable(posInTable), posInBuffer(-1),
		  isPinned(false), isDirty(false), referenceCnt(0), lruCounter(-1), bufferMgr(nullptr) {}

	// Access and modify page state
	MyDB_TablePtr getTable() const { return whichTable; }
	void setTable(MyDB_TablePtr table) { whichTable = table; }
	
	long getPosInTable() const { return posInTable; }
	void setPosInTable(long pos) { posInTable = pos; }
	
	int getPosInBuffer() const { return posInBuffer; }
	void setPosInBuffer(int pos) { 
		posInBuffer = pos; 
	}
	
	bool getIsPinned() const { return isPinned; }
	void setPinned(bool pinned) { isPinned = pinned; }
	
	bool getIsDirty() const { return isDirty; }
	void setDirty(bool dirty) { isDirty = dirty; }

	int getLruCounter() const { return lruCounter; }
	void setLruCounter(int counter) { lruCounter = counter; }

	// LRU双向链表指针
	shared_ptr<MyDB_Page> lruPrev;
	shared_ptr<MyDB_Page> lruNext;

	// increment and decrement reference count for handles
	void registerHandle();
	void unregisterHandle();

	// Set BufferManager reference for anonymous page management (because we need to notify the buffer manager to call releasePage)
	void setBufferManager(MyDB_BufferManager *mgr) { bufferMgr = mgr; }

private:
	MyDB_TablePtr whichTable;
	long posInTable;
	int posInBuffer;
	bool isPinned;
	bool isDirty;
	int referenceCnt;
	int lruCounter;

	// BufferManager reference for anonymous page recycling
	MyDB_BufferManager *bufferMgr;
};

#endif
