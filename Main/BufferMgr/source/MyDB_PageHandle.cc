
#ifndef PAGE_HANDLE_C
#define PAGE_HANDLE_C

#include <memory>
#include "MyDB_PageHandle.h"
#include "MyDB_BufferManager.h"

void *MyDB_PageHandleBase ::getBytes()
{
	return bufferMgr->getBytes(page->getTable(), page->getPosInTable(), page);
}

void MyDB_PageHandleBase ::wroteBytes()
{
	page->setDirty(true);
}

MyDB_PageHandleBase ::~MyDB_PageHandleBase()
{
	if (page) {
		page->unregisterHandle();
	}
}

#endif

