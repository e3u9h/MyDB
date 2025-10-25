#ifndef PAGE_C
#define PAGE_C

#include "MyDB_Page.h"
#include "MyDB_PageHandle.h"
#include "MyDB_BufferManager.h"
#include <algorithm>
#include <iostream>

using namespace std;

void MyDB_Page::registerHandle()
{
    referenceCnt++;
}

void MyDB_Page::unregisterHandle()
{
    referenceCnt--;

    if (referenceCnt == 0)
    {
        if (bufferMgr == nullptr)
        {
            cout << "Warning: BufferMgr is nullptr in Page::unregisterHandle()" << endl;
            return;
        }

        // if the page is pinned and not anonymous, automatically unpin
        if (isPinned && !bufferMgr->isTemporaryTable(whichTable))
        {
            isPinned = false;
        }

        // if it is an anonymous page, notify BufferManager to recycle the buffer slot
        if (bufferMgr->isTemporaryTable(whichTable))
        {
            bufferMgr->releasePage(shared_from_this());
        }
        // if it's a regular page that's no longer in buffer, it should also be released
        else if (posInBuffer < 0)
        {
            bufferMgr->releasePage(shared_from_this());
        }
    }
}

#endif
