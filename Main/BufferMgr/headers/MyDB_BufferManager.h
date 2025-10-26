
#ifndef BUFFER_MGR_H
#define BUFFER_MGR_H

#include "MyDB_PageHandle.h"
#include "MyDB_Table.h"
#include "MyDB_Page.h"
#include <unordered_map>

using namespace std;
class MyDB_BufferManager;
typedef shared_ptr<MyDB_BufferManager> MyDB_BufferManagerPtr;
class MyDB_BufferManager {

public:

	// THESE METHODS MUST APPEAR AND THE PROTOTYPES CANNOT CHANGE!

	// gets the i^th page in the table whichTable... note that if the page
	// is currently being used (that is, the page is current buffered) a handle 
	// to that already-buffered page should be returned
	MyDB_PageHandle getPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page that will no longer exist (1) after the buffer manager
	// has been destroyed, or (2) there are no more references to it anywhere in the
	// program.  Typically such a temporary page will be used as buffer memory.
	// since it is just a temp page, it is not associated with any particular 
	// table
	MyDB_PageHandle getPage ();

	// gets the i^th page in the table whichTable... the only difference 
	// between this method and getPage (whicTable, i) is that the page will be 
	// pinned in RAM; it cannot be written out to the file
	MyDB_PageHandle getPinnedPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page, like getPage (), except that this one is pinned
	MyDB_PageHandle getPinnedPage ();

	// un-pins the specified page
	void unpin (MyDB_PageHandle unpinMe);

	// creates an LRU buffer manager... params are as follows:
	// 1) the size of each page is pageSize 
	// 2) the number of pages managed by the buffer manager is numPages;
	// 3) temporary pages are written to the file tempFile
	MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile);
	
	// when the buffer manager is destroyed, all of the dirty pages need to be
	// written back to disk, any necessary data needs to be written to the catalog,
	// and any temporary files need to be deleted
	~MyDB_BufferManager ();

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS
	void *getBytes(MyDB_TablePtr whichTable, long posInTable, shared_ptr<MyDB_Page> page);
	void releasePage(shared_ptr<MyDB_Page> page);
	bool isTemporaryTable(MyDB_TablePtr table) const { return table == tempTable; }
	// returns the page size
	size_t getPageSize() { return pageSize; }

private:

	// YOUR STUFF HERE
	char* data;
	size_t pageSize;
	size_t numPages;
	string tempFile;
	MyDB_TablePtr tempTable; // For temporary pages
	int tempPageIndex;

	unordered_map<string, shared_ptr<MyDB_Page>> pageIndex; // table_name:pos -> page
	vector<shared_ptr<MyDB_Page>> bufferSlots;				// buffer slot -> Page

	// double linked list for LRU management
	shared_ptr<MyDB_Page> lruHead; // most recently used page (head)
	shared_ptr<MyDB_Page> lruTail; // least recently used page (tail)

	// file descriptors keyed by filename
	unordered_map<string, int> fileDescriptors;

	MyDB_PageHandle getPageInternal(MyDB_TablePtr whichTable, long pos, bool isPinned);
	string makePageKey(MyDB_TablePtr table, long pos);

	void loadPageFromDisk(int slotIndex, MyDB_TablePtr table, long posInTable);
	void writePageToDisk(int slotIndex);

	int getFileDescriptor(const string &filename);

	// LRU double linked list operations
	void addToHead(shared_ptr<MyDB_Page> page);
	void removeFromLRU(shared_ptr<MyDB_Page> page);
	void moveToHead(shared_ptr<MyDB_Page> page);
};

#endif


