
#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include "MyDB_BufferManager.h"
#include <string>
#include <cstring>
#include <algorithm>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <climits>

using namespace std;

MyDB_PageHandle MyDB_BufferManager::getPageInternal(MyDB_TablePtr whichTable, long pos, bool isPinned)
{
	string key = makePageKey(whichTable, pos);
	auto it = pageIndex.find(key);
	
	shared_ptr<MyDB_Page> page = nullptr;
	if (it != pageIndex.end()) {
		// page already exists
		page = it->second;
	} else {
		// create new page
		page = make_shared<MyDB_Page>(whichTable, pos);
		pageIndex[key] = page;
		page->setBufferManager(this); // Set BufferManager reference
	}

	if (isPinned)
	{
		page->setPinned(true);
	}

	MyDB_PageHandle handle = make_shared<MyDB_PageHandleBase>(this, page);
	return handle;
}

MyDB_PageHandle MyDB_BufferManager ::getPage(MyDB_TablePtr whichTable, long i)
{
	return getPageInternal(whichTable, i, false);
}

MyDB_PageHandle MyDB_BufferManager :: getPage () {
	return getPageInternal(tempTable, tempPageIndex++, false);
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (MyDB_TablePtr whichTable, long i) {
	return getPageInternal(whichTable, i, true);
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {
	return getPageInternal(tempTable, tempPageIndex++, true);
}

void MyDB_BufferManager ::unpin(MyDB_PageHandle unpinMe)
{
	if (!unpinMe) return;
	
	auto page = unpinMe->getPageObject();
	if (page) {
		page->setPinned(false);
	}
}

MyDB_BufferManager :: MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile) {
	this->data = (char*)mmap(0, pageSize * numPages, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
	if (this->data == MAP_FAILED) {
		throw runtime_error("Failed to allocate buffer memory");
	}
	
	this->pageSize = pageSize;
	this->numPages = numPages;
	this->tempFile = tempFile;
	this->tempTable = make_shared<MyDB_Table>("tempTable1qwoudq", tempFile);
	this->tempPageIndex = 0;
	this->pageIndex.clear();
	this->fileDescriptors.clear();
	this->bufferSlots = vector<shared_ptr<MyDB_Page>>(numPages, nullptr);
	this->lruCounter = 0;
}

MyDB_BufferManager :: ~MyDB_BufferManager () {
	// Write all dirty pages to disk before cleanup
	for (int i = 0; i < (int)numPages; i++) {
		if (bufferSlots[i] && bufferSlots[i]->getIsDirty()) {
			writePageToDisk(i);
		}
	}

	// Clean up pageIndex and bufferSlots
	pageIndex.clear();
	// to ensure all page objects are destructed, we should ensure no references remain. As bufferSlots also hold references to pages, we should clear it.
	bufferSlots.clear();

	// close fds
	for (auto &pair : fileDescriptors)
	{
		if (pair.second >= 0)
		{
			close(pair.second);
		}
	}
	fileDescriptors.clear();

	// Close and unmap the memory-mapped file
	if (data != nullptr) {
		munmap(data, pageSize * numPages);
		data = nullptr;
	}
}

void MyDB_BufferManager::loadPageFromDisk(int slotIndex, MyDB_TablePtr table, long posInTable) {
	if (table != nullptr) {
		string filename = table->getStorageLoc();
		int fd = getFileDescriptor(filename);
		if (fd >= 0) {
			lseek(fd, posInTable * pageSize, SEEK_SET);
			ssize_t bytesRead = read(fd, data + (slotIndex * pageSize), pageSize);

			// error handlings
			if (bytesRead < 0) {
				memset(data + (slotIndex * pageSize), 0, pageSize);
			}
		}
		else
		{
			memset(data + (slotIndex * pageSize), 0, pageSize);
		}
	}
	else
	{
		memset(data + (slotIndex * pageSize), 0, pageSize);
	}
}

void MyDB_BufferManager::writePageToDisk(int slotIndex) {
	if (slotIndex >= 0 && slotIndex < (int)numPages && bufferSlots[slotIndex]) {
		auto page = bufferSlots[slotIndex];
		string filename = page->getTable()->getStorageLoc();
		int fd = getFileDescriptor(filename);
		if (fd >= 0) {
			lseek(fd, page->getPosInTable() * pageSize, SEEK_SET);
			write(fd, data + (slotIndex * pageSize), pageSize);
		}
	}
}

void *MyDB_BufferManager::getBytes(MyDB_TablePtr whichTable, long posInTable, shared_ptr<MyDB_Page> page)
{

	int slotIndex = page->getPosInBuffer();
	bool needsLoading = false;

	if (slotIndex < 0) {
		// the page is not currently in buffer, need to allocate a slot. First, try to find a free slot in buffer
		for (size_t i = 0; i < numPages; i++) {
			if (bufferSlots[i] == nullptr) {
				bufferSlots[i] = page;
				page->setPosInBuffer(i);
				slotIndex = i;
				needsLoading = true;
				break;
			}
		}

		// No free slot found, need to evict a page using LRU
		if (slotIndex < 0) {
			int victimSlot = -1;
			int oldestTime = INT_MAX;

			// find the unpinned page with the lowest lruCounter
			for (size_t i = 0; i < numPages; i++) {
				if (bufferSlots[i] && !bufferSlots[i]->getIsPinned()) {
					int pageTime = bufferSlots[i]->getLruCounter();
					if (pageTime < oldestTime) {
						oldestTime = pageTime;
						victimSlot = i;
					}
				}
			}
			
			if (victimSlot == -1) {
				throw runtime_error("No available buffer space - all pages pinned");
			}

			// write back victim page if dirty
			auto victimPage = bufferSlots[victimSlot];
			if (victimPage->getIsDirty() && victimPage->getPosInBuffer() >= 0)
			{
				writePageToDisk(victimSlot);
			}

			// mark the victim page as not in buffer
			victimPage->setPosInBuffer(-1);

			bufferSlots[victimSlot] = page;
			page->setPosInBuffer(victimSlot);
			slotIndex = victimSlot;
			needsLoading = true;
		}
	}

	if (needsLoading)
	{
		loadPageFromDisk(slotIndex, whichTable, posInTable);
	}

	// set the page's LRU counter to the current value, and increment the global counter to a newer value
	page->setLruCounter(++lruCounter);

	return data + (slotIndex * pageSize);
}

string MyDB_BufferManager::makePageKey(MyDB_TablePtr table, long pos)
{
	return table->getStorageLoc() + ":" + to_string(pos);
}

int MyDB_BufferManager::getFileDescriptor(const string &filename)
{
	auto it = fileDescriptors.find(filename);
	if (it != fileDescriptors.end())
	{
		return it->second;
	}

	int fd = open(filename.c_str(), O_RDWR | O_CREAT | O_FSYNC, 0666);
	if (fd >= 0)
	{
		fileDescriptors[filename] = fd;
	}
	return fd;
}

// release an anonymous page when no handles reference it
void MyDB_BufferManager::releasePage(shared_ptr<MyDB_Page> page)
{
	string key = makePageKey(page->getTable(), page->getPosInTable());

	// if page is in buffer, clear the buffer slot
	int bufferPos = page->getPosInBuffer();
	if (bufferPos >= 0 && bufferPos < (int)numPages)
	{
		bufferSlots[bufferPos] = nullptr;
	}

	// remove from pageIndex. At this point, there should be no other references to this page, so the destructor of the Page will be called automatically.
	pageIndex.erase(key);
}

#endif


