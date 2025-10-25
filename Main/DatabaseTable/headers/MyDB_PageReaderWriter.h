
#ifndef PAGE_RW_H
#define PAGE_RW_H

#include "MyDB_PageType.h"
#include "MyDB_PageHandle.h"
#include "MyDB_Record.h"
#include "MyDB_RecordIterator.h"
#include <memory>

class MyDB_PageReaderWriter {

public:

	// isNewPage: true if this action is creating a new page (needs to use clear to initialize), false if reading existing page
	MyDB_PageReaderWriter(MyDB_PageHandle page, size_t pageSize, bool isNewPage = false);

	// ANY OTHER METHODS YOU WANT HERE

	// empties out the contents of this page, so that it has no records in it
	// the type of the page is set to MyDB_PageType :: RegularPage
	void clear ();	

	// return an itrator over this page... each time returnVal->next () is
	// called, the resulting record will be placed into the record pointed to
	// by iterateIntoMe
	MyDB_RecordIteratorPtr getIterator (MyDB_RecordPtr iterateIntoMe);

	// appends a record to this page... return false is the append fails because
	// there is not enough space on the page; otherwise, return true
	bool append (MyDB_RecordPtr appendMe);

	// gets the type of this page... this is just a value from an ennumeration
	// that is stored within the page
	MyDB_PageType getType ();

	// sets the type of the page
	void setType (MyDB_PageType toMe);
	
private:
	
	// ANYTHING ELSE YOU WANT HERE
	MyDB_PageHandle myPage;
	size_t pageSize;

	struct PageHeader
	{
		MyDB_PageType pageType;
		int recordCount;
		size_t offset;
		char bytes[0]; // Start of the actual data
	};
};

#endif
