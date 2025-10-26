

/****************************************************
** COPYRIGHT 2016, Chris Jermaine, Rice University **
**                                                 **
** The MyDB Database System, COMP 530              **
** Note that this file contains SOLUTION CODE for  **
** A2.  You should not be looking at this file     **
** unless you have completed A2!                   **
****************************************************/


#ifndef PAGE_REC_ITER_ALT_C
#define PAGE_REC_ITER_ALT_C

#include "MyDB_PageRecIteratorAlt.h"

#include "MyDB_PageType.h"

// Implementation that mirrors the non-Alt page iterator but preserves the
// Alt interface: getCurrent fills the provided record and sets nextRecSize;
// getCurrentPointer returns the address of the current record; advance()
// moves to the next record and returns whether more records remain.

void MyDB_PageRecIteratorAlt :: getCurrent (MyDB_RecordPtr intoMe) {
	if (myPage == nullptr) return;

	// read the page header to locate the record area and the used bytes
	struct PageHeader {
		MyDB_PageType pageType;
		int recordCount;
		size_t offset;
		char bytes[0];
	};

	PageHeader *header = (PageHeader *) myPage->getBytes();
	char *pos = header->bytes + bytesConsumed;
	char *nextPos = (char *) intoMe->fromBinary(pos);
	nextRecSize = (int) (nextPos - pos);
}

void *MyDB_PageRecIteratorAlt :: getCurrentPointer () {
	if (myPage == nullptr) return nullptr;
	struct PageHeader {
		MyDB_PageType pageType;
		int recordCount;
		size_t offset;
		char bytes[0];
	};
	PageHeader *header = (PageHeader *) myPage->getBytes();
	return header->bytes + bytesConsumed;
}

bool MyDB_PageRecIteratorAlt :: advance () {
	if (nextRecSize == -1) {
		cout << "You can't call advance without calling getCurrent!!\n";
		exit (1);
	}

	struct PageHeader {
		MyDB_PageType pageType;
		int recordCount;
		size_t offset;
		char bytes[0];
	};
	PageHeader *header = (PageHeader *) myPage->getBytes();
	// cout << "DEBUG: Current byte " << bytesConsumed  << endl;

	bytesConsumed += nextRecSize;
	nextRecSize = -1;
	// cout << "DEBUG: Advanced to byte " << bytesConsumed << " of " << header->offset << endl;
	return bytesConsumed != (int) header->offset;
}

MyDB_PageRecIteratorAlt :: MyDB_PageRecIteratorAlt (MyDB_PageHandle myPageIn) {
	myPage = myPageIn;
	bytesConsumed = 0;    // start at first record in header->bytes
	nextRecSize = 0;
}

MyDB_PageRecIteratorAlt :: ~MyDB_PageRecIteratorAlt () {}

#endif
