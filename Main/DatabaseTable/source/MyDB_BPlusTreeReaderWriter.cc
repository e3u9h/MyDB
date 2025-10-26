
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "MyDB_PageListIteratorAlt.h"
#include "RecordComparator.h"

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = getTable ()->getRootLocation ();

}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	vector<MyDB_PageReaderWriter> list;
	discoverPages(rootLocation, list, low, high);

	MyDB_RecordPtr lhs = getEmptyRecord();
	MyDB_RecordPtr rhs = getEmptyRecord();
	MyDB_RecordPtr myRec = getEmptyRecord();
	MyDB_INRecordPtr llow = getINRecord();
	llow->setKey(low);
	MyDB_INRecordPtr hhigh = getINRecord();
	hhigh->setKey(high);
	function<bool()> comparator = buildComparator(lhs, rhs);
	function<bool()> lowComparator = buildComparator(myRec, llow);
	function<bool()> highComparator = buildComparator(hhigh, myRec);

	return make_shared<MyDB_PageListIteratorSelfSortingAlt>(list, lhs, rhs, comparator, myRec, lowComparator, highComparator, true);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter ::getRangeIteratorAlt(MyDB_AttValPtr low, MyDB_AttValPtr high)
{
	vector <MyDB_PageReaderWriter> list;
	discoverPages (rootLocation, list, low, high);

	MyDB_RecordPtr lhs = getEmptyRecord ();
	MyDB_RecordPtr rhs = getEmptyRecord ();
	MyDB_RecordPtr myRec = getEmptyRecord ();
	MyDB_INRecordPtr llow = getINRecord ();
	llow->setKey (low);
	MyDB_INRecordPtr hhigh = getINRecord ();
	hhigh->setKey (high);
	function <bool ()> comparator = buildComparator (lhs, rhs);
	function <bool ()> lowComparator = buildComparator (myRec, llow);	
	function <bool ()> highComparator = buildComparator (hhigh, myRec);	

	return make_shared <MyDB_PageListIteratorSelfSortingAlt> (list, lhs, rhs, comparator, myRec, lowComparator, highComparator, false);	
}

bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <MyDB_PageReaderWriter> &list,
        	MyDB_AttValPtr low, MyDB_AttValPtr high) {
	// cout << "[discoverPages] enter whichPage=" << whichPage << " low=" << (low ? low->toString() : string("NULL")) << " high=" << (high ? high->toString() : string("NULL")) << " rootLocation=" << rootLocation << endl;

	// recurisive version

	// MyDB_PageReaderWriter currentPage = (*this)[whichPage];
	// // if this is a leaf page, then add it to the list and return
	// if (currentPage.getType () == MyDB_PageType :: RegularPage) {
	// 	list.push_back (currentPage);
	// 	return true;
	// }
	// // find the first key >= low, and the first key > high (why it is key > high? because it is possible to have keys with equal values, and if the key equal to high has equal values, we still need to search for keys after it until we find a key > high)
	// MyDB_RecordIteratorAltPtr it = currentPage.getIteratorAlt();
	// MyDB_INRecordPtr tempRec = getINRecord();
	// MyDB_INRecordPtr templowRec = getINRecord();
	// templowRec->setKey(low);
	// MyDB_INRecordPtr temphighRec = getINRecord();
	// temphighRec->setKey(high);
	// function <bool ()> comparaterWithLow = buildComparator(tempRec, templowRec); // return true if tempRec < low
	// function <bool ()> comparaterWithHigh = buildComparator(temphighRec, tempRec); // return true if high < tempRec
	// bool lowFound = false;
	// bool childIsLeaf = false;
	// while (it->advance()) {
	// 	it->getCurrent(tempRec);
	// 	if (!lowFound && !comparaterWithLow()) { // tempRec >= low
	// 		lowFound = true;
	// 	}
	// 	if (lowFound) {
	// 		if (childIsLeaf) {
	// 			int pageNum = tempRec->getPtr();
	// 			MyDB_PageReaderWriter pageToAdd = (*this)[pageNum];
	// 			list.push_back(pageToAdd);
	// 		} else {
	// 			childIsLeaf = discoverPages(tempRec->getPtr(), list, low, high);
	// 		}
	// 	}
	// 	if (lowFound && comparaterWithHigh()) { // tempRec > high, this is placed after searching this rec because the first key > high also needs to be searched
	// 		break;
	// 	}
	// }
	// return false;

	// iterative version
	MyDB_PageReaderWriter rootPage = (*this)[whichPage];
	vector<MyDB_PageReaderWriter> currentLayer;
	currentLayer.push_back(rootPage);
	while (true) {
		if (currentLayer[0].getType() == MyDB_PageType::RegularPage) {
			list.insert(list.end(), currentLayer.begin(), currentLayer.end());
			return true;
		}
		vector<MyDB_PageReaderWriter> nextLayer;
		MyDB_INRecordPtr tempRec = getINRecord();
		MyDB_INRecordPtr templowRec = getINRecord();
		templowRec->setKey(low);
		MyDB_INRecordPtr temphighRec = getINRecord();
		temphighRec->setKey(high);
		function <bool ()> comparaterWithLow = buildComparator(tempRec, templowRec); // return true if tempRec < low
		function <bool ()> comparaterWithHigh = buildComparator(temphighRec, tempRec); // return true if high < tempRec
		cout << "[discoverPages] create iterator for currentLayer size=" << currentLayer.size() << endl;
		MyDB_RecordIteratorAltPtr it = make_shared<MyDB_PageListIteratorAlt>(currentLayer);
		bool lowFound = false;
		while (it->advance()) {
			it->getCurrent(tempRec);
			if (!lowFound && !comparaterWithLow()) { // tempRec >= low
				lowFound = true;
			}
			if (lowFound) {
				int pageNum = tempRec->getPtr();
				MyDB_PageReaderWriter pageToAdd = (*this)[pageNum];
				nextLayer.push_back(pageToAdd);
			}
			if (lowFound && comparaterWithHigh()) { // tempRec > high, this is placed after searching this rec because the first key > high also needs to be searched
				break;
			}
		}
		cout << "[discoverPages] nextLayer size=" << nextLayer.size() << endl;
		if (nextLayer.empty()) {
			break;
		}
		currentLayer = nextLayer;
	}

	return false;
}
	

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr appendMe) {

	// in loadFromTextFile, last page is set to 0, and getNumPages() is lastPage + 1. So we use 1 here.
	// cout << "[BPlusTree::append(public)] getTable()=" << getTable() << " getTable->lastPage=" << (getTable()?getTable()->lastPage():-999) << endl;
	int gn = getNumPages();
	// cout << "[BPlusTree::append(public)] getNumPages()=" << gn << endl;
	if (gn <= 1) {
		// cout << "[BPlusTree::append(public)] creating root page via operator[] index 0" << endl;
		MyDB_PageReaderWriter root = (*this)[0];
		// cout << "[BPlusTree::append(public)] got root page obj @" << &root << " root.getType()=" << root.getType() << endl;
		rootLocation = 0;
		getTable ()->setRootLocation (0);
		MyDB_INRecordPtr internalNodeRec = getINRecord (); // automatically has max key
		internalNodeRec->setPtr (1);
		getTable ()->setLastPage (1);
		// cout << "[BPlusTree::append(public)] clearing root and appending internalNodeRec" << endl;
		root.clear ();
		// cout << "[BPlusTree::append(public)] before root.append(internalNodeRec)" << endl;
		root.append (internalNodeRec);
		// cout << "[BPlusTree::append(public)] after root.append(internalNodeRec)" << endl;
		root.setType (MyDB_PageType :: DirectoryPage);    
		// note that [] operator automatically creates new pages if it is larger than last page. However, here last page is already set to 1 above, so we need to manually call clear()
		// cout << "[BPlusTree::append(public)] creating leaf page via operator[] index 1" << endl;
		MyDB_PageReaderWriter leaf = (*this)[1];
		// cout << "[BPlusTree::append(public)] got leaf page obj @" << &leaf << " leaf.getType()=" << leaf.getType() << endl;
		leaf.clear();
		// cout << "[BPlusTree::append(public)] after leaf.clear() leaf.NUM_BYTES_USED should be reset" << endl;
		leaf.setType (MyDB_PageType :: RegularPage);
		leaf.append (appendMe);

	} else {
		// cout << "[BPlusTree::append(public)] appending to existing root" << endl;
		auto res = append (rootLocation, appendMe);
		// cout << "[BPlusTree::append(public)] append to rootLocation=" << rootLocation << " returned res=" << (res ? getKey(res)->toString() : string("NULL")) << endl;
		if (res != nullptr) { // root was split, and the return value is the new key pointing to the smaller half
			// cout << "[BPlusTree::append(public)] root was split, new key is " << getKey(res)->toString() << endl;
			int newRootLoc = getTable ()->lastPage () + 1;
			getTable ()->setLastPage (newRootLoc);
			MyDB_PageReaderWriter newRoot = (*this)[newRootLoc];
			newRoot.clear ();
			newRoot.setType (MyDB_PageType :: DirectoryPage);
			newRoot.append (res);
			MyDB_INRecordPtr newRec = getINRecord ();
			newRec->setPtr (rootLocation); // original root is now the larger half
			newRoot.append (newRec);

			rootLocation = newRootLoc;
			getTable ()->setRootLocation (rootLocation);
			// cout << "[BPlusTree::append(public)] updated rootLocation=" << rootLocation << endl;
		}
	}
	printTree ();
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter ::split(MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe)
{

	// cout << "[BPlusTree::spl/it] ENTER this=" << this << " splitMe_addr=" << &splitMe << " andMe=" << andMe.get() << endl;
	MyDB_PageType myType = splitMe.getType();
	// cout << "[BPlusTree::split] myType=" << (myType==MyDB_PageType::RegularPage?"Regular":"Directory") << " lastPage=" << getTable()->lastPage() << endl;
	MyDB_RecordPtr lhs, rhs;
	if (splitMe.getType () == MyDB_PageType :: RegularPage) {
		lhs = getEmptyRecord ();
		rhs = getEmptyRecord ();
	} else if (splitMe.getType () == MyDB_PageType :: DirectoryPage) {
		lhs = getINRecord ();
		rhs = getINRecord ();
	}
	int newPageLoc = getTable()->lastPage() + 1;
	getTable()->setLastPage(newPageLoc);
	MyDB_PageReaderWriter newPage = (*this)[newPageLoc];
	newPage.clear ();
	newPage.setType (myType);

	function <bool ()> comparator = buildComparator (lhs, rhs);	

	// cout << "[BPlusTree::split] calling splitMe.sort(...)" << endl;
	MyDB_PageReaderWriterPtr sortedPage = splitMe.sort (comparator, lhs, rhs);
	// cout << "[BPlusTree::split] sort returned sortedPage=" << (sortedPage ? sortedPage.get() : nullptr) << endl;
	// find the position of andMe
	lhs = andMe;
	function<bool()> findComparator = buildComparator(lhs, rhs); // why should we build again? Becuase upon building, the address that the comparator will look at is fixed. As lhs=andMe changed the address of lhs, the new address is not known by the comparator, so we should build a new one.
	int numOfRecs = 0;
	bool found = false;
	int posOfAndMe = -1;
	MyDB_RecordIteratorAltPtr it = sortedPage->getIteratorAlt ();
	// cout << "[BPlusTree::split] created iterator for sortedPage" << endl;
	// it->advance ();
	while (it->advance())
	{
		// cout << "[BPlusTree::split] iterating to find position of andMe, numOfRecs=" << numOfRecs << endl;
		numOfRecs++;
		it->getCurrent (rhs);
		// cout << "[BPlusTree::split] comparing andMe with rhs," <<  " lhs key=" << (getKey(lhs) ? getKey(lhs)->toString() : string("NULL")) << " rhs key=" << (getKey(rhs) ? getKey(rhs)->toString() : string("NULL")) << endl;
		if (findComparator()) { // andMe < rhs
			found = true;
			posOfAndMe = numOfRecs - 1;
			// cout << "[BPlusTree::split] found position of andMe=" << posOfAndMe << " total numOfRecs=" << numOfRecs + 1 << endl;
			break;
		}
	}
	if (!found) { // andMe is the largest
		posOfAndMe = numOfRecs;
	} else {
		// finish counting the number of records (this and !found cannot both occur)
		while (it->advance ()) {
			it->getCurrent (rhs);
			numOfRecs++;
		}
	}
	// cout << "[BPlusTree::split] 111found position of andMe=" << posOfAndMe << " total numOfRecs=" << numOfRecs + 1 << endl;
	// now, first, copy the lower half to the new page
	MyDB_RecordIteratorAltPtr newIt = sortedPage->getIteratorAlt ();
	MyDB_RecordPtr temp;
	if (myType == MyDB_PageType :: RegularPage) {
		temp = getEmptyRecord ();
	} else if (myType == MyDB_PageType :: DirectoryPage) {
		temp = getINRecord ();
	}
	numOfRecs++; // include andMe
	for (int i = 0; i < numOfRecs / 2; i++) {
		if (i == posOfAndMe) {
			newPage.append (andMe);
			temp = andMe;
			continue;
		}
		newIt->advance();
		newIt->getCurrent (temp);
		newPage.append (temp);
	}
	// now, create the return value, which is the last key at the left side
	MyDB_RecordPtr medianKey = temp;
	MyDB_INRecordPtr returnVal = getINRecord();
	returnVal->setKey (getKey(medianKey));
	returnVal->setPtr (newPageLoc);
	// now, copy the upper half to the original page
	splitMe.clear ();
	splitMe.setType (myType); // because clear() sets it to RegularPage. Debugged for this for a long time...
	for (int i = numOfRecs / 2; i < numOfRecs; i++) {
		if (i == posOfAndMe) {
			splitMe.append (andMe);
			continue;
		}
		newIt->advance();
		newIt->getCurrent (temp);
		splitMe.append (temp);
	}
	return returnVal;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter ::append(int whichPage, MyDB_RecordPtr appendMe)
{
	MyDB_PageReaderWriter pageToAddTo = (*this)[whichPage];
	// cout << "[BPlusTree::append] pageToAddTo addr=" << &pageToAddTo << " type=" << (pageToAddTo.getType()==MyDB_PageType::RegularPage?"Regular":"Directory") << endl;

	if (pageToAddTo.getType () == MyDB_PageType :: RegularPage) {
		// we are at a leaf node
		if (pageToAddTo.append (appendMe)) {
			return nullptr;
		}
		// cout << "[BPlusTree::append] append failed, calling split(...)" << endl;
		auto newRecPtr = split (pageToAddTo, appendMe);
		// cout << "[BPlusTree::append] split returned newRecPtr=" << (newRecPtr ? newRecPtr.get() : nullptr) << endl;
		return newRecPtr;

	} else {
		// we are at a directory node, and need to find the child to recurse on
		MyDB_RecordIteratorAltPtr temp = pageToAddTo.getIteratorAlt ();
		// cout << "[BPlusTree::append] at directory: iterator created temp=" << temp.get() << endl;
		// create a container for the record in the internal node
		MyDB_INRecordPtr otherRec = getINRecord ();
		function <bool ()> comparator = buildComparator (appendMe, otherRec);	
		while (temp->advance ()) {
			temp->getCurrent (otherRec);
			if (comparator ()) { // appendMe < otherRec
				// note that the pointer in the internal node is the records <= key
				auto res = append (otherRec->getPtr (), appendMe);
				// cout << "[BPlusTree::append] recursive append returned res=" << (res ? getKey(res)->toString() : string("NULL")) << endl;
				if (res != nullptr) { // child was split, and the return value is the new key pointing to the smaller half
					// cout << "[BPlusTree::append] child split, try to append new key to this internal node" << endl;
					if (pageToAddTo.append (res)) { // try to add the new key to this internal node
						// cout << "[BPlusTree::append] append of new key to internal node failed, sorting in-place and returning" << endl;
						MyDB_INRecordPtr bufL = getINRecord ();
						MyDB_INRecordPtr bufR = getINRecord ();
						function <bool ()> comparator = buildComparator (bufL, bufR);
						pageToAddTo.sortInPlace (comparator, bufL, bufR);
						return nullptr;
					}
					// cout << "[BPlusTree::append] appended new key to internal node successfully, calling split on internal node" << endl;
					auto newRecPtr = split (pageToAddTo, res);
					return newRecPtr;
				}
				return nullptr;
			}
		}
		// to avoid warning
		return nullptr;
	}
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
	// cout << "BPlusTree Structure: rootLocation=" << rootLocation << endl;
	MyDB_PageReaderWriter rootPage = (*this)[rootLocation];
	vector<MyDB_PageReaderWriter> list;
	list.push_back(rootPage);
	while (true) {
		vector<MyDB_PageReaderWriter> newList;
		bool isLeafLayer = true;
		for (auto page : list) {
			MyDB_RecordIteratorAltPtr it = page.getIteratorAlt();
			cout << "|||";
			if (page.getType() == MyDB_PageType::DirectoryPage) {
				isLeafLayer = false;
				MyDB_INRecordPtr tempRec = getINRecord();
				while (it->advance()) {
					it->getCurrent(tempRec);
					int pageNum = tempRec->getPtr();
					MyDB_PageReaderWriter childPage = (*this)[pageNum];
					newList.push_back(childPage);
					cout << " " << tempRec->getKey()->toString() << "->" << pageNum << " |";
				}
			} else {
				MyDB_RecordPtr tempRec = getEmptyRecord();
				while (it->advance()) {
					it->getCurrent(tempRec);
					cout << " " << getKey(tempRec)->toString() << " |";
				}

			}
			cout << "|||   ";
		}
		cout << "end of line \n";
		if (isLeafLayer) {
			break;
		}
		list = newList;
	}
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}

#endif
