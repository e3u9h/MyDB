
#ifndef SORT_C
#define SORT_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "Sorting.h"
#include <vector>
#include <queue>
#include "MyDB_PageListIteratorAlt.h"

using namespace std;

void mergeIntoFile (MyDB_TableReaderWriter &sortIntoMe, vector <MyDB_RecordIteratorAltPtr> &mergeUs, 
                   function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
    auto compare = [&](int left, int right) {
		mergeUs[left]->getCurrent (lhs);
		mergeUs[right]->getCurrent (rhs);
		return !comparator ();
	};
	priority_queue <int, vector <int>, decltype(compare)> pq(compare);
	for (int i = 0; i < mergeUs.size (); i++) {
		// before pushing into the priority queue, we need to check whether the iterator has records
		if (mergeUs[i]->advance()) {
			pq.push (i);
		}
	}
	MyDB_RecordPtr toAppend = lhs;
	while (!pq.empty ()) {
		int which = pq.top ();
		pq.pop ();
		mergeUs[which]->getCurrent (toAppend);
		sortIntoMe.append (toAppend);
		if (mergeUs[which]->advance ()) {
			pq.push(which);
		}
	}

}

vector <MyDB_PageReaderWriter> mergeIntoList (MyDB_BufferManagerPtr parent, MyDB_RecordIteratorAltPtr leftIter,
                                             MyDB_RecordIteratorAltPtr rightIter, function <bool ()> comparator, 
                                             MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
    
	vector <MyDB_PageReaderWriter> result;
	MyDB_PageReaderWriterPtr curPage = make_shared <MyDB_PageReaderWriter> (*parent);
	result.push_back (*curPage);

	
	// according to the code of MyDB_PageRecIteratorAlt, calling advance at the beginning is valid, and it can check whether the page is empty
	bool leftCanAdvance = leftIter->advance();
	bool rightCanAdvance = rightIter->advance();
	
	while (leftCanAdvance || rightCanAdvance) {
		if (leftCanAdvance)
			leftIter->getCurrent (lhs);
		if (rightCanAdvance)
			rightIter->getCurrent (rhs);

		MyDB_RecordPtr toAppend;
		
		if (!leftCanAdvance) {
			toAppend = rhs;
		} else if (!rightCanAdvance) {
			toAppend = lhs;
		} else {
			if (comparator ()) {
				toAppend = lhs;
			} else {
				toAppend = rhs;
			}
		}
		
		if (!curPage->append (toAppend)) {
			curPage = make_shared <MyDB_PageReaderWriter> (*parent);
			result.push_back (*curPage);
			curPage->append (toAppend);
		}
		
		if (toAppend == lhs && leftCanAdvance) { // because advance can be false for at most once (because it uses != to check), so we need to avoid using advance again if it is already false
			leftCanAdvance = leftIter->advance();
		} else if (toAppend == rhs && rightCanAdvance) {
			rightCanAdvance = rightIter->advance();
		}
	}
	return result;
}

void sort (int runSize, MyDB_TableReaderWriter &sortMe, MyDB_TableReaderWriter &sortIntoMe, 
          function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	// cout << sortMe.getNumPages () << " pages to sort." << endl << flush;
    if (sortMe.getNumPages () == 0) {
        return;
    }
    
    vector<vector<MyDB_PageReaderWriter>> individualPages;
	for (int i = 0; i < sortMe.getNumPages (); i++) {
		MyDB_PageReaderWriterPtr sorted = sortMe[i].sort (comparator, lhs, rhs);
		vector <MyDB_PageReaderWriter> curRun;
		curRun.push_back (*sorted);
		individualPages.push_back(curRun);
	}
	vector<vector<MyDB_PageReaderWriter>> sortedRuns = individualPages;
	int numOfGroups = (individualPages.size() + runSize - 1) / runSize; // ceil(individualPages.size() / runSize)
	int currentNumOfGroupsEachRun = runSize;

	// our goal is to make each run (except possibly the last one) have exactly runSize (not necessarily a power of 2) pages
	while (sortedRuns.size() > numOfGroups)
	{ // (sortMe.getNumPages() + runSize - 1) / runSize = ceil(sortMe.getNumPages() / runSize)，means we will stop when the number of runs decreases to the number of runs we want
		vector<vector<MyDB_PageReaderWriter>> newRuns;
		
		for (int i = 0; i < sortedRuns.size(); i += 2)
		{
			if ((i + 1) % currentNumOfGroupsEachRun != 0 && i + 1 < sortedRuns.size())
			{ // we use (i + 1) % currentNumOfGroupsEachRun != 0 to ensure that we only merge runs within one group (the pages within one group will become one run after merging), when i + 1 is a multiple of currentNumOfGroupsEachRun, it means i and i + 1 are in different groups, so we should not merge them
				MyDB_RecordIteratorAltPtr leftIter = make_shared<MyDB_PageListIteratorAlt>(sortedRuns[i]);
				MyDB_RecordIteratorAltPtr rightIter = make_shared<MyDB_PageListIteratorAlt>(sortedRuns[i + 1]);
				vector<MyDB_PageReaderWriter> merged = mergeIntoList(sortMe.getBufferMgr(), leftIter, rightIter, comparator, lhs, rhs);
				newRuns.push_back(merged);
			}
			else
			{
				newRuns.push_back(sortedRuns[i]);
				i--; // to avoid skipping i+1 because we did not merge i and i+1 and will add i+=2 in the next iteration
			}
		}
		sortedRuns = newRuns;
		currentNumOfGroupsEachRun = (currentNumOfGroupsEachRun + 1) / 2; // after each merging, the number of groups in each run divides by 2
	}

	vector<MyDB_RecordIteratorAltPtr> finalMergeList;
	for (auto &run : sortedRuns)
	{
		finalMergeList.push_back(make_shared<MyDB_PageListIteratorAlt>(run));
	}
	mergeIntoFile(sortIntoMe, finalMergeList, comparator, lhs, rhs);
}

#endif
