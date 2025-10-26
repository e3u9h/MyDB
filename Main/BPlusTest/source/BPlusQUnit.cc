
#ifndef BPLUS_TEST_H
#define BPLUS_TEST_H

#include "MyDB_AttType.h"  
#include "MyDB_BufferManager.h"
#include "MyDB_Catalog.h"  
#include "MyDB_Page.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_Record.h"
#include "MyDB_Table.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_Schema.h"
#include "QUnit.h"
#include "Sorting.h"
#include <iostream>

#define FALLTHROUGH_INTENDED do {} while (0)

int main (int argc, char *argv[]) {

	int start = 1;
	if (argc > 1 && argv[1][0] >= '0' && argv[1][0] <= '9') {
		start = argv[1][0] - '0';
	}

	QUnit::UnitTest qunit(cerr, QUnit::normal);

	// create a catalog
	MyDB_CatalogPtr myCatalog = make_shared <MyDB_Catalog> ("catFile");

	// now make a schema
	MyDB_SchemaPtr mySchema = make_shared <MyDB_Schema> ();
	mySchema->appendAtt (make_pair ("suppkey", make_shared <MyDB_IntAttType> ()));
	mySchema->appendAtt (make_pair ("name", make_shared <MyDB_StringAttType> ()));
	mySchema->appendAtt (make_pair ("address", make_shared <MyDB_StringAttType> ()));
	mySchema->appendAtt (make_pair ("nationkey", make_shared <MyDB_IntAttType> ()));
	mySchema->appendAtt (make_pair ("phone", make_shared <MyDB_StringAttType> ()));
	mySchema->appendAtt (make_pair ("acctbal", make_shared <MyDB_DoubleAttType> ()));
	mySchema->appendAtt (make_pair ("comment", make_shared <MyDB_StringAttType> ()));

	// use the schema to create a table
	MyDB_TablePtr myTable = make_shared <MyDB_Table> ("supplier", "supplier.bin", mySchema);

	cout << "Using small page size.\n";

	switch (start) {
	case 1:
	{
		cout << "\nTOY TEST: loading supplierToy.tbl (debug)" << endl;
		MyDB_BufferManagerPtr toyMgr = make_shared<MyDB_BufferManager>(1024, 128, "tempToy");
		MyDB_BPlusTreeReaderWriter toyTable("suppkey", myTable, toyMgr);

		// try relative path first; if your working directory is project root, this should work
		cout << "about to call loadFromTextFile()" << endl;
		toyTable.loadFromTextFile("supplierToy.tbl");
		cout << "returned from loadFromTextFile()" << endl;
		// print the B+ tree structure to help debugging
		cout << "\n--- BEGIN TOY TABLE PRINTTREE ---\n";
		toyTable.printTree();
		cout << "--- END TOY TABLE PRINTTREE ---\n\n";

		MyDB_RecordPtr rec = toyTable.getEmptyRecord();
		MyDB_RecordIteratorAltPtr it = toyTable.getIteratorAlt();
		int total = 0;
		int printed = 0;
		while (true) {
			if (!it->advance()) {
				break;
			}
			it->getCurrent(rec);
			if (printed < 5)
			{
				cout << "RECORD[" << (total + 1) << "] ";
				cout << "suppkey=" << rec->getAtt(0)->toInt() << "; ";
				cout << "name=\"" << rec->getAtt(1)->toString() << "\"; ";
				cout << "acctbal=" << rec->getAtt(5)->toDouble() << "; ";
				cout << "comment=\"" << rec->getAtt(6)->toString() << "\"\n";
				printed++;
			}
			total++;
		}
	cout << "TOY TEST: total records read = " << total << endl;
	QUNIT_IS_TRUE(total == 1000);

		// --- additional range-query check (toy) ---
		{
			MyDB_IntAttValPtr low = make_shared<MyDB_IntAttVal>();
			MyDB_IntAttValPtr high = make_shared<MyDB_IntAttVal>();
			low->set(10);
			high->set(20);
			cout << "TOY TEST: running range query [10,20] (debug)" << endl;
			MyDB_RecordIteratorAltPtr rangeIt = toyTable.getRangeIteratorAlt(low, high);
			MyDB_RecordPtr rrec = toyTable.getEmptyRecord();
			int rcnt = 0;
			while (rangeIt->advance()) {
				rangeIt->getCurrent(rrec);
				int k = rrec->getAtt(0)->toInt();
				if (k < 10 || k > 20) {
					cout << "ERROR: range iterator returned out-of-range key=" << k << endl;
				}
				rcnt++;
			}
			cout << "TOY TEST: range [10,20] count = " << rcnt << endl;
			QUNIT_IS_TRUE(rcnt == 11);
		}

		 // --- additional sorted-range check (toy) ---
		 {
			 MyDB_IntAttValPtr slow = make_shared<MyDB_IntAttVal>();
			 MyDB_IntAttValPtr shigh = make_shared<MyDB_IntAttVal>();
			 slow->set(1);
			 shigh->set(1000);
			 cout << "TOY TEST: running sorted range query [1,1000] (debug)" << endl;
			 MyDB_RecordIteratorAltPtr sIt = toyTable.getSortedRangeIteratorAlt(slow, shigh);
			 MyDB_RecordPtr trec = toyTable.getEmptyRecord();
			 int scnt = 0;
			 bool sOk = true;
			 int expect = 1;
			 while (sIt->advance()) {
				 sIt->getCurrent(trec);
				 int k = trec->getAtt(0)->toInt();
				 if (k != expect) {
					 cout << "ERROR: sorted range returned key=" << k << " expected=" << expect << endl;
					 sOk = false;
				 }
				 expect++;
				 scnt++;
			 }
			 cout << "TOY TEST: sorted range count = " << scnt << endl;
			 QUNIT_IS_TRUE(sOk && scnt == 1000);
		 }

		 // --- point query checks (toy) ---
		 {
			 bool pOk = true;
			 for (int i = 1; i <= 10; i++) {
				 int val = i * 100;
				 MyDB_IntAttValPtr plow = make_shared<MyDB_IntAttVal>();
				 MyDB_IntAttValPtr phigh = make_shared<MyDB_IntAttVal>();
				 plow->set(val);
				 phigh->set(val);
				 MyDB_RecordIteratorAltPtr pIt = toyTable.getSortedRangeIteratorAlt(plow, phigh);
				 MyDB_RecordPtr pr = toyTable.getEmptyRecord();
				 int pcnt = 0;
				 while (pIt->advance()) {
					 pIt->getCurrent(pr);
					 int k = pr->getAtt(0)->toInt();
					 if (k != val) {
						 cout << "ERROR: point query for " << val << " returned " << k << endl;
						 pOk = false;
					 }
					 pcnt++;
				 }
				 if (pcnt != 1) {
					 cout << "ERROR: point query for " << val << " count = " << pcnt << endl;
					 pOk = false;
				 }
			 }
			 QUNIT_IS_TRUE(pOk);
		 }
	}
	// case 1:
	// {
	// 	cout << "TEST 1... creating tree for small table, on suppkey " << flush;
	// 	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
	// 	MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);
	// 	supplierTable.loadFromTextFile ("supplier.tbl");

    //             // there should be 10000 records
    //             MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
    //             MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

    //             int counter = 0;
    //             while (myIter->advance ()) {
    //                     myIter->getCurrent (temp);
    //                     counter++;
    //             }
	// 	bool result = (counter == 10000);
	// 	if (result)
	// 		cout << "\tTEST PASSED\n";
	// 	else
	// 		cout << "\tTEST FAILED\n";
    //             QUNIT_IS_TRUE (result);
	// }
	// FALLTHROUGH_INTENDED;
	// case 2:
	// {
	// 	cout << "TEST 2... creating tree for small table, on nationkey " << flush;
	// 	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
	// 	MyDB_BPlusTreeReaderWriter supplierTable ("nationkey", myTable, myMgr);
	// 	supplierTable.loadFromTextFile ("supplier.tbl");

    //             // there should be 10000 records
    //             MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
    //             MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

    //             int counter = 0;
    //             while (myIter->advance ()) {
    //                     myIter->getCurrent (temp);
    //                     counter++;
    //             }
	// 	bool result = (counter == 10000);
	// 	if (result)
	// 		cout << "\tTEST PASSED\n";
	// 	else
	// 		cout << "\tTEST FAILED\n";
    //             QUNIT_IS_TRUE (result);
	// }
	// FALLTHROUGH_INTENDED;
	// case 3:
	// {
	// 	cout << "TEST 3... creating tree for small table, on comment " << flush;
	// 	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
	// 	MyDB_BPlusTreeReaderWriter supplierTable ("comment", myTable, myMgr);
	// 	supplierTable.loadFromTextFile ("supplier.tbl");

    //             // there should be 10000 records
    //             MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
    //             MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

    //             int counter = 0;
    //             while (myIter->advance ()) {
    //                     myIter->getCurrent (temp);
    //                     counter++;
    //             }
	// 	bool result = (counter == 10000);
	// 	if (result)
	// 		cout << "\tTEST PASSED\n";
	// 	else
	// 		cout << "\tTEST FAILED\n";
    //             QUNIT_IS_TRUE (result);
	// }
	// FALLTHROUGH_INTENDED;
	// case 4:
	// {
	// 	cout << "TEST 4... creating tree for large table, on comment " << flush;
	// 	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
	// 	MyDB_BPlusTreeReaderWriter supplierTable ("comment", myTable, myMgr);
	// 	supplierTable.loadFromTextFile ("supplierBig.tbl");

    //             // there should be 320000 records
    //             MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
    //             MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

    //             int counter = 0;
    //             while (myIter->advance ()) {
    //                     myIter->getCurrent (temp);
    //                     counter++;
    //             }
	// 	bool result = (counter == 320000);
	// 	if (result)
	// 		cout << "\tTEST PASSED\n";
	// 	else
	// 		cout << "\tTEST FAILED\n";
    //             QUNIT_IS_TRUE (result);
	// }
	// FALLTHROUGH_INTENDED;
	// case 5:
	// {
	// 	cout << "TEST 5... creating tree for large table, on comment asking some queries" << flush;
	// 	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
	// 	MyDB_BPlusTreeReaderWriter supplierTable ("comment", myTable, myMgr);
	// 	supplierTable.loadFromTextFile ("supplierBig.tbl");

    //             // there should be 320000 records
    //             MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
    //             MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

	// 	MyDB_StringAttValPtr low = make_shared <MyDB_StringAttVal> ();
	// 	low->set ("slyly ironic");
	// 	MyDB_StringAttValPtr high = make_shared <MyDB_StringAttVal> ();
	// 	high->set ("slyly ironic~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

	// 	myIter = supplierTable.getRangeIteratorAlt (low, high);
	// 	int counter = 0;
	// 	bool foundIt = false;
    //    		while (myIter->advance ()) {
    //    		      	myIter->getCurrent (temp);
	// 		counter++;
	// 		if (temp->getAtt (0)->toInt () == 4171)
	// 			foundIt = true;
    //    	        }
	// 	bool result = foundIt && (counter = 4192);
	// 	if (result)
	// 		cout << "\tTEST PASSED\n";
	// 	else
	// 		cout << "\tTEST FAILED\n";
    //             QUNIT_IS_TRUE (result);
	// }
	// FALLTHROUGH_INTENDED;
	// case 6:
	// {
	// 	cout << "TEST 6... creating tree for small table, on suppkey, checking for sorted order " << flush;
	// 	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
	// 	MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);
	// 	supplierTable.loadFromTextFile ("supplier.tbl");

    //             // there should be 10000 records
    //             MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();

    //             int counter = 0;
	// 	MyDB_IntAttValPtr low = make_shared <MyDB_IntAttVal> ();
	// 	low->set (1);
	// 	MyDB_IntAttValPtr high = make_shared <MyDB_IntAttVal> ();
	// 	high->set (10000);

	// 	MyDB_RecordIteratorAltPtr myIter = supplierTable.getSortedRangeIteratorAlt (low, high);
	// 	bool res = true;
    //             while (myIter->advance ()) {
    //                     myIter->getCurrent (temp);
    //                     counter++;
	// 		if (counter != temp->getAtt (0)->toInt ()) {
	// 			res = false;
	// 			cout << "Found key of " << temp->getAtt (0)->toInt () << ", expected " << counter << "\n";
	// 		}
    //             }
	// 	if (res && (counter == 10000))
	// 		cout << "\tTEST PASSED\n";
	// 	else
	// 		cout << "\tTEST FAILED\n";
    //             QUNIT_IS_TRUE (res && (counter == 10000));
	// }
	// FALLTHROUGH_INTENDED;
	// case 7:
	// {
	// 	cout << "TEST 7... creating tree for small table, on suppkey, running point queries " << flush;
	// 	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
	// 	MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);
	// 	supplierTable.loadFromTextFile ("supplier.tbl");

    //             // there should be 10000 records
    //             MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();

	// 	int counter = 0;
	// 	bool res = true;
	// 	for (int i = 1; i < 101; i++) {
	// 		MyDB_IntAttValPtr low = make_shared <MyDB_IntAttVal> ();
	// 		low->set (i * 19);
	// 		MyDB_IntAttValPtr high = make_shared <MyDB_IntAttVal> ();
	// 		high->set (i * 19);
	
	// 		MyDB_RecordIteratorAltPtr myIter = supplierTable.getSortedRangeIteratorAlt (low, high);
	// 		while (myIter->advance ()) {
	// 			myIter->getCurrent (temp);
	// 			counter++;
	// 			if (i * 19 != temp->getAtt (0)->toInt ()) {
	// 				res = false;
	// 				cout << "Found key of " << temp->getAtt (0)->toInt () << ", expected " << i * 19 << "\n";
	// 			}
	// 		}
	// 	}
	// 	if (res && (counter == 100))
	// 		cout << "\tTEST PASSED\n";
	// 	else
	// 		cout << "\tTEST FAILED\n";
	// 	QUNIT_IS_TRUE (res && (counter == 100));
	// }
	// FALLTHROUGH_INTENDED;
	// case 8:
	// {
	// 	cout << "TEST 8... creating tree for small table, on comment, running point queries with no answer " << flush;
	// 	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
	// 	MyDB_BPlusTreeReaderWriter supplierTable ("comment", myTable, myMgr);
	// 	supplierTable.loadFromTextFile ("supplier.tbl");

    //             // there should be 10000 records
    //             MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();

	// 	int counter = 0;
	// 	for (int i = 0; i < 26; i++) {
	// 		MyDB_StringAttValPtr low = make_shared <MyDB_StringAttVal> ();
	// 		char a = 'a' + i;
	// 		low->set (string (&a));
	// 		MyDB_StringAttValPtr high = make_shared <MyDB_StringAttVal> ();
	// 		high->set (string (&a));
	
	// 		MyDB_RecordIteratorAltPtr myIter = supplierTable.getSortedRangeIteratorAlt (low, high);
	// 		while (myIter->advance ()) {
	// 			myIter->getCurrent (temp);
	// 			counter++;
	// 		}
	// 	}
	// 	if (counter == 0)
	// 		cout << "\tTEST PASSED\n";
	// 	else
	// 		cout << "\tTEST FAILED\n";
	// 	QUNIT_IS_TRUE (counter == 0);
	// }
	// FALLTHROUGH_INTENDED;
	// case 9:
	// {
	// 	cout << "TEST 9... creating tree for small table, on suppkey, running point queries under and over range " << flush;
	// 	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
	// 	MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);
	// 	supplierTable.loadFromTextFile ("supplier.tbl");

    //             // there should be 10000 records
    //             MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();

	// 	int counter = 0;
	// 	for (int i = -1; i <= 10001; i += 10002) {
	// 		MyDB_IntAttValPtr low = make_shared <MyDB_IntAttVal> ();
	// 		low->set (i);
	// 		MyDB_IntAttValPtr high = make_shared <MyDB_IntAttVal> ();
	// 		high->set (i);
	
	// 		MyDB_RecordIteratorAltPtr myIter = supplierTable.getSortedRangeIteratorAlt (low, high);
	// 		while (myIter->advance ()) {
	// 			myIter->getCurrent (temp);
	// 			counter++;
	// 		}
	// 	}
	// 	if (counter == 0)
	// 		cout << "\tTEST PASSED\n";
	// 	else
	// 		cout << "\tTEST FAILED\n";
	// 	QUNIT_IS_TRUE (counter == 0);
	// }
	// FALLTHROUGH_INTENDED;
	// case 10:
	// {
	// 	cout << "TEST 10... mega test using tons of range queries " << flush;
	// 	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024 * 128, 128, "tempFile");
	// 	MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);

	// 	// load it from a text file
	// 	supplierTable.loadFromTextFile ("supplierBig.tbl");

    //             // there should be 320000 records
    //             MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
    //             MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

	// 	// now, we check 100 different random suppliers queries
	// 	bool allOK = true;
	// 	for (int time = 0; time < 2; time++) {
	// 		for (int i = 0; i < 100; i++) {
	
	// 			// we are looping through twice; the first time, ask only point queries
	// 			srand48 (i);
	// 			int lowBound = lrand48 () % 10000;
	// 			int highBound = lrand48 () % 10000;
	// 			if (time % 2 == 0)
	// 				highBound = lowBound;
	
	// 			// make sure the low bound is less than the high bound
	// 			if (lowBound > highBound) {
	// 				int temp = lowBound;
	// 				lowBound = highBound;
	// 				highBound = temp;
	// 			}
	
	// 			// ask a range query
	// 			MyDB_IntAttValPtr low = make_shared <MyDB_IntAttVal> ();
	// 			low->set (lowBound);
	// 			MyDB_IntAttValPtr high = make_shared <MyDB_IntAttVal> ();
	// 			high->set (highBound);

	// 			if (i % 2 == 0) 
	// 				myIter = supplierTable.getRangeIteratorAlt (low, high);
	// 			else
	// 				myIter = supplierTable.getSortedRangeIteratorAlt (low, high);
		
	// 			// verify we got exactly the correct count back
	// 			int counter = 0;
    //    		         	while (myIter->advance ()) {
    //    		                	myIter->getCurrent (temp);
	// 				counter++;
    //    	         		}
	
	// 			if (counter != 32 * (highBound - lowBound + 1))
	// 				allOK = false;
	// 		}
	// 	}
	// 	if (allOK)
	// 		cout << "\tTEST PASSED\n";
	// 	else
	// 		cout << "\tTEST FAILED\n";
	// 	QUNIT_IS_TRUE (allOK);
	// }
		
	}
}

#endif


