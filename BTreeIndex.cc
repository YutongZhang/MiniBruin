/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = 1;    //initial root stored in pid 1
    treeHeight = 1;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{

	RC rc;
	if ((rc = pf.open(indexname,mode))<0){
		return rc;
	}
	//the index is empty, initialize a empty root.
	if(pf.endPid()<=0){
		BTLeafNode lNode;
		lNode.write(1, pf); //initialize the root node as a leaf node
		return 0;
	}
	//Pid 0 of the index is used to store rootPid and treeHeight.
	// index already exists.
	// read rootPid, treeHeight from PID 0
	else{
		char *buf = new char[PageFile::PAGE_SIZE];
		if ((rc = pf.read(0,buf))<0)
			return rc;
		memcpy(&rootPid,buf,sizeof(PageId));
		memcpy(&treeHeight,buf+sizeof(PageId),sizeof(int));
	}
    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	//write rootPid, treeHeight back into Pid 0
	RC rc;
	char *buf = new char[PageFile::PAGE_SIZE];
    memcpy(buf,&rootPid,sizeof(PageId));
    memcpy(buf+sizeof(PageId),&treeHeight,sizeof(int));
    if ((rc = pf.write(0,buf))<0)
		return rc;
	if ((rc = pf.close())<0)
		return rc;
    return 0;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	PageId pid = rootPid;

	//initialize the b+ tree
	if (treeHeight == 1) 
	{
		BTLeafNode lNode;
		lNode.read(pid, pf);
		if (lNode.getKeyCount() < lNode.MAX_LEAF_COUNT) //leaf node not full
			lNode.insert(key, rid);
		else                                            //leaf node full
		{
			BTLeafNode lSibling;
			BTNonLeafNode newRoot;
			int upKey;
			lNode.insertAndSplit(key, rid, lSibling, upKey);
			PageId sibling = pf.endPid();
			lSibling.write(sibling, pf);
			lNode.setNextNodePtr(sibling);
			newRoot.initializeRoot(pid, upKey, sibling); //new tree root
			PageId endPid = pf.endPid();
			newRoot.write(endPid, pf);
			rootPid = endPid; //update root and height info.
			treeHeight++;
		}
		lNode.write(pid, pf);
		return 0;
	}

	PageId newSibling = -1; //pageId of the new sibling, default -1 means no new sibling
	int newKey;
	insertRecursive(0, pid, key, rid, newSibling, newKey); //start recursive

	if (newSibling == -1)  //no new node added
		return 0;
	//new node added. create new root            
	BTNonLeafNode newRoot;
    newRoot.initializeRoot(pid, newKey, newSibling); //new tree root
	PageId endPid = pf.endPid();
	newRoot.write(endPid, pf);
	rootPid = endPid; //update root and height info.
	treeHeight++;
	return 0;
}

RC BTreeIndex::insertRecursive(int height, PageId pid, int key, const RecordId& rid, PageId& sibling, int& upKey)
{
	//leaf node
	if (height == treeHeight - 1)
	{
		BTLeafNode lNode;
		lNode.read(pid, pf);
		if (lNode.getKeyCount() < lNode.MAX_LEAF_COUNT) //leaf node not full
			lNode.insert(key, rid);
		else                                            //leaf node full
		{
			BTLeafNode lSibling;
			lNode.insertAndSplit(key, rid, lSibling, upKey);
			sibling = pf.endPid();
			lSibling.setNextNodePtr(lNode.getNextNodePtr());
			lNode.setNextNodePtr(sibling);  //set next node pointer
			lSibling.write(sibling, pf);
		}
		lNode.write(pid, pf);
		return 0;
	}

	//nonleaf node
	BTNonLeafNode nlNode;
	nlNode.read(pid, pf);

	PageId newPid;
	nlNode.locateChildPtr(key, newPid);
	PageId newSibling = -1;
	int newKey;
	insertRecursive(height + 1, newPid, key, rid, newSibling, newKey); //recursive

	if (newSibling == -1) //no new pair added
		return 0;

	if (nlNode.getKeyCount() < nlNode.MAX_NONLEAF_COUNT)  //node not full
	{
		nlNode.insert(newKey, newSibling);
	}
	else                                                 //node full
	{
		BTNonLeafNode nlSibling;
		nlNode.insertAndSplit(newKey, newSibling, nlSibling, upKey);
		sibling = pf.endPid();
		nlSibling.write(sibling, pf);
	}

	nlNode.write(pid, pf);
	return 0;
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	PageId pid;
	pid = rootPid;//initializaiton
	BTNonLeafNode nlNode;
	BTLeafNode lNode;
	RC rc;
	int eid;
	cout<<" treeHEIGHT: "<<treeHeight<<" rootPid: "<<rootPid<<endl;
	for (int i = 0; i < treeHeight-1; ++i)
	{	cout<<"in the for"<<endl;
		if((rc=nlNode.read(pid,pf))<0)
			cout<<"return 1"<<endl;
			return rc;
		if ((rc=nlNode.locateChildPtr(searchKey,pid))<0)
			cout<<"return 2"<<endl;
			return rc;
		cout<<"?????"<<endl;
	}
	cout<<"out the for"<<endl;
	//arrive at a leaf node
	//the pid points to a correct leaf node.
	if ((rc=lNode.read(pid,pf))<0){
		cout<<"return 3"<<endl;
		return rc;
	}
	//locate the entry.
	if ((rc=lNode.locate(searchKey,eid))<0){
		cout<<"leaf locate: "<<rc<<" "<<eid<<" "<<pid<<endl;
		cursor.eid = eid;
		cursor.pid = pid;
		return rc;
	}
	cout<<"locate return successfully " <<eid <<" "<<pid<<endl;
    return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	RC rc;
	BTLeafNode lNode;
	if ((rc=lNode.read(cursor.pid,pf))<0)
		return rc;
	if (cursor.eid==lNode.getKeyCount()) { //at the end of the node.
		cursor.pid = lNode.getNextNodePtr();
		cursor.eid =0; //set to next sibling
		if(cursor.pid ==-1) // for the last leaf node, what should we set the next node pointer? suppose -1.
			return RC_END_OF_TREE;
		else
			return 1; //Do not know what should return.. 
	}
	if ((rc=lNode.readEntry(cursor.eid,key,rid))<0)
		return rc;	
	cursor.eid++;
    return 0;
}
