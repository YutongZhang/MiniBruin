#include "BTreeNode.h"

using namespace std;

BTLeafNode::BTLeafNode(){

}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 	
	return pf.read(pid,buffer); 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	return pf.write(pid,buffer); 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 	int keyCount=0;
	memcpy(&keyCount,buffer+PageFile::PAGE_SIZE-8,sizeof(int));
	return keyCount; 
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 	int keyCount = getKeyCount();
	if (keyCount>=MAX_LEAF_COUNT)
		return RC_NODE_FULL;
	int eid;
	locate(key,eid);
	// insert the pair into location eid.
	
	//firstly, copy entries after eid to a temp buffer.
	int rightSize = (keyCount-eid)*sizeof(LeafEntry);
	char tmpBuffer[rightSize];
	memcpy(tmpBuffer,buffer+eid*sizeof(LeafEntry),rightSize);
	//secondly,insert the pair into entry eid.
	LeafEntry le;
	le.key =key;
	le.rid = rid;
	memcpy(buffer+eid*sizeof(LeafEntry),&le,sizeof(LeafEntry));
	//increase keyCount.
	keyCount++; 
	//write maxKey back into node.
	memcpy(buffer+PageFile::PAGE_SIZE-8,&keyCount,sizeof(int));
	return 0; 
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	int keyCount = getKeyCount(); //number of entries before insert. should be 84, 0-83
	//after insert and split, left:0-42, right:0-41
	int halfCount = keyCount/2;
	int eid;
	locate(key,eid);
	if (eid<=halfCount){ //inserted pair should be in left half
		for (int i = halfCount; i < keyCount; i++) //move right half to the sibling node
		{
			int sibKey;
			RecordId sibRid;
			readEntry(i,sibKey,sibRid);
			sibling.insert(sibKey,sibRid);
		}
		insert(key,rid); //insert pair into left half
		int leftCount = halfCount+1;
		memcpy(buffer+PageFile::PAGE_SIZE-8, &leftCount,sizeof(int));  //update keyCount of left half
	}
	else{//inserted pair should be in right half
		for (int i = halfCount+1; i < keyCount; i++) //move right half to the sibling node
		{
			int sibKey;
			RecordId sibRid;
			readEntry(i,sibKey,sibRid);
			sibling.insert(sibKey,sibRid);
		}
		sibling.insert(key,rid); //insert pair into right sibling half.
		int leftCount = halfCount;
		memcpy(buffer+PageFile::PAGE_SIZE-8, &leftCount,sizeof(int));  //update keyCount of left half
	}
	int sibKey;
	RecordId sibRid;
	readEntry(0,sibKey,sibRid);
	siblingKey = sibKey;
	return 0; 

}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{ 
	// loop through the entire leaf node to find the first key that is larger or equal to searchKey
	int maxKey = getKeyCount();
	int key;
	RecordId rid;
	for (int i = 0; i < maxKey; ++i)
	{
		readEntry(i,key,rid);
		if (key==searchKey) // find the key
		{
			eid = i;
			return 0;
		}
		else if(key>searchKey){ //not found, set eid to the entry immediately after the largest index key that is smaller than searchKey.
			eid = i;
			return RC_NO_SUCH_RECORD;
		}
	}
	eid = maxKey; // last item.
	return RC_NO_SUCH_RECORD; 
}


/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 	
	int maxKey = getKeyCount();
	if (eid<0 ||eid>=maxKey){
		return RC_NO_SUCH_RECORD;
	}
	LeafEntry le;
	memcpy(&le,buffer+(eid*sizeof(LeafEntry)), sizeof(LeafEntry));
	rid = le.rid;
	key = le.key;
	return 0; 

}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 	PageId pid;
	memcpy(&pid,buffer+PageFile::PAGE_SIZE-sizeof(PageId),sizeof(PageId));
	return 0; 
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 	memcpy(buffer+PageFile::PAGE_SIZE-sizeof(PageId),&pid,sizeof(PageId));
	return 0; 
}


/*=============================================================================*/


BTNonLeafNode::BTNonLeafNode(){


}
/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ return 0; }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ return 0; }

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ return 0; }


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ return 0; }

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ return 0; }

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ return 0; }

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ return 0; }
