/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  bool hasIndex=false;
  bool useIndex=false;
  BTreeIndex btIdx;
  IndexCursor cursor;

  int equalKey=-1;
  bool hasEqual=false;
  int minKey = -200;
  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  if((rc =btIdx.open(table + ".idx",'r'))<0)
    hasIndex=false;
  else
    hasIndex=true;
  if(attr == 4&&hasIndex){
    useIndex = true;
  }
  if(hasIndex){//to check whether we need to use index.
      for (unsigned i = 0; i < cond.size(); i++) {
          if (cond[i].attr==2)
            continue;
          else if (cond[i].attr==1) {
                switch(cond[i].comp){
                  case SelCond::EQ:
                    useIndex=true;
                    hasEqual=true;
                    equalKey = atoi(cond[i].value);
                    break;
                  case SelCond::LT:
                    useIndex=true;
                    break;
                  case SelCond::GT:
                    useIndex=true;
                    if (atoi(cond[i].value) > minKey)
                      minKey = atoi(cond[i].value);
                    break;                  
                  case SelCond::LE:
                    useIndex=true;
                    break;                  
                  case SelCond::GE:
                    if (atoi(cond[i].value) > minKey)
                      minKey = atoi(cond[i].value);
                    useIndex=true;
                    break;                  
                  default: //if NE not equal, has nothing to do with the index
                    break;
                }
          }
      }  
  }
  //cout<<"hasIndex:  "<<hasIndex<<" useIndex:  "<<useIndex<<endl;
  if (hasIndex && useIndex)
  {
    count=0;
    if (hasEqual){
    //  cout<<"has Equal Key"<<endl;
      btIdx.locate(equalKey,cursor);
    }
    else
      btIdx.locate(minKey,cursor);
    //cout<<"equalKey: "<<equalKey<<" minKey: "<<minKey<<endl;
    //cout<<"cursor.pid: "<<cursor.pid<<" cursor.eid: "<<cursor.eid<<endl;
    while(true) {
        rc=btIdx.readForward(cursor,key,rid);
        //cout<<"RC: "<<rc<<" key: "<<key <<"cursor.pid: "<<cursor.pid<<" cursor.eid: "<<cursor.eid<<endl;
        if(rc<0)
          break;
        if(rc!=0) //rc may be 1, at the end of a node, but this node is not the end of the tree.
          continue;
        //cout<<"key read: "<<key<<endl;
        //if count(*), we do not need to read the record.
        if(attr!=4){  
          if ( (rc = rf.read(rid, key, value)) < 0){
            goto exit_select;
          }
        }
        for (unsigned i = 0; i < cond.size(); i++)
        {
          switch (cond[i].attr) 
          {
              case 1:
                diff = key - atoi(cond[i].value);
                break;
              case 2:
                diff = strcmp(value.c_str(), cond[i].value);
                break;
          }
          switch (cond[i].comp) {
            case SelCond::EQ:
              //cout<<"equal condition, diff is : " <<diff<<endl;
              if (diff != 0)
                if (cond[i].attr == 1) 
                  goto exit_select; // key is not found in the index.        
                else 
                  continue; // it is value. continue to next tuple
              break;
          case SelCond::NE: //actually it is not possible.
              if (diff == 0) continue;
              break;
          case SelCond::GT:
              if (diff <= 0) continue;
              break;
          case SelCond::LT:
              if (diff >= 0)
                if (cond[i].attr == 1) goto exit_select;
                else continue;
              break;
          case SelCond::GE:
              if (diff < 0) continue;
              break;
          case SelCond::LE:
              if (diff > 0)
                if (cond[i].attr == 1) goto exit_select;
                else continue;
              break;
          }
        }
      

        count++;
        // cout<<"print a tuple"<<endl;
        // cout<<"attr is "<<attr<<endl;
      // print the tuple 
      switch (attr) {
      case 1:  // SELECT key
        fprintf(stdout, "%d\n", key);
        break;
      case 2:  // SELECT value
        fprintf(stdout, "%s\n", value.c_str());
        break;
      case 3:  // SELECT *
        fprintf(stdout, "%d '%s'\n", key, value.c_str());
        break;
      }
     }

    }
      else{
          //if index is not used.
          // scan the table file from the beginning
          rid.pid = rid.sid = 0;
          count = 0;
          while (rid < rf.endRid()) {
            // read the tuple
            if ((rc = rf.read(rid, key, value)) < 0) {
              fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
              goto exit_select;
            }

            // check the conditions on the tuple
            for (unsigned i = 0; i < cond.size(); i++) {
              // compute the difference between the tuple value and the condition value
              switch (cond[i].attr) {
              case 1:
        	diff = key - atoi(cond[i].value);
        	break;
              case 2:
        	diff = strcmp(value.c_str(), cond[i].value);
        	break;
              }

              // skip the tuple if any condition is not met
              switch (cond[i].comp) {
              case SelCond::EQ:
        	if (diff != 0) goto next_tuple;
        	break;
              case SelCond::NE:
        	if (diff == 0) goto next_tuple;
        	break;
              case SelCond::GT:
        	if (diff <= 0) goto next_tuple;
        	break;
              case SelCond::LT:
        	if (diff >= 0) goto next_tuple;
        	break;
              case SelCond::GE:
        	if (diff < 0) goto next_tuple;
        	break;
              case SelCond::LE:
        	if (diff > 0) goto next_tuple;
        	break;
              }
            }

            // the condition is met for the tuple. 
            // increase matching tuple counter
            count++;

            // print the tuple 
            switch (attr) {
            case 1:  // SELECT key
              fprintf(stdout, "%d\n", key);
              break;
            case 2:  // SELECT value
              fprintf(stdout, "%s\n", value.c_str());
              break;
            case 3:  // SELECT *
              fprintf(stdout, "%d '%s'\n", key, value.c_str());
              break;
            }

            // move to the next tuple
            next_tuple:
            ++rid;
          }
      }
  // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  // close the table file and return
  exit_select:
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  /* your code here */
  RecordFile rf;
  rf.open(table+".tbl",'w');
  ifstream infile;
  infile.open(loadfile.c_str(),ifstream::in);
 // infile.open (loadfile.c_str(), std::fstream::in | std::fstream::out | std::fstream::app);
  if (!infile.is_open()){
      fprintf(stderr, "Error: failed to open file, %s\n",loadfile.c_str()); 
      return -1;
  }
  int key;
  string value;
  string line;
  RecordId rid;
  RC rc;
  BTreeIndex btIdx;
  if (index){ //open Btree index file.
      string idxName = table+".idx";
      if ((rc=btIdx.open(idxName,'w'))<0) {
        return rc;
      } 
  }
  while (getline(infile,line)){
      if(parseLoadLine(line,key,value) != 0){ //parse the line into key and value
        fprintf(stderr, "Error: failed to parse, key: %d  value: %s\n",key,value.c_str()); 
        return -1;
      }
      if(rf.append(key,value,rid)){
        fprintf(stderr, "Error: failed to append, key: %d value: %s\n",key,value.c_str()); 
        return -1;
      }
      if(index){
        if( (rc = btIdx.insert(key,rid))<0 )
            //cout<<"insert error!!  key is : "<<key<<endl;
            return rc;
      }
  }
  infile.close();
  if (index)
    btIdx.close();
  return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
