#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

#define MIN_CNT -1

//Declare global variable for pKey Tag 
int globalPkeyTag=0;

#define RECORD_PRINTER(inputRecord, schema)			\
  do {									\
    Record *_lR = inputRecord;                                                   \
                                                      \
    int i;								\
    for(i = 0; i < schema->numAttr; i++)				\
      {									\
        Value *lVal, *rVal;                                             \
		    char *lSer; \
        getAttr(_lR, schema, i, &lVal);                                  \
        lSer = serializeValue(lVal); \
        printf("\nThe record is %s --\n",lSer); \
		    free(lVal); \
		    free(lSer); \
      }\
  } while(0)

typedef struct RecMgr {

    int rowCount;
	int emptyPage;
    BM_PageHandle pageHandle;
    BM_BufferPool bufferPool;

} RecMgr;

//**************************************FUNCTIONS BY INDRAJIT GHOSH START***********************************************//

/*
 * This is a custom data structure used by the Record Manager to manage records in a table.
 * It includes the following components:
 * - The BM_PageHandle used by the Buffer Manager to access Page files.
 * - The BM_BufferPool used by the Buffer Manager to manage the buffer pool.
 * - The record ID used to uniquely identify a record.
 * - The condition used to define the criteria for scanning the records in the table.
 * - The total number of tuples in the table.
 * - The location of the first free page that has empty slots in the table.
 * - The count of the number of records scanned.
 */

RecMgr *recordManager;

// ******** FUNCTIONS FOR TABLE MANAGER******** //

// Intialize the Storage Manager 
RC initRecordManager (void *mgmtData) {
	// Initiliazing Storage Manager
	printf("Record Manager Initialized\n");
	initStorageManager();
	//Success Code
	return RC_OK;
}

/*
 * This function shuts down the Record Manager by deallocating its memory and setting the recordManager
 * pointer to NULL.
 *
 * Returns:
 * - RC_OK: indicating success
 */
RC shutdownRecordManager() {

    // Deallocate the memory used by the record manager
    free(recordManager);

    // Set the recordManager pointer to NULL
    recordManager = NULL;

    // Return success
    return RC_OK;
}

// This function creates a TABLE with table name "name" having schema specified by "schema"
RC createTable (char *name, Schema *schema) {	
	
	RC code;
	RC_message = "Create Table Successfull";
	//Calculate the max number of Attributes of a Schema
	int maxNumAttr= (PAGE_SIZE - 16)/(64+4+4+4);
	//Return error if the schema size is too big
	if(schema->numAttr>maxNumAttr) {
		RC_message = "Schema Size too Big";
		code = RC_RM_SCHEMA_SIZE_TOO_BIG;
	}
	else {
		//Get record size of the schema passed for creation
		int maxRecordSize=PAGE_SIZE - 3 * 4 - 1; 
		//Return error is record size is too big  
		if(getRecordSize(schema)>maxRecordSize) {
			RC_message = "Record Size too Big";
			code = RC_RM_RECORD_SIZE_TOO_BIG;
		}
		else {
			//Capture the return value of functions
			// Allocating memory space to the record manager custom data structure
			recordManager = (RecMgr*) malloc(sizeof(RecMgr));
			recordManager->rowCount=0;
			if (recordManager == NULL) { 
				RC_message = "Memory Allcation failed in CreateTable()";
				code = RC_MALLOC_FAILED;
			}
			else {
				code = initBufferPool(&recordManager->bufferPool, name, 120, RS_LRU, NULL);
				if (code != RC_OK) {
					free(recordManager);
					RC_message = "initBufferPool failed in CreateTable()";
				} 
				else {
					//This data array will be used to store the table schema details
					char pageData[PAGE_SIZE];
					//This points to the first location of data[0],hence no & is used its the same
					//The pageHandle will be manipulating the pageData array ,therefor storing the 
					//address of the pageData
					char *pageHandle = pageData;
					// let the first location hold the number of tuples , intially it should be zero
					*(int*)pageHandle = 0; 
					// Point to the next memory location with an offset of size of int for the next data
					pageHandle += sizeof(int);
					// Set the first page to 1 (0th page is for schema/meta data)
					*(int*)pageHandle = 1;
					//Point to the next memory location
					pageHandle += sizeof(int);
					// Set the number of attributes
					*(int*)pageHandle = schema->numAttr;
					//Datatype of numAttr is int , hence next pointer offset will be shifted by size of int
					pageHandle += sizeof(int);
					// Setting the Key Size of the attributes
					*(int*)pageHandle = schema->keySize;
					pageHandle += sizeof(int);
					int i=0;
					while(i<schema->numAttr) {
							
						// In the buffer data setting the attribute names 
							strncpy(pageHandle, schema->attrNames[i], 15);
							pageHandle += 15;
					
						// In the buffer data setting the datatypes 
							*(int*)pageHandle = (int)schema->dataTypes[i];
							pageHandle+= sizeof(int);

						// In the buffer data setting the typelength
							*(int*)pageHandle = (int) schema->typeLength[i];
							pageHandle += sizeof(int);
							i++;		
					}	
					// Creates the pageFile for the table 
					code = createPageFile(name);
					if (code == RC_OK) {
						//To open the created File
						SM_FileHandle recordMgrFileHandler;
						code = openPageFile(name, &recordMgrFileHandler);
						if (code == RC_OK) { 
							// The schema is written to the first location of the pageFile
							code = writeBlock(0, &recordMgrFileHandler, pageData);
							if (code == RC_OK) { 
								// CLose the page file after writing has been done
								code = closePageFile(&recordMgrFileHandler);
								if (code == RC_OK) {
									// Set the table's metadata to the recordManager
									RM_TableData* tableData = (RM_TableData*) malloc(sizeof(RM_TableData));
									if (tableData == NULL) {
										free(recordManager);
										code = RC_MALLOC_FAILED;
									}
									else {
										tableData->name = name;
										tableData->schema = schema;
										tableData->mgmtData = recordManager;
									}
								}
							}
						}	
					}
				}
			}
		}
	}
	//Return status 
	return code;
}

// This function opens the table with table name "name"
extern RC openTable(RM_TableData *rel, char *name)
{
	/*
	 * RM_TableData has three members:
	 * 1. schema - which contains metadata about the table's attributes
	 * 2. mgmtData - which contains metadata specific to our custom record manager
	 * 3. name - which is the name of the table
	 */
	
	// Allocating memory space for 'schema'
	Schema *schema;
	schema = (Schema*) malloc(sizeof(Schema));  
	if(schema == NULL) return RC_SCHEMA_NOT_CREATED;  


	//// pageHandle; // pointer to a page of memory
	int attrCnt; // variable to store the number of attributes in the table

	// Setting the table's metadata to our custom record manager metadata structure
	rel->mgmtData = recordManager;
	// Setting the table's name
	rel->name = name;

	// Pinning a page i.e. putting a page in Buffer Pool using Buffer Manager
	
	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, 0);
	
	// Setting the initial pointer (0th location) if the record manager's page data
	SM_PageHandle pageHandle = (char*) recordManager->pageHandle.data;	
	pageHandle += sizeof(int);

	// Getting free page from the page file
	recordManager->emptyPage= *(int*) pageHandle;
	pageHandle += sizeof(int);
	
	// Getting the number of attributes from the page file
	attrCnt = *(int*)pageHandle;
	pageHandle += sizeof(int);
 	
	// Setting schema's parameters
	schema->numAttr = attrCnt;
	schema->attrNames = (char**) calloc(attrCnt, sizeof(char*));
	if(schema->attrNames == NULL) {
		free(schema);
		return RC_SCHEMA_NOT_CREATED;
	}
	schema->dataTypes = (DataType*) calloc(attrCnt, sizeof(DataType));
	if(schema->dataTypes == NULL) {
		free(schema->attrNames);
		free(schema);
		return RC_SCHEMA_NOT_CREATED;
	}
	schema->typeLength = (int*) calloc(attrCnt, sizeof(int));
	if(schema->typeLength == NULL) {
		free(schema->dataTypes);
		free(schema->attrNames);
		free(schema);		
		return RC_SCHEMA_NOT_CREATED;
	}
	

	// Allocate memory space for storing attribute name for each attribute
	int i=0;
	while(i< attrCnt) {
		schema->attrNames[i] = (char *)calloc(15, sizeof(char));
		i++;
	}
	
	int j=0;
	while(j < schema->numAttr) {
		// Setting attribute name then point to the next location
		//strncpy(schema->attrNames[j], pageHandle, ATTRIBUTE_SIZE);pageHandle += ATTRIBUTE_SIZE;	   
		 char *attrName = schema->attrNames[j];
    	 for (int i = 0; i < 15; i++) {
         attrName[i] = *(pageHandle++);
    	 }
		// Setting data type of attribute then point to the next location
		schema->dataTypes[j] = *(int*) pageHandle;pageHandle += sizeof(int);
		// Setting length of datatype (length of STRING) of the attribute then point to the next location
		schema->typeLength[j] = *(int*)pageHandle;pageHandle += sizeof(int);
		j++;
	}
	
	// Setting newly created schema to the table's schema
	rel->schema = schema;	

	// Unpinning the page i.e. removing it from Buffer Pool using Buffer Manager
	RC output;
	output=unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);
	if(output!=RC_OK) return RC_BUFFERPOOL_PAGE_NOTFOUND_IN_UNPINPAGE;
	// Write the page back to disk using Buffer Manager
	output=forcePage(&recordManager->bufferPool, &recordManager->pageHandle);
	if(output!=RC_OK) return RC_BUFFERPOOL_PAGE_NOTFOUND_IN_FORCEPAGE;

	return RC_OK;
}

// This function closes the table referenced by "rel"
extern RC closeTable(RM_TableData *rel) {
	//Check is the table data is null
	if (rel == NULL) {
        return RC_TABLE_NOT_FOUND;
    }

	// Retrieve the table's metadata
	RecMgr *recordManager = rel->mgmtData;

	// Shut down the Buffer Pool for this table
	shutdownBufferPool(&recordManager->bufferPool);

	// Set the table's metadata to NULL
	rel->mgmtData = NULL;

	// Return success code
	return RC_OK;
}

// This function will be used for deletion of table 
extern RC deleteTable(char *name) {
	// Use the storage manager to delete the page file associated with the table
	if (name != NULL || strlen(name) != 0 )
	{
	destroyPageFile(name);
	}
	// Return success code
	return RC_OK;
}

// This function returns the number of tuples (records) in the table referenced by "rel"
extern int getNumTuples(RM_TableData *rel) {

    // Check if the table or its metadata is NULL, return error code if so
    if (rel == NULL || rel->mgmtData == NULL) {
		RC_message = "The table or its metadata is NULL in getNumTuples()";
        return RC_TABLE_NOT_FOUND;
    }
    // Accessing the table's record manager metadata structure
    RecMgr *recordManager = rel->mgmtData;
    // Returning the total number of tuples (records) in the table
    return recordManager->rowCount;
}

//**************************************FUNCTIONS BY INDRAJIT GHOSH END***********************************************//



//**************************************FUNCTIONS BY VIVEKANAND REDDY MALIPATEL START***********************************************//

//*********RECORD FUNCTIONS**********//

int recordSize;

RC markandunpin(RecMgr *recordmgr){

	RC code = markDirty(&recordmgr->bufferPool, &recordmgr->pageHandle);
	if(code!=RC_OK) {
		return code;
	}
	return unpinPage(&recordmgr->bufferPool, &recordmgr->pageHandle);
}

RC checkPrimaryKey(RM_TableData *rel, char *slot, Record *record){

	RC code = RC_OK;

	Record *r;

	code = createRecord(&r, rel->schema);
	RecMgr *recordmgr = rel->mgmtData;

	Value *invalue = (Value *) malloc(sizeof(Value));
	Value *result = (Value *) malloc(sizeof(Value));

	code = getAttr(record, rel->schema, 0, &invalue);

	RID rid;
	int page = 1;
	int slota = 0;

	for(int i=0;i<recordmgr->rowCount;i++) {
		if(i>0){
			slota++;
			if(slota >= PAGE_SIZE / recordSize) {
				slota = 0;
				page +=1;
			}
		}
		rid.slot = slota;
		rid.page = page;
		code = getRecord(rel,rid,r);
		code = getAttr(r, rel->schema, 0, &result);

		if(invalue->v.intV == result->v.intV){
			RC_message = "Cannot Insert Record - Primary Key Violation";
			code = RC_PRIMARYKEY_VIOLATION;
			return code;
		}
		free(result);
	}
	RC_message = "Primary Key Satisfied";
	return code;
}

RC insertRecord (RM_TableData *rel, Record *record) {

    RC code;
	RecMgr *recordmgr = rel->mgmtData;	
	char *slot;
	recordSize = getRecordSize(rel->schema);
    bool firstcheck = 0,slotassigned = 0;
    record->id.page = recordmgr->emptyPage;
    do {
        if(firstcheck == 1) {
            code = unpinPage(&recordmgr->bufferPool, &recordmgr->pageHandle);
			if(code != RC_OK){
				RC_message = "Error in unpinpage during insertRecord() method";
				return code;
			}
            record->id.page++;
        }
        code = pinPage(&recordmgr->bufferPool, &recordmgr->pageHandle, record->id.page);
		if(code != RC_OK){
			RC_message = "Error in pinpage during insertRecord() method";
			return code;
		}
        slot = recordmgr->pageHandle.data;
        for (int i = 0; i < PAGE_SIZE / recordSize; i++) {
            if (slot[i * recordSize] != '+') {
                record->id.slot = i;
                slotassigned = 1;
                break;
            }
        }
		firstcheck = 1;
    } while (slotassigned == 0);
	
	//Run the pkey comparison when global variable is 1
	if(globalPkeyTag==1){
	
	if(code = checkPrimaryKey(rel,slot,record) != RC_OK)
	{
		
		return code;
	}
	}
	
	
	slot += (record->id.slot * recordSize);
	*slot = '+';
	memcpy(++slot, record->data + 1, recordSize - 1);
	code = markandunpin(recordmgr);
	if(code != RC_OK) {
		RC_message = "Error in marking page dirty and unpinning the page during insertRecord() method";
		return code;
	}
	recordmgr->rowCount+=1;
	code = pinPage(&recordmgr->bufferPool, &recordmgr->pageHandle, 0);
	if(code != RC_OK){
		RC_message = "Error in pinning back the page during insertRecord() method";
	}
	else{
		RC_message = "Record insert Successfull";
	}
	rel->mgmtData = recordmgr;
	return code;
}

RC deleteRecord (RM_TableData *rel, RID id) {

	RC code;
	RecMgr *recordmgr = rel->mgmtData;
	code = pinPage(&recordmgr->bufferPool, &recordmgr->pageHandle, id.page);
	if(code != RC_OK){
		RC_message = "Error in pinpage during deleteRecord() method";
		return code;
	}
	recordmgr->emptyPage = id.page;
	char *slot = recordmgr->pageHandle.data;
	slot += (id.slot * recordSize);
	*slot = '-';
	code = markandunpin(recordmgr);
	if(code != RC_OK){
		RC_message = "Error in pinning back the page during deleteRecord() method";
	}
	else{
		RC_message = "Record delete Successfull";
	}
	return code;
}

RC updateRecord (RM_TableData *rel, Record *record) {
	
	RC code;
	RecMgr *recordmgr = rel->mgmtData;
	code = pinPage(&recordmgr->bufferPool, &recordmgr->pageHandle, record->id.page);
	if(code != RC_OK){
		RC_message = "Error in pinpage during updateRecord() method";
		return code;
	}
	char *slot = recordmgr->pageHandle.data;
	slot += (record->id.slot * recordSize);
	*slot = '+';
	memcpy(++slot, record->data + 1, recordSize - 1 );
	code = markandunpin(recordmgr);
	if(code != RC_OK){
		RC_message = "Error in pinning back the page during updateRecord() method";
	}
	else{
		RC_message = "Record update Successfull";
	}
	return code;	
}

RC getRecord (RM_TableData *rel, RID id, Record *record) {

	RC code;
	RecMgr *recordmgr = rel->mgmtData;

	code = pinPage(&recordmgr->bufferPool, &recordmgr->pageHandle, id.page);
	if(code != RC_OK) {
		RC_message = "Error in pinpage during getRecord() method";
		return code;
	}
	char *slot = recordmgr->pageHandle.data;
	slot += (id.slot *  recordSize);
	if(*slot != '+') {
		RC_message = "Record not found with the given RID";
		code = RC_RM_NO_RECORD_FOUND;
		return code;
	}
	else {
		record->id.page = id.page;
		record->id.slot = id.slot;
		char *data = record->data;
		memcpy(++data, slot + 1,  recordSize-1);
	}
	code = unpinPage(&recordmgr->bufferPool, &recordmgr->pageHandle);
	if(code != RC_OK) {
		RC_message = "Error in unpinpage during getRecord() method";
	}
	else {
		RC_message = "Record update Successfull";
	}
	return code;
}

//*********SCAN FUNCTIONS**********//

typedef struct ScMgr {

	RID rID;
	Expr *scanCond;
	int NumScans;

} ScMgr;


void resetScMgr(ScMgr *scanMgr) {

	scanMgr->rID.page = 1;
	scanMgr->rID.slot = 0;
	scanMgr->NumScans = 0;
}

RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {

	RC code = openTable(rel, "ScanTable");
	if(code != RC_OK) {
		RC_message = "Error in Open table during statScan() method";
		return code;
	}
    ScMgr *scanMgr = (ScMgr*) malloc(sizeof(ScMgr));
    resetScMgr(scanMgr);
    scanMgr->scanCond = cond;
	scan->mgmtData = scanMgr;
    scan->rel= rel;
	RC_message = "Scan initiated";
	return code;
}

RC next (RM_ScanHandle *scan, Record *record) {

	RC code = RC_OK;
	ScMgr *scanMgr =  scan->mgmtData;
	Value *result = (Value *) malloc(sizeof(Value));
	char *slot;
	for(;(scanMgr->NumScans) < getNumTuples(scan->rel);)
	{  
		if(scanMgr->NumScans > 0){
			scanMgr->rID.slot++;
			if(scanMgr->rID.slot >= PAGE_SIZE / recordSize) {
				scanMgr->rID.slot = 0;
				scanMgr->rID.page+=1;
			}
		}
		code = getRecord(scan->rel, scanMgr->rID, record);
		if(code != RC_OK)
		{
			RC_message = "Error in get record in next() method";
			return code;
		}
		scanMgr->NumScans+=1;
		if(scanMgr->scanCond == NULL){
			result->v.boolV = TRUE;
		}
		else{
			code = evalExpr(record, scan->rel->schema, scanMgr->scanCond, &result);
			if(code != RC_OK){
				RC_message = "Error in evaluate Expression during next() method";
				return code;
			} 
		}
		if(result->v.boolV == TRUE) {
			return code;
		}
	}
	resetScMgr(scanMgr);
	RC_message = "No More Tupples";
	code = RC_RM_NO_MORE_TUPLES;
	return code;
}

RC closeScan (RM_ScanHandle *scan) {

	RC code = RC_OK;
    free(scan->mgmtData); 
	scan->mgmtData = NULL; 
	RC_message = "Scan Closed Successfully";
	return code;
}

//**************************************FUNCTIONS BY VIVEKANAND REDDY MALIPATEL END***********************************************//

// ******** SCHEMA FUNCTIONS ******** //

//**************************************FUNCTIONS BY SAI RAM ODURI START***********************************************//

//GetRecordSize function calculates the actual size of the schema on
// the disk by taking each bytes' size and adding them up

int getSumof(int a, int b){

	int sum=0;
	sum = a + b;
	return sum;
}

extern int getRecordSize(Schema *schema){

	// We start from the beginning position of the schema, i.e., 0
	int cell_size =0;
	//temporary variable for iteration
	int m=0;

	// Standard variables defined to reduce the usage
	int INT_SIZE = sizeof(int);
	int BOOL_SIZE = sizeof(bool);
	int FLOAT_SIZE = sizeof(float);

	int x = schema->numAttr;
	//int m =0;
	// this loop will deal with schema and update the size of the row 
	//accordingly
	while(m < x){

		if(schema->dataTypes[m]==DT_STRING){
			//The length of string type into the cell if the datatype is string.
			cell_size = cell_size + schema->typeLength[m];
		}
		//The length of the float type into the cell if the datatype is float.
		else if(schema->dataTypes[m]==DT_FLOAT){
			//Add the length of float size into the cell
			
			cell_size = getSumof(cell_size, FLOAT_SIZE);
		}
		//The length of the float type into the cell if the datatype is bool.
		else if(schema->dataTypes[m]==DT_BOOL){
			//Add the length of bool size into the cell
			
			cell_size = getSumof(cell_size, BOOL_SIZE);
		}
		//The length of the float type into the cell if the datatype is int.
		else if(schema->dataTypes[m]==DT_INT){
			//Add the length of int size into the cell
			
			cell_size = getSumof(cell_size, INT_SIZE);
		}
		m++;
	}
	return ++cell_size;
}

//Schema functions start here

//to create a new schema build a blueprint of the schema, including the attributes required
//this function helps to build a new schema.

Schema *createSchema(int numAttr, char **attrNames,DataType *dataTypes, int *typeLength, int keySize, int *keys){

	RC return_code = RC_OK;
	int schema_size = sizeof(Schema);
	//Create a new schema
    Schema *blueprint = (Schema *)malloc(schema_size);

	if (blueprint== ((Schema*)0))
	{
		RC_message = "Create Schema-- Schema allocation to dynamic memory failed";
		return_code = RC_MEM_ALLOCATION_FAILED;
		return return_code;
	}
	
	
	//Key Size attribute will set the size of keys
	blueprint->keySize = keySize;
	//This line helps to set the attr Names
	(*blueprint).attrNames = attrNames;
	//The row type length is set here by accessing typelength abstract variable with
	//blueprint
	(*blueprint).typeLength = typeLength;
	//Datatypes are set here by accessing datatypes from the blueprint class
   	(*blueprint).dataTypes = dataTypes;
	//Keys are set here by accessing datatypes from the blueprint class
	(*blueprint).keyAttrs = keys;
	//Number Attributes are set here by accessing datatypes from the blueprint class
    blueprint->numAttr = numAttr;
	
	RC_message = "Schema Created";
	//Return schema created if flow is uninterrupted

    return blueprint;

}

// Completely deallocate schema memory from the database.

extern RC freeSchema (Schema *schema){

	RC_message = "Free schema Function started";
	//Using free to delete all the references in the memory space 
	
    free(schema);
	schema = NULL;
	RC_message = "Schema Deleted From the database";
	RC_message = "Free Schema Function ended";
	int code;
	if(code == RC_OK)
	return RC_OK;
	
}

//Now schema has been built, so build a record in the schema, referring to "schema"


extern RC createRecord (Record **record, Schema *schema)
{
	
	//creating new record required space and a record pointer

	//This line allocates dynamic space for record creation
	if (schema==NULL)
	{
		RC_message = "Create a schema first";
		return RC_SCHEMA_NOT_CREATED;
	}
	
	Record *new_tuple;
	new_tuple= (Record*) malloc(sizeof(Record));
	if(new_tuple==NULL){
		RC_message = "Create Record -- Failed memory allocation for a new Record";
		return RC_RM_NO_RECORD_FOUND;
	}
	//Get the schema size
	int tuple_size=0;
	char tmb = '-';
	tuple_size = getRecordSize(schema);
	//Allocate space for new data that is created in the schema
	new_tuple->data= (char*) malloc(tuple_size);
	//#define MIN_CNT -1
	new_tuple->id.slot = MIN_CNT;
	//setting the default position of the record to -1 and the page position to -1
	new_tuple->id.page = MIN_CNT; 
	//the record pointer will point to the data position on the disk
	char *tuple_pointer;
	tuple_pointer = new_tuple->data;
	//'-' indicates tombstone and used to indicate position of empty records or freed up records
	*tuple_pointer = tmb;
	//Appending a NULL pointer location
	*(++tuple_pointer) = '\0';
	//Copy the contents of new tuple into the argument record
	
	*record = new_tuple;
	return RC_OK;
}
//This function is taken from the Record manager serializer , to calculate
//position of the pointer from the previously passed location.
//This will set the attributes on the disk in a serial fashion
int *getSumPtr(int *a,int b){
	int *sum =0;
	sum = *a + b;
	return *sum;
}

RC bytes_offsetting (Schema *schema, int attrNum, int *result)
{
	RC return_code = RC_OK;
	if (schema==NULL)
	{
		RC_message = "Create a schema first";
		return RC_SCHEMA_NOT_CREATED;
	}
	*result = 1;

	//Standard variables for size-of(int)

	int INT_SIZE = sizeof(int);
	//Standard variables for size-of(float)
    int FLOAT_SIZE= sizeof(float);
	//Standard variables for size-of(bool)
    int BOOL_SIZE = sizeof(bool);

	//This control will iterate through the attributes in the schema
	int idx = 0;
	while(idx < attrNum)
	{
		if(schema->dataTypes[idx]==DT_STRING){
			//Make pointer point to the offset position of the string length
			*result = *result  + schema->typeLength[idx];
		}
		//Make pointer point to the offset position of the float length
		else if(schema->dataTypes[idx]==DT_FLOAT){
			*result = *result + FLOAT_SIZE;
			
		}
		//Make pointer point to the offset position of the boolean length
		else if(schema->dataTypes[idx]==DT_BOOL){
			*result = *result + BOOL_SIZE;
		}
		//Make pointer point to the offset position of the int length
		else if(schema->dataTypes[idx]==DT_INT){
			*result = *result + INT_SIZE;
		}
		idx++;
	}
	return return_code;
}
//We need to freeRecord from the memory.
extern RC freeRecord (Record *record)
{
	//use free pointer to remove all instances of the record and erase 
	//from the memory.
	RC return_code = RC_OK;
	free(record);
	record = NULL;
	RC_message = "Record Deleted From the database";
	return return_code;
}

/*Get Attr function takes 

	Record with all the data,
	Schema with all the data in the schema,
	Attribute Numbers to retrieve the attributes in the schema
	Value of the expression,


 */
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	//Code to say that function has started
	RC_message = "Get Attr Function started";
	RC return_code = RC_OK;
	if(record==NULL){
		RC_message = "Failed to create a new Record";
		return RC_RM_NO_RECORD_FOUND;
	}
	if (schema==NULL)
	{
		RC_message = "Create a schema first";
		return RC_SCHEMA_NOT_CREATED;
	}
	//Size of Value in a variable, to reduce code.

	int size_V = sizeof(Value);
	//Datapointer to point to the record's data in the memory, so we can 
	//stream line through memory.
	char *dtptr = record->data;
	//Type of value being dealt with , allocate the space for Value class
	Value *type;
	type = (Value*) malloc(size_V);
	//0 is the default value
    int off_bt = 0;
	int att_size;
	//Use the offset function from the RM Serializer to set the offset
	//and at each offset the data will be retrieved from the record
	//This function will setup the redundant code to move the pointer in the memory
	
	return_code = bytes_offsetting(schema, attrNum, &off_bt);	
	
	if (return_code!=RC_OK)
	{
		RC_message = "Bytes Offsetting function Failed";
		//return_code = RC_NOT_OK;
		return return_code;
	}
	//offsetting to the number of bytes the data has occupied.
	dtptr += off_bt;

	//if the number of attributes is 1. then the position is set to 1
	if(attrNum == 1)  
    schema->dataTypes[attrNum] = 1;

    int INT_SIZE = sizeof(int);
    int FLOAT_SIZE= sizeof(float);
    int BOOL_SIZE = sizeof(bool);

	char null_ptr = '\0';
	int max_len = schema->typeLength[attrNum];

	DataType attrDT = schema->dataTypes[attrNum];
	

	//This will retrieve the data type string and and allocates the memory.

	if(attrDT==DT_STRING)
	{	
			//the max length will calculate length
			//this will increase the memory space by 1,
			//allocating memory for the null pointer
			int m_length = 4;
			type->v.stringV  = (char *) malloc(m_length);
			//This will copy all the contents into stringV 
			//and sets the null pointer at the end
			strncpy(type->v.stringV, dtptr, m_length);
			//apppending the null pointer to the ending.
			type->v.stringV[m_length] = null_ptr;
			//setting the type to Datatype String
			type->dt = 1;
			
    }	
	
	else if(attrDT== DT_BOOL)
	{
			//copying the contents of the data into datatype bool
			bool contents;
			//copying the bool data to location and set the size to Bool Size
			type->v.boolV = (dtptr[off_bt]=='1') ? true : false;
			//Set the bool size to the type of bool size
			//type->v.boolV = contents;
			//set the bool size into the memory of value
			type->dt = DT_BOOL;
	}
	else if(attrDT==DT_FLOAT)
		{
			//copying the contents of the data into datatype float,
	  		float contents;
			//copying the float data to location and set the size to Bool Size
	  		memcpy(&contents, dtptr, FLOAT_SIZE);
			//Set the float size to the type of float size
	  		type->v.floatV = contents;
			//set the float size into the memory of value
			type->dt =2;
			
		}
		else if(attrDT==DT_INT)
		{
			//copying the contents of the data into datatype float,
			int contents;
			//copying the float data to location and set the size to Bool Size

			memcpy(&contents, dtptr,INT_SIZE);
			//Set the float size to the type of float size
			type->v.intV = contents;
			//set the float size into the memory of value
			type->dt = 0;
      		
		}
		else{
			printf("Given datatype has no serializer \n");
	}
	//Set all the contents of type into Value structure
	*value = type;
	//Code to say the function control end successfully
	RC_message = "Get Attr Function ended";
	//A return code for OK.
	return RC_OK;
}

extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	RC_message = "Set Attr Function started";
	RC return_code = RC_OK;
	if(record==NULL){
		RC_message = "Failed to create a new Record";
		return_code = RC_RM_NO_RECORD_FOUND;
		return return_code;
	}
	if (schema==NULL)
	{
		RC_message = "Create a schema first";
		return_code = RC_SCHEMA_NOT_CREATED;;
		return return_code;
	}

	//Set the bytes = 0 to calculate the offset
	//RC return_code = RC_OK;
	int off_bt = 0;
	//Call the bytes offsetting function to retrieve the data for bytes and
	//attribute numbers
	return_code = bytes_offsetting(schema, attrNum, &off_bt);	

	if (return_code!=RC_OK)
	{
		RC_message = "Bytes Offsetting function Failed";
		return return_code;
	}
	
	//get the location of the pointer
	char *off_loc = record->data;
	off_loc =off_loc + off_bt;

    int INT_SIZE = sizeof(int);
    int FLOAT_SIZE= sizeof(float);
    int BOOL_SIZE = sizeof(bool);

	//Store dataType[attrnum] into dt_type
	int dt_type = schema->dataTypes[attrNum];

	if (value->dt!=schema->dataTypes[attrNum]){
		
		return RC_RM_UNKOWN_DATATYPE;
	}
	
	if(dt_type==DT_STRING)
		{	
			//Set the attribute value to String type
			RC_message = "Setting the offset pointer for String Datatype in SetAttr()";
			int length = schema->typeLength[attrNum];
			//copy the contents of the memory of the data pointer
        	strncpy(off_loc, value->v.stringV, length);
			//increase the pointer location
			off_loc =off_loc+ length;
		  	
		}
        else if(dt_type== DT_BOOL)

		{RC_message = "Setting the offset pointer for Boolean Datatype in SetAttr()";
			
			//Set the attribute value to bool type
			//copy the contents of the memory of the data pointer
			sprintf(off_loc,"%i",value->v.boolV);
			//increase the pointer location
			
			//off_loc= off_loc+ BOOL_SIZE;
			
		}
	    else if(dt_type==DT_FLOAT)	
		{	//Set the attribute value to float type
			//copy the contents of the memory of the data pointer
			RC_message = "Setting the offset pointer for Float Datatype in SetAttr()";
			*(float *) off_loc= value->v.floatV;
			//increase the pointer location
			off_loc =off_loc+ FLOAT_SIZE;
			
		}
        else if(dt_type==DT_INT)
		{	
			//Set the attribute value to int type
			//copy the contents of the memory of the data pointer
			RC_message = "Setting the offset pointer for Int Datatype in SetAttr()";
			memcpy(off_loc,&((value->v.intV)),INT_SIZE);
			//increase the pointer location
		}
		else{
			RC_message = "No Serialization possible for this datatype";
	}
	//Code to indicate the function flow has ended successfully
	RC_message = "Set Attr Function ended";
	return return_code;
	//Returns return code okay.
}
//******************************************************END*****************************************************************************//