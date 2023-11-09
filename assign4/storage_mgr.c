#include "storage_mgr.h"
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include "dberror.h"
FILE *file;

//initate Storage Manager
void initStorageManager()
{
    printf("Sucessfully Initate Storage Manager\n");
}

//Local function prototype declaration 
RC createFile(char *fileName);
RC validateFileHandle (SM_FileHandle *filehandle);
RC validateFile (int pageNum, SM_FileHandle *filehandle);

// create new file of one page
RC createFile(char *fileName) {

	RC code;
	file = fopen(fileName,"w");
	//Allocate memory a single page with page size as 2mb i.e 2048 bytes with calloc function
	char *ptr=(char *)calloc(PAGE_SIZE,sizeof(char));
	//Write the single page of 2mb size to a file
	if(fwrite(ptr,sizeof(char),PAGE_SIZE,file) == PAGE_SIZE) {
		//Move the file pointer to the end of the file
		fseek(file,0,SEEK_END);
		//Free the memory allocated by calloc
		free(ptr);
		//Close the file
		fclose(file);
		RC_message="File creation has been completed!!";
		code = RC_OK;
	}
	else {
		free(ptr);
		RC_message="Page Creation Failed";
		code = RC_WRITE_FAILED;
	}
	return code;
}

RC createPageFile (char *fileName) { 
	/*Here we are creating a new file,as per the requirement the initial 
	file size should be one page , and this single page should be filled 
	with 0 bytes. Hence we need to use calloc() version of dynamic memory 
	allocation , since by default all the blocks created are intialized to 0
	instead of garbage values
   */
  	RC code;
  	//Initialize the input variable
  	char input;
  	//Open File 
  	file = fopen(fileName,"r");
	code = createFile(fileName);
	return code;
}

RC openPageFile(char *fileName,SM_FileHandle *fileHandle) {
/*This function opens a page file ans sets the properties of the file handler like 
mgmtIngo , page count and current position of the new file. The function also calculates 
the number of pages in the file*/

	RC code = RC_OK;
    //Open a a page file 
	fileHandle -> mgmtInfo = fopen(fileName,"r+");
	int pageCnt,filesize;
	if(fileHandle->mgmtInfo==NULL){
        RC_message ="File does not exist";
        code = RC_FILE_NOT_FOUND;
    }
    else{
		fileHandle -> fileName = fileName;
		fileHandle -> curPagePos = ftell(fileHandle ->mgmtInfo)/PAGE_SIZE;
		fseek(fileHandle -> mgmtInfo,0,SEEK_END);
		filesize = ftell(fileHandle->mgmtInfo);
		pageCnt = filesize/PAGE_SIZE;
		fileHandle->totalNumPages = pageCnt;
		RC_message="File Opened Successfully";
		code = RC_OK;
	}
	return code;
}

//Implementation of the closePageFile interface
RC closePageFile (SM_FileHandle *fileHandle) {
/*The close page file function closes the file stream */

	/*mgmtInfo stored the open file pointer previously*/
	//If fclose returns 0 , then it means the file has been successfully closed
	if(fclose(fileHandle->mgmtInfo)==0) {
		RC_message="File closed successfully \n";
		return RC_OK;
	}
	else {
		return RC_FILE_NOT_FOUND;
	}
}


extern RC destroyPageFile (char *fileName) {

	RC code = RC_OK;
	// Open file stream in write mode to delete the file
    FILE *file = fopen(fileName, "w");
	
	if(file == NULL){
		code  = RC_FILE_NOT_FOUND; 
	}
	else {
	// Deleting the given filename so that it is no longer accessible.	
	remove(fileName);
	}
	return code;
}


//////////////////////////////////////////////////////////////////////////////////////////////////////////

//Validate File handle
RC validateFileHandle (SM_FileHandle *filehandle){

	//Initialising the return code to OK
    RC code = RC_OK;
	//Checking if FileHandle is null
    if(filehandle == NULL) {
		//So The File handle given is null
		//Setting the RC message as "Requested File is not initalized - filehandle == NULL"
		RC_message = "Requested File is not initalized - filehandle == NULL";
		//Setting return code to RC_FILE_HANDLE_NOT_INIT
		code = RC_FILE_HANDLE_NOT_INIT;
	}
    //Check for file existance
    else if (filehandle->mgmtInfo ==NULL) {
		//So The File is not created or there in the database
		RC_message = "File Not Found";
		//Setting return code to RC_FILE_NOT_FOUND
		code = RC_FILE_NOT_FOUND;
	}
	//Returning the code
    return code;
}

//This Funtion validates the given File Handle and Page number to read
RC validateFile (int pageNum, SM_FileHandle *filehandle){

    // Initialising the Return code to validate the input file handle
	RC code = validateFileHandle(filehandle);
	//If the code from "validateFileHandle(filehandle)" function is RC_OK, Then check whether the given page exists in the File
    if ((code == RC_OK) && (pageNum+1) > filehandle->totalNumPages || pageNum < 0) {
		//So The Pagenumber doesn't exist in the File
		RC_message = "Requested page to be read doesn't exist";
		//Setting the return code to RC_READ_NON_EXISTING_PAGE
		code = RC_READ_NON_EXISTING_PAGE;
	}
	//Returning the code
    return code;
}

//Reading the File with the given pagenumber, filehandle and Buffer
RC readBlock (int pageNum, SM_FileHandle *fileHandle, SM_PageHandle memPage){

	 //Initialise the Return code variable to Validate the page number and File handle
    RC code = validateFile(pageNum, fileHandle);
    //If the validation returns RC_OK, Then check whether Pagehandler is null
	if((code == RC_OK) && (memPage !=NULL)) {
		fseek(fileHandle->mgmtInfo,(((pageNum)*PAGE_SIZE)),SEEK_SET);//seeking to requested page number.
		fread(memPage,sizeof(char),PAGE_SIZE,fileHandle->mgmtInfo);//reading to requested page.
		fileHandle->curPagePos = (ftell(fileHandle->mgmtInfo)/PAGE_SIZE);//updating current page position
		RC_message="File Read Successfull.";
	}	
	//Returning the code
	return code;
}

//This Function will return the integer containing the current page position of the file handle
int getBlockPos (SM_FileHandle *filehandle){

	//Initialising the page position variable
	int pos;
	//Validate the given filehandle, If Validation returns RC_OK then check the current page position
	if(validateFileHandle(filehandle) == RC_OK){
        //Print the Position in the File and return the integer
	    printf("%d \n",filehandle->curPagePos);
	    pos = filehandle->curPagePos;
    }
	//Return the Position
	return pos;
}

//Read the First Page in the File
RC readFirstBlock (SM_FileHandle *filehandle, SM_PageHandle memPage){

    //Initializing page number to first page of the fuke handle
	int pageNum = 0;
	return readBlock(pageNum,filehandle, memPage);
}

//Read Previous page in the File
RC readPreviousBlock (SM_FileHandle *filehandle, SM_PageHandle memPage) {

    //Initializing page number to current page position of the file handle - 1
	int pageNum = getBlockPos(filehandle)-1;
	return readBlock (pageNum,filehandle, memPage);
}

//Read current page in the File
RC readCurrentBlock (SM_FileHandle *filehandle, SM_PageHandle memPage){

    //Initializing page number to current page position of the file handle
	int pageNum = getBlockPos(filehandle);
	return readBlock (pageNum,filehandle, memPage);
}

//Read Next page in the File
RC readNextBlock (SM_FileHandle *filehandle, SM_PageHandle memPage){

    //Initializing page number to current page position of the file handle + 1
	int pageNum = getBlockPos(filehandle)+1;
	return readBlock (pageNum,filehandle, memPage);
}

//Read last page of the file
RC readLastBlock (SM_FileHandle *filehandle, SM_PageHandle memPage){

    //Initializing page number to last page position of the file handle
	int pageNum = (filehandle->totalNumPages)-1;
	return readBlock (pageNum,filehandle, memPage);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

RC writeBlock (int pageNum, SM_FileHandle *fileHandle, SM_PageHandle memPage) {


	// Initialising the Return code to validate the input file handle and the page number
	RC code = validateFile(pageNum, fileHandle);
	//If the Validation returns RC_OK, Then Check whether the buffer storage pointer is not null
	if((code == RC_OK) && (memPage !=NULL)){
		ensureCapacity(strlen(memPage)/PAGE_SIZE,fileHandle);
		fseek(fileHandle->mgmtInfo, pageNum*PAGE_SIZE , SEEK_SET);
		if(fwrite(memPage,PAGE_SIZE,1,fileHandle->mgmtInfo) == 1) {
			fseek(fileHandle->mgmtInfo, (pageNum+1)*PAGE_SIZE , SEEK_SET);
			fileHandle->curPagePos = (ftell(fileHandle->mgmtInfo)/PAGE_SIZE);
			//fclose(filehandle->mgmtInfo);
			RC_message="Data write successful.";
		}
		else
		{
			RC_message="Write Request Failed";
			code = RC_WRITE_FAILED;
		}
	}
	return code;
}


RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {

	//Set the Page number to the current page position in the file and write to the page
	return writeBlock(fHandle->curPagePos,fHandle,memPage);
}

RC appendEmptyBlock (SM_FileHandle *fileHandle) {

	RC code = validateFileHandle(fileHandle);
	if(code == RC_OK)
	{
		fseek(fileHandle->mgmtInfo,0,SEEK_END);
		char *len = (char*)calloc(PAGE_SIZE,sizeof(char));
		fwrite(len,PAGE_SIZE,sizeof(char),fileHandle->mgmtInfo);
		free(len);
		fileHandle->totalNumPages =(ftell(fileHandle->mgmtInfo)/PAGE_SIZE);
	}
	return code;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fileHandle) {

	RC code = validateFileHandle(fileHandle);
	if((code == RC_OK) && (fileHandle->totalNumPages < numberOfPages)) {
		for(;fileHandle->totalNumPages != numberOfPages;) {
			appendEmptyBlock(fileHandle);
		}
	}
	return code;
}