#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

/*********************************Page Frame Structure In Memory*************************************
 
SM_PageHandle page_data - represents the page_data of page as a file, to be taken from Storage Manager

PageNumber page_Num - ID for each page in the buffer

dirty - indicate wthether the page is changed.

fCnt - Used to check the no. of clients that have pinned the page.

When Fix Count = 0, the page is released and the page is evicted
Fix Count = 1 the page is pinned.

lruNum - No. of times the page has been accessed. Used for the LRU strategy.

lfuNum - Used by the Clock algorithm and the LFU to get the reference status of the page

*****************************************************************************************************
*/

typedef struct Frame {
	SM_PageHandle page_data;
	PageNumber page_num; 
	int dirty; 
	int fCnt; 
	int lruNum; 
	int lfuNum; 
} Frame;

/************************************************
 * Parameters for Buffer Pool, Pages
 * 
 * 
 * Size_of_Buffer means max no. of pages that can be allocated in the buffer pool
 * 
 * rearIndex - Count of pages that have been read from disk.
 * 
 * 
 * Write Count - No of pages written to the disk, i.e., no of. write IO instructions
 * 
 * Hit count a general incremental variable used, to determine the number of times a
 * page is accessed and found in the buffer pool during the execution of the process
 * 
 * 
 * clockIndex - The clock pointer is used by the Clock Replacement Strategy
 * to keep track of the location of the most recently used page in the pool.
 * 
 * lfuIndex - Used to keep track of the page frame with the least frequent use.
 * 
 *  * **********************************************/

int maxBufferSize = 0;
int readCnt = 0;
int writeCnt = 0;
int pageFrameCount = 0;
int clockIndex = 0;
int lfuIndex = 0;

// Implementing the FIFO replacement strategy
void FIFO(BM_BufferPool *const bm, Frame *newPageFrame)
{
	Frame *pframe= (Frame *) bm->mgmtData;
	
	int replaceIndex = readCnt % maxBufferSize;

	for(int i = 0; i < maxBufferSize; i++)
	{
		if(pframe[replaceIndex].fCnt == 0)
		{
			if(pframe[replaceIndex].dirty == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pframe[replaceIndex].page_num, &fh, pframe[replaceIndex].page_data);
				writeCnt++;
			}
			pframe[replaceIndex].page_data = newPageFrame->page_data;
			pframe[replaceIndex].page_num = newPageFrame->page_num;
			pframe[replaceIndex].dirty = newPageFrame->dirty;
			pframe[replaceIndex].fCnt = newPageFrame->fCnt;
			break;
		}
		else
		{
			replaceIndex++;
			replaceIndex = (replaceIndex % maxBufferSize == 0) ? 0 : replaceIndex;
		}
	}
}

// Implementing the Least Frequently Used replacement strategy
void LFU(BM_BufferPool *const bm, Frame *newPageFrame) {

	Frame *pframe = (Frame *) bm->mgmtData;
	int i, j, lFI, lFR;
	lFI = lfuIndex;
	for (i = 0; i < maxBufferSize; i++) {
		if (pframe[lFI].fCnt == 0) {
			lFI = (lFI + i) % maxBufferSize;
			lFR = pframe[lFI].lfuNum;
			break;
		}
	}
	i = (lFI + 1) % maxBufferSize;
	for (j = 0; j < maxBufferSize; j++) {
		if (pframe[i].lfuNum < lFR) {
			lFI = i;
			lFR = pframe[i].lfuNum;
		}
		i = (i + 1) % maxBufferSize;
	}
	if (pframe[lFI].dirty == 1) {
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pframe[lFI].page_num, &fh, pframe[lFI].page_data);
		writeCnt++;
	}	
	pframe[lFI].page_data = newPageFrame->page_data;
	pframe[lFI].page_num = newPageFrame->page_num;
	pframe[lFI].dirty = newPageFrame->dirty;
	pframe[lFI].fCnt = newPageFrame->fCnt;
	lfuIndex = lFI + 1;
}

// Implementing the Least Frequently Used replacement strategy
void LRU(BM_BufferPool *const bm, Frame *newPageFrame) {
	Frame *pframe = (Frame *) bm->mgmtData;
	int i, leastRecentlyUsedIndex = 0, leastRecentlyUsedNum = pframe[0].lruNum;
	for (i = 0; i < maxBufferSize; i++) {
		if (pframe[i].fCnt == 0) {
			leastRecentlyUsedIndex = i;
			leastRecentlyUsedNum = pframe[i].lruNum;
			break;
		}
	}	
	for (i = leastRecentlyUsedIndex + 1; i < maxBufferSize; i++) {
		if (pframe[i].lruNum < leastRecentlyUsedNum) {
			leastRecentlyUsedIndex = i;
			leastRecentlyUsedNum = pframe[i].lruNum;
		}
	}
	if (pframe[leastRecentlyUsedIndex].dirty == 1) {
		SM_FileHandle fileHandle;
		openPageFile(bm->pageFile, &fileHandle);
		writeBlock(pframe[leastRecentlyUsedIndex].page_num, &fileHandle, pframe[leastRecentlyUsedIndex].page_data);
		writeCnt++;
	}
	pframe[leastRecentlyUsedIndex].page_data = newPageFrame->page_data;
	pframe[leastRecentlyUsedIndex].page_num = newPageFrame->page_num;
	pframe[leastRecentlyUsedIndex].dirty = newPageFrame->dirty;
	pframe[leastRecentlyUsedIndex].fCnt = newPageFrame->fCnt;
	pframe[leastRecentlyUsedIndex].lruNum = ++pageFrameCount;
}

// Define a function to implement the CLOCK algorithm.
void CLOCK(BM_BufferPool *const bm, Frame *newPageFrame)
{
	Frame *pframe = (Frame *) bm->mgmtData;
	while (1)
	{
		clockIndex = (clockIndex % maxBufferSize == 0) ? 0 : clockIndex;
		if (pframe[clockIndex].lruNum == 0)
		{
			if (pframe[clockIndex].dirty == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pframe[clockIndex].page_num, &fh, pframe[clockIndex].page_data);
				writeCnt++;
			}
			pframe[clockIndex].page_data = newPageFrame->page_data;
			pframe[clockIndex].page_num = newPageFrame->page_num;
			pframe[clockIndex].dirty = newPageFrame->dirty;
			pframe[clockIndex].fCnt = newPageFrame->fCnt;
			pframe[clockIndex].lruNum = newPageFrame->lruNum;
			clockIndex++;
			break;	
		}
		else
		{
			pframe[clockIndex++].lruNum = 0;		
		}
	}
}


RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData)
{
	RC code = RC_OK;
	if(numPages<=0){
		code = RC_NUMPAGES_NEGATIVE;
	}
	else{
		//if(openPageFile((char*)pageFileName,&smfh)!=RC_OK){
		//		return RC_FILE_NOT_FOUND;
		//}

		bm->pageFile = (char*)pageFileName;
		bm->numPages = numPages;
		bm->strategy = strategy;
		
		//Create memory allocation for the page frame (number of pages * capacity of single page)

		Frame *buffer_frame = malloc(sizeof(Frame)*numPages);

		//Buffer size = No. of pages in disk
		maxBufferSize = numPages;
		
		//Intiliaze the fields of the page =0 and page_Num = -1
		// the memset sets the  memory block of Frame[pageIndex] to 0
		//initialzing to 0 or NULL.
		//Page number should be = -1
		


		for(int pageInd=0;pageInd< maxBufferSize;pageInd++){
			
			Frame *pageIndex = &buffer_frame[pageInd];

			pageIndex->page_data = NULL;
			pageIndex->page_num = -1;
			pageIndex->dirty = 0;
			pageIndex->fCnt = 0;
			pageIndex->lruNum = 0;	
			pageIndex->lfuNum = 0;

			//pageIndex++;
			//go the next field in the page

		}
		writeCnt = clockIndex = lfuIndex =(unsigned int)0;
		//set the pointers to 0 because no pages have yet been referenced or frequented

		bm->mgmtData = buffer_frame;
		code = RC_OK;
	}
	return code;
}

//Shutting down buffer pool is essentially removing the pages from the buffer pool in the memory
// and free up all resources and release the memory space.

extern RC shutdownBufferPool(BM_BufferPool *const bm){

	RC code = RC_OK;
	Frame *pframe = (Frame*)bm->mgmtData;

	//Force push pages back to disk memory

	forceFlushPool(bm);
	int pin_pages = 0;


	while(pin_pages < bm->numPages){

		//check for fix count, if the fixcount is 1, then there are some pages are modified
		//and changes are not saved to disk

		if(pframe[pin_pages].fCnt!=0){
			code = RC_PINNED_PAGES_IN_BUFFER;
			break;
		}
		pin_pages = pin_pages+1;

	}

	// Free up memory

	free(pframe);
	bm->mgmtData = NULL;
	return code;

}

/* Manages modified pages in the buffer. and saves these changes back to the disk so that they are permanently stored.
 It is important to call this function before closing the buffer pool or ensuring a consistent view of the data on the disk
*/
extern RC forceFlushPool(BM_BufferPool *const bm)
{
	Frame *pgFrame = (Frame*)bm->mgmtData;
	
	int buffer_index;
	// First move all dirty pages to disk
	for(buffer_index = 0; buffer_index < maxBufferSize; buffer_index++)
	{
		Frame *buffer = &pgFrame[buffer_index];

		if(buffer->fCnt == 0 && buffer->dirty == 1)
		{
			SM_FileHandle smfh;
			
			// Opening page file available on disk
			openPageFile(bm->pageFile, &smfh);

			// Storing a chunk of information onto the file located on the disk.
			
			writeBlock(buffer->page_num, &smfh, buffer->page_data);
			
			buffer->dirty = 0;
			
			writeCnt++;
		}
	}	
	return RC_OK;
}


// Marks the page as dirty
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
	RC code = RC_OK;
	Frame *pf = (Frame *)bm->mgmtData;
	if (pf == NULL) {
		code = RC_BUFFER_POOL_NOT_INIT;
	} else {
		int isFound = 0;
		for (int i = 0; i < maxBufferSize; i++) {
			if (pf[i].page_num == page->pageNum) {
				isFound = 1;
				pf[i].dirty = 1;
				break;
			}
		}
		if (isFound == 0) {
			RC_message = "Page number not found in the buffer";
			code = RC_BUFFERPOOL_PAGE_NOTFOUND_IN_MARKDIRTY;
		}
	}
	return code;
}

// Removes the page from the bbufferpool
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
	RC code = RC_OK;
	Frame *pf = (Frame *)bm->mgmtData;
	if (pf== NULL) {
		code = RC_BUFFER_POOL_NOT_INIT;
	}
	else {
		int isPageFound = 0;
		for (int i = 0; i < maxBufferSize; i++) {
		if (pf[i].page_num == page->pageNum) {
		isPageFound = 1;
		pf[i].fCnt--;
		break;
		}
		}
		if (isPageFound == 0) {
			code = RC_BUFFERPOOL_PAGE_NOTFOUND_IN_UNPINPAGE;
		}
	}
	return code;
}

// Writes back the page to the disk
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
	RC code = RC_OK;
	Frame *pf = (Frame *)bm->mgmtData;
	if(pf  == NULL){
		code = RC_BUFFER_POOL_NOT_INIT;
	}
	else {
		int found = 1;
		for(int i = 0; i < maxBufferSize; i++){
			if(pf[i].page_num == page->pageNum){	
				found = 0;	
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pf[i].page_num, &fh, pf[i].page_data);
				pf[i].dirty = 0;
				writeCnt++;
				break;
			}
			if(found == 1){
				RC_message = "Page number not found in the buffer";
				code = RC_BUFFERPOOL_PAGE_NOTFOUND_IN_FORCEPAGE;
			}
		}
	}	
	return code;
}

//Add the page to the bufferpool
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
	Frame *pf= (Frame *)bm->mgmtData;
	RC code = RC_OK;
	if(pf[0].page_num == -1){
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		pf[0].page_data = (SM_PageHandle) malloc(PAGE_SIZE);
		ensureCapacity(pageNum+1,&fh);
		readBlock(pageNum, &fh, pf[0].page_data);
		pf[0].page_num = pageNum;
		pf[0].fCnt++;
		readCnt = pageFrameCount = 0;
		pf[0].lruNum = pageFrameCount;	
		pf[0].lfuNum = 0;
		page->pageNum = pageNum;
		page->data = pf[0].page_data;
		closePageFile(&fh);	
	}
	else {
		bool BufferFullIndicator = true;
		for(int i = 0; i < maxBufferSize; i++)
		{
			if(pf[i].page_num != -1)
			{	
				if(pf[i].page_num == pageNum)
				{
					pf[i].fCnt++;
					BufferFullIndicator = false;
					pageFrameCount++;

					if(bm->strategy == RS_LRU){
						pf[i].lruNum = pageFrameCount;
					}
					else if(bm->strategy == RS_CLOCK) {
						pf[i].lruNum = 1;
					}
					else if(bm->strategy == RS_LFU){
						pf[i].lfuNum++;
					}
				
					page->pageNum = pageNum;
					page->data = pf[i].page_data;

					clockIndex++;
					break;
				}							
			} else {
				SM_FileHandle fh;                
				code = openPageFile(bm->pageFile, &fh);     
				pf[i].page_data = (SM_PageHandle) malloc(PAGE_SIZE);  
				code = ensureCapacity(pageNum+1,&fh);         
				code = readBlock(pageNum, &fh, pf[i].page_data);            
				pf[i].page_num = pageNum;        
				pf[i].fCnt = 1;             
				pf[i].lfuNum = 0;               
				readCnt++;                          
				pageFrameCount++;                                 
				if(bm->strategy == RS_LRU) {           
					pf[i].lruNum = pageFrameCount;         
				} 
				else if(bm->strategy == RS_CLOCK) {  
					pf[i].lruNum = 1;           
				}
				page->pageNum = pageNum;            
				page->data = pf[i].page_data;        
				BufferFullIndicator = false;                  
				code = closePageFile(&fh);                    
				break;                                 

			}
		}
		if(BufferFullIndicator == true) {
			Frame *newPage = (Frame *) malloc(sizeof(Frame));		
			SM_FileHandle fh;
			code = openPageFile(bm->pageFile, &fh);
			newPage->page_data = (SM_PageHandle) malloc(PAGE_SIZE);
			code = ensureCapacity(pageNum+1,&fh);
			code = readBlock(pageNum, &fh, newPage->page_data);
			newPage->page_num = pageNum;
			newPage->dirty = 0;		
			newPage->fCnt = 1;
			newPage->lfuNum = 0;
			readCnt++;
			pageFrameCount++;
			if(bm->strategy == RS_LRU)
				newPage->lruNum = pageFrameCount;				
			else if(bm->strategy == RS_CLOCK)
				newPage->lruNum = 1;
			page->pageNum = pageNum;
			page->data = newPage->page_data;			
			switch(bm->strategy)
			{			
				case RS_FIFO: //FIFO Replacement Strategy
					FIFO(bm, newPage);
					break;
				
				case RS_LRU: //LRU Replacement Strategy
					LRU(bm, newPage);
					break;
				
				case RS_CLOCK: //CLOCK Replacement Strategy
					CLOCK(bm, newPage);
					break;
						
				case RS_LFU: //LFU Replacement Strategy
					LFU(bm, newPage);
					break;
				
				default:
					printf("\nAlgorithm has not been Implemented in the Buffer_mgr.c\n");
					break;
			}		
		}		
	}	
	return code;
}


//After initializing the buffer pool, the function returns the number of pages that have been read.
int getNumReadIO (BM_BufferPool *const bm)
{
	
	//Since the back Index is zero initially, it is increased by one.
	return (readCnt + 1);
}

//The function returns the number of pages that have been written since the buffer pool's initialization.
int getNumWriteIO (BM_BufferPool *const bm)
{	
	//Indicates how many write I/O operations were made on the buffer pool.
	return writeCnt;
}

//The purpose of the function is to return an array of page numbers.
PageNumber *getFrameContents (BM_BufferPool *const bm)
{	
	//Frame holds one page of data from the disk
	//Allocate memory block for an array of Pagenumber values with length  of maxBufferSize 
	//frameContents points to a array of pageNumbers
	PageNumber *frame_Content_Array = malloc(sizeof(PageNumber) * maxBufferSize);
	Frame *page_Frame_Array = (Frame *) bm->mgmtData;
	
	
	//This initializes a loop counter variable i to zero. This variable is used to iterate through each frame in the buffer pool.
	//The loop executes through each page frame and stores the page num as either no page[-1] if 
	//there is no page in the frame or the page number if the Frame has any pages,the data 
	//is stored in the frame contents array
	//
	for (int i = 0; i < bm->numPages; i++) {
        if (page_Frame_Array[i].page_num == NO_PAGE) {
            frame_Content_Array[i] = NO_PAGE;
        } else {
            frame_Content_Array[i] = page_Frame_Array[i].page_num;
        }
    }

	//Returns array of pointers
	return frame_Content_Array;
}

// This function returns an array of bools, each element represents the isDirty of the respective page.
//The function is used to return an array of boolean values having true or false , where true corresponds to the fact
//if the Frame has a dirty flag set or its false
bool *getDirtyFlags (BM_BufferPool *const bm)
{
	bool *dirty_Flags_Array = malloc(sizeof(bool) * maxBufferSize);
	Frame *page_Frame_Array = (Frame *)bm->mgmtData;
	
	
	// Iterating through all the pages in the buffer pool and setting dirtyFlags' value to TRUE if page is dirty else FALSE
	//The loop iterates through all the pagefram in the buffer pool and checks if it has the dirty flag set to 1 or not ,
	//if set to 1 then it will update it with True else False
	for(int i = 0; i < maxBufferSize; i++)
	{
		if(page_Frame_Array[i].dirty == 1)
		{
			dirty_Flags_Array[i]=true;
		}else {

			dirty_Flags_Array[i]=false;
		}
		
	}	
	return dirty_Flags_Array;
}

// The ith element of the array of ints (numPages) returned by this function is the fix count of the page that is stored in the ith page frame.
int *getFixCounts (BM_BufferPool *const bm)
{
	int *fix_Counts_Array = malloc(sizeof(int) * maxBufferSize);
	Frame *page_Frame_Array= (Frame *)bm->mgmtData;
	
	for(int i=0;i<maxBufferSize;i++)
	{
		if(page_Frame_Array[i].fCnt != -1)
		{

			fix_Counts_Array[i]=page_Frame_Array[i].fCnt;
		}else {

			
			fix_Counts_Array[i]=0;

		}


	}
	return fix_Counts_Array;
}