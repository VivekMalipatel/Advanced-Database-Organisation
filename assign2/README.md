Assignment 1 - Storage Manager
______________________________

 
TEAM MEMBERS (GROUP 3)
______________________

1. Vivekanand Reddy Malipatel - A20524971
2. Indrajit Ghosh - A20525793
3. Sai Ram Oduri - A20522183

------------------------
Description----->

Script Execution---->

1) Go to the terminal
2) Inside project folder 
3) To build test_assign2_1 i.e default test case --->run command :make test_assign2_1
4) To build test_assign2_2 i.e our new test case --->run command :make test_assign2_2
5) To execute run command for test_assign2_1 :make run1
6) To execute run command for test_assign2_2 :make run2
7) The Object files can be removed using : make clean

----------------------------

Functions Implemented- 


--------- IMPLEMENTED BY INDRAJIT GHOSH ------------------------------------------
<br><br>
Statistics Function

Information on the buffer pool is gathered using functions linked to statistics. As a result, it offers a variety of statistical data regarding the buffer pool.

1) getFrameContents(...)

An array of PageNumbers is returned by this function. buffer size = the array size (numPages).
To obtain the pageNum value of the page frames existing in the buffer pool, we iterate through all of the page frames in the buffer pool.
The page number of the page saved in the "n"th page frame is the "n"th element.


2) getDirtyFlags(...)

An array of bools is returned by this function. buffer size = the array size (numPages).
To obtain the dirtyBit value for each page frame in the buffer pool, we iterate through all of the page frames in the pool.
If TRUE, the "n"th pageframe is dirty.

3) getFixCounts(...) 

An array of ints is the result of this function. buffer size = the array size (numPages).
To determine the fixCount value for the page frames that are currently in the buffer pool, we iterate through all of the page frames there.
The fixCount of the page saved in the "n"th page frame is the "n"th element.


4) getNumReadIO(...)

The function returns the total number of IO reads, or the number of pages read from the disk, that were carried out by the buffer pool.
The rearIndex variable is used to keep this data current.


5) getNumWriteIO(...)

This function  returns the total number of IO writes carried out by the buffer pool, or the number of pages written to the disk.
The writeCount variable is used to keep track of this data. When the buffer pool is created, the writeCount variable is initialized
to 0 and is increased each time a page frame is written to disk.


--------- IMPLEMENTED BY VIVEKANAND REDDY MALIPATEL ------------------------------------------
<br><br>
PAGE MANAGEMENT FUNCTIONS

1) markDirty(...)
This function marks the page as dirty indicating that the data of the page has been modified by the client.

2) unpinPage(...)
This function unpins a page from the memory i.e. removes a page from the memory.

3) forcePage(...)
This function writes the contents of the modified pages back to the page file on disk

4) pinPage(...)
This function pins a page with page number pageNum i.e. adds the page with page number pageNum to the buffer pool. If the buffer pool is full, then it uses appropriate page replacement strategy to replace a page in memory with the new page being pinned.

5) FIFO(...)
Implements the FIFO replacement strategy

6) LFU(...)
Implements the LFU replacement strategy

7) LRU(...)
Implements the LRU replacement strategy

8) CLOCK(...)
Implements the CLOCK replacement strategy

---------------------------------------------------------- IMPLEMENTED BY SAI RAM ODURI ------------------------------------------------------------------------------

The storage manager implemented in Assignment 1 was enhanced with a buffer pool that utilizes FIFO, LRU, LFU, and CLOCK page replacement algorithms. These algorithms are used to determine which page in the buffer pool should be replaced when a new page needs to be added and the buffer is full.

__initBufferPool()__

To create a buffer pool, the initBufferPool function is used. The function takes three parameters: the number of page frames that can be stored in the buffer (numPages), the name of the page file whose pages are being cached in memory (pageFileName), and the page replacement strategy to be used (strategy). Additionally, the stratData parameter is used to pass parameters to the page replacement strategy if needed.

__shutDownPool()__

When shutting down the buffer pool, the shutdownBufferPool function is called. This function frees up all the resources and memory space used by the buffer manager for the buffer pool. Before destroying the buffer pool, the forceFlushPool function is called to write all the dirty pages (modified pages) to the disk. If any page is being used by any client, an error is thrown indicating that the page cannot be removed from the buffer pool.

__forceFlushPool__

The forceFlushPool function writes all the dirty pages to the disk. It does this by checking all the page frames in the buffer pool and verifying that the dirtyBit is set to 1 (indicating that the content of the page frame has been modified by some client) and the fixCount is set to 0 (indicating that no user is using that page frame). If both conditions are met, the page frame is written to the page file on disk.
The page replacement strategy functions determine which page should be replaced from the buffer pool when a new page needs to be added, and the buffer pool is full. These algorithms include FIFO, LRU, LFU, and CLOCK. These strategies ensure that the most appropriate page is replaced from the buffer pool based on the page's usage and frequency of use.

__FIFO()__

FIFO (First In First Out) is a fundamental page replacement strategy. It is based on a queue-like structure where the page that arrived first in the buffer pool is at the front, and if the buffer pool is full, this page will be the first one to be replaced. After finding the page to be replaced, we write its content to the page file on disk and add the new page in its place.
The LRU page replacement strategy removes the least recently used page frame from the buffer pool. Each page frame has a hitNum field that keeps track of how many times it has been accessed and pinned by the client. Additionally, there is a global variable "hit" that serves the same purpose. To implement LRU, we identify the page frame with the lowest hitNum value and replace it with the new page. We then write the content of the page frame to the page file on disk and add the new page at that location.

__LFU()__

LFU (Least Frequently Used) is a page replacement strategy that removes the page frame from the buffer pool that has been accessed the least number of times compared to other page frames. Each page frame has a variable called "refNum" that keeps track of the number of times that the client has accessed it. We find the position of the page frame with the lowest "refNum" value, write its content to the page file on disk, and then add the new page to that location. We also store the position of the least frequently used page frame in a variable called "lfuPointer" so that it can be used for the next page replacement, which reduces the number of iterations needed from the second page replacement onwards.

__CLOCK()__

The CLOCK algorithm tracks the most recently added page frame in the buffer pool using a clockPointer counter. When a page needs to be replaced, the algorithm checks the page frame at the current clockPointer position. If the hitNum of that page is not 1 (meaning it wasn't the last page added), it is replaced with the new page. If hitNum is 1, the algorithm sets it to 0 and increments the clockPointer to check the next page frame. This process continues until a page is found to replace. HitNum is set to 0 to avoid getting stuck in an infinite loop.

-------------------------------------------------------------------------------------------------------------------------------------------

New Testcase File Implemented :- 

A new test case file has been implemented by us , testing the LFU and Clock page replacement strategy.
The test cases are used to verify the correct functioning of different functionalities provided by the buffer manager module.
The first test case verifies the ability of the buffer manager to create and read back dummy pages.
The second test case checks the ability of the buffer manager to read pages from the disk, mark them dirty, and then force them back to the disk.
The third test case tests the clock replacement strategy by verifying the content of the buffer pool after each operation.
The fourth test case tests the least frequently used (LFU) replacement strategy by verifying the content of the buffer pool after each operation.




