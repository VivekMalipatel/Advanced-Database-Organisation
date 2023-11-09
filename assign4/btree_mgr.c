#include "dberror.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "tables.h"
#include <stdlib.h>
#include <string.h>

// Structure that holds the actual data of an entry
typedef struct NodeData {
	RID rid;
} NodeData;

// Structure that represents a node in the B+ Tree
typedef struct Node {
	void ** pointers;
	Value ** keys;
	struct Node * parent;
	bool is_leaf;
	int num_keys;
	struct Node * next; // Used for queue.
} Node;

// Structure that stores additional information of B+ Tree
typedef struct BTreeManager {
	BM_BufferPool bufferPool;
	BM_PageHandle pageHandler;
	int order;
	int numNodes;
	int numEntries;
	Node * root;
	Node * queue;
	DataType keyType;
} BTreeManager;


//************************************INDRAJIT GHOSH IMPLEMENTATION START***********************************

// This structure stores the metadata for our Index Manager
BTreeManager * treeManager = NULL;

RC initIndexManager(void *mgmtData) {
    // Initialize the storage manager.
    do{
	initStorageManager();
	}while(0);

    // Return success code.
    return RC_OK;
}


//Shuts down the index manager by deallocating the B+ Tree metadata structure and its buffer pool.
RC shutdownIndexManager() {
	 if (treeManager == NULL) {
        return RC_INVALID_PARAMETER; // Return an error code if the treeManager is not initialized.
    }

    // Deallocate the memory for the B+ Tree metadata structure.
    free(treeManager);
    treeManager = NULL;

    return RC_OK; // Return success code.
}


int numPages=1000;

//Tree Manager initializer
RC intializeTreeManager(int n,int numNodes,int numEntries ,Node *root,Node * queue,DataType keyType)
{
	// Allocate memory for the B+ Tree metadata structure and initialize its members.
    treeManager = (BTreeManager *) malloc(sizeof(BTreeManager));
    if (treeManager == NULL) {
        return RC_MALLOC_FAILED;
    }else{

    treeManager->order = n + 2;              // Set the order of the B+ Tree.
    treeManager->numNodes = numNodes;        // Set the initial number of nodes.
    treeManager->numEntries = numEntries;    // Set the initial number of entries.
    treeManager->root = root;                // Set the root node.
    treeManager->queue = queue;              // Set the first node in the queue for printing.
    treeManager->keyType = keyType;          // Set the data type of the keys.

    // Allocate memory for the Buffer Manager, initialize it, and store it in the B+ Tree metadata structure.
    BM_BufferPool *bm = (BM_BufferPool *) malloc(sizeof(BM_BufferPool));
    if (bm == NULL) {
        free(treeManager);
        return RC_MALLOC_FAILED;
    }
	 treeManager->bufferPool = *bm;

	}


}

RC createBtree(char *idxId, DataType keyType, int n) {
    // Calculate the maximum number of nodes that can be stored in a page.
    int nodeLimit = PAGE_SIZE / sizeof(Node);

    // Check if the given order is valid; if not, return an error code.
    if (n > nodeLimit) {
        return RC_ORDER_TOO_HIGH_FOR_PAGE;
    } else if (n < 1) {
        return RC_INVALID_ORDER;
    }

    // Initialize the tree manager.
    intializeTreeManager(n, 0, 0, NULL, NULL, keyType);

    RC succ;
    char data[PAGE_SIZE];
	// Initialize data buffer with zeros
	memset(data, 0, sizeof(data));

    // Create and open a new page file for the B++ Tree.
    succ = createPageFile(idxId);
    if (succ != RC_OK) {
        return succ;
    }

    SM_FileHandle fileHandle;
    succ = openPageFile(idxId, &fileHandle);
    if (succ != RC_OK) {
        return succ;
    }

    // Write the root node of the B++ Tree to the page file.
    succ = writeBlock(0, &fileHandle, data);
    if (succ != RC_OK) {
        return succ;
    }

    // Close the page file.
    succ = closePageFile(&fileHandle);
    if (succ != RC_OK) {
        return succ;
    }

    return RC_OK;
}



// This functions opens an existing B+ Tree from the specified page "idxId"
RC openBtree(BTreeHandle **tree, char *idxId) {
	// Retrieve B+ Tree handle and assign our metadata structure
	*tree = (BTreeHandle *) calloc(1, sizeof(BTreeHandle));
	if (*tree == NULL) {
   	 return RC_CALLOC_FAILED;
	}
	(*tree)->mgmtData = treeManager;

	// Initialize a Buffer Pool using Buffer Manager
	RC succ = initBufferPool(&treeManager->bufferPool, idxId, numPages, RS_FIFO, NULL);

       if (succ != RC_OK) {
        free((*tree)->mgmtData);
        free(*tree);
        return succ;
    } else {
        return RC_OK;
    }
}

RC closeBtree(BTreeHandle *tree) {
    // Check for valid input
    if (tree == NULL) {
        return RC_INVALID_PARAMETER;
    }

    // Access the B+ Tree metadata.
    BTreeManager *treeManager = (BTreeManager *)tree->mgmtData;
    if (treeManager == NULL) {
        return RC_INVALID_PARAMETER;
    }

    // Mark page as dirty and terminate the buffer pool.
    RC result;
    
        result = markDirty(&treeManager->bufferPool, &treeManager->pageHandler);
			

		do {
        result = shutdownBufferPool(&treeManager->bufferPool);;
        if (result != RC_OK) {
            break;
        }
    } while (0);
    free(tree);

    return result;
}


// This method deleted the B+ Tree by deleting the associated page with it.
RC deleteBtree(char *idxId) {
	do{
  // Check for valid input
    if (idxId == NULL) {
        return RC_INVALID_PARAMETER;
    }

    // Attempt to destroy the page file associated with the B+ Tree
    RC result = destroyPageFile(idxId);

    // Check the result of the destroyPageFile operation
    if (result != RC_OK) {
        // If the operation failed, return the error code
        return result;
    }
	}while(0);

    // If the operation succeeded, return the success status code
    return RC_OK;
}

//********************************INDRAJIT GHOSH FUNCTION ENDS************************************

//************************************SAI IMPLEMENTATION START***********************************

//Function to print tree

Node *printNodes(BTreeManager *treeManager)
{
	//if tree is Null , return Null.
	if (treeManager == NULL)
	{
		printf("B+ Tree Manager is Null - Initialization Failed");
		return NULL;
	}
	//Move forward while printing nodes
	(*treeManager).queue = treeManager->queue->next;
	Node *temp_node = (*treeManager).queue;
	(*temp_node).next = NULL;
	return temp_node;
}

//To get all the nodes in B+Tree, use Tree Management data from Btree Handle
//Store the number of nodes by accessing tree Manager.


RC getNumNodes(BTreeHandle *tree, int *entries)
{
	BTreeManager *treeManager;
	treeManager = (BTreeManager *)tree->mgmtData;
	if (treeManager == NULL) {
        return RC_INVALID_PARAMETER;
    }
	if (treeManager->root == NULL) {
		printf("Tree does not exist.\n");
		return RC_IM_KEY_NOT_FOUND;
	}
	RC code;
	//Place the result into entries param.
	*entries = treeManager->numNodes;
	//Return code.
	return RC_OK;
}

//To get the number of keys,B+Tree, use Tree Management data from Btree Handle
//Store the number of nodes by accessing tree Manager.


RC getNumEntries(BTreeHandle *tree, int *keys) {
	BTreeManager * treeManager;
	
	treeManager = (BTreeManager *) tree->mgmtData;
	if (treeManager == NULL) {
        return RC_INVALID_PARAMETER;
    }
	if (treeManager->root == NULL) {
		printf("Tree does not exist.\n");
		return RC_IM_KEY_NOT_FOUND;
	}
	//Store all keys count in the *keys parameter
	*keys = treeManager->numEntries;
	return RC_OK;
}

//To retrieve all the record's datatype in B+Tree,B+Tree, use Tree Management data from Btree Handle
//Store the number of nodes by accessing tree Manager.

RC getKeyType(BTreeHandle *tree, DataType *keytype)
{
	BTreeManager *treeManager;
	treeManager = (BTreeManager *)tree->mgmtData;
	if (treeManager == NULL) {
        return RC_INVALID_PARAMETER;
    }
	if (treeManager->root == NULL) {
		printf("Tree does not exist.\n");
		return RC_IM_KEY_NOT_FOUND;
	}
	//Store all the keytypes in *keytype parameter
	*keytype = treeManager->keyType;
	return RC_OK;

}

//Print B+ tree using this function.
extern char *printTree(BTreeHandle *tree)
{	
	//Retrieve tree data & information
	BTreeManager *treeManager = (BTreeManager *)tree->mgmtData;
	char empty = '\0';
	//if root or data is not present just return null.
	if(treeManager == NULL && treeManager->root ==NULL)
		return empty;

	Node *print = NULL;
	int level=0,l2=0;
	if (treeManager->root==NULL)
	{
		printf("Tree does not exist.\n");
		return empty;
	}	
	
	//Append all the next nodes to the root, so that root points to all the next nodes.

	treeManager->queue = NULL;
	Node *nnode; 
	if (treeManager->queue != NULL)
	{	
		nnode = treeManager->queue;
		//if next pointer is not null then
		while (nnode->next != NULL)
		{
			nnode = nnode->next;
		}
		nnode->next = treeManager->root;
		treeManager->root->next = NULL;

	}
	//if next pointer is null
	//make the root point into queue, append all nodes and make the last node pont to null.
	else
	{
		treeManager->queue = treeManager->root;
		treeManager->queue->next = NULL;
	}
	int move = 0,idx=0;
	while (treeManager->queue!=NULL)
	{
		print = printNodes(treeManager);
		if (print == print->parent->pointers[0] && print->parent !=NULL)
		{
			Node *temp;
			temp = print;
			
	
		while (temp != treeManager->root)
		{
			temp = (*temp).parent;
			move++;
		}
		}
		//increase the level / depth while printing the tree.
		l2 = move;

		if(level!=l2){
		level = l2;
		printf("\n");
	}
	//Print keys while retrieving the datatype from the new node.
	
	while( idx < print->num_keys) {
			
			 //for bool datatype
			if(treeManager->keyType==DT_BOOL){
				printf("%d ", print->keys[idx]->v.boolV);
			}	
			//float datatype
			else if(treeManager->keyType == DT_FLOAT){
				printf("%.02f ", print->keys[idx]->v.floatV);
			}
			//string datatype
			else if (treeManager->keyType==DT_STRING){
				printf("%s ", print->keys[idx]->v.stringV);
			}
			//integer datatype
			else if(treeManager->keyType == DT_INT){
				printf("%d ", print->keys[idx]->v.intV);
			}
			//increase index ptr
				idx++;
			//print the page and the slot where the records exist.
			printf("(%d - %d) ", ((NodeData *) print->pointers[idx])->rid.page, ((NodeData *) print->pointers[idx])->rid.slot);
		}
		//Get all the pointers pointing next to the data and print into the tree.
		if (print->is_leaf!=TRUE){
			int tidx =0;
			while(tidx <= print->num_keys){
				Node *temp_node = (*treeManager).queue;
				if (treeManager->queue != NULL){
				while (temp_node != NULL){
					temp_node = temp_node->next;
				}
				temp_node->next = print->pointers[idx];
				print->next = NULL;
				}
				//if data is null, then just return the only record present 
				//make next point to null and return.
				else{
					temp_node = print;
					temp_node->next = NULL;
					}
				tidx++;
			}
		}
		printf("[ ");
	}
	printf("\n");

	return empty;
}

// This function compares two keys and returns TRUE if first key is less than second key.

bool mincmp(Value *key1, Value *key2){

	if (key1 == NULL || key2 == NULL)
	{
		printf("Keys are null");
		return FALSE;
	}
	
	if (key1->dt == DT_STRING)
	{
		int x = strcmp(key1->v.stringV,key2->v.stringV);
		bool res = (x==-1) ? TRUE : FALSE;
		return res;
	}
	else if(key1->dt==DT_BOOL){

		return FALSE;
	}
	else if(key1->dt==DT_FLOAT){
		
		bool res = (key1->v.floatV < key2->v.floatV) ? TRUE : FALSE;
		return res;
	}
	else if (key1->dt==DT_INT)
	{
		bool res = (key1->v.floatV < key2->v.floatV) ? TRUE : FALSE;
		return res;
	}
}

// This function compares two keys and returns TRUE if first key is greater than second key.
bool maxCompare(Value *key1, Value *key2)
{
	if (key1 == NULL || key2 == NULL)
	{
		printf("Keys are null");
		return FALSE;
	}
	if (key1->dt == DT_STRING)
	{
		int x = strcmp(key1->v.stringV,key2->v.stringV);

		bool res = (x==1) ? TRUE : FALSE;
		return res;
	}
	else if(key1->dt==DT_BOOL){

		return FALSE;
	}
	else if(key1->dt==DT_FLOAT){
		
		bool res = (key1->v.floatV > key2->v.floatV) ? TRUE : FALSE;
		return res;
	}
	else if (key1->dt==DT_INT)
	{
		bool res = (key1->v.intV > key2->v.intV) ? TRUE : FALSE;
		return res;
	}
}

// This function compares two keys and returns TRUE if first key is equal to the second key else returns FALSE.
bool keysAreEqual(Value *key1, Value *key2)
{
	if (key1 == NULL || key2 == NULL)
	{
		printf("Keys are null");
		return FALSE;
	}
	
	if (key1->dt == DT_STRING)
	{
		int x = strcmp(key1->v.stringV,key2->v.stringV);

		bool res = (x==0) ? TRUE : FALSE;
		return res;
	}
	else if(key1->dt==DT_BOOL){

		return FALSE;
	}
	else if(key1->dt==DT_FLOAT){
		
		bool res = (key1->v.floatV == key2->v.floatV) ? TRUE : FALSE;
		return res;
	}
	else if (key1->dt==DT_INT)
	{
		bool res = (key1->v.intV == key2->v.intV) ? TRUE : FALSE;
		return res;
	}
}

//************************************SAI IMPLEMENTATION END***********************************



















//********************************************FUNCTIONS BY VIVEKANAND REDDY MALIPATEL START***************************************************//

BTreeManager *treeManager;


int getSplit(bool is_leaf){

	int split;
	if (is_leaf) {
		if ((treeManager->order - 1) % 2 == 0)
			split = (treeManager->order - 1) / 2;
		else
			split = (treeManager->order - 1) / 2 + 1;
	} else {
		if ((treeManager->order) % 2 == 0)
			split = (treeManager->order) / 2;
		else
			split = (treeManager->order) / 2 + 1;
		split--;
	}
	return split;
}

extern RC findKey(BTreeHandle *tree, Value *key, RID *result) {

	RC code = RC_OK;
	int i = 0;
	Node * node = treeManager->root;

	while (node->is_leaf != true) {
		i=0;
		while (i < node->num_keys) {
			if (mincmp(key, node->keys[i])) {
				break;
			} else {
				i++;
			}
		}
		node = (Node *) node->pointers[i];
	}
	if (node == NULL) {
		RC_message = "The Given Key Not Found";
		code = RC_IM_KEY_NOT_FOUND;
	} else {
		i=0;
		while(i < node->num_keys) {
			if (keysAreEqual(node->keys[i], key)){
				break;
			}
			i++;
		}

		if (node == NULL || i == node->num_keys) {
			RC_message = "The Given Key Not Found";
			code = RC_IM_KEY_NOT_FOUND;
		} 
		else {
			RC_message = "Find Key Successfull";
			*result = ((NodeData *) node->pointers[i])->rid;
		}
	}
	return code;
}

RC nullParent(BTreeManager *treeManager,Value * key, NodeData * pointer, Node* left, Node* right, bool is_leaf){

	RC code = RC_OK;
	treeManager->numNodes++;
	treeManager->root = malloc(sizeof(Node));
	treeManager->root->keys = malloc((treeManager->order - 1) * sizeof(Value *));
	treeManager->root->keys[0] = key;
	treeManager->root->pointers = malloc(treeManager->order * sizeof(void *));
	treeManager->root->is_leaf = is_leaf;
	treeManager->root->num_keys = 1;
	if(left != NULL && right !=NULL){
		treeManager->root->pointers[0] = left;
		treeManager->root->pointers[1] = right;
		left->parent = right->parent = treeManager->root;
	}
	else{
		treeManager->root->pointers[0] = pointer;
		treeManager->numEntries++;
	}
	RC_message = "Parent was Empty, Created a new parent and inserted the key";
	return code;

}

RC insertKey(BTreeHandle *tree, Value *key, RID rid) {

	RC code = RC_OK;
	treeManager = (BTreeManager *) tree->mgmtData;
	NodeData * pointer = (NodeData *) malloc(sizeof(NodeData));
	pointer->rid.page = rid.page;
	pointer->rid.slot = rid.slot;
	if (treeManager->root == NULL) {
		code = nullParent(treeManager,key,pointer,NULL,NULL,true);
	}
	else{
		int i;
		Node * leaf = treeManager->root;
		while (!leaf->is_leaf) {
			for (i=0 ; i < leaf->num_keys;) {
				if(mincmp(key, leaf->keys[i]))
				{
					break;
				}
				else {
					i++;
				}
			}
			leaf = (Node *) leaf->pointers[i];
		}
		if (leaf->num_keys < (treeManager->order) - 1) {

			int insertion_index = 0;
			treeManager->numEntries++;
			while (insertion_index < leaf->num_keys && mincmp(leaf->keys[insertion_index], key))
				insertion_index++;

			int current_key = leaf->num_keys;
			while (current_key >= insertion_index) {
				if(current_key == insertion_index)
				{
					leaf->pointers[insertion_index] = pointer;
					leaf->keys[insertion_index] = key;
					leaf->num_keys++;
					current_key--;
				}
				else{
					leaf->pointers[current_key] = leaf->pointers[current_key - 1];
					leaf->keys[current_key] = leaf->keys[current_key - 1];
					current_key--;
				}
			}
			RC_message = "Insert Successfull";
		} 
		else {

			Node * new_leaf;
			Value ** temp_keys = malloc(treeManager->order * sizeof(Value));
			void ** temp_pointers = malloc(treeManager->order * sizeof(void *));
			Value * keyToParent;
			
			int i,j,inserted = 0;
			for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
				if (inserted == 0 && maxCompare(leaf->keys[i], key)){
					temp_keys[j] = key;
					temp_pointers[j] = pointer;
					j++;
					inserted = 1;
				}
				temp_keys[j] = leaf->keys[i];
				temp_pointers[j] = leaf->pointers[i];
			}
			if(inserted == 0)
			{
				temp_keys[j] = key;
				temp_pointers[j] = pointer;
			}

			leaf->num_keys = 0;
			treeManager->numNodes++;
			new_leaf = malloc(sizeof(Node));
			new_leaf->keys = malloc((treeManager->order - 1) * sizeof(Value *));
			new_leaf->pointers = malloc(treeManager->order * sizeof(void *));
			new_leaf->num_keys = 0;
			new_leaf->is_leaf = true;

			int split = getSplit(new_leaf->is_leaf);

			for (int i = 0,j = 0; i < treeManager->order; i++) {
				if(i < split){
					leaf->pointers[i] = temp_pointers[i];
					leaf->keys[i] = temp_keys[i];
					leaf->num_keys++;
				}
				else{
					new_leaf->pointers[j] = temp_pointers[i];
					new_leaf->keys[j] = temp_keys[i];
					new_leaf->num_keys++;
					j++;
				}
				if(i==treeManager->order - 1){
					new_leaf->pointers[i] = leaf->pointers[i];
					leaf->pointers[i] = new_leaf;
				}
			}

			for (int i = leaf->num_keys, j = new_leaf->num_keys; i,j < treeManager->order - 1; i++, j++){
				leaf->pointers[i] = NULL;
				new_leaf->pointers[j] = NULL;
			}

			new_leaf->parent = leaf->parent;
			keyToParent = new_leaf->keys[0];;
			treeManager->numEntries++;
			
			while(1){

				if (leaf->parent == NULL || leaf->parent->pointers == NULL){
					
					code = nullParent(treeManager,keyToParent,NULL,leaf,new_leaf,false);
					break;
				}
				else{

					int left_index = 0;
					while (left_index <= leaf->parent->num_keys && leaf->parent->pointers[left_index] != leaf){
						left_index++;
					}

					if ((leaf->parent->num_keys) < (treeManager->order - 1)) {
						for (int i = leaf->parent->num_keys; i > left_index; i--) {
							leaf->parent->pointers[i + 1] = leaf->parent->pointers[i];
							leaf->parent->keys[i] = leaf->parent->keys[i - 1];
						}

						leaf->parent->pointers[left_index + 1] = new_leaf;
						leaf->parent->keys[left_index] = new_leaf->keys[0];
						leaf->parent->num_keys++;
						RC_message = "Insert Successfull";
						break;
					}
					else {

						int i, j, k;
						int num_keys = leaf->parent->num_keys;

						for (i = 0, j = 0, k=0; i < num_keys + 1; i++, j++, k++) {
							if (j == left_index + 1) {
								j++;
							}
							temp_pointers[j] = leaf->parent->pointers[i];
							
							if (i < num_keys){
								if(k != left_index) {
									temp_keys[k] = leaf->parent->keys[i];
								}
								else{
									k++;
								}
							}
						}

						temp_pointers[left_index + 1] = new_leaf;
						temp_keys[left_index] = new_leaf->keys[0];
						treeManager->numNodes++;

						new_leaf->is_leaf = false;
						new_leaf->num_keys = 0;
						leaf->parent->num_keys = 0;

						for(i=0,j=0;i<treeManager->order;i++){
							if(i<split - 1){
								leaf->parent->pointers[i] = temp_pointers[i];
								leaf->parent->keys[i] = temp_keys[i];
								leaf->parent->num_keys++;
							}
							else if(i>split-1){
								new_leaf->keys[j] = temp_keys[i];
								new_leaf->num_keys++;
								new_leaf->pointers[j] = temp_pointers[i];
								j++;
							}
							else{
								leaf->parent->pointers[i] = temp_pointers[i];
								keyToParent = temp_keys[split - 1];
								i++;
							}
						}
						new_leaf->pointers[j] = temp_pointers[i];
						new_leaf->parent = leaf->parent->parent;
						treeManager->numEntries++;
						leaf = leaf->parent;
					}
				}
			}
		}
	}
	return code;
}


Node * deleteEntry(BTreeManager * treeManager, Node * n, Value * key, void * pointer) {
	

	int i;

	for(i=0;i < n->num_keys; i++)
	{
		if(maxCompare(n->keys[i], key)){
			n->keys[i - 1] = n->keys[i];
		}
	}

	int found=0;
	for(i=0; i < (n->is_leaf ? n->num_keys : n->num_keys + 1); )
	{
		if(found == 0 && n->pointers[i] != pointer){
			i++;
		}
		else if(n->pointers[i] == pointer){
			found = 1;
			i++;
		}
		if(found == 1){
			n->pointers[i - 1] = n->pointers[i];
			i++;
		}
	}

	treeManager->numEntries--;
	n->num_keys--;

	for(i = n->num_keys+1; i < treeManager->order; i++)
	{
		if (i < treeManager->order - 1 && n->is_leaf){
			n->pointers[i-1] = NULL;
		}
		n->pointers[i] = NULL;
	}

	if (n == treeManager->root){
		if (treeManager->root->num_keys > 0){
			return treeManager->root;
		}
		else if (!treeManager->root->is_leaf) {
			treeManager->root = treeManager->root->pointers[0];
			treeManager->root->parent = NULL;
			return treeManager->root;
		} else {
			return NULL;
		}
	}

	int split = getSplit(n->is_leaf);
	if (n->num_keys >= split){
		return treeManager->root;
	}
	i=0;
	while(i <= n->parent->num_keys && (n->parent->pointers[i] != n)){
		i++;
	}
	i=i-1;
	int k_prime_index = i == -1 ? 0 : i;
	Node * neighbor = (i == -1) ? n->parent->pointers[1] : n->parent->pointers[i];
	if((neighbor)->num_keys + n->num_keys < (n->is_leaf ? treeManager->order : treeManager->order - 1)){

		Node * new_node;
		Value * k_prime = n->parent->keys[k_prime_index];
		if (i == -1) {
			new_node = n;
			n = neighbor;
			neighbor = new_node;
		}
		int insertion_index = neighbor->num_keys;
		int j,found=0;

		for (i = insertion_index, j = 0; j < n->num_keys; i++, j++) {
				if (!n->is_leaf) {
					if(found == 0){
						neighbor->keys[insertion_index] = k_prime;
						neighbor->num_keys++;
						found = 1;
					}
					neighbor->keys[i+1] = n->keys[j];
					neighbor->pointers[i+1] = n->pointers[j];
					n->num_keys--;
					if(j==n->num_keys-1){
						neighbor->pointers[i] = n->pointers[j];
					}
				}
				else{
					neighbor->keys[i] = n->keys[j];
					neighbor->pointers[i] = n->pointers[j];
					if(j==n->num_keys-1){
						neighbor->pointers[treeManager->order - 1] = n->pointers[treeManager->order - 1];
					}
				}
				neighbor->num_keys++;
		}
		if (!n->is_leaf) {
			i = 0;
			while (i < neighbor->num_keys + 1) {
				new_node = (Node *) neighbor->pointers[i];
				new_node->parent = neighbor;
				i++;
			}
		}
		return deleteEntry(treeManager, n->parent, k_prime, n);
	}
	else {
		Node * new_node;
		int j,k;
		if (!n->is_leaf){
			if (i != -1) {
				n->pointers[n->num_keys + 1] = n->pointers[n->num_keys];
			}
			else{
				n->keys[n->num_keys] = n->parent->keys[k_prime_index];
				n->pointers[n->num_keys + 1] = neighbor->pointers[0];
				new_node = (Node *) n->pointers[n->num_keys + 1];
				new_node->parent = n;
				n->parent->keys[k_prime_index] = neighbor->keys[0];
			}
		}
		else{
			if (i == -1) {
				n->keys[n->num_keys] = neighbor->keys[0];
				n->pointers[n->num_keys] = neighbor->pointers[0];
				n->parent->keys[k_prime_index] = neighbor->keys[1];
			}
		}
		for (j = n->num_keys,k = 0; j > 0 || k < neighbor->num_keys - 1; j--,k++) {
			if(i != -1){
				n->keys[j] = n->keys[j - 1];
				n->pointers[j] = n->pointers[j - 1];
			}
			else{
				neighbor->keys[k] = neighbor->keys[k + 1];
				neighbor->pointers[k] = neighbor->pointers[k + 1];
			}
		}
		if (!n->is_leaf){
			if(i!=-1){
				n->pointers[0] = neighbor->pointers[neighbor->num_keys];
				new_node = (Node *) n->pointers[0];
				new_node->parent = n;
				neighbor->pointers[neighbor->num_keys] = NULL;
				n->keys[0] = n->parent->keys[k_prime_index];
				n->parent->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1];
			}
			else{
				neighbor->pointers[k] = neighbor->pointers[k + 1];
			}
		}
		else{
			if(i!=-1){
				n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
				neighbor->pointers[neighbor->num_keys - 1] = NULL;
				n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
				n->parent->keys[k_prime_index] = n->keys[0];
			}
		}
		n->num_keys++;
		neighbor->num_keys--;
		return treeManager->root;
	}
}

RC deleteKey(BTreeHandle *tree, Value *key) {

	RC code = RC_OK;
	int i;
	Node * node = treeManager->root;
	NodeData * pointer;

	for (;node->is_leaf != true;) {
		for (i = 0;i < node->num_keys;i++) {
			if (mincmp(key, node->keys[i])) {
				break;
			}
		}
		node = (Node *) node->pointers[i];
	}
	if (node == NULL) {
		RC_message = "The Given Key Not Found";
		code = RC_IM_KEY_NOT_FOUND;
	} 
	else{
		i=0;
		while(i < node->num_keys) {
			if (keysAreEqual(node->keys[i], key)){
				break;
			}
			i++;
		}

		if (node == NULL || i == node->num_keys) {
			RC_message = "The Given Key Not Found";
			code = RC_IM_KEY_NOT_FOUND;
		} 
		else {
			pointer = ((NodeData *) node->pointers[i]);
			treeManager->root = deleteEntry(treeManager, node, key, pointer);
		}
	}

	return code;
}

/*************************************SCAN FUNCTIONS**********************************/


typedef struct ScanManager {
	int keyIndex;
	int totalKeys;
	int order;
	Node * node;
} ScanManager;

RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {

	RC code = RC_OK;
	BTreeManager *treeManager = (BTreeManager *) tree->mgmtData;
	ScanManager *scanmeta = malloc(sizeof(ScanManager));
	*handle = malloc(sizeof(BT_ScanHandle));

	if (treeManager->root == NULL) {
		code = RC_NO_RECORDS_FOUND;
	} else {
		while (!treeManager->root->is_leaf){
			treeManager->root = (Node*) treeManager->root->pointers[0];
		}
		scanmeta->keyIndex = 0;
		scanmeta->totalKeys = treeManager->root->num_keys;
		scanmeta->node = treeManager->root;
		scanmeta->order = treeManager->order;
		(*handle)->mgmtData = scanmeta;
		(*handle)->tree = NULL;
	}
	return code;
}

RC nextEntry(BT_ScanHandle *handle, RID *result) {

	RC code = RC_OK;
	ScanManager * scanmeta = (ScanManager *) handle->mgmtData;

	if (scanmeta->node == NULL && scanmeta->node->num_keys == NULL) {
		RC_message = "No more entreis in the tree";
		code = RC_IM_NO_MORE_ENTRIES;
	}
	else{
		if (scanmeta->keyIndex < scanmeta->totalKeys) {
			*result = ((NodeData *) scanmeta->node->pointers[scanmeta->keyIndex])->rid;
			scanmeta->keyIndex++;
			RC_message = "Next Scan Successfull";

		} else {
			if (scanmeta->node->num_keys != NULL && scanmeta->node->pointers[scanmeta->order - 1] != NULL) {
				scanmeta->node = scanmeta->node->pointers[scanmeta->order - 1];
				if(scanmeta->node->pointers!=NULL){
					scanmeta->keyIndex = 1;
					scanmeta->totalKeys = scanmeta->node->num_keys;
					*result = ((NodeData *) scanmeta->node->pointers[0])->rid;
					RC_message = "Next Scan Successfull";
				}
				else{
					RC_message = "No more entreis in the tree";
					code = RC_IM_NO_MORE_ENTRIES;
				}
			} else {
				RC_message = "No more entreis in the tree";
				code = RC_IM_NO_MORE_ENTRIES;
			}
		}
	}
	return code;
}


// Function to terminate the B+ tree scanning process and release allocated resources
RC terminateBTreeScan(BT_ScanHandle *scan_handle) {
	RC code=RC_OK;
    // Use do-while loop with a single iteration to provide an easy way to break out in case of errors or issues
    do {
        // Check if the management data in the scan_handle is not NULL
        if (scan_handle->mgmtData != NULL) {
            // Set the management data to NULL, effectively releasing the resources associated with it
            scan_handle->mgmtData = NULL;
        }
        
        // Free the memory occupied by the scan_handle itself
        free(scan_handle);
		
		 // Return the success code
		return code;

    // The loop will only execute once, as the condition is set to 0
    } while (0);
}


extern RC closeTreeScan(BT_ScanHandle *handle) {
	
	return terminateBTreeScan(handle);
}
//********************************************FUNCTIONS BY VIVEKANAND REDDY MALIPATEL END***************************************************//