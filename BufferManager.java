import java.io.*;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Buffer manager. Manages a memory-based buffer pool of pages.
 * @author Dave Musicant, with considerable material reused from the
 * UW-Madison Minibase project
 * @author Modified by Yawen Chen and Tao Liu
 * @phase 2
 */

public class BufferManager
{
    public static class PageNotPinnedException
        extends RuntimeException {};
    public static class PagePinnedException extends RuntimeException {};

    /**
     * Value to use for an invalid page id.
     */
    public static final int INVALID_PAGE = -1;

    private static class FrameDescriptor
    {
        private int pageNum;
        private String fileName;
        private int pinCount;
        private boolean dirty;
        private boolean pinned;
        
        public FrameDescriptor()
        {
            pageNum = INVALID_PAGE;
            pinCount = 0;
            fileName = null;
            dirty = false;
            pinned = true;
        }

        private void setFileName(String str){
            this.fileName= fileName;
        }

        private String getFileName(){
            return this.fileName;
        }

        private void setpageNum(int pageID){
            this.pageNum = pageID;
        }

        private int getPageNum(){
            return this.pageNum;
        }
        
        private void increasePinCount(){
            this.pinCount++;
        }

        private void decreasePinCount(){
            this.pinCount--;
        }

        private int getPinCount(){
            return this.pinCount;
        }

        private void setDirty(){
            this.dirty = !this.dirty;
        }

        private boolean getDirty(){
            return this.dirty;
        }

        private void setUnpinned(){
            this.pinned = !this.pinned;
        }

        private boolean ifPinned(){
            return this.pinned;
        }
        
    }
    
    
    public int checkPinCount (int temp){
        return frameTable[temp].getPinCount();
    }
    // Here are some private variables to get you started. You'll
    // probably need more.
    private Page[] bufferPool;
    private FrameDescriptor[] frameTable;
    // We use this hash table to store the page ID as the key and the index of the page in the bufferpool as the value;
    private HashMap<Integer, Integer> myMap = new HashMap<Integer, Integer>();
    private int curClockIndex;
    private final int FRAME_IS_FULL = -20; //frameTable is full
    private final int FRAME_PIN_FULL = -30; //frameTable is full and all frames are pinned
    
    /**
     * Creates a buffer manager with the specified size.
     * @param poolSize the number of pages that the buffer pool can hold.
     */
    public BufferManager(int poolSize)
    {
        bufferPool = new Page[poolSize];
        frameTable= new FrameDescriptor[poolSize];
        curClockIndex = 0;
    }
    
    /**
     *Check if the FrameDescriptor is full
     *@param frameTable is a list of FrameDescriptor we are about to check
     *@return the index of replacement for which we can pin the page or a constant int value of -20 if it is full
     **/
    public int ifFull(FrameDescriptor[] frameTable){
        for (FrameDescriptor descriptor: frameTable){
            if (descriptor == null){
                return Arrays.asList(frameTable).indexOf(descriptor);
            }
        }
        return FRAME_IS_FULL;
    }
    
    /**
    *if the frameTable is full, 
    *we use the clock replacement policy and return the index of the frame in FDescriptor for which we can use to replace.
    *@param frameTable is a list of FrameDescriptor we are about to check
    *@return index of replacement on the frameDescriptor
    **/
    private int getClockIndex(FrameDescriptor[] frameTable)
    {
        FrameDescriptor curFDescriptor;
        boolean isFound = false;
        int poolSize = poolSize();
        int curIndex = (curClockIndex%poolSize);
        int count = 0;
        while (!isFound && count<poolSize+1){
            curFDescriptor = frameTable[(curIndex%poolSize)];
            if (curFDescriptor.getPinCount() == 0){
                if (!frameTable[curIndex%poolSize].ifPinned()){
                    this.curClockIndex = Arrays.asList(frameTable).indexOf(curFDescriptor);
                    isFound = true;
                }else {
                    curFDescriptor.setUnpinned();
                }
            }
            else count++;
            curIndex++;
        }
        if (count >= poolSize) return FRAME_PIN_FULL;
        return this.curClockIndex;
    }
    
    
    /**
     * Returns the pool size.
     * @return the pool size.
     */
    public int poolSize()
    {
        return bufferPool.length ;
    }


    /**
    * this method checks if given page is in the buffer pool.
    *@param given pageId 
    *@return boolean, true if the given page is in bufferPool already.
    **/
    private boolean checkIfInPool(int pageId)
    {
        if (!myMap.containsKey(pageId)) return false;
        return true;
    }
    /**
     * Checks if this page is in buffer pool. If it is, returns a
     * pointer to it. Otherwise, it finds an available frame for this
     * page, reads the page, and pins it. Writes out the old page, if
     * it is dirty, before reading.
     * @param pinPageId the page id for the page to be pinned
     * @param fileName the name of the database that contains the page
     * to be pinned
     * @param emptyPage determines if the page is known to be
     * empty. If true, then the page is not actually read from disk
     * since it is assumed to be empty.
     * @return a reference to the page in the buffer pool. If the buffer
     * pool is full, null is returned.
     * @throws IOException passed through from underlying file system.
     */
    public Page pinPage(int pinPageId, String fileName, boolean emptyPage)
        throws IOException
    {
        DBFile curDBFile = new DBFile(fileName); // create a new DBFile 
        Page curPage;
        int poolSize = poolSize();
        // If  the page is already in the pool, return a pointer to it;
        if (checkIfInPool(pinPageId)) {
            int pageIndex = myMap.get(pinPageId); //hash table stroing the pageId as the key and the index of the page in the buffer pool as the value
            frameTable[pageIndex] .increasePinCount(); //increase pin count
            return bufferPool[pageIndex];
        }
        //Now that the page is not in the buffer pool yet, check if the frameTable is full
        if (ifFull(frameTable) == FRAME_IS_FULL){
            //replace
            int indexOfReplace = getClockIndex(frameTable); //find the index of replacement
            if (indexOfReplace == FRAME_PIN_FULL) {
                return null; 
                }//If all frames are pinned, return null
            int localPageId=frameTable[indexOfReplace].getPageNum();
            flushPage(localPageId,fileName); // flushPage takes care of page that is dirty
            // read the actual page from the database.
            curPage = new Page();
            //check to see if the page is empty, if so we don't need to read the page from the disk . but we still need to
            //add this empty page and pin it
            if (!emptyPage){
                curDBFile.readPage(localPageId, curPage);
            }
            // update the current FDescriptor.
            FrameDescriptor curFDescriptor = new FrameDescriptor();
            curFDescriptor.setpageNum(pinPageId);
            curFDescriptor.increasePinCount();
            curFDescriptor.setFileName(fileName);
            //add the new frame in to the frame tableble
            frameTable[indexOfReplace] = curFDescriptor;
            // delete the old page in the buffer pool and add replace with new page
            bufferPool[myMap.get(localPageId)] = curPage;
            // delete the old entry in the hashmap and insert new entry
            int indexForInsert = Arrays.asList(bufferPool).indexOf(curPage);
            myMap.put(pinPageId, indexForInsert);
            this.curClockIndex++;
            return curPage;          
        }
        // When the frame descriptor still has empty space: add to existing empty frames
        int indexOfemptyFrame = ifFull(frameTable);
        FrameDescriptor curFDescriptor = new FrameDescriptor();
        curFDescriptor.setpageNum(pinPageId);
        curFDescriptor.increasePinCount();
        curFDescriptor.setFileName(fileName);
        //add the new frame in to the frame table
        frameTable[indexOfemptyFrame] = curFDescriptor;
        
        // read the actual page from the database.
        curPage = new Page();
        if (!emptyPage){
            curDBFile.readPage(pinPageId, curPage);
        }
        // push the page to the buffer pool
        bufferPool[indexOfemptyFrame] = curPage;
        // establish the key-value relationship in the myMap
        int indexOfValue = Arrays.asList(bufferPool).indexOf(curPage);
        myMap.put(pinPageId, indexOfValue);
        this.curClockIndex++;
        return curPage;        
    }

    /**
     * If the pin count for this page is greater than 0, it is
     * decremented. If the pin count becomes zero, it is appropriately
     * included in a group of replacement candidates.
     * @param unpinPageId the page id for the page to be unpinned
     * @param fileName the name of the database that contains the page
     * to be unpinned
     * @param dirty if false, then the page does not actually need to
     * be written back to disk.
     * @throws PageNotPinnedException if the page is not pinned, or if
     * the page id is invalid in some other way.
     * @throws IOException passed through from underlying file system.
     */
    public void unpinPage(int unpinPageId, String fileName, boolean dirty)
        throws IOException
    {
            for (FrameDescriptor FDescriptor: frameTable){        
            if (FDescriptor != null && unpinPageId == FDescriptor.getPageNum()) {
                if (FDescriptor.getPinCount() > 0) {
                    FDescriptor.decreasePinCount();
                }
                else if (FDescriptor.getPinCount()==0) FDescriptor.setUnpinned();
                else throw new PageNotPinnedException();
                 }
    }
    }

    /**
     * Requests a run of pages from the underlying database, then
     * finds a frame in the buffer pool for the first page and pins
     * it. If the buffer pool is full, no new pages are allocated from
     * the database.
     * @param numPages the number of pages in the run to be allocated.
     * @param fileName the name of the database from where pages are
     * to be allocated.
     * @return an Integer containing the first page id of the run, and
     * a references to the Page which has been pinned in the buffer
     * pool. Returns null if there is not enough space in the buffer
     * pool for the first page.
     * @throws DBFile.FileFullException if there are not enough free pages.
     * @throws IOException passed through from underlying file system.
     */
    public Pair<Integer,Page> newPage(int numPages, String fileName)
        throws IOException
    {
        DBFile dbFile = new DBFile(fileName);
        if  (ifFull(frameTable) == FRAME_IS_FULL){
            return null;
       }
        int firstPageId = dbFile.allocatePages(numPages);
        // Summing the first page is not empty
        Page curPage = pinPage(firstPageId, fileName, false);
        Pair pair = new Pair(firstPageId, curPage);
        return pair;     
    }

    /**
     * Deallocates a page from the underlying database. Verifies that
     * page is not pinned.
     * @param pageId the page id to be deallocated.
     * @param fileName the name of the database from where the page is
     * to be deallocated.
     * @throws PagePinnedException if the page is pinned
     * @throws IOException passed through from underlying file system.
     */
    public void freePage(int pageId, String fileName) throws IOException
    {
        DBFile dbFile = new DBFile(fileName);
        for (FrameDescriptor FDescriptor: frameTable){
            if (FDescriptor != null && FDescriptor.getPinCount()== 0){
                dbFile.deallocatePages(pageId, 1);
            }
            else {
                throw new PagePinnedException();
            }
        }
    }

    /**
     * Flushes page from the buffer pool to the underlying database if
     * it is dirty. If page is not dirty, it is not flushed,
     * especially since an undirty page may hang around even after the
     * underlying database has been erased. If the page is not in the
     * buffer pool, do nothing, since the page is effectively flushed
     * already.
     * @param pageId the page id to be flushed.
     * @param fileName the name of the database where the page should
     * be flushed.
     * @throws IOException passed through from underlying file system.
     */
    public void flushPage(int pageId, String fileName) throws IOException
    {
        DBFile dbFile = new DBFile(fileName);
        int bufferIndex = myMap.get(pageId);
        Page curPage = bufferPool[bufferIndex];
        for (FrameDescriptor fDestriptor: frameTable){
            if (fDestriptor.getPageNum()== pageId){
                 if (fDestriptor.getDirty()) {
                    dbFile.writePage(pageId,curPage);
                    fDestriptor.setDirty();
                 }
            }
        }
    }

    /**
     * Flushes all dirty pages from the buffer pool to the underlying
     * databases. If page is not dirty, it is not flushed, especially
     * since an undirty page may hang around even after the underlying
     * database has been erased.
     * @throws IOException passed through from underlying file system.
     */
    public void flushAllPages() throws IOException
    {
        DBFile dbFile;
        for (FrameDescriptor FDescriptor: frameTable){
            if (FDescriptor != null && FDescriptor.getDirty()){
                String curFileName = FDescriptor.getFileName();
                int curPageId=FDescriptor.getPageNum();
                Page curPage = bufferPool[myMap.get(curPageId)];
                dbFile = new DBFile(curFileName);
                dbFile.writePage(curPageId, curPage);
            }
        }
    }
        
    /**
     * Returns buffer pool location for a particular pageId. This
     * method is just used for testing purposes: it probably doesn't
     * serve a real purpose in an actual database system.
     * @param pageId the page id to be looked up.
     * @param fileName the file name to be looked up.
     * @return the frame location for the page of interested. Returns
     * -1 if the page is not in the pool.
    */
    public int findFrame(int pageId, String fileName)
    {
        //Qeustion: shall we check if the data is actually in the database system?
        //Question: is Buffer pool location the same as frame location?
        if (myMap.containsKey(pageId)) {
            return myMap.get(pageId);
        }
        return -1;
    }
  }  