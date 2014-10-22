import java.io.*;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Buffer manager. Manages a memory-based buffer pool of pages.
 * @author Dave Musicant, with considerable material reused from the
 * UW-Madison Minibase project
 * @author Modified by Yawen Chen and Tao Liu
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
    
    

    // Here are some private variables to get you started. You'll
    // probably need more.
    private Page[] bufferPool;
    private FrameDescriptor[] frameTable;
    // We use this hash table to store the page ID as the key and the index of the page in the bufferpool as the value;
    private HashMap<Integer, Integer> myMap = new HashMap<Integer, Integer>();
    private int curClockIndex = 0;
    private final int FRAME_IS_FULL = -20;
    //private int poolSize;
    
    /**
     * Creates a buffer manager with the specified size.
     * @param poolSize the number of pages that the buffer pool can hold.
     */
    public BufferManager(int poolSize)
    {
        //this.poolSize = poolSize;
        bufferPool = new Page[poolSize];
        frameTable= new FrameDescriptor[poolSize];
    }
    
    /**
     *Check if the FrameDescriptor is full
     *Return a constant int value of -20 if it is full
     *Otherwise,return the index of frameDescriptor for which we can pin the page
     **/
    public int ifFull(FrameDescriptor[] frameTable){
        for (FrameDescriptor descriptor: frameTable){
            if (descriptor == null){
                return Arrays.asList(frameTable).indexOf(descriptor);
            }
        }
        return FRAME_IS_FULL;
    }
    
    // if the frameTable is full, we use the clock replacement policy and return the index of the frame in FDescriptor for which we can use to replace.
    private int getClockIndex(FrameDescriptor[] frameTable)
    {
        int curIndex = curClockIndex;
        curIndex++;
        FrameDescriptor curFDescriptor;
        boolean isFound = false;
        int poolSize = poolSize();
        while (!isFound){
            curFDescriptor = frameTable[(curIndex%poolSize)];
            if (curFDescriptor.getPinCount() == 0){
                if (!frameTable[curIndex%poolSize].ifPinned()){
                    this.curClockIndex = Arrays.asList(frameTable).indexOf(curFDescriptor);
                    isFound = true;
                }else {
                    curFDescriptor.setUnpinned();
                }
            }
                    curIndex++;
        }
        return this.curClockIndex;
    }
    
//    private void increaseClockIndex(){
//        int curIndex = this.curClockIndex;
//        if (0 <=curIndex <poolSize-1) this.curClockIndex++;
//        else if (curIndex = poolSize-1) this.curClockIndex = 0;
//    }
    
    
    /**
     * Returns the pool size.
     * @return the pool size.
     */
    public int poolSize()
    {
        return bufferPool.length ;
    }


    /*
    * this method checks if given page is in the buffer pool.
    *@param: pageID.
    *@return boolean
    */
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
        DBFile curDBFile = new DBFile(fileName);
        Page curPage;
        int poolSize = poolSize();
        //if (emptyPage) return null;
        if (checkIfInPool(pinPageId)) {
            int pageIndex = myMap.get(pinPageId);
            for (int i=0; i<poolSize; i++){
                if (frameTable[i].getPageNum() == pinPageId){
                    if (frameTable[i].getDirty()){
                        curPage = bufferPool[myMap.get(pinPageId)];
                        frameTable[i].setDirty();                       
                    }
                    frameTable[i].increasePinCount();
                }
            }
            return bufferPool[pageIndex];
        }
        //Check if the frameTable is full
        if (ifFull(frameTable) == FRAME_IS_FULL){
            //replace
            int localPageId= 0;
            int indexOfReplace = getClockIndex(frameTable);
            if (frameTable[indexOfReplace].getDirty()){
                localPageId = frameTable[indexOfReplace].getPageNum();
                curDBFile.writePage(localPageId, bufferPool[myMap.get(localPageId)]);
            }
            // read the actual page from the database.
            curPage = new Page();
            //check to see if the page is empty, if so we don't need to read the page from the disk . but we still need to
            //add this empty page and pin it
            if (!emptyPage){
                curDBFile.readPage(localPageId, curPage);
            }
            // update the current FDescriptor.
            FrameDescriptor curFDescriptor = new FrameDescriptor();
            curFDescriptor.setpageNum(localPageId);
            curFDescriptor.increasePinCount();
            curFDescriptor.setFileName(fileName);
            //add the new frame in to the frame tableble
            frameTable[indexOfReplace] = curFDescriptor;
    //This might be wrong: frameTable.add(indexOfReplace, curFDescriptor);
            // delete the old page in the buffer pool and add replace with new page
            bufferPool[myMap.get(localPageId)] = curPage;


    // List<Page> list = new ArrayList<Page>(Arrays.asList(bufferPool));
     //list.removeAll(Arrays.asList(bufferPool[myMap[localPageId]]));
    //bufferPool = list.toArray(bufferPool);
    //bufferPool.add(curPage);

            // delete the old entry in the hashmap and insert new entry
            myMap.remove(localPageId);
            int indexOfValue = Arrays.asList(bufferPool).indexOf(curPage);
            myMap.put(pinPageId, indexOfValue);
            return curPage;
            
        }
        // add to existing empty frames
        int indexOfemptyFrame = ifFull(frameTable);
        FrameDescriptor curFDescriptor = new FrameDescriptor();
        curFDescriptor.setpageNum(pinPageId);
        curFDescriptor.increasePinCount();
        curFDescriptor.setFileName(fileName);
        //add the new frame in to the frame table
        frameTable[indexOfemptyFrame] = curFDescriptor;
//frameTable.add(indexOfemptyFrame, curFDescriptor);
        
        // read the actual page from the database.
        curPage = new Page();
        if (!emptyPage){
            curDBFile.readPage(pinPageId, curPage);
        }
        // push the page to the buffer pool
        bufferPool.add(curPage);
        // establish the key-value relationship in the myMap
        int indexOfValue = Arrays.asList(bufferPool).indexOf(curPage);
        myMap.put(pinPageId, indexOfValue);
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
            if (FDescriptor.getPinCount() > 0) FDescriptor.decreasePinCount();
            else if (FDescriptor.getPinCount()==0) FDescriptor.setUnpinned();
            else throw new PageNotPinnedException();
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
        if (iffull(frameTable) != FRAME_IS_FULL){
            int firstPageID = dbFile.allocatePages(numPages);
            if (empty){
                Page curPage = pinPage(firstPageID, fileName, true);
            } else {
                Page curPage = pinPage(firstPageID, fileName, false);
            }
        }
        Pair pair = new Pair(firstPageID, curPage);
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
    }
}
