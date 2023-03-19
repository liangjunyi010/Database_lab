package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.Catalog.Help_key;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Random;
/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    public class RAM_helper {
        public Page page;
        public int RAM;
        public RAM_helper(Page page, int RAM) {
            this.page = page;
            this.RAM = RAM;
        }
    }
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private Map<PageId, RAM_helper> bp_List;

    private int cur_num_pages;
    private int randomNumber;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */

    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.bp_List = new HashMap<>(numPages);
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     * @throws IOException
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException{
        // some code goes here
        for (PageId pageid :this.bp_List.keySet()){
            if (pid.equals(pageid)){
                return this.bp_List.get(pageid).page;
            }
        }
        for (Help_key key :Catalog.catalog_List.keySet()) {
            if(key.table_id == pid.getTableId()){
                if(this.bp_List.size() < this.numPages){
                    Page newpage = Database.getCatalog().getDatabaseFile(key.table_id).readPage(pid);
                    RAM_helper RAM_helper = new RAM_helper(newpage,this.bp_List.size());
                    this.bp_List.put(pid, RAM_helper);
                    return newpage;
                }
                else{
                    this.evictPage();
                    Page newpage = Database.getCatalog().getDatabaseFile(key.table_id).readPage(pid);
                    RAM_helper RAM_helper = new RAM_helper(newpage,this.randomNumber);
                    this.bp_List.put(pid, RAM_helper);
                    return newpage;
                }
            }
        }
        return null;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pageList = file.insertTuple(tid, t);  // return an List contains the pages that were modified
        for(Page page: pageList){
            page.markDirty(true, tid);
            if (!this.bp_List.containsKey(page.getId()) && this.bp_List.size() >= this.numPages) {
                this.evictPage();
                RAM_helper RAM_helper = new RAM_helper(page,this.randomNumber);
                this.bp_List.put(page.getId(), RAM_helper);
            } else if (!this.bp_List.containsKey(page.getId())) {
                RAM_helper RAM_helper = new RAM_helper(page,this.bp_List.size());
                this.bp_List.put(page.getId(), RAM_helper);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pageList = file.deleteTuple(tid, t);  // return an List contains the pages that were modified
        for(Page page: pageList){
            page.markDirty(true, tid);
            if (!this.bp_List.containsKey(page.getId()) && this.bp_List.size() >= this.numPages) {
                this.evictPage();
                RAM_helper RAM_helper = new RAM_helper(page,this.randomNumber);
                this.bp_List.put(page.getId(), RAM_helper);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     * @throws DbException
     * @throws TransactionAbortedException
     */
    public synchronized void flushAllPages() throws IOException{
        // some code goes here
        // not necessary for lab1
        for (PageId pageid :this.bp_List.keySet()){
            Page page = bp_List.get(pageid).page;
            if (page.isDirty() != null){
                this.flushPage(pageid);
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache，so they can be reused safely
     * @throws DbException
     * @throws TransactionAbortedException
     */
    public synchronized void discardPage(PageId pid) throws TransactionAbortedException, DbException {
        // some code goes here
        // not necessary for lab
        if (pid == null) {
            return;
        }
        this.bp_List.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     * @throws DbException
     * @throws TransactionAbortedException
     */
    private synchronized  void flushPage(PageId pid) throws IOException{
        // some code goes here
        // not necessary for lab1

        Page dpage = bp_List.get(pid).page;
        if (this.bp_List.containsKey(pid)) {
            TransactionId dirty = dpage.isDirty();
            if (dpage.isDirty() != null) {
                Database.getLogFile().logWrite(dirty, dpage.getBeforeImage(), dpage);
                Database.getLogFile().force();
                DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                file.writePage(dpage);
                dpage.markDirty(false, null);
            }
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * @throws TransactionAbortedException
     * @throws IOException
     */
    private synchronized  void evictPage() throws DbException{
        // some code goes here
        // not necessary for lab1
        Random random = new Random();
        this.randomNumber = random.nextInt(numPages); // generates a random int between 0 and numpages-1

        PageId temp_pageid = null;
        for (PageId pageId :this.bp_List.keySet()){
            if(bp_List.get(pageId).RAM == randomNumber){
                temp_pageid = pageId;
            }
        }
        try{
            this.flushPage(temp_pageid);
            this.discardPage(temp_pageid);
        } catch (IOException | TransactionAbortedException e){
            throw new DbException("Page could not be flushed.");
        }

    }
}
