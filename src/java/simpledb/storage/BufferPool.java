//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package simpledb.storage;


import simpledb.common.Database;
import simpledb.common.Permissions;

import simpledb.common.DbException;
import simpledb.lock.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;


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


    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private Map<PageId, Page> bp_list;


    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.bp_list = new HashMap<>(numPages);
        this.lockManager = new LockManager();
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
        pageSize = DEFAULT_PAGE_SIZE;
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException {
        if (perm == Permissions.READ_WRITE) {
            this.lockManager.acquireWriteLock(tid, pid);
        } else {
            if (perm != Permissions.READ_ONLY) {
                throw new DbException("Invalid permission requested.");
            }
            this.lockManager.acquireReadLock(tid, pid);
        }
            if (this.bp_list.containsKey(pid)) {
                Page newpage = this.bp_list.get(pid);
                this.discardPage(pid);
                this.bp_list.put(pid, newpage);
                return newpage;
            } else {
                Page newpage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                if (this.bp_list.size() >= this.numPages) {
                    this.evictPage();
                    if (perm == Permissions.READ_WRITE) {
                        newpage.markDirty(true, tid);
                    }
                    this.bp_list.put(pid, newpage);
                    return newpage;
                }else{
                    if (perm == Permissions.READ_WRITE) {
                        newpage.markDirty(true, tid);
                    }
                    this.bp_list.put(pid, newpage);
                    return newpage;
                }


            }
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
        this.lockManager.releaseLock(tid, pid);
    }

    public void transactionComplete(TransactionId tid) {
        this.transactionComplete(tid, true);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        boolean result = false;
        if(this.lockManager.tid_pageids.containsKey(tid)){
            if(this.lockManager.tid_pageids.get(tid).contains(pid)){
                result = true;
            }
        }
        return result;
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
        Set<PageId> pages;
        if (this.lockManager.tid_pageids.containsKey(tid)) {
             pages = lockManager.tid_pageids.get(tid);
        }
        else {
            pages = null;
        }
        if (pages == null || pages.isEmpty()) {
            // The transaction does not hold any pages, so there is nothing to do.
            return;
        }
        // Flush or discard the pages based on the commit parameter.
        if (commit) {
            for (PageId page : pages) {
                try {
                    flushPage(page);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            for (PageId page : pages) {
                this.discardPage(page);
            }
        }
        // Release all locks held by the transaction.
        lockManager.releaseAllLocks(tid);
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
    public void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pageArray = file.insertTuple(tid, t);
        for (Page pg : pageArray) {
            pg.markDirty(true, tid);
            if (!this.bp_list.containsKey(pg.getId()) && this.bp_list.size() >= this.numPages) {
                this.evictPage();
                this.discardPage(pg.getId());
                this.bp_list.put(pg.getId(), pg);
            }
            else if(!this.bp_list.containsKey(pg.getId())){
                this.discardPage(pg.getId());
                this.bp_list.put(pg.getId(), pg);
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
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pageArray = file.deleteTuple(tid, t);

        for (Page pg : pageArray) {
            pg.markDirty(true, tid);
            if (!this.bp_list.containsKey(pg.getId()) && this.bp_list.size() >= this.numPages) {
                this.evictPage();
            }
            this.discardPage(pg.getId());
            this.bp_list.put(pg.getId(), pg);
        }

    }
    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     * @throws DbException
     * @throws TransactionAbortedException
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pageid :this.bp_list.keySet()){
            Page page = bp_list.get(pageid);
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
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab
        if (pid != null) {
            this.bp_list.remove(pid);
        }
    }
    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     * @throws DbException
     * @throws TransactionAbortedException
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page pg = this.bp_list.get(pid);
        if (this.bp_list.containsKey(pid)) {
            TransactionId dirty = pg.isDirty();
            if (dirty != null) {
                Database.getLogFile().logWrite(dirty, pg.getBeforeImage(), pg);
                Database.getLogFile().force();
                DbFile hpFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                hpFile.writePage(pg);
                pg.markDirty(false, null);
            }
        }

    }
    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }
    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * @throws TransactionAbortedException
     * @throws IOException
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        Page lruPage = null;

        for (Page page: this.bp_list.values()){
            if(page.isDirty()== null){
                lruPage = page;
            }
        }

        if (lruPage == null) {
            throw new DbException("There are no pages to evict in the buffer pool.");
        } else {
            try {
                this.flushPage(lruPage.getId());
                this.discardPage(lruPage.getId());
            } catch (IOException e) {
                throw new DbException("Page could not be flushed.");
            }
        }
    }
}
