package simpledb.lock;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**

 ReadAndWriteLock class provides a simple implementation of a read-write lock.
 It allows multiple transactions to read a shared resource at the same time
 while ensuring that only one transaction has the ability to write or modify the resource
 at any given time.
 This class keeps track of the set of transactions that hold the lock,
 the number of readers, and the number of writers that are waiting to acquire the lock.
 The implementation of readLock() and writeLock() methods guarantees fairness
 and prevents starvation by using a synchronized wait-notify mechanism.
 This class is used by LockManager to control access to database pages in a concurrent manner.
 */
class ReadAndWriteLock {
    Set<TransactionId> lockusers;
    Map<TransactionId, Boolean> lockwaiters;
    boolean isWrite;
    private int readNum;
    private int writeNum;

    /**
     Creates a new instance of ReadAndWriteLock with initial values for its fields:
     isWrite is set to false, readNum and writeNum are set to 0,
     lockusers is initialized to a new HashSet of TransactionId objects, and
     lockwaiters is initialized to a new HashMap with TransactionId keys and Boolean values.
     */
    public ReadAndWriteLock() {
        this.isWrite = false;
        this.readNum = 0;
        this.writeNum = 0;
        this.lockusers = new HashSet<TransactionId>();
        this.lockwaiters = new HashMap<TransactionId, Boolean>();
    }

    /**
     Returns true if the given transaction currently holds the lock.
     @param tid the transaction ID to check if it holds the lock
     @return true if the transaction holds the lock, false otherwise
     */
    public boolean holdBy(TransactionId tid) {
        return this.lockusers.contains(tid);
    }

    /**
     Acquires a read lock on this ReadAndWriteLock instance for the specified transaction id.
     If the transaction already holds the read lock, or a write lock is held by another transaction,
     the method returns immediately. Otherwise, the method waits until there are no write locks
     held by other transactions before granting the read lock to the transaction.
     @param tid the transaction id that is requesting the read lock
     @throws InterruptedException if the current thread is interrupted while waiting for the read lock
     */
    public void readLock(TransactionId tid) throws InterruptedException {
        if (this.lockusers.contains(tid) & !this.isWrite) {
            return;
        }
        this.lockwaiters.put(tid, false);
        synchronized (this) {
                while (this.writeNum > 0) {
                    this.wait();
                }
                this.isWrite = false;
                this.readNum+=1;
                this.lockusers.add(tid);

        }
        this.lockwaiters.remove(tid);
    }

    /**
     Acquires the write lock if it's available, otherwise waits until it's available.
     If the calling transaction already holds the write lock, returns immediately.
     If the calling transaction already holds a read lock, this method releases the read lock
     and waits until all other read locks have been released before acquiring the write lock.
     @param tid the ID of the transaction requesting the write lock.
     @throws InterruptedException if the thread is interrupted while waiting for the lock.
     */
    public void writeLock(TransactionId tid) throws InterruptedException {
        if (this.lockusers.contains(tid) & this.isWrite) {
            return;
        }
        if(this.lockwaiters.containsKey(tid)) {
            if (this.lockwaiters.get(tid)) {
                return;
            }
        }
        this.lockwaiters.put(tid, true);
        synchronized (this) {
            if (this.lockusers.contains(tid)) {
                while (this.lockusers.size() > 1) {
                    this.wait();
                }
                unlock(tid);
            }
            while (this.readNum != 0 | this.writeNum != 0) {
                this.wait();
            }
            this.isWrite = true;
            this.writeNum+=1;
            this.lockusers.add(tid);
        }
        this.lockwaiters.remove(tid);
    }
    /**

     Releases the lock held by the specified transaction, allowing other transactions to acquire the lock.
     If the lock is held in write mode, the transaction must hold the write lock to release it.
     If the lock is held in read mode, the transaction must hold a read lock to release it.
     If the specified transaction does not hold the lock, this method does nothing.
     After releasing the lock, this method notifies all waiting threads that the lock is available.
     @param tid the ID of the transaction releasing the lock
     */
    public void unlock(TransactionId tid) {
        if (isWrite){
            if (!lockusers.contains(tid)) {
                return;
            }
            synchronized (this) {
                writeNum-=1;
                lockusers.remove(tid);
                notifyAll();
            }
        }
        else {
            if (!lockusers.contains(tid)) {
                return;
            }
            synchronized (this) {
                readNum-=1;
                lockusers.remove(tid);
                notifyAll();
            }
        }
    }

}

public class LockManager {
    Map<PageId, ReadAndWriteLock> pageid_locks;
    public Map<TransactionId, Set<PageId>> tid_pageids;
    Map<TransactionId, Set<TransactionId>> deadLockGraph;

    public LockManager() {
        this.pageid_locks = new HashMap<PageId, ReadAndWriteLock>();
        this.tid_pageids = new HashMap<TransactionId, Set<PageId>>();
        this.deadLockGraph = new HashMap<TransactionId, Set<TransactionId>>();
    }

    public void acquireReadLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        ReadAndWriteLock lock;
        synchronized (this) {
            if (!pageid_locks.containsKey(pid)) {
                pageid_locks.put(pid, new ReadAndWriteLock());
            }
            lock = pageid_locks.get(pid);
            if (lock.holdBy(tid)) {
                return;
            }
            if (lock.isWrite) {
                deadLockGraph.put(tid, lock.lockusers);
                if (hasDeadLock(tid)) {
                    deadLockGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
        }
        try {
            lock.readLock(tid);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        synchronized (this) {
            deadLockGraph.remove(tid);
            if (!tid_pageids.containsKey(tid)) {
                tid_pageids.put(tid, new HashSet<PageId>());
            }
            tid_pageids.get(tid).add(pid);
        }
    }

    public void acquireWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        ReadAndWriteLock lock;
        synchronized (this) {
            if (!pageid_locks.containsKey(pid)) {
                pageid_locks.put(pid, new ReadAndWriteLock());
            }
            lock = pageid_locks.get(pid);
            if (!lock.lockusers.isEmpty()){
                deadLockGraph.put(tid, lock.lockusers);
                if (hasDeadLock(tid)) {
                    deadLockGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
        }
        try {
            lock.writeLock(tid);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        synchronized (this) {
            deadLockGraph.remove(tid);
            if (!tid_pageids.containsKey(tid)) {
                tid_pageids.put(tid, new HashSet<PageId>());
            }
            tid_pageids.get(tid).add(pid);
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (!pageid_locks.containsKey(pid)) {
            return;
        }
        ReadAndWriteLock lock = pageid_locks.get(pid);
        tid_pageids.get(tid).remove(pid);
        lock.unlock(tid);
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        if (!tid_pageids.containsKey(tid)) {
            return;
        }
        Set<PageId> pages = tid_pageids.get(tid);
        PageId[] page_array = new PageId[pages.size()];
        pages.toArray(page_array);
        for (PageId pageId: page_array) {
            releaseLock(tid, pageId);
        }
        tid_pageids.remove(tid);
    }

    private boolean hasDeadLock(TransactionId tid) {
        Set<TransactionId> visited = new HashSet<TransactionId>();
        return dfs(tid, visited);
    }
    private boolean dfs(TransactionId curr, Set<TransactionId> visited) {
        visited.add(curr);
        if (!deadLockGraph.containsKey(curr)) {
            return false;
        }
        for (TransactionId adj : deadLockGraph.get(curr)) {
            if (adj.equals(curr)) continue;
            if (visited.contains(adj)) {
                return true;
            }
            if (dfs(adj, visited)) {
                return true;
            }
        }
        visited.remove(curr);
        return false;
    }
}