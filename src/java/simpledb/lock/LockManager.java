package simpledb.lock;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


class ReadAndWriteLock {
    Set<TransactionId> lockusers;
    Map<TransactionId, Boolean> lockwaiters;
    boolean isWrite;
    private int readNum;
    private int writeNum;

    public ReadAndWriteLock() {
        this.isWrite = false;
        this.readNum = 0;
        this.writeNum = 0;
        this.lockusers = new HashSet<TransactionId>();
        this.lockwaiters = new HashMap<TransactionId, Boolean>();
    }

    public boolean holdBy(TransactionId tid) {
        return this.lockusers.contains(tid);
    }

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

/**
 * LockManager keeps track of which locks each transaction holds and checks to see if a lock should be granted to a
 * transaction when it is requested.
 */
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
            releaseLock(tid, ( pageId));
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