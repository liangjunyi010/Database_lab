package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pagesize = BufferPool.getPageSize();
        int pagenumber = pid.getPageNumber();
        long offset = (long) pagesize * pagenumber;
        byte[] data = new byte[pagesize];
        
        try {
            RandomAccessFile randomaccessfile = new RandomAccessFile(f, "r");
            randomaccessfile.seek(offset);
            randomaccessfile.read(data);
            randomaccessfile.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

        // PageId pid = page.getId();
        // byte[] data = page.getPageData();
        // int pagesize = BufferPool.getPageSize();
        // int pagenumber = pid.getPageNumber();
        // long offset = (long) pagesize * pagenumber;
        // try{
        //     RandomAccessFile randomaccessfile = new RandomAccessFile(f, "rw");
        //     randomaccessfile.seek(offset);
        //     randomaccessfile.write(data);
        //     randomaccessfile.close();
        // }catch(IOException e){
        //     e.printStackTrace();
        // }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

// see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }


    public class HeapFileIterator implements DbFileIterator {

        TransactionId tid;
        int pageNumber;
        int tableId;
        int numPages;
        Iterator<Tuple> tuples;
        HeapPageId pid;

        public HeapFileIterator(TransactionId tid, int tableId, int numPages) {
            this.tid = tid;
            this.tableId = tableId;
            this.numPages = numPages;
        }

        private Iterator<Tuple> getTuples(int pageNumber) throws DbException, TransactionAbortedException {
            pid = new HeapPageId(this.tableId, pageNumber);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return heapPage.iterator();
        }

        public void open() throws DbException, TransactionAbortedException {
            pageNumber = 0;
            tuples = getTuples(pageNumber);
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            // If there are no tuples
            if(tuples == null)
                return false;
            // Check if tuple has next
            if(tuples.hasNext())
                return true;
            // Check if all pages are iterated
            if(pageNumber + 1 >= numPages)
                return false;
            // Else check if there is next page
            // If Page is exhausted get new page tuples
            while(pageNumber + 1 < numPages && !tuples.hasNext()){
                // Get tuples of next page
                pageNumber++;
                tuples = getTuples(pageNumber);
            }
            return this.hasNext();
        }

        public Tuple next() throws DbException, NoSuchElementException {
            if(tuples == null) 
                throw new NoSuchElementException();
            try {
                if(this.hasNext()){
                    return tuples.next();
                }
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            }
            return null;
        }

        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
            tuples = null;
            pid = null;
        }

    }
    
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this.getId(), this.numPages());
    }
}