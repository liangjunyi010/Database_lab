package simpledb.execution;

import java.io.IOException;
import simpledb.common.Type;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private boolean alreadyCalled = false;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.t = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // If the method has already been called, return null.
        if (alreadyCalled) {
            return null;
        }
        alreadyCalled = true; // Otherwise, mark that the method has been called.

        int numTuplesInserted = 0; // Counter for number of tuples inserted.

        // Retrieve buffer pool instance.
        BufferPool bufferPool = Database.getBufferPool();
        try{
            while (this.child.hasNext()) {
                Tuple tuple = this.child.next(); // Get the next tuple.
                bufferPool.insertTuple(this.t, this.tableId, tuple); // Insert the tuple into the buffer pool.
                numTuplesInserted++; // Increment the counter.
            }
        }catch(IOException ioe){
            throw new DbException("IOException: " + ioe.getMessage());
        }
        // Create a new tuple with a single field for the number of tuples inserted.
        Tuple returnTuple = new Tuple(this.getTupleDesc());
        returnTuple.setField(0, new IntField(numTuplesInserted));

        // Return the new tuple.
        return returnTuple;

    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}