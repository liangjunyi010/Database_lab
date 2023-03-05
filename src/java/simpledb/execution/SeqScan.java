package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleDesc.TDItem;
import simpledb.common.Catalog;
import simpledb.common.Catalog.Help_key;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    public TransactionId tid;
    public int tableid;
    public String tableAlias;
    private DbFileIterator dbfileiterator;

    /**
     * a list used to store TDItem
     */
    public List<TDItem> tditem_list = new ArrayList<TDItem>();

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
	// some code goes here
    for (Help_key key :Catalog.catalog_List.keySet()) {
        if (key.table_id == this.tableid){
            return Catalog.catalog_List.get(key).table_name;
        }
    }
    throw new NoSuchElementException();
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableAlias = tableAlias;
        this.tableid = tableid;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        for (Help_key key :Catalog.catalog_List.keySet()){
            if(key.table_id==this.tableid){
                this.dbfileiterator =  Catalog.catalog_List.get(key).table_file.iterator(this.tid);
                dbfileiterator.open();
                break;
            }
        }
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        for (Help_key key :Catalog.catalog_List.keySet()) {
            if (key.table_id == this.tableid){
                TupleDesc old_tupledesc =  Catalog.catalog_List.get(key).table_file.getTupleDesc();
                Type[] typeAr = new Type[old_tupledesc.numFields()];
                String[] fieldAr = new String[old_tupledesc.numFields()];
                for (int i = 0; i < old_tupledesc.numFields(); i++){
                    fieldAr[i] = this.tableAlias+'.'+ old_tupledesc.getFieldName(i);
                    typeAr[i] = old_tupledesc.getFieldType(i);
                }
                TupleDesc new_TupleDesc = new TupleDesc(typeAr,fieldAr);
                return new_TupleDesc;
            }
        }
        throw new NoSuchElementException();
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return this.dbfileiterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here

        return this.dbfileiterator.next();
    }

    public void close() {
        // some code goes here
        this.dbfileiterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.dbfileiterator.rewind();
    }
}
