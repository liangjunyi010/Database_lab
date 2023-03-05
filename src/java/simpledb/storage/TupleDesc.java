package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        
        public boolean equals(Object o) {
            // some code goes here
            if (o == this) {
                return true;
            }
            if (!(o instanceof TDItem)) {
                return false;
            }
            TDItem other = (TDItem) o;
            if (this.fieldName == other.fieldName & (this.fieldType == other.fieldType)){
                return true;
            }
            return false;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

    }

    /**
     * a list used to store TDItem
     */
    public List<TDItem> tditem_list = new ArrayList<TDItem>();


    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        Iterator<TDItem> it = this.tditem_list.iterator();
        return it;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.tditem_list = new ArrayList<TDItem>(); // Initialize tditem_list
        // some code goes here
        for(int i=0;i<fieldAr.length;i++){
            TDItem tdItem = new TDItem(typeAr[i],fieldAr[i]);
            this.tditem_list.add(tdItem);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this.tditem_list = new ArrayList<TDItem>(); // Initialize tditem_list
        // some code goes here
        for(int i=0;i<typeAr.length;i++){
            TDItem tdItem = new TDItem(typeAr[i],null);
            this.tditem_list.add(tdItem);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tditem_list.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return this.tditem_list.get(i).fieldName;
    }


    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return this.tditem_list.get(i).fieldType;
    }



    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        int flag = 0;
        for(int i = 0;i<this.tditem_list.size();i++){
            if(this.tditem_list.get(i).fieldName == null){
                flag = 1;
            }
        }
        if (flag == 1){
            throw new NoSuchElementException();
        }
        for(int i = 0;i<this.tditem_list.size();i++){
            if(this.tditem_list.get(i).fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem item : tditem_list) {
            size += item.fieldType.getLen();
        }
//        return 0;
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int newNumFields = td1.numFields() + td2.numFields();
        Type[] newTypes = new Type[newNumFields];
        String[] newNames = new String[newNumFields];

        for (int i = 0; i < td1.numFields(); i++) {
            newTypes[i] = td1.getFieldType(i);
            newNames[i] = td1.getFieldName(i);
        }

        for (int i = 0; i < td2.numFields(); i++) {
            newTypes[td1.numFields() + i] = td2.getFieldType(i);
            newNames[td1.numFields() + i] = td2.getFieldName(i);
        }

        TupleDesc newTd = new TupleDesc(newTypes, newNames);
        return newTd;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == this) {
            return true;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc other = (TupleDesc) o;
        if (this.tditem_list.size() != other.tditem_list.size()) {
            return false;
        }
        for (int i = 0; i < this.tditem_list.size(); i++) {
            if (!this.tditem_list.get(i).equals(other.tditem_list.get(i))) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tditem_list.size(); i++) {
            sb.append(tditem_list.get(i).fieldType);
            sb.append("(");
            sb.append(tditem_list.get(i).fieldName);
            sb.append(")");
            if (i != tditem_list.size() - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }
}
