package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;


    private int gbfield;
    private Type gbfieldtype;
    private int afielf;
    private Op aggOperator;
    private HashMap<Field, Integer> groupCounts;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) throw new IllegalArgumentException();
        this.gbfield = gbfield;
        this.afielf = afield;
        this.gbfieldtype = gbfieldtype;
        this.aggOperator = what;
        this.groupCounts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupByField;
        if (this.gbfield == Aggregator.NO_GROUPING) {
            groupByField = null;
        } else {
            groupByField = tup.getField(this.gbfield);
        }
        int currentCount = this.groupCounts.getOrDefault(groupByField, 0) + 1;
        this.groupCounts.put(groupByField, currentCount);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc tupleDesc;
        ArrayList<Tuple> tuples = new ArrayList<>();
        boolean hasGrouping = (this.gbfield != Aggregator.NO_GROUPING);
        if (hasGrouping) {
            tupleDesc = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
        } else {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        }

        for (Map.Entry<Field, Integer> entry : this.groupCounts.entrySet()) {
            Tuple tuple = new Tuple(tupleDesc);

            if (hasGrouping) {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
            } else {
                tuple.setField(0, new IntField(entry.getValue()));
            }
            tuples.add(tuple);
        }
        if (tuples.isEmpty()) {
            return new TupleIterator(tupleDesc, new ArrayList<Tuple>());
        } else {
            return new TupleIterator(tupleDesc, tuples);
        }
    }
}
