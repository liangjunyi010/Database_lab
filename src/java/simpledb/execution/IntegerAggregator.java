package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op aggOperator;
    private HashMap<Field, Integer> groupCounts;
    private HashMap<Field, Integer> groupAggregates;
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.aggOperator = what;
        this.groupCounts = new HashMap<>();
        this.groupAggregates = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupByField;
        if (this.gbfield == Aggregator.NO_GROUPING){
            groupByField = null;
        }
        else{
            groupByField = tup.getField(this.gbfield);
        }
        int currentCount = this.groupCounts.getOrDefault(groupByField, 0)+1;

        this.groupCounts.put(groupByField,currentCount);

        IntField tupleAggregate = (IntField)tup.getField((this.afield));
        Integer defaultValue;
        if (this.aggOperator == Op.MIN || this.aggOperator == Op.MAX) {
            defaultValue = tupleAggregate.getValue();
        } else {
            defaultValue = 0;
        }
        Integer currentAggregateValue = this.groupAggregates.getOrDefault(groupByField, defaultValue);
        Integer updatedAggregateValue = currentAggregateValue;

        switch (this.aggOperator) {
            case AVG:
                updatedAggregateValue = currentAggregateValue + tupleAggregate.getValue();
                break;
            case MAX:
                updatedAggregateValue = Math.max(currentAggregateValue, tupleAggregate.getValue());
                break;
            case MIN:
                updatedAggregateValue = Math.min(currentAggregateValue, tupleAggregate.getValue());
                break;
            case SUM:
                updatedAggregateValue = currentAggregateValue + tupleAggregate.getValue();
                break;
            case COUNT:
                // This case is handled by groupCount
                break;
            default:
                break;
        }
        this.groupAggregates.put(groupByField,updatedAggregateValue);

    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc groupAggregateTd;
        ArrayList<Tuple> tuples = new ArrayList<>();
        boolean hasGrouping = (this.gbfield != Aggregator.NO_GROUPING);

        if (hasGrouping) {
            groupAggregateTd = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
        } else {
            groupAggregateTd = new TupleDesc(new Type[]{Type.INT_TYPE});
        }

        for (Map.Entry<Field, Integer> groupAggregateEntry: this.groupAggregates.entrySet()) {
            Tuple groupAggregateTuple = new Tuple(groupAggregateTd);
            Integer finalAggregateValue;
            int totalCount = groupCounts.get(groupAggregateEntry.getKey());
            int aggregateValue = groupAggregateEntry.getValue();
            switch (this.aggOperator){
                case AVG:
                    finalAggregateValue = aggregateValue / totalCount;
                    break;
                case COUNT:
                    finalAggregateValue = totalCount;
                    break;
                default:
                    finalAggregateValue = aggregateValue;
                    break;
            }

            // If there is a grouping, we return a tuple in the form {groupByField, aggregateVal}
            // If there is no grouping, we return a tuple in the form {aggregateVal}
            if (hasGrouping) {
                groupAggregateTuple.setField(0, groupAggregateEntry.getKey());
                groupAggregateTuple.setField(1, new IntField(finalAggregateValue));
            } else {
                groupAggregateTuple.setField(0, new IntField(finalAggregateValue));
            }
            tuples.add(groupAggregateTuple);
        }
        return new TupleIterator(groupAggregateTd, tuples);
    }


}
