package com.wr.mr.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by spark on 5/18/18.
 */
public class SecondSortGroupingComparator extends WritableComparator {

    public SecondSortGroupingComparator() {
        super(DateTemperaturePair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DateTemperaturePair pair1 = (DateTemperaturePair) a;
        DateTemperaturePair pair2 = (DateTemperaturePair) b;

        return pair1.getYearMonth().compareTo(pair2.getYearMonth());
    }
}
