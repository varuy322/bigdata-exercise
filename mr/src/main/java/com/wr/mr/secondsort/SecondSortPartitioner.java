package com.wr.mr.secondsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by spark on 5/18/18.
 */
public class SecondSortPartitioner extends Partitioner<DateTemperaturePair,Text> {
    @Override
    public int getPartition(DateTemperaturePair dateTemperaturePair, Text text, int numPartitions) {
        return Math.abs(dateTemperaturePair.getYearMonth().hashCode()%numPartitions);
    }
}
