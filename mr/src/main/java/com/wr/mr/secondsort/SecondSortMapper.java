package com.wr.mr.secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by spark on 5/18/18.
 */
public class SecondSortMapper extends Mapper<LongWritable ,Text,DateTemperaturePair,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens=value.toString().split(",");
        String yearMonth=tokens[0]+"-"+tokens[1];
        String day=tokens[2];
        int temperature=Integer.parseInt(tokens[3]);

        DateTemperaturePair comboKey=new DateTemperaturePair();
        comboKey.setYearMonth(yearMonth);
        comboKey.setDay(day);
        comboKey.setTemperature(temperature);

        context.write(comboKey,new IntWritable(temperature));

    }
}