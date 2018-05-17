package com.wr.mr.secondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by spark on 5/18/18.
 */
public class SecondSortDriver extends Configured implements Tool {

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "SecondSort for Year-Month-Temperature");

        job.setJarByClass(SecondSortDriver.class);
        job.setJobName("SecondSort for Year-Month-Temperature");

        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapOutputKeyClass(DateTemperaturePair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(SecondSortMapper.class);
        job.setReducerClass(SecondSortReducer.class);
        job.setPartitionerClass(SecondSortPartitioner.class);
        job.setGroupingComparatorClass(SecondSortGroupingComparator.class);

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;


    }

    public static void main(String[] args) throws Exception {

        if(args.length!=2){
            throw new IllegalArgumentException();
        }
        int ret= ToolRunner.run(new SecondSortDriver(),args);
        System.exit(ret);

    }
}
