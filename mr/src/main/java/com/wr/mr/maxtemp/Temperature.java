package com.wr.mr.maxtemp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by spark on 5/6/18.
 */
public class Temperature {

    private static final String INPUT_DIR="hdfs://master1:9000/datetemperature";
    private static final String OUTPUT_DIR="hdfs://master1:9000/output/datatemperature_out4";

    static class TempMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 打印样本: Before Mapper: 0, 2000010115
            System.out.print("Before Mapper: "+ key +", "+value);
            String line=value.toString();
            String year=line.substring(0,4);
            int temperature=Integer.parseInt(line.substring(8));
            context.write(new Text(year),new IntWritable(temperature));
            System.out.println("======" + "After Mapper:" + new Text(year) + ", " + new IntWritable(temperature));
        }
    }

    static class TempReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxValue=Integer.MIN_VALUE;
            StringBuffer sb=new StringBuffer();
            // get max
            for (IntWritable value:values){
                maxValue=Math.max(maxValue,value.get());
                sb.append(value).append(", ");
            }
            // 打印样本： Before Reduce: 2000, 15, 23, 99, 12, 22,
            System.out.print("Before Reduce: " + key + ", " + sb.toString());
            context.write(key,new IntWritable(maxValue));
            // 打印样本： After Reduce: 2000, 99
            System.out.println("======" + "After Reduce: " + key + ", " + maxValue);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();

        Job job=new Job(conf);
        job.setJobName("maxTemperatureOfYear");

        FileInputFormat.addInputPath(job, new Path(INPUT_DIR));
        FileOutputFormat.setOutputPath(job,new Path(OUTPUT_DIR));

        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
        System.out.println("Finished!");

    }

}
