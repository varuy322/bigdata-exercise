package com.wr.mr.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by spark on 5/18/18.
 */
public class Join extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        if (args.length<2){
            new IllegalArgumentException("Usage: <input> <output>");
            return;
        }
        ToolRunner.run(new Join(),args);
        System.out.println("========done========");

    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, Join.class.getSimpleName());
        job.setJarByClass(Join.class);

        // 添加customer cache文件
        job.addCacheFile(URI.create(CUSTOMER_CACHE_URL));

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //map setting
        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(CusOrderMapOutKey.class);
        job.setMapOutputValueClass(Text.class);

        //reduce setting
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(CusOrderMapOutKey.class);
        job.setOutputValueClass(Text.class);

        boolean res = job.waitForCompletion(true);


        return res ? 0 : 1;
    }

    //customer file:hdfs
    //format: 客户编号/t姓名/t地址/t电话
    private static final String CUSTOMER_CACHE_URL = "hdfs://master1:9000/input/mr-join/customer";


    private static class CustomerBean {
        private int custId;
        private String name;
        private String address;
        private String phone;

        public CustomerBean() {}

        public CustomerBean(int custId, String name, String address, String phone) {
            super();
            this.custId = custId;
            this.name = name;
            this.address = address;
            this.phone = phone;
        }

        public int getCustId() {
            return custId;
        }

        public String getName() {
            return name;
        }

        public String getAddress() {
            return address;
        }

        public String getPhone() {
            return phone;
        }
    }


    private static class CusOrderMapOutKey implements WritableComparable<CusOrderMapOutKey> {

        private int custId;
        private int orderId;

        public void set(int custId, int orderId) {
            this.custId = custId;
            this.orderId = orderId;
        }

        public int getCustId() {
            return custId;
        }

        public int getOrderId() {
            return orderId;
        }

        @Override
        public int compareTo(CusOrderMapOutKey o) {
            int res = Integer.compare(custId, o.getCustId());
            return res == 0 ? Integer.compare(orderId, o.getOrderId()) : res;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CusOrderMapOutKey) {
                CusOrderMapOutKey o = (CusOrderMapOutKey) obj;
                return custId == o.getCustId() && orderId == o.getOrderId();
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return custId + "\t" + orderId;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(custId);
            out.writeInt(orderId);

        }

        @Override
        public void readFields(DataInput in) throws IOException {
            custId = in.readInt();
            orderId = in.readInt();
        }
    }


    private static class JoinMapper extends Mapper<LongWritable, Text, CusOrderMapOutKey, Text> {

        private final CusOrderMapOutKey outputKey = new CusOrderMapOutKey();
        private final Text outputValue = new Text();

        /**
         * 在内存中customer数据
         */
        private static final Map<Integer, CustomerBean> CUSTOMER_MAP = new HashMap<Integer, Join.CustomerBean>();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(URI.create(CUSTOMER_CACHE_URL), context.getConfiguration());
            FSDataInputStream fdis = fs.open(new Path(CUSTOMER_CACHE_URL));

            BufferedReader reader = new BufferedReader(new InputStreamReader(fdis));
            String line = null;
            String[] cols = null;

            while ((line = reader.readLine()) != null) {
                cols = line.split("\t");
                if (cols.length < 4) {
                    continue;
                }
                CustomerBean bean = new CustomerBean(Integer.parseInt(cols[0]), cols[1], cols[2], cols[3]);
                CUSTOMER_MAP.put(bean.getCustId(), bean);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 格式: 订单编号 客户编号    订单金额
            String[] cols = value.toString().split("\t");
            if (cols.length < 3) {
                return;
            }

            int custId = Integer.parseInt(cols[1]);
            CustomerBean customerBean = CUSTOMER_MAP.get(custId);

            if (customerBean == null) {
                return;
            }

            StringBuffer sb = new StringBuffer();
            sb.append(cols[2])
                    .append("\t")
                    .append(customerBean.getName())
                    .append("\t")
                    .append(customerBean.getAddress())
                    .append("\t")
                    .append(customerBean.getPhone());

            outputValue.set(sb.toString());
            outputKey.set(custId, Integer.parseInt(cols[0]));
            System.out.println(outputKey+"--"+outputValue);

            context.write(outputKey, outputValue);
        }
    }

    private static class JoinReducer extends Reducer<CusOrderMapOutKey, Text, CusOrderMapOutKey, Text> {
        @Override
        protected void reduce(CusOrderMapOutKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }


}
