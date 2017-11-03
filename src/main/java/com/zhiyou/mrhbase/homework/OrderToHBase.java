package com.zhiyou.mrhbase.homework;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class OrderToHBase {
	public static class OrderToHBaseMap 
	extends Mapper<LongWritable, Text, Text, Text>{
		
		private String[] infos;
		private Text outKey = new Text();
		private Text outValue = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			infos = value.toString().split("\\|");
			outKey.set(infos[0]);
			outValue.set(value);
			context.write(outKey, outValue);
		}
	}
	
	
	public static class OrderToHBaseReduce extends TableReducer<Text, Text, NullWritable>{
		
		private NullWritable outKey = NullWritable.get();
		private Put outValue;
		private String[] infos;
		@Override
		protected void reduce(
				Text key
				, Iterable<Text> values
				, Reducer<
				Text
				, Text
				, NullWritable
				, Mutation>.Context context)
				throws IOException, InterruptedException {
			
			for(Text value : values){
				
				infos = value.toString().split("\\|");
				
				outValue = new Put(Bytes.toBytes(key.toString()));
				outValue.addColumn(Bytes.toBytes("i"), Bytes.toBytes("order_id"), Bytes.toBytes(infos[0]));
				outValue.addColumn(Bytes.toBytes("i"), Bytes.toBytes("order_date"), Bytes.toBytes(infos[1]));
				outValue.addColumn(Bytes.toBytes("i"), Bytes.toBytes("order_customer_id"), Bytes.toBytes(infos[2]));
				outValue.addColumn(Bytes.toBytes("i"), Bytes.toBytes("order_status"), Bytes.toBytes(infos[3]));
				
//				System.out.println("reduce");
//				System.out.println("\t"+infos[0]);
				context.write(outKey, outValue);
			}
		}
	}
	
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "统计orders到HBase");
		job.setJarByClass(OrderToHBase.class);
		
		job.setMapperClass(OrderToHBaseMap.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		TableMapReduceUtil
		.initTableReducerJob(
				"bd14:orders"
				, OrderToHBaseReduce.class
				, job);
		
		FileInputFormat
		.addInputPath(job, new Path("/user/orderdata/orders"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
