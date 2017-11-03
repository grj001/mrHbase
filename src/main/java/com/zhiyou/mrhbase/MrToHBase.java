package com.zhiyou.mrhbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

//wordCount 把结果保存到hbase中
public class MrToHBase {

	public static class MrToHBaseMap extends
	Mapper<LongWritable, Text, Text, IntWritable>{
		public static final IntWritable ONE = new IntWritable(1);
		
		private String[] infos;
		private Text outKey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			infos = value.toString().split("\\s");
			for(String word : infos){
				outKey.set(word);
				context.write(outKey, ONE);
			}
		}
		
	}
	
	//bd17:wc 列簇:c 列名称:count 用单词作为rowkey
	public static class MrToHBaseReduce extends TableReducer
	<Text, IntWritable, NullWritable>{
		
		private NullWritable outKey = NullWritable.get();
		private Put outValue;
		
		private int sum;

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer
				<Text, IntWritable, NullWritable, Mutation>
		.Context context)
				throws IOException, InterruptedException {
			sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			//构造put对象, 既往hbase里面插入一条数据的具体内容
			outValue = new Put(Bytes.toBytes(key.toString()));
			
			outValue.addColumn(
					Bytes.toBytes("c")
					, Bytes.toBytes("count")
					, Bytes.toBytes(sum+""));
			
			context.write(outKey, outValue);
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "wordCount, to hbase");
		job.setJarByClass(MrToHBase.class);
		
		job.setMapperClass(MrToHBaseMap.class);
//		job.setReducerClass(MrToHBaseReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		
//		job.setOutputKeyClass(NullWritable.class);
//		job.setOutputValueClass(Mutation.class);
		
		TableMapReduceUtil
		.initTableReducerJob("bd14:wc", MrToHBaseReduce.class, job);

		FileInputFormat.addInputPath(job, new Path("/user/user_info.txt"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
