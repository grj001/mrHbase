package com.zhiyou.mrhbase20171103.homework;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
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


public class OrderFirstTable {

	public static Connection connection;
	public static Admin admin;
	
	
	public OrderFirstTable() throws IOException{
		connection = ConnectionFactory.createConnection();
		admin = connection.getAdmin();
	}
	
	
	
	public static  byte[] getOrderRowKey(
			int order_item_id
			, int order_id
			, int product_id
			){
		
		ByteBuffer result = ByteBuffer.allocate(16);
		result.put(Bytes.toBytes(order_item_id));
		result.put(Bytes.toBytes(order_id));
		result.put(Bytes.toBytes(product_id));
		
		return result.array();
	}
	
	
	public static class OrderFirstTableMap extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		public final NullWritable NULL = NullWritable.get();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(value, NULL);
		}
	}
	
	
	
	
	
	//create 'bd14:order', 'i'
	public static class OrderFirstTableReduce 
	extends TableReducer<Text, NullWritable, NullWritable>{

		private String[] infos;
		private Put outValue;
		public final NullWritable NULL = NullWritable.get();
		
		private int order_item_id;
		private int order_id;
		private int product_id;
		
		@Override
		protected void reduce(
				Text key
				, Iterable<NullWritable> values
				, Reducer<Text, NullWritable, NullWritable, Mutation>
				.Context context)
				throws IOException, InterruptedException {
			
			infos = key.toString().split("\\|");
			
			order_item_id = Integer.valueOf(infos[0]);
			order_id = Integer.valueOf(infos[1]);
			product_id = Integer.valueOf(infos[2]);
			outValue = new Put(getOrderRowKey(order_item_id, order_id, product_id));
			
			outValue.addColumn(Bytes.toBytes("i"), Bytes.toBytes("quantity"), Bytes.toBytes(infos[3]));
			outValue.addColumn(Bytes.toBytes("i"), Bytes.toBytes("subtotal"), Bytes.toBytes(infos[4]));
			outValue.addColumn(Bytes.toBytes("i"), Bytes.toBytes("price"), Bytes.toBytes(infos[5]));
			
			context.write(NULL, outValue);
		}
	}
	
	
	
	
	
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "order_items_first");
		job.setJarByClass(OrderFirstTable.class);
		
		job.setMapperClass(OrderFirstTableMap.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		
		//判读表是否存在
		orTableExists();
		
		
		TableMapReduceUtil
		.initTableReducerJob(
				"orderdata:order_items", OrderFirstTableReduce.class, job);
		
		FileInputFormat.addInputPath(job, new Path("/user/orderdata/order_items"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
	//判读表是否存在
	public static void orTableExists() throws IOException{
		
		OrderFirstTable orderFirstTable = new OrderFirstTable();
		
		TableName tName = TableName.valueOf("orderdata:order_items");
		HTableDescriptor hDescriptor = new HTableDescriptor(tName);
		if(!admin.tableExists(tName)){
			System.out.println("表"+tName+"不存在");
			HColumnDescriptor family = new HColumnDescriptor("i");
			hDescriptor.addFamily(family);
			admin.createTable(hDescriptor);
			System.out.println("表"+tName+"不存在创建成功");
		}else if(admin.tableExists(tName)){
			System.out.println("表"+tName+"已存在");
			admin.disableTable(tName);
			admin.truncateTable(tName, false);
		}
	}
	
	
}
