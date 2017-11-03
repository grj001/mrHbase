package com.zhiyou.mrhbase20171103;

import java.io.IOException;
import java.nio.ByteBuffer;
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

//订单row设计: 
// customId(4 bytes)+日期反向(8)+order(4)
public class OrderEtl {
	
	public static Connection connection;
	public static Admin admin;
	
	
	public OrderEtl() throws IOException{
		connection = ConnectionFactory.createConnection();
		admin = connection.getAdmin();
	}
	
	
	
	
	
	
	
	public byte[] getOrderRowKey(
			int customId
			, Date date
			, int orderId
			){
		
		ByteBuffer result = ByteBuffer.allocate(16);
		result.put(Bytes.toBytes(customId));
		result.put(Bytes.toBytes(Long.MAX_VALUE - date.getTime()));
		result.put(Bytes.toBytes(orderId));
		
		return result.array();
	}
	
	
	public static class OrderEtlMap extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println(key);
		}
	}
	
	public static class OrderEtlReduce 
	extends TableReducer<Text, Text, NullWritable>{

		@Override
		protected void reduce(
				Text key
				, Iterable<Text> values
				, Reducer<Text, Text, NullWritable, Mutation>
				.Context context)
				throws IOException, InterruptedException {
			
		}
	}
	
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "orderInfo, 字节");
		
		job.setMapperClass(OrderEtlMap.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		
		//判读表是否存在
		OrderEtl orderEtl = new OrderEtl();
		
		
		TableName tName = TableName.valueOf("orderdata:orders");
		HTableDescriptor hDescriptor = new HTableDescriptor(tName);
		if(!admin.tableExists(tName)){
			System.out.println("表"+tName+"不存在");
			HColumnDescriptor family = new HColumnDescriptor("i");
			hDescriptor.addFamily(family);
			admin.createTable(hDescriptor);
			System.out.println("表"+tName+"不存在创建成功");
		}else if(admin.tableExists(tName)){
			System.out.println("表"+tName+"已存在");
			admin.truncateTable(tName, false);
		}
		
		
		
		
		
		TableMapReduceUtil
		.initTableReducerJob(
				"orderdata:orders", OrderEtlReduce.class, job);
		
		FileInputFormat.addInputPath(job, new Path("/user/orderdata/orders"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
