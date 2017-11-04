package com.zhiyou.mrhbase20171103;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

//订单row设计: 
// customId(4 bytes)+日期反向(8)+order(4)
public class OrderEtl {
	
	public static Connection connection;
	public static Admin admin;
	
	
	public OrderEtl() throws IOException{
		connection = ConnectionFactory.createConnection();
		admin = connection.getAdmin();
	}
	
	
	
	public static  byte[] getOrderRowKey(
			int customerId
			, Date date
			, int orderId
			){
		
		ByteBuffer result = ByteBuffer.allocate(16);
		result.put(Bytes.toBytes(customerId));
		result.put(Bytes.toBytes(Long.MAX_VALUE - date.getTime()));
		result.put(Bytes.toBytes(orderId));
		
		return result.array();
	}
	
	
	public static class OrderEtlMap extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		public final NullWritable NULL = NullWritable.get();
		
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(value, NULL);
		}
	}
	
	
	
	
	
	//create 'bd14:order', 'i'
	public static class OrderEtlReduce 
	extends TableReducer<Text, NullWritable, NullWritable>{

		private String[] infos;
		private Put outValue;
		public final NullWritable NULL = NullWritable.get();
		
		private int orderId;
		private int customerId;
		private Date orderDate;
		
		private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		@Override
		protected void reduce(
				Text key
				, Iterable<NullWritable> values
				, Reducer<Text, NullWritable, NullWritable, Mutation>
				.Context context)
				throws IOException, InterruptedException {
			
			infos = key.toString().split("\\|");
			
			try {
				orderId = Integer.valueOf(infos[0]);
				orderDate = dateFormat.parse(infos[1]);
				customerId = Integer.valueOf(infos[2]);
				outValue = new Put(getOrderRowKey(customerId, orderDate, orderId));
				
				outValue.addColumn(Bytes.toBytes("i"), Bytes.toBytes("status"), Bytes.toBytes(infos[3]));
				outValue.addColumn(Bytes.toBytes("i"), Bytes.toBytes("date"), Bytes.toBytes(infos[1]));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			context.write(NULL, outValue);
		}
	}
	
	
	
	
	
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "orderInfo, 字节");
		
		job.setMapperClass(OrderEtlMap.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		
		//判读表是否存在
		orTableExists();
		
		
		TableMapReduceUtil
		.initTableReducerJob(
				"orderdata:orders", OrderEtlReduce.class, job);
		
		FileInputFormat.addInputPath(job, new Path("/user/orderdata/orders"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
	//判读表是否存在
	public static void orTableExists() throws IOException{
		
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
			admin.disableTable(tName);
			admin.truncateTable(tName, false);
		}
	}
	
	
	
	
	
	
	
	
	
	
}
