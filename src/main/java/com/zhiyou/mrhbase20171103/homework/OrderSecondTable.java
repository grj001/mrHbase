package com.zhiyou.mrhbase20171103.homework;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;


public class OrderSecondTable {

	
	public static Connection connection;
	public static Admin admin;
	
	
	public OrderSecondTable() throws IOException{
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
	
	
	public static class OrderSecondTableMap 
	extends TableMapper<Text, Text>{
		private Text outKey = new Text();
		private Text outValue = new Text();

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<
				ImmutableBytesWritable, Result, Text, Text>
				.Context context)
				throws IOException, InterruptedException {
			
			byte[] rowKey = value.getRow();
			byte[] subtotal = value.getValue(Bytes.toBytes("i"), Bytes.toBytes("subtotal"));
			
			int order_item_id = Bytes.toInt(rowKey,0,4);
			int order_id = Bytes.toInt(rowKey,4,4);
			int product_id = Bytes.toInt(rowKey,8,4);
			
			outKey.set(subtotal);
			this.outValue.set(order_item_id+","+order_id+","+product_id);
			
			// key:subtotal  value:rowKey
			context.write(outKey, this.outValue);
		}
	}
	
	
	
	
	
	//create 'bd14:order', 'i'
	public static class OrderSecondTableReduce 
	extends TableReducer<Text, Text, NullWritable>{
		private Put outValue;
		private String[] infos;
		private NullWritable NULL = NullWritable.get();
		private int sum;
		
		@Override
		protected void reduce(
				Text key //subtotal
				, Iterable<Text> values //Iterable<rowKey>
				, Reducer<Text, Text, NullWritable, Mutation>
				.Context context)
				throws IOException, InterruptedException {
			
			sum = 1;
			
			outValue = 
					new Put(
							Bytes.toBytes("subtotal:"+key.toString())
						);
			System.out.println(key.toString());
			for(Text value : values){
				infos = value.toString().split(",");
				System.out.println("\tvalue:"+value);
				
				
				outValue.addColumn(
						Bytes.toBytes("i")
						, Bytes.toBytes("key"+(sum++))
						, getOrderRowKey(
										Integer.valueOf(infos[0])
										, Integer.valueOf(infos[1])
										, Integer.valueOf(infos[2]))
							);
			}
			context.write(NULL, outValue);
		}
	}
	
	
	
	
	
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "order_items_second");
		job.setJarByClass(OrderSecondTable.class);
		
		
		
		//判读表是否存在
		orTableExists();
		
		Scan scan = new Scan();
		TableMapReduceUtil
		.initTableMapperJob(
				"orderdata:order_items"
				, scan
				, OrderSecondTableMap.class
				, Text.class
				, Text.class
				, job);
				
		
		
		
		
		TableMapReduceUtil
		.initTableReducerJob(
				"orderdata:order_items02"
				, OrderSecondTableReduce.class
				, job);
		
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
	//判读表是否存在
	public static void orTableExists() throws IOException{
		
		OrderSecondTable orderSecondTable = new OrderSecondTable();
		
		TableName tName = TableName.valueOf("orderdata:order_items02");
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
	
	
	
	@Test
	public void test01(){
		int order_item_id = 123;
		int order_id = 123;
		int product_id = 123;
		
		
		ByteBuffer result = ByteBuffer.allocate(16);
		result.put(Bytes.toBytes(order_item_id));
		result.put(Bytes.toBytes(order_id));
		result.put(Bytes.toBytes(product_id));
		result.put(Bytes.toBytes(123));
		result.put(Bytes.toBytes(123));
		
		System.out.println(Arrays.toString(result.array()));
		System.out.println(Bytes.toInt(result.array()));
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
