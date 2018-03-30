package com.zhiyou;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderEtlCopy {

	public static Connection connection;
	public static Admin admin;
	
	public OrderEtlCopy() throws IOException{
		connection = ConnectionFactory.createConnection();
		admin = connection.getAdmin();
	}
	
	
	
	public static byte[] getOrderRowKey(int customerId, Date date, int orderId){
		
		ByteBuffer result = ByteBuffer.allocate(16);
		result.put(Bytes.toBytes(customerId));
		result.put(Bytes.toBytes(Long.MAX_VALUE - date.getTime()));
		result.put(Bytes.toBytes(orderId));
		return result.array();
	}
	
	
	public static class OrderEtlCopyMap extends
	Mapper<LongWritable, Text, Text, NullWritable>{
		
		public final NullWritable NULL = NullWritable.get();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(value, NULL);
		}
	}
	
	
	public static class OrderEtlCopyReduce
	extends TableReducer<Text, NullWritable, NullWritable>{
		
		private String[] infos;
		private Put outValue;
		public final NullWritable NULL =
				NullWritable.get();
		
		private int orderId;
		private int customerId;
		private Date orderDate;
		
		private SimpleDateFormat dateFormat = 
				new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		@Override
		protected void reduce(Text arg0, Iterable<NullWritable> arg1,
				Reducer<Text, NullWritable, NullWritable, Mutation>.Context arg2)
				throws IOException, InterruptedException {
			super.reduce(arg0, arg1, arg2);
		}
		
		
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
