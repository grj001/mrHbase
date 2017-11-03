package com.zhiyou.mrhbase20171103;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class FindOrder {
	
	public void findByDate(
			Date StartDate, Date endDate, int customerId) throws IOException{
		
		byte[] startRowKey = 
				OrderEtl
				.getOrderRowKey(
						customerId
						, endDate
						, Integer.MIN_VALUE);
		
		byte[] endRowKey = 
				OrderEtl
				.getOrderRowKey(
						customerId
						, StartDate
						, Integer.MAX_VALUE);
		
		Scan scan = new Scan();
//		scan.setStartRow(startRowKey);
//		scan.setStopRow(endRowKey);
		
		//连接hbase
		Connection connection = 
				ConnectionFactory.createConnection(
						HBaseConfiguration.create());
		
		Table orderTable = connection.getTable(
				TableName.valueOf("orderdata:orders"));
		ResultScanner rs = orderTable.getScanner(scan);
		Result result = new Result();
		while((result = rs.next()) != null){
			System.out.println(
					Bytes.toString(result.getValue(Bytes.toBytes("i")
							, Bytes.toBytes("date")))
					+"---"+ 
					Bytes.toString(result.getValue(
							Bytes.toBytes("i")
							, Bytes.toBytes("status")
							))
					+"---"+Bytes.toInt(result.getRow(),0,4)+", "+
					Bytes.toInt(result.getRow(),12,4)
					);
		}
	}
	
	
	public static void main(String[] args) 
			throws Exception {
		FindOrder findOrder = new FindOrder();
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Date startDate = dateFormat.parse("2014-02-20 00:00:00");
		Date endDate = dateFormat.parse("2014-02-25 00:00:00");
		
		findOrder.findByDate(startDate, endDate, 7155);
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
