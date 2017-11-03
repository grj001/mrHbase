package com.zhiyou.mrhbase.homework;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator.RowComparator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

//使用hbse api 从表中查询出对应order下的订单信息
public class OrderAndOrderItemFromHBase {

	public static Configuration conf = HBaseConfiguration.create();
	
	public Connection connection;
	
	public Table Orders;
	public Table OrderItems;
	
	public Admin admin;
	
	public OrderAndOrderItemFromHBase() throws IOException{
		connection = ConnectionFactory.createConnection();
		Orders = connection.getTable(TableName.valueOf("bd14:orders"));
		OrderItems = connection.getTable(TableName.valueOf("bd14:order_items"));
		admin = connection.getAdmin();
	}
	
	
	
	// 1. 查询所有Orders表
	public void getOrderInfos() throws IOException{
		
		Scan scan = new Scan();
		//Orders查全部
		ResultScanner rs = Orders.getScanner(scan);
		showResult(rs);
		
	}
	
	public void showResult(ResultScanner rs) throws IOException{
		Result result = new Result();
		while((result = rs.next()) != null){
			CellScanner cs = result.cellScanner();
			System.out.println("1. order订单:"+Bytes.toString(result.getRow()));
			
			String family = "";
			String qualify = "";
			String value= "";
			String OrderId = "";
			while(cs.advance()){
				Cell cell = cs.current();
				family = Bytes.toString(CellUtil.cloneFamily(cell));
				qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
				value = Bytes.toString(CellUtil.cloneValue(cell));
				
				if(qualify.equals("order_id")){
					OrderId = value;
				}
				
				System.out.println("\tqualify:\t"+qualify+"\tvalue:\t"+value);
			}
			
			showOrderItem(OrderId, OrderItems);
		}
	}
	

	
	//2. 对应一个order的order_id, 去搜索Order_items找到对应的RowKey
	public static void showOrderItem(
			String OrderId
			, Table orderItems) throws IOException{
			
		Filter ValueFilter = 
				new ValueFilter(
						CompareFilter.CompareOp.EQUAL
						, new BinaryComparator(Bytes.toBytes(OrderId)));
		
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("order_item_order_id"));
		scan.setFilter(ValueFilter);
		
		ResultScanner resultScanner = orderItems.getScanner(scan);
		
		Result rs = new Result();
		while((rs = resultScanner.next()) != null){
			
			CellScanner cs = rs.cellScanner();
			System.out.println("\t\t2. 对应订单号的货序号:"+Bytes.toString(rs.getRow()));
			showOneRowKey(orderItems, rs.getRow());
		}
			
	}
	
	
	
	
	
	// 3. 对应一个rowKey, 去搜索所有Order_items, 找到对应rowKey的信息
	public static void showOneRowKey(
			Table table, byte[] rowKey) throws IOException{
		RowFilter rowFilter = new RowFilter(
					CompareOp.EQUAL
					, new BinaryComparator(rowKey)
				);
		
		Scan scan = new Scan();
		scan.setFilter(rowFilter);
		
		ResultScanner scanner = table.getScanner(scan);
		Result rOneRowKey = new Result();
		while((rOneRowKey = scanner.next()) != null){
			CellScanner cs = rOneRowKey.cellScanner();
			while(cs.advance()){
				Cell cell = cs.current();
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.println("\t\t\tqualify:\t"+qualify+"\tvalue:\t"+value);
			}
			System.out.println();
		}
	}
	
	
	
	/*
	 * 1. 查询所有Orders表
	 * 2. 对应一个order的order_id, 去搜索Order_items找到对应的RowKey
	 * 3. 对应一个rowKey, 去搜索所有Order_items, 找到对应rowKey的信息
	 */
	
	public static void main(String[] args) throws IOException {
		OrderAndOrderItemFromHBase o = new OrderAndOrderItemFromHBase();
		o.getOrderInfos();
	}
	
	
	
	
	
	
}
