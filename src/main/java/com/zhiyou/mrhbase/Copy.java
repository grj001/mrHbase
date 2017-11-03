package com.zhiyou.mrhbase;

import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

public class Copy {
	public byte[] getOrderRowKey(int customId, Date date, int orderId){
		
		ByteBuffer result = ByteBuffer.allocate(16);
		result.put(Bytes.toBytes(customId));
		result.put(Bytes.toBytes(Long.MAX_VALUE - date.getTime()));
		result.put(Bytes.toBytes(orderId));
		
		return result.array();
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
