package com.zhiyou.mrhbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MrFromHBase {

	public static class MrFromHBaseMap extends TableMapper<Text, Text> {

		private Text outKey = new Text();
		private Text outValue = new Text();
		private Cell cell;
		private String rowKey;
		private String columnFamily;
		private String columnQualify;
		private String columnValue;

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>
				.Context context)
				throws IOException, InterruptedException {
			// 从Result那暑
			CellScanner scanner = value.cellScanner();
			while (scanner.advance()) {
				cell = scanner.current();
				rowKey = Bytes.toString(CellUtil.cloneRow(cell));
				columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
				columnQualify = Bytes.toString(CellUtil.cloneQualifier(cell));
				columnValue = Bytes.toString(CellUtil.cloneValue(cell));

				outKey.set(rowKey);
				outValue.set(columnFamily 
						+ ":" + columnQualify 
						+ ":" + columnValue);

				context.write(outKey, outValue);
			}
		}
	}

	
	
	public static void main(String[] args) 
			throws Exception {

		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "mapreduce从hbase中读取数据");
		job.setJarByClass(MrFromHBase.class);

//		job.setMapperClass(MrFromHBaseMap.class);
		// job.setReducerClass(MrToHBaseReduce.class);
		job.setNumReduceTasks(0);

		
		Scan scan = new Scan();
		
		

		// job.setOutputKeyClass(NullWritable.class);
		// job.setOutputValueClass(Mutation.class);

		TableMapReduceUtil.initTableMapperJob(
				"bd14:fromjava"
				, scan
				, MrFromHBaseMap.class
				, Text.class
				, Text.class
				, job);
		
		

		Path outputDir = new Path("/user/output/MrFromHBase");
		outputDir.getFileSystem(conf).delete(outputDir,true);
		FileOutputFormat.setOutputPath(job, outputDir);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
