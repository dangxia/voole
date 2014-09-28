/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.gc;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * @author XuehuiHe
 * @date 2014年9月28日
 */
public class StormOrderHbaseGcMapper extends TableMapper<Text, LongWritable> {
	private HTable table;

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		table.delete(new Delete(key.get()));
		context.getCounter("delete", "rows").increment(1l);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		table = new HTable(context.getConfiguration(), "storm_order_session");
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		table.close();
	}

}
