/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order.mapreduce;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputReducer extends
		Reducer<Text, AvroKey<Record>, NullWritable, Text> {
	private NullWritable _null = NullWritable.get();
	private Text count = new Text();

	@Override
	protected void reduce(Text sessionId, Iterable<AvroKey<Record>> iterable,
			Context context) throws IOException, InterruptedException {

		int i = 0;
		for (AvroKey<Record> avroKey : iterable) {
			i++;
		}
		count.set(sessionId + "_" + i);
		context.write(_null, count);
	}
}
