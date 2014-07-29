/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order.mapreduce;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputMapper extends
		Mapper<AvroKey<Record>, NullWritable, Text, AvroKey<Record>> {
	private Text sessionId = new Text();

	@Override
	protected void map(AvroKey<Record> key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		sessionId.set((String) key.datum().get("sessID"));
		context.write(sessionId, key);
	}

}
