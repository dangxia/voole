/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.hive.order.mapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputMapper
		extends
		Mapper<AvroKey<SpecificRecordBase>, NullWritable, Text, AvroValue<SpecificRecordBase>> {
	private Text sessionId;
	private AvroValue<SpecificRecordBase> record;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		sessionId = new Text();
		record = new AvroValue<SpecificRecordBase>();
	}

	@Override
	protected void map(AvroKey<SpecificRecordBase> key, NullWritable value,
			Context context) throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer();
		SpecificRecordBase recordBase = key.datum();
		sb.append((String) recordBase.get("sessID"));
		sb.append((Long) recordBase.get("natip"));
		sessionId.set(sb.toString());
		record.datum(recordBase);
		context.write(sessionId, record);
	}

}
