/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order.mapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.voole.hobbit.avro.hive.HiveOrderRecord;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputMapper
		extends
		Mapper<AvroKey<SpecificRecordBase>, NullWritable, Text, AvroValue<SpecificRecordBase>> {
	private Text sessionId = new Text();
	private AvroValue<SpecificRecordBase> record = new AvroValue<SpecificRecordBase>();

	@Override
	protected void map(AvroKey<SpecificRecordBase> key, NullWritable value,
			Context context) throws IOException, InterruptedException {
		SpecificRecordBase recordBase = key.datum();
		if (recordBase instanceof HiveOrderRecord) {
			sessionId.set((String) ((HiveOrderRecord) recordBase).getSessID());
		} else {
			sessionId.set((String) recordBase.get("sessID"));
		}
		record.datum(key.datum());
		context.write(sessionId, record);

	}

}
