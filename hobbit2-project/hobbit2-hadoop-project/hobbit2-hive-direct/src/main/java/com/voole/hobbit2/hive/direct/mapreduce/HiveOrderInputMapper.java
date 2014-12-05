/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.hive.direct.mapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputMapper
		extends
		Mapper<AvroKey<SpecificRecordBase>, NullWritable, NullWritable, AvroValue<SpecificRecordBase>> {
	private AvroValue<SpecificRecordBase> record;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		record = new AvroValue<SpecificRecordBase>();
	}

	@Override
	protected void map(AvroKey<SpecificRecordBase> key, NullWritable value,
			Context context) throws IOException, InterruptedException {
		record.datum(key.datum());
		context.write(NullWritable.get(), record);
	}

}
