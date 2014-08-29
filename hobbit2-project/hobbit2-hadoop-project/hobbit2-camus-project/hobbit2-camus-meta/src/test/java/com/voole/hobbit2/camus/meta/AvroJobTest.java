/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroKeyComparator;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobContext;

/**
 * @author XuehuiHe
 * @date 2014年8月29日
 */
public class AvroJobTest {
	public static void setMapOutputKeySchema(Configuration conf, Schema schema) {
		conf.setClass(JobContext.MAP_OUTPUT_KEY_CLASS, AvroKey.class,
				Object.class);
		conf.setClass(JobContext.GROUP_COMPARATOR_CLASS,
				AvroKeyComparator.class, RawComparator.class);
		conf.setClass(JobContext.KEY_COMPARATOR, AvroKeyComparator.class,
				RawComparator.class);
		AvroSerialization.setKeyWriterSchema(conf, schema);
		AvroSerialization.setKeyReaderSchema(conf, schema);
		AvroSerialization.addToConfiguration(conf);
	}

	public static void setMapOutputValueSchema(Configuration conf, Schema schema) {
		conf.setClass(JobContext.MAP_OUTPUT_VALUE_CLASS, AvroValue.class,
				Object.class);
		AvroSerialization.setValueWriterSchema(conf, schema);
		AvroSerialization.setValueReaderSchema(conf, schema);
		AvroSerialization.addToConfiguration(conf);
	}

	public static void setOutputValueSchema(Configuration conf, Schema schema) {
		conf.setClass(JobContext.OUTPUT_VALUE_CLASS, AvroValue.class,
				Object.class);
		conf.set("avro.schema.output.value", schema.toString());
	}

	public static void setOutputKeySchema(Configuration conf, Schema schema) {
		conf.setClass(JobContext.OUTPUT_KEY_CLASS, AvroKey.class, Object.class);
		conf.set("avro.schema.output.key", schema.toString());
	}
}
