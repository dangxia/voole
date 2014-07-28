/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.order.record.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit.transformer.KafkaTerminalAvroTransformer;

/**
 * @author XuehuiHe
 * @date 2014年7月28日
 */
public class OrderRecordInputFormat<K, V> extends
		FileInputFormat<AvroKey<K>, AvroValue<V>> {
	private static final Logger LOG = LoggerFactory
			.getLogger(AvroKeyValueInputFormat.class);

	@Override
	public RecordReader<AvroKey<K>, AvroValue<V>> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		FileSplit fsplit = (FileSplit) split;
		String path = fsplit.getPath().toUri().getPath();
		path = path.replace("/kafka/", "");
		String topic = path.substring(0, path.indexOf("/"));
		Schema valueReaderSchema = KafkaTerminalAvroTransformer
				.getKafkaTopicSchema(topic);
		if (null == valueReaderSchema) {
			LOG.warn("Value reader schema was not set. Use AvroJob.setInputValueSchema() if desired.");
			LOG.info("Using a value reader schema equal to the writer schema.");
		}
		return new AvroKeyValueRecordReader<K, V>(Schema.create(Type.NULL),
				valueReaderSchema);
	}

}
