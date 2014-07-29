/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.voole.hobbit.hive.order.HiveOrderConfigs;
import com.voole.hobbit.transformer.KafkaTerminalAvroTransformer;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputSplitSchemaManager implements
		InputSplitSchemaManager {

	@Override
	public Schema getSchema(InputSplit split, JobContext context)
			throws IOException {
		FileSplit fSplit = (FileSplit) split;
		String path = fSplit.getPath().toUri().getPath();
		path = path.replace(HiveOrderConfigs.getCamusDestinationPath(context),
				"");
		if (path.startsWith("/")) {
			path = path.substring(1);
		}
		String topic = path;
		if (path.indexOf("/") != -1) {
			topic = path.substring(0, path.indexOf("/"));
		}
		return KafkaTerminalAvroTransformer.getKafkaTopicSchema(topic);
	}

}
