/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public interface InputSplitSchemaManager {
	Schema getSchema(InputSplit split, JobContext context) throws IOException;
}
