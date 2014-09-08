/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.dungbeetle.api;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.voole.dungbeetle.api.model.HiveTable;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public interface IDumgBeetleTransformer<SOURCE> {
	public void setup(TaskAttemptContext context) throws IOException,
			InterruptedException;

	public void cleanup(TaskAttemptContext context) throws IOException,
			InterruptedException;

	public Map<HiveTable, List<SpecificRecordBase>> transform(SOURCE source)
			throws DumgBeetleTransformException;
}
