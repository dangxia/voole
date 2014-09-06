/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.dungbeetle.api;

import java.io.IOException;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public interface IDumgBeetleTransformer<SOURCE> {
	public void setup(TaskAttemptContext context) throws IOException,
			InterruptedException;

	public void cleanup(TaskAttemptContext context) throws IOException,
			InterruptedException;

	public Iterable<DumgBeetleResult> transform(SOURCE source)
			throws DumgBeetleTransformException;
}
