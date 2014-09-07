/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.hive.order.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyRecordReader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.voole.hobbit2.hive.order.HiveOrderHDFSUtils;
import com.voole.hobbit2.hive.order.HiveOrderMetaConfigs;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderRecordInputFormat<T> extends
		FileInputFormat<AvroKey<T>, NullWritable> {
	@Override
	public RecordReader<AvroKey<T>, NullWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new AvroKeyRecordReader<T>(
				HiveOrderMetaConfigs.getOrderUnionSchema(context));
	}

	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		HiveOrderInputFileFilter fileFilter = new HiveOrderInputFileFilter(job);
		List<FileStatus> result = new ArrayList<FileStatus>();
		List<FileStatus> list = super.listStatus(job);
		for (FileStatus fileStatus : list) {
			if (fileFilter.accept(fileStatus.getPath())) {
				result.add(fileStatus);
			}
		}
		if (result.size() == 0) {
			throw new IOException("No input paths specified in job");
		}
		HiveOrderHDFSUtils.writeCurrCamusExecTime(job,
				fileFilter.getCurrCamusExecTime());
		return result;

	}
}
