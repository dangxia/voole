/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyRecordReader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit.hive.order.HiveOrderConfigs;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderRecordInputFormat<T> extends
		FileInputFormat<AvroKey<T>, NullWritable> {
	private static final Logger LOG = LoggerFactory
			.getLogger(HiveOrderRecordInputFormat.class);

	/** {@inheritDoc} */
	@Override
	public RecordReader<AvroKey<T>, NullWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		Schema readerSchema = HiveOrderConfigs.getInputSplitSchema(context)
				.getSchema(split, context);
		if (null == readerSchema) {
			LOG.warn("Reader schema was not set. Use AvroJob.setInputKeySchema() if desired.");
			LOG.info("Using a reader schema equal to the writer schema.");
		}
		return new AvroKeyRecordReader<T>(readerSchema);
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
		HiveOrderConfigs.setCurrCamusMaxStamp(job, fileFilter.getMaxStamp());
		return result;

	}
}
