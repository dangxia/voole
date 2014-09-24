/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.hive.order.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit2.hive.order.CamusHDFSUtils;
import com.voole.hobbit2.hive.order.HiveOrderHDFSUtils;
import com.voole.hobbit2.hive.order.HiveOrderMetaConfigs;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderRecordInputFormat<T> extends
		FileInputFormat<AvroKey<T>, NullWritable> {
	private static SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd-HH-mm-ss");
	private static Logger log = LoggerFactory
			.getLogger(HiveOrderRecordInputFormat.class);

	@Override
	public RecordReader<AvroKey<T>, NullWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new AvroKeyRecordReader<T>(
				HiveOrderMetaConfigs.getOrderUnionSchema(context));
	}

	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		CamusHDFSUtils.writePrevPartionsStates(job);
		HiveOrderInputFileFilter fileFilter = new HiveOrderInputFileFilter(job);
		List<FileStatus> result = new ArrayList<FileStatus>();
		List<FileStatus> list = super.listStatus(job);
		for (FileStatus fileStatus : list) {
			if (fileFilter.accept(fileStatus.getPath())) {
				result.add(fileStatus);
			}
		}
		long currCamusExecTime = fileFilter.getCurrCamusExecTime();
		if (result.size() == 0
				|| currCamusExecTime == fileFilter.getPrevCamusExecTime()) {
			throw new IOException("No input paths specified in job");
		}

		log.info("currCamusExecTime:" + currCamusExecTime + ",format:"
				+ df.format(new Date(currCamusExecTime)));
		HiveOrderMetaConfigs.setCurrCamusExecTime(job, currCamusExecTime);
		HiveOrderHDFSUtils.writeCurrCamusExecTime(job, currCamusExecTime);
		return result;

	}
}
