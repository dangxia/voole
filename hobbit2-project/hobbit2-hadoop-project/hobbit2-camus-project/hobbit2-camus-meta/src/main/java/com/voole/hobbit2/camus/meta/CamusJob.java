/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.voole.hobbit2.camus.meta.common.CamusMapperTimeKey;
import com.voole.hobbit2.camus.meta.mapreduce.CamusInputFormat;
import com.voole.hobbit2.camus.meta.mapreduce.CamusMapper;
import com.voole.hobbit2.camus.meta.mapreduce.CamusReducer;
import com.voole.hobbit2.config.props.Hobbit2Configuration;
import com.voole.hobbit2.kafka.avro.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.kafka.avro.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV3;

/**
 * @author XuehuiHe
 * @date 2014年8月27日
 */
public class CamusJob extends Configured implements Tool {
	private static org.apache.log4j.Logger log = Logger
			.getLogger(CamusJob.class);
	private static SimpleDateFormat df = new SimpleDateFormat(
			"YYYY-MM-dd-HH-mm-ss");

	@Override
	public int run(String[] args) throws Exception {
		initConfigs();
		Job job = createJob();
		check(job);
		Path newExecutionOutput = new Path(
				CamusMetaConfigs.getExecBasePath(job), df.format(new Date()));
		FileOutputFormat.setOutputPath(job, newExecutionOutput);
		log.info("New execution temp location: "
				+ newExecutionOutput.toString());

		job.setInputFormatClass(CamusInputFormat.class);
		job.setMapperClass(CamusMapper.class);

		job.setMapOutputKeyClass(CamusMapperTimeKey.class);
		AvroJob.setMapOutputValueSchema(job, getMapValueSchema());

		job.setReducerClass(CamusReducer.class);
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		AvroJob.setOutputValueSchema(job, getMapValueSchema());

		job.setNumReduceTasks(6);
		//
		try {
			job.submit();
		} catch (Exception e) {
			e.printStackTrace();
		}

		job.waitForCompletion(true);

		// TODO Auto-generated method stub
		return 0;
	}

	public static Schema getMapValueSchema() throws IOException {

		List<Schema> schemas = new ArrayList<Schema>();
		schemas.add(OrderPlayBgnReqV2.getClassSchema());
		schemas.add(OrderPlayBgnReqV3.getClassSchema());

		schemas.add(OrderPlayAliveReqV2.getClassSchema());
		schemas.add(OrderPlayAliveReqV3.getClassSchema());

		schemas.add(OrderPlayEndReqV2.getClassSchema());
		schemas.add(OrderPlayEndReqV3.getClassSchema());

		return Schema.createUnion(schemas);
	}

	private void check(Job job) throws IOException {
		CamusHDFSUtils.checkCamsuPath(job.getConfiguration(),
				CamusMetaConfigs.getDestPath(job),
				CamusMetaConfigs.getExecBasePath(job),
				CamusMetaConfigs.getExecHistoryPath(job));
		CamusHDFSUtils.checkExecHistoryQuota(job.getConfiguration(),
				CamusMetaConfigs.getExecBasePath(job),
				CamusMetaConfigs.getExecHistoryPath(job),
				CamusMetaConfigs.getExecHistoryMaxOfQuota(job));
	}

	private Job createJob() throws IOException {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(CamusJob.class);
		job.setJobName(CamusMetaConfigs.getJobName(job));
		job.setNumReduceTasks(0);
		return job;
	}

	private boolean initConfigs() throws IOException, ParseException {
		if (getConf() == null) {
			setConf(new Configuration());
		}
		Configuration conf = getConf();
		Hobbit2Configuration hobbit2Configuration = new Hobbit2Configuration();
		for (@SuppressWarnings("unchecked")
		Iterator<String> iterator = hobbit2Configuration.getKeys(); iterator
				.hasNext();) {
			String key = iterator.next();
			conf.set(key, hobbit2Configuration.getString(key));
		}
		conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
		return true;
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		CamusJob job = new CamusJob();
		ToolRunner.run(job, args);
	}

}
