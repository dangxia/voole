/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.gc;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author XuehuiHe
 * @date 2014年9月28日
 */
public class StormOrderHbaseGcJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		initConfigs();

		Job job = Job.getInstance(getConf());
		job.setJarByClass(StormOrderHbaseGcJob.class);
		job.setJobName("mr-storm-order-hbase-gc");
		job.setNumReduceTasks(0);
		TableMapReduceUtil.initTableMapperJob("storm_order_session",
				createScan(), StormOrderHbaseGcMapper.class, null, null, job);
		job.setOutputFormatClass(NullOutputFormat.class);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		return 0;
	}

	private Scan createScan() {
		Scan scan = new Scan();
		scan.setCaching(5000);
		scan.setCacheBlocks(false);
		return scan;
	}

	private boolean initConfigs() throws IOException, ParseException,
			ConfigurationException {
		Configuration conf = getConf();
		conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
		conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);
		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
		return true;
	}

	public static void main(String[] args) throws Exception {
		StormOrderHbaseGcJob job = new StormOrderHbaseGcJob();
		job.setConf(HBaseConfiguration.create());
		ToolRunner.run(job, args);
	}

}
