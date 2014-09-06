package com.voole.hobbit2.hive.order;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.voole.hobbit2.common.Hobbit2Configuration;

public class HiveOrderJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		initConfigs(args);
		Job job = createJob();
		check(job);
		return 0;
	}

	private Job createJob() throws IOException {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(HiveOrderJob.class);
		job.setJobName("hive_order");
		job.setNumReduceTasks(10);
		return job;
	}

	private void check(Job job) throws IOException {
	}

	public static void main(String[] args) throws Exception {
		HiveOrderJob job = new HiveOrderJob();
		ToolRunner.run(job, args);
	}

	private boolean initConfigs(String[] args) throws IOException,
			ParseException, ConfigurationException {
		if (getConf() == null) {
			setConf(new Configuration());
		}
		Configuration conf = getConf();
		CompositeConfiguration hobbit2Configuration = Hobbit2Configuration
				.initConfig(args);
		for (@SuppressWarnings("unchecked")
		Iterator<String> iterator = hobbit2Configuration.getKeys(); iterator
				.hasNext();) {
			String key = iterator.next();
			conf.set(key, hobbit2Configuration.getString(key));
		}
		conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
		conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);
		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
		return true;
	}

}
