package com.voole.hobbit2.camus.mr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.voole.hobbit2.camus.mr.mapreduce.CamusInputFormat;
import com.voole.hobbit2.camus.mr.mapreduce.CamusMultiOutputFormat;
import com.voole.hobbit2.common.Hobbit2Configuration;

public class CamusJobCreator extends Configured {

	private static org.apache.log4j.Logger log = Logger
			.getLogger(CamusJobCreator.class);
	private static SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd-HH-mm-ss");

	public Job create(String[] args) throws ConfigurationException,
			IOException, ParseException {
		initConfigs(args);
		Job job = createJob();
		check(job);
		Path newExecutionOutput = new Path(
				CamusMetaConfigs.getExecBasePath(job), df.format(new Date()));
		FileOutputFormat.setOutputPath(job, newExecutionOutput);
		log.info("New execution temp location: "
				+ newExecutionOutput.toString());
		job.setInputFormatClass(CamusInputFormat.class);
		job.setOutputFormatClass(CamusMultiOutputFormat.class);

		job.setNumReduceTasks(0);
		CamusMetaConfigs.setExecStartTime(job);
		return job;
	}

	public static void finishJob(Job job) throws IOException {
		if (job.isSuccessful()) {
			FileSystem fs = FileSystem.get(job.getConfiguration());
			fs.rename(FileOutputFormat.getOutputPath(job),
					CamusMetaConfigs.getExecHistoryPath(job));
			log.info("CamusJob finished");
		} else {
			log.info("CamusJob failed");
		}

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
		job.setJarByClass(CamusJobCreator.class);
		job.setJobName(CamusMetaConfigs.getJobName(job));
		job.setNumReduceTasks(0);
		return job;
	}

}
