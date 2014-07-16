/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.camus.etl.kafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import com.voole.hobbit.camus.etl.kafka.common.DateUtils;
import com.voole.hobbit.camus.etl.kafka.mapred.EtlInputFormat;
import com.voole.hobbit.camus.etl.kafka.mapred.EtlMultiOutputFormat;

/**
 * @author XuehuiHe
 * @date 2014年7月11日
 */
public class CamusJob extends Configured implements Tool {
	private static HashMap<String, Long> timingMap = new HashMap<String, Long>();

	private static org.apache.log4j.Logger log = Logger
			.getLogger(CamusJob.class);

	public CamusJob() {
	}

	public static void main(String[] args) throws Exception {
		CamusJob job = new CamusJob();
		ToolRunner.run(job, args);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (!processArgs(args)) {
			return 1;
		}
		startTiming("pre-setup");
		startTiming("total");
		Job job = createJob();
		if (CamusConfigs.getLog4jConfigure(job)) {
			DOMConfigurator.configure("log4j.xml");
		}
		FileSystem fs = FileSystem.get(job.getConfiguration());
		log.info("Dir Destination set to: "
				+ CamusConfigs.getDestinationPath(job));
		Path execBasePath = CamusConfigs.getEtlExecutionBasePath(job);
		Path execHistory = CamusConfigs.getEtlExecutionHistoryPath(job);

		preprocessExecutionPath(job, fs, execBasePath, execHistory);

		DateTimeFormatter dateFmt = DateUtils
				.getDateTimeFormatter("YYYY-MM-dd-HH-mm-ss");
		Path newExecutionOutput = new Path(execBasePath,
				new DateTime().toString(dateFmt));
		FileOutputFormat.setOutputPath(job, newExecutionOutput);
		log.info("New execution temp location: "
				+ newExecutionOutput.toString());

		// KafkaMetaUtils.getKafkaMetadata(job);

		job.setInputFormatClass(EtlInputFormat.class);
		job.setOutputFormatClass(EtlMultiOutputFormat.class);
		job.setNumReduceTasks(0);
		//
		stopTiming("pre-setup");
		try {
			job.submit();
		} catch (Exception e) {
			e.printStackTrace();
		}

		job.waitForCompletion(true);
		// dump all counters
		Counters counters = job.getCounters();
		for (String groupName : counters.getGroupNames()) {
			CounterGroup group = counters.getGroup(groupName);
			log.info("Group: " + group.getDisplayName());
			for (Counter counter : group) {
				log.info(counter.getDisplayName() + ":\t" + counter.getValue());
			}
		}

		stopTiming("hadoop");
		startTiming("commit");

		// Send Tracking counts to Kafka
		// sendTrackingCounts(job, fs, newExecutionOutput);

		// Print any potentail errors encountered
		// printErrors(fs, newExecutionOutput);

		fs.rename(newExecutionOutput, execHistory);

		log.info("Job finished");
		stopTiming("commit");
		stopTiming("total");
		// createReport(job, timingMap);

		if (!job.isSuccessful()) {
			JobClient client = new JobClient(
					new JobConf(job.getConfiguration()));

			TaskCompletionEvent[] tasks = job.getTaskCompletionEvents(0);

			for (TaskReport task : client.getMapTaskReports(tasks[0]
					.getTaskAttemptId().getJobID())) {
				if (task.getCurrentStatus().equals(TIPStatus.FAILED)) {
					for (String s : task.getDiagnostics()) {
						System.err.println("task error: " + s);
					}
				}
			}
			throw new RuntimeException("hadoop job failed");
		}

		return 0;
	}

	private void preprocessExecutionPath(Job job, FileSystem fs,
			Path execBasePath, Path execHistory) throws IOException,
			FileNotFoundException {
		if (!fs.exists(execBasePath)) {
			log.info("The execution base path does not exist. Creating the directory");
			fs.mkdirs(execBasePath);
		}
		if (!fs.exists(execHistory)) {
			log.info("The history base path does not exist. Creating the directory.");
			fs.mkdirs(execHistory);
		}

		// enforcing max retention on the execution directories to avoid
		// exceeding HDFS quota. retention is set to a percentage of available
		// quota.
		ContentSummary content = fs.getContentSummary(execBasePath);
		long limit = (long) (content.getQuota() * CamusConfigs
				.getEtlMaxHistoryQuota(job));
		limit = limit == 0 ? 50000 : limit;

		long currentCount = content.getFileCount()
				+ content.getDirectoryCount();

		FileStatus[] executions = fs.listStatus(execHistory);

		// removes oldest directory until we get under required % of count
		// quota. Won't delete the most recent directory.
		for (int i = 0; i < executions.length - 1 && limit < currentCount; i++) {
			FileStatus stat = executions[i];
			log.info("removing old execution: " + stat.getPath().getName());
			ContentSummary execContent = fs.getContentSummary(stat.getPath());
			currentCount -= execContent.getFileCount()
					- execContent.getDirectoryCount();
			fs.delete(stat.getPath(), true);
		}

		// determining most recent execution and using as the starting point for
		// this execution
		if (executions.length > 0) {
			Path previous = executions[executions.length - 1].getPath();
			FileInputFormat.setInputPaths(job, previous);
			log.info("Previous execution: " + previous.toString());
		} else {
			System.out
					.println("No previous execution, all topics pulled from earliest available offset");
		}
	}

	private boolean processArgs(String[] args) throws IOException,
			ParseException {
		Properties props = CamusOptions.process(args);
		if (props == null) {
			return false;
		}
		if (getConf() == null) {
			setConf(new Configuration());
		}
		Configuration conf = getConf();
		for (Object key : props.keySet()) {
			conf.set(key.toString(), props.getProperty(key.toString()));
		}
		return true;
	}

	private Job createJob() throws IOException {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(CamusJob.class);
		job.setJobName(CamusConfigs.getJobName(job));

		FileSystem fs = FileSystem.get(job.getConfiguration());
		String hadoopCacheJarDir = CamusConfigs.getHdfsClassPathDir(job);
		if (hadoopCacheJarDir != null) {
			FileStatus[] status = fs.listStatus(new Path(hadoopCacheJarDir));

			if (status != null) {
				for (int i = 0; i < status.length; ++i) {
					if (!status[i].isDirectory()) {
						log.info("Adding Jar to Distributed Cache Archive File:"
								+ status[i].getPath());
						job.addFileToClassPath(status[i].getPath());
					}
				}
			} else {
				System.out.println(CamusConfigs.HDFS_DEFAULT_CLASSPATH_DIR
						+ hadoopCacheJarDir + " is empty.");
			}
		}

		// Adds External jars to hadoop classpath
		String[] externaljarFiles = CamusConfigs.getHdfsExteranlJarFiles(job);
		if (externaljarFiles != null) {
			for (String jarFile : externaljarFiles) {
				log.info("Adding external jar File:" + jarFile);
				job.addFileToClassPath(new Path(jarFile));
			}
		}
		return job;
	}

	public static void startTiming(String name) {
		timingMap.put(name,
				(timingMap.get(name) == null ? 0 : timingMap.get(name))
						- System.currentTimeMillis());
	}

	public static void stopTiming(String name) {
		timingMap.put(name,
				(timingMap.get(name) == null ? 0 : timingMap.get(name))
						+ System.currentTimeMillis());
	}

	public static void setTime(String name) {
		timingMap.put(name,
				(timingMap.get(name) == null ? 0 : timingMap.get(name))
						+ System.currentTimeMillis());
	}

}
