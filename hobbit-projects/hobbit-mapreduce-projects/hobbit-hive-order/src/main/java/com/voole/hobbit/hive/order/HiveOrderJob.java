/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.voole.hobbit.avro.hive.HiveOrderRecord;
import com.voole.hobbit.hive.order.mapreduce.HiveOrderInputMapper;
import com.voole.hobbit.hive.order.mapreduce.HiveOrderInputReducer;
import com.voole.hobbit.hive.order.mapreduce.HiveOrderRecordInputFormat;
import com.voole.hobbit.transformer.KafkaTerminalAvroTransformer;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderJob extends Configured implements Tool {
	private static org.apache.log4j.Logger log = Logger
			.getLogger(HiveOrderJob.class);
	private static SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd-HH-mm-ss");

	@Override
	public int run(String[] args) throws Exception {
		if (!processArgs(args)) {
			return 1;
		}
		System.setProperty("HADOOP_USER_NAME", "root");
		Job job = Job.getInstance(getConf());
		job.setJarByClass(HiveOrderJob.class);
		job.setJobName(HiveOrderConfigs.getHiveOrderJobName(job));

		FileSystem fs = FileSystem.get(job.getConfiguration());
		log.info("Dir Destination set to: "
				+ HiveOrderConfigs.getHiveOrderrDestinationPath(job));
		Path execBasePath = HiveOrderConfigs.getHiveOrderExecutionBasePath(job);
		Path execHistory = HiveOrderConfigs
				.getHiveOrderrExecutionHistoryPath(job);

		preprocessExecutionPath(job, fs, execBasePath, execHistory);

		Path newExecutionOutput = new Path(execBasePath, df.format(new Date()));
		FileOutputFormat.setOutputPath(job, newExecutionOutput);
		log.info("New execution temp location: "
				+ newExecutionOutput.toString());

		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job,
				new Path(HiveOrderConfigs.getCamusDestinationPath(job)));

		job.setInputFormatClass(HiveOrderRecordInputFormat.class);

		job.setMapOutputKeyClass(Text.class);

		AvroJob.setMapOutputValueSchema(job, getMapValueSchema());

		job.setMapperClass(HiveOrderInputMapper.class);

		job.setReducerClass(HiveOrderInputReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

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

		fs.rename(newExecutionOutput, execHistory);

		log.info("Job finished");

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

	public static Schema getMapValueSchema() throws IOException {
		return SchemaBuilder
				.unionOf()
				.type(KafkaTerminalAvroTransformer
						.getKafkaTopicSchema("t_playbgn_v2"))
				.and()
				.type(KafkaTerminalAvroTransformer
						.getKafkaTopicSchema("t_playbgn_v3"))
				.and()
				.type(KafkaTerminalAvroTransformer
						.getKafkaTopicSchema("t_playalive_v2"))
				.and()
				.type(KafkaTerminalAvroTransformer
						.getKafkaTopicSchema("t_playalive_v3"))
				.and()
				.type(KafkaTerminalAvroTransformer
						.getKafkaTopicSchema("t_playend_v2"))
				.and()
				.type(KafkaTerminalAvroTransformer
						.getKafkaTopicSchema("t_playend_v3")).and()
				.type(HiveOrderRecord.getClassSchema()).endUnion();
	}

	public static void main(String[] args) throws Exception {
		HiveOrderJob job = new HiveOrderJob();
		ToolRunner.run(job, args);
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
		long limit = (long) (content.getQuota() * 0.5f);
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
			System.out.println("No previous execution");
		}
	}

	private boolean processArgs(String[] args) throws IOException,
			ParseException {
		Properties props = new Properties();
		props.load(HiveOrderJob.class.getClassLoader().getResourceAsStream(
				"hive_order.properties"));
		if (getConf() == null) {
			setConf(new Configuration());
		}
		Configuration conf = getConf();
		for (Object key : props.keySet()) {
			conf.set(key.toString(), props.getProperty(key.toString()));
		}
		return true;
	}

}
