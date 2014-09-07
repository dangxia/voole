package com.voole.hobbit2.hive.order;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;
import com.voole.hobbit2.common.Hobbit2Configuration;
import com.voole.hobbit2.hive.order.mapreduce.HiveOrderInputMapper;
import com.voole.hobbit2.hive.order.mapreduce.HiveOrderInputReducer;
import com.voole.hobbit2.hive.order.mapreduce.HiveOrderMultiOutputFormat;
import com.voole.hobbit2.hive.order.mapreduce.HiveOrderRecordInputFormat;

public class HiveOrderJob extends Configured implements Tool {
	private static org.apache.log4j.Logger log = Logger
			.getLogger(HiveOrderJob.class);
	private static SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd-HH-mm-ss");

	@Override
	public int run(String[] args) throws Exception {
		initConfigs(args);
		Job job = createJob();
		checkAndLoad(job);

//		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path execBasePath = HiveOrderMetaConfigs.getExecBasePath(job);
		Path newExecutionOutput = new Path(execBasePath, df.format(new Date()));
		FileOutputFormat.setOutputPath(job, newExecutionOutput);
		log.info("New execution temp location: "
				+ newExecutionOutput.toString());

		FileInputFormat.setInputDirRecursive(job, true);
		job.setInputFormatClass(HiveOrderRecordInputFormat.class);

		job.setMapperClass(HiveOrderInputMapper.class);
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job,
				HiveOrderMetaConfigs.getOrderUnionSchema(job));

		job.setReducerClass(HiveOrderInputReducer.class);
		job.setOutputFormatClass(HiveOrderMultiOutputFormat.class);
		try {
			job.submit();
		} catch (Exception e) {
			e.printStackTrace();
		}

		job.waitForCompletion(true);
		return 0;
	}

	private Job createJob() throws IOException {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(HiveOrderJob.class);
		job.setJobName(HiveOrderMetaConfigs.getJobName(job));
		job.setNumReduceTasks(HiveOrderMetaConfigs.getJobReduces(job));
		return job;
	}

	private void checkAndLoad(Job job) throws IOException {
		Path camusDestPath = HiveOrderMetaConfigs.getCamusDestPath(job);
		String[] topics = HiveOrderMetaConfigs.getWhiteTopics(job);
		HiveOrderHDFSUtils.checkAndLoadCamsuPath(job, camusDestPath, topics);

		Path execBasePath = HiveOrderMetaConfigs.getExecBasePath(job);
		Path execHistoryPath = HiveOrderMetaConfigs.getExecHistoryPath(job);
		HiveOrderHDFSUtils.checkHiveOrderPath(job.getConfiguration(),
				execBasePath, execHistoryPath);
		HiveOrderHDFSUtils.checkExecHistoryQuota(job.getConfiguration(),
				execBasePath, execHistoryPath,
				HiveOrderMetaConfigs.getExecHistoryMaxOfQuota(job));

		loadPrevHiveOrderExec(job, execHistoryPath);

	}

	private void loadPrevHiveOrderExec(Job job, Path execHistoryPath)
			throws IOException {
		Optional<Path> prevExecPath = HiveOrderHDFSUtils.getPrevExecPath(
				job.getConfiguration(), execHistoryPath);
		if (prevExecPath.isPresent()) {
			FileInputFormat.addInputPath(job, prevExecPath.get());
		}
		long prevCamusExecTime = HiveOrderHDFSUtils.readPrevCamusExecTime(job,
				prevExecPath);
		HiveOrderMetaConfigs.setPrevCamusExecTime(job, prevCamusExecTime);
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