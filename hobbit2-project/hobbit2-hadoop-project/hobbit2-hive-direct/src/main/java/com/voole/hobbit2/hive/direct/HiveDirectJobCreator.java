package com.voole.hobbit2.hive.direct;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.api.model.HiveTablePartition;
import com.voole.hobbit2.common.Hobbit2Configuration;
import com.voole.hobbit2.hive.direct.mapreduce.HiveDirectRecordInputFormat;
import com.voole.hobbit2.hive.direct.mapreduce.HiveOrderInputMapper;
import com.voole.hobbit2.hive.direct.mapreduce.HiveOrderInputReducer;
import com.voole.hobbit2.hive.direct.mapreduce.HiveOrderMultiOutputFormat;

public class HiveDirectJobCreator extends Configured {

	private static org.apache.log4j.Logger log = Logger
			.getLogger(HiveDirectJobCreator.class);
	private static SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd-HH-mm-ss");
	private static String fileEnd;

	public Job create(String[] args) throws ConfigurationException,
			IOException, ParseException {
		initConfigs(args);
		Job job = createJob();
		checkAndLoad(job);
		Path execBasePath = HiveDirectMetaConfigs.getExecBasePath(job);
		fileEnd = df.format(new Date());
		Path newExecutionOutput = new Path(execBasePath, fileEnd);
		FileOutputFormat.setOutputPath(job, newExecutionOutput);
		log.info("New execution temp location: "
				+ newExecutionOutput.toString());

		FileInputFormat.setInputDirRecursive(job, true);
		job.setInputFormatClass(HiveDirectRecordInputFormat.class);

		job.setMapperClass(HiveOrderInputMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		AvroJob.setMapOutputValueSchema(job,
				HiveDirectMetaConfigs.getUnionSchema(job));

		job.setReducerClass(HiveOrderInputReducer.class);
		job.setOutputFormatClass(HiveOrderMultiOutputFormat.class);
		HiveDirectMetaConfigs.setExecStartTime(job);

		return job;
	}

	public static void finishJob(Job job) throws IOException {
		if (job.isSuccessful()) {
			FileSystem fs = FileSystem.get(job.getConfiguration());
			Path newExecutionOutput = FileOutputFormat.getOutputPath(job);
			insertIntoHive(newExecutionOutput, job);
			fs.rename(newExecutionOutput,
					HiveDirectMetaConfigs.getExecHistoryPath(job));
			log.info("HiveOrderJob finished");
		} else {
			log.info("HiveOrderJob failed");
		}
	}

	private static void insertIntoHive(Path newExecutionOutput, Job job)
			throws IOException {
		Map<String, HiveTable> fileNameToHiveTableMap = HiveDirectHDFSUtils
				.readFileNameToHiveTableMap(newExecutionOutput, job);
		ClassPathXmlApplicationContext cxt = new ClassPathXmlApplicationContext(
				"hive-db.xml");
		JdbcTemplate hiveClient = cxt.getBean(JdbcTemplate.class);
		log.info("load in hive file size:" + fileNameToHiveTableMap.size());
		FileSystem fs = FileSystem.get(job.getConfiguration());
		for (Entry<String, HiveTable> entry : fileNameToHiveTableMap.entrySet()) {
			String fileName = entry.getKey();
			HiveTable table = entry.getValue();
			Path sourcePath = new Path(newExecutionOutput, fileName);
			Path targetPath = new Path(newExecutionOutput, fileName + "-"
					+ fileEnd);
			fs.rename(sourcePath, targetPath);
			String resultFilePath = targetPath.toUri().getPath();
			String sql = "LOAD DATA  INPATH '" + resultFilePath
					+ "'  INTO TABLE " + table.getName();
			if (table.hasPartition()) {
				List<String> paritionStrs = new ArrayList<String>();
				for (HiveTablePartition partition : table.getPartitions()) {
					paritionStrs.add(partition.getName() + "='"
							+ partition.getValue() + "'");
				}
				sql += " PARTITION (" + Joiner.on(',').join(paritionStrs)
						+ ") ";
			}
			hiveClient.update(sql);
			log.info("load file:" + resultFilePath);
		}
		cxt.close();
	}

	private Job createJob() throws IOException {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(HiveDirectJobCreator.class);
		job.setJobName(HiveDirectMetaConfigs.getJobName(job));
		job.setNumReduceTasks(HiveDirectMetaConfigs.getJobReduces(job));
		return job;
	}

	private void checkAndLoad(Job job) throws IOException {
		Path camusDestPath = HiveDirectMetaConfigs.getCamusDestPath(job);
		String[] topics = HiveDirectMetaConfigs.getWhiteTopics(job);
		HiveDirectHDFSUtils.checkAndLoadCamsuPath(job, camusDestPath, topics);

		Path execBasePath = HiveDirectMetaConfigs.getExecBasePath(job);
		Path execHistoryPath = HiveDirectMetaConfigs.getExecHistoryPath(job);
		HiveDirectHDFSUtils.checkPath(job.getConfiguration(), execBasePath,
				execHistoryPath);
		HiveDirectHDFSUtils.checkExecHistoryQuota(job.getConfiguration(),
				execBasePath, execHistoryPath,
				HiveDirectMetaConfigs.getExecHistoryMaxOfQuota(job));

		loadPrevHiveOrderExec(job, execHistoryPath);

	}

	private void loadPrevHiveOrderExec(Job job, Path execHistoryPath)
			throws IOException {
		Optional<Path> prevExecPath = HiveDirectHDFSUtils.getPrevExecPath(
				job.getConfiguration(), execHistoryPath);
		if (prevExecPath.isPresent()) {
			FileInputFormat.addInputPath(job, prevExecPath.get());
		}
		long prevCamusExecTime = HiveDirectHDFSUtils.readPrevCamusExecTime(job,
				prevExecPath);
		log.info("prevCamusExecTime:" + prevCamusExecTime + ",format:"
				+ df.format(new Date(prevCamusExecTime)));
		HiveDirectMetaConfigs.setPrevCamusExecTime(job, prevCamusExecTime);
	}

	public static void main(String[] args) throws Exception {
		HiveDirectJob job = new HiveDirectJob();
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
