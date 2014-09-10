/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.hive.order;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.base.Joiner;
import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.api.model.HiveTablePartition;
import com.voole.hobbit2.common.Hobbit2Configuration;

/**
 * @author XuehuiHe
 * @date 2014年9月10日
 */
public class InsertIntoHive extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		initConfigs(args);
		Job job = createJob();
		insertIntoHive(new Path("/hive_order/history/2014-09-10-15-45-52"), job);
		return 0;
	}

	private Job createJob() throws IOException {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(HiveOrderJob.class);
		job.setJobName(HiveOrderMetaConfigs.getJobName(job));
		job.setNumReduceTasks(HiveOrderMetaConfigs.getJobReduces(job));
		return job;
	}

	private void insertIntoHive(Path newExecutionOutput, Job job)
			throws IOException {
		Map<String, HiveTable> fileNameToHiveTableMap = HiveOrderHDFSUtils
				.readFileNameToHiveTableMap(newExecutionOutput, job);
		ClassPathXmlApplicationContext cxt = new ClassPathXmlApplicationContext(
				"hive-db.xml");
		JdbcTemplate hiveClient = cxt.getBean(JdbcTemplate.class);
		System.out.println("load in hive file size:"
				+ fileNameToHiveTableMap.size());
		for (Entry<String, HiveTable> entry : fileNameToHiveTableMap.entrySet()) {
			String fileName = entry.getKey();
			HiveTable table = entry.getValue();
			String resultFilePath = new Path(newExecutionOutput, fileName)
					.toUri().getPath();
			String sql = "LOAD DATA  INPATH '" + resultFilePath
					+ "'  INTO TABLE " + table.getName();
			if (table.hasPartition()) {
				List<String> paritionStrs = new ArrayList<String>();
				List<Object> args = new ArrayList<Object>();
				List<Integer> types = new ArrayList<Integer>();
				for (HiveTablePartition partition : table.getPartitions()) {
					paritionStrs.add(partition.getName() + "=?");
					args.add(partition.getValue());
					types.add(partition.getType().toJavaSQLType());
				}
				sql += " PARTITION (" + Joiner.on(',').join(paritionStrs)
						+ ") ";
				hiveClient.update(sql, args.toArray(new Object[] {}),
						types.toArray(new Integer[] {}));
			} else {
				hiveClient.update(sql);
			}

			System.out.println("load file:" + resultFilePath);
		}
		cxt.close();
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
