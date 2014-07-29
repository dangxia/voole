/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

import com.voole.hobbit.hive.order.mapreduce.InputSplitSchemaManager;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderConfigs {
	public static final String INPUT_SPLIT_SCHEMA_MANAGER = "input.split.schema.manager";
	public static final String CAMUS_DESTINATION_PATH = "camus.destination.path";
	public static final String HIVE_ORDER_JOB_NAME = "hive.order.job.name";
	public static final String HIVE_ORDER_EXECUTION_BASE_PATH = "hive.order.execution.base.path";
	public static final String HIVE_ORDER_EXECUTION_HISTORY_PATH = "hive.order.execution.history.path";
	public static final String HIVE_ORDER_DESTINATION_PATH = "hive.order.destination.path";
	private static InputSplitSchemaManager inputSplitSchemaManager = null;

	public static InputSplitSchemaManager getInputSplitSchema(JobContext job) {
		if (inputSplitSchemaManager != null) {
			return inputSplitSchemaManager;
		}
		List<InputSplitSchemaManager> managers = job.getConfiguration()
				.getInstances(INPUT_SPLIT_SCHEMA_MANAGER,
						InputSplitSchemaManager.class);
		if (managers.size() > 0) {
			inputSplitSchemaManager = managers.get(0);
		}
		return inputSplitSchemaManager;
	}

	public static String getCamusDestinationPath(JobContext job) {
		return job.getConfiguration().get(CAMUS_DESTINATION_PATH, "");
	}

	public static String getHiveOrderJobName(JobContext job) {
		return job.getConfiguration().get(HIVE_ORDER_JOB_NAME,
				"default_hive_order_job_name");
	}

	public static Path getHiveOrderExecutionBasePath(JobContext job) {
		return new Path(job.getConfiguration().get(
				HIVE_ORDER_EXECUTION_BASE_PATH));
	}

	public static Path getHiveOrderrExecutionHistoryPath(JobContext job) {
		return new Path(job.getConfiguration().get(
				HIVE_ORDER_EXECUTION_HISTORY_PATH));
	}

	public static Path getHiveOrderrDestinationPath(JobContext job) {
		return new Path(job.getConfiguration().get(HIVE_ORDER_DESTINATION_PATH));
	}

}
