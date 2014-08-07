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

	public static final String CAMUS_MAX_STAMP_FILE_NAME = "camus_max_stamp_file";

	public static final String CURR_CAMUS_MAX_STAMP = "curr.camus.max.stamp";
	public static final String PREV_CAMUS_MAX_STAMP = "prev.camus.max.stamp";
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

	public static long getCurrCamusMaxStamp(JobContext job) {
		return job.getConfiguration().getLong(CURR_CAMUS_MAX_STAMP, 0l);
	}

	public static void setCurrCamusMaxStamp(JobContext job, long maxStamp) {
		job.getConfiguration().setLong(CURR_CAMUS_MAX_STAMP, maxStamp);
	}

	public static long getPrevCamusMaxStamp(JobContext job) {
		return job.getConfiguration().getLong(PREV_CAMUS_MAX_STAMP, 0l);
	}

	public static void setPrevCamusMaxStamp(JobContext job, long maxStamp) {
		job.getConfiguration().setLong(PREV_CAMUS_MAX_STAMP, maxStamp);
	}

}
