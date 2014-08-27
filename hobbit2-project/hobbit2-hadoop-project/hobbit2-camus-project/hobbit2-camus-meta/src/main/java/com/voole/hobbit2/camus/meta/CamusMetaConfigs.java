/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author XuehuiHe
 * @date 2014年8月27日
 */
public class CamusMetaConfigs {
	public static final String OFFSET_PREFIX = "offsets";
	public static final String ERRORS_PREFIX = "errors";
	public static final String COUNTS_PREFIX = "counts";

	public static final String REQUESTS_FILE = "previous.partition.states";

	public static final String JOB_NAME = "camus.job.name";
	public static final String JOB_MAPS = "camus.job.maps";
	public static final String JOB_SPLIT_MIN_SIZE = "camus.job.split.min.size";

	public static final String DEST_PATH = "camus.dest.path";
	public static final String EXEC_BASE_PATH = "camus.exec.base.path";
	public static final String EXEC_HISTORY_PATH = "camus.exec.history.path";
	public static final String EXEC_HISTORY_MAX_OF_QUOTA = "camus.exec.history.max.of.quota";

	public static final String WHITELIST_TOPICS = "camus.whitelist.topics";

	public static final String KAFKA_FETCH_REQUEST_CORRELATIONID = "camus.kafka.fetch.request.correlationid";
	public static final String KAFKA_FETCH_REQUEST_MAX_WAIT = "camus.kafka.fetch.request.max.wait";
	public static final String KAFKA_FETCH_REQUEST_MIN_BYTES = "camus.kafka.fetch.request.min.bytes";
	public static final String KAFKA_FETCH_REQUEST_CLIENT_ID = "camus.kafka.fetch.request.client.id";

	public static String getJobName(JobContext job) {
		return job.getConfiguration().get(JOB_NAME);
	}

	public static int getJobMaps(JobContext job) {
		return job.getConfiguration().getInt(JOB_MAPS, 20);
	}

	public static int getSplitMinSize(JobContext job) {
		return job.getConfiguration().getInt(JOB_SPLIT_MIN_SIZE, 100000);
	}

	public static Path getDestPath(JobContext job) {
		return new Path(job.getConfiguration().get(DEST_PATH));
	}

	public static Path getExecBasePath(JobContext job) {
		return new Path(job.getConfiguration().get(EXEC_BASE_PATH));
	}

	public static Path getExecHistoryPath(JobContext job) {
		return new Path(job.getConfiguration().get(EXEC_HISTORY_PATH));
	}

	public static float getExecHistoryMaxOfQuota(JobContext job) {
		return job.getConfiguration().getFloat(EXEC_HISTORY_MAX_OF_QUOTA, .5f);
	}

	public static String[] getWhiteTopics(JobContext job) {
		return job.getConfiguration().getStrings(WHITELIST_TOPICS);
	}

	public static int getKafkaFetchRequestCorrelationid(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_FETCH_REQUEST_CORRELATIONID,
				-1);
	}

	public static int getKafkaFetchRequestMaxWait(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_FETCH_REQUEST_MAX_WAIT, 10);
	}

	public static int getKafkaFetchRequestMinBytes(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_FETCH_REQUEST_MIN_BYTES,
				10240);
	}

	public static String getKafkaFetchRequestClientId(JobContext job) {
		return job.getConfiguration().get(KAFKA_FETCH_REQUEST_CLIENT_ID,
				"CAMUS_KAFKA_FETCH_REQUEST_CLIENT_ID");
	}
}
