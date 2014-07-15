/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.camus.etl.kafka;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author XuehuiHe
 * @date 2014年7月11日
 */
public class CamusConfigs {
	public static final String CAMUS_JOB_NAME = "camus.job.name";
	public static final String CAMUS_MESSAGE_ENCODER_CLASS = "camus.message.encoder.class";
	public static final String HDFS_DEFAULT_CLASSPATH_DIR = "hdfs.default.classpath.dir";
	public static final String HDFS_EXTERNAL_JARFILES = "hdfs.external.jarFiles";

	public static final String LOG4J_CONFIGURATION = "log4j.configuration";

	public static final String ETL_DESTINATION_PATH = "etl.destination.path";
	public static final String ETL_EXECUTION_BASE_PATH = "etl.execution.base.path";
	public static final String ETL_EXECUTION_HISTORY_PATH = "etl.execution.history.path";

	public static final String ETL_EXECUTION_HISTORY_MAX_OF_QUOTA = "etl.execution.history.max.of.quota";
	public static final String ETL_OUTPUT_FILE_TIME_PARTITION_MINS = "etl.output.file.time.partition.mins";
	public static final String ETL_DEFAULT_TIMEZONE = "etl.default.timezone";
	public static final String ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY = "etl.destination.path.topic.sub.dir";

	public static final String ETL_AVRO_WRITER_SYNC_INTERVAL = "etl.avro.writer.sync.interval";
	public static final String ETL_OUTPUT_CODEC = "etl.output.codec";
	public static final String ETL_DEFAULT_OUTPUT_CODEC = "deflate";
	public static final String ETL_DEFLATE_LEVEL = "etl.deflate.level";

	public static final String KAFKA_BROKERS = "kafka.brokers";
	public static final String KAFKA_TIMEOUT_VALUE = "kafka.timeout.value";
	public static final String KAFKA_FETCH_BUFFER_SIZE = "kafka.fetch.buffer.size";
	public static final String KAFKA_CLIENT_NAME = "kafka.client.name";

	public static final String POST_TRACKING_COUNTS_TO_KAFKA = "post.tracking.counts.to.kafka";
	public static final String KAFKA_FETCH_REQUEST_MAX_WAIT = "kafka.fetch.request.max.wait";
	public static final String KAFKA_FETCH_REQUEST_MIN_BYTES = "kafka.fetch.request.min.bytes";
	public static final String KAFKA_FETCH_REQUEST_CORRELATION_ID = "kafka.fetch.request.correlationid";

	public static final String KAFKA_BLACKLIST_TOPIC = "kafka.blacklist.topics";
	public static final String KAFKA_WHITELIST_TOPIC = "kafka.whitelist.topics";
	public static final String KAFKA_MOVE_TO_LAST_OFFSET_LIST = "kafka.move.to.last.offset.list";

	public static final String OFFSET_PREFIX = "offsets";
	public static final String ERRORS_PREFIX = "errors";
	public static final String COUNTS_PREFIX = "counts";

	public static final String REQUESTS_FILE = "requests.previous";

	public static String getJobName(JobContext job) {
		return job.getConfiguration().get(CAMUS_JOB_NAME, "Camus Job");
	}

	public static String getEtlPartitioner(JobContext job) {
		// TODO set default Partitionr
		return job.getConfiguration().get(CAMUS_JOB_NAME, "Camus Job");
	}

	public static String getHdfsClassPathDir(JobContext job) {
		return job.getConfiguration().get(HDFS_DEFAULT_CLASSPATH_DIR, null);
	}

	public static String[] getHdfsExteranlJarFiles(JobContext job) {
		String jarFilesStr = job.getConfiguration().get(HDFS_EXTERNAL_JARFILES,
				null);
		if (jarFilesStr != null) {
			return jarFilesStr.split(",");
		}
		return null;
	}

	public static boolean getLog4jConfigure(JobContext job) {
		return job.getConfiguration().getBoolean(LOG4J_CONFIGURATION, false);
	}

	public static Path getDestinationPath(JobContext job) {
		return new Path(job.getConfiguration().get(ETL_DESTINATION_PATH));
	}

	public static Path getEtlExecutionBasePath(JobContext job) {
		return new Path(job.getConfiguration().get(ETL_EXECUTION_BASE_PATH));
	}

	public static Path getEtlExecutionHistoryPath(JobContext job) {
		return new Path(job.getConfiguration().get(ETL_EXECUTION_HISTORY_PATH));
	}

	public static float getEtlMaxHistoryQuota(JobContext job) {
		return job.getConfiguration().getFloat(
				ETL_EXECUTION_HISTORY_MAX_OF_QUOTA, 0.5f);
	}

	public static String getKafkaBrokers(JobContext job) {
		return job.getConfiguration().get(KAFKA_BROKERS);
	}

	public static String getKafkaClientName(JobContext job) {
		return job.getConfiguration().get(KAFKA_CLIENT_NAME);
	}

	public static int getKafkaTimeoutValue(JobContext job) {
		int timeOut = job.getConfiguration().getInt(KAFKA_TIMEOUT_VALUE, 30000);
		return timeOut;
	}

	public static int getKafkaBufferSize(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_FETCH_BUFFER_SIZE,
				1024 * 1024);
	}

	public static int getEtlOutputFileTimePartitionMins(JobContext job) {
		return job.getConfiguration().getInt(
				ETL_OUTPUT_FILE_TIME_PARTITION_MINS, 60);
	}

	public static String getDefaultTimeZone(JobContext job) {
		return job.getConfiguration()
				.get(ETL_DEFAULT_TIMEZONE, "Asia/Shanghai");
	}

	public static Path getDestPathTopicSubDir(JobContext job) {
		return new Path(job.getConfiguration().get(
				ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY, "hourly"));
	}

	public static int getKafkaFetchRequestCorrelationId(JobContext job) {
		return job.getConfiguration().getInt(
				KAFKA_FETCH_REQUEST_CORRELATION_ID, -1);
	}

	public static int getKafkaFetchRequestMaxWait(JobContext job) {
		return job.getConfiguration()
				.getInt(KAFKA_FETCH_REQUEST_MAX_WAIT, 1000);
	}

	public static int getKafkaFetchRequestMinBytes(JobContext context) {
		return context.getConfiguration().getInt(KAFKA_FETCH_REQUEST_MIN_BYTES,
				1024);
	}

	public static int getEtlAvroWriterSyncInterval(JobContext job) {
		return job.getConfiguration().getInt(ETL_AVRO_WRITER_SYNC_INTERVAL,
				16000);
	}

	public static String getEtlOutputCodec(JobContext job) {
		return job.getConfiguration().get(ETL_OUTPUT_CODEC,
				ETL_DEFAULT_OUTPUT_CODEC);
	}

	public static int getEtlDeflateLevel(JobContext job) {
		return job.getConfiguration().getInt(ETL_DEFLATE_LEVEL, 6);
	}

	public static String[] getKafkaWhitelistTopic(JobContext job) {
		if (job.getConfiguration().get(KAFKA_WHITELIST_TOPIC) != null
				&& !job.getConfiguration().get(KAFKA_WHITELIST_TOPIC).isEmpty()) {
			return job.getConfiguration().getStrings(KAFKA_WHITELIST_TOPIC);
		} else {
			return new String[] {};
		}
	}

	public static String[] getKafkaBlacklistTopic(JobContext job) {
		if (job.getConfiguration().get(KAFKA_BLACKLIST_TOPIC) != null
				&& !job.getConfiguration().get(KAFKA_BLACKLIST_TOPIC).isEmpty()) {
			return job.getConfiguration().getStrings(KAFKA_BLACKLIST_TOPIC);
		} else {
			return new String[] {};
		}
	}

	public static String[] getMoveToLatestTopics(JobContext job) {
		return job.getConfiguration()
				.getStrings(KAFKA_MOVE_TO_LAST_OFFSET_LIST);
	}
}
