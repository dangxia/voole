/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.camus.etl.kafka;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

import com.voole.hobbit.camus.coders.Partitioner;
import com.voole.hobbit.camus.etl.RecordWriterProvider;
import com.voole.hobbit.camus.etl.kafka.coders.DefaultMessageDecoderFactory;
import com.voole.hobbit.camus.etl.kafka.coders.DefaultPartitioner;
import com.voole.hobbit.camus.etl.kafka.coders.MessageDecoderFactory;
import com.voole.hobbit.camus.etl.kafka.common.StringRecordWriterProvider;

/**
 * @author XuehuiHe
 * @date 2014年7月11日
 */
public class CamusConfigs {
	public static final String CAMUS_JOB_NAME = "camus.job.name";
	public static final String HDFS_DEFAULT_CLASSPATH_DIR = "hdfs.default.classpath.dir";
	public static final String HDFS_EXTERNAL_JARFILES = "hdfs.external.jarFiles";

	public static final String CAMUS_MESSAGE_ENCODER_CLASS = "camus.message.encoder.class";
	public static final String CAMUS_MESSAGE_DECODER_CLASS = "camus.message.decoder.class";
	public static final String CAMUS_MESSAGE_DECODER_FACTORY_CLASS = "camus.message.decoder.factory.class";
	public static final String ETL_RECORD_WRITER_PROVIDER_CLASS = "etl.record.writer.provider.class";

	public static final String LOG4J_CONFIGURATION = "log4j.configuration";

	public static final String ETL_DESTINATION_PATH = "etl.destination.path";
	public static final String ETL_EXECUTION_BASE_PATH = "etl.execution.base.path";
	public static final String ETL_EXECUTION_HISTORY_PATH = "etl.execution.history.path";

	public static final String ETL_EXECUTION_HISTORY_MAX_OF_QUOTA = "etl.execution.history.max.of.quota";
	public static final String ETL_OUTPUT_FILE_TIME_PARTITION_MINS = "etl.output.file.time.partition.mins";
	public static final String ETL_DEFAULT_TIMEZONE = "etl.default.timezone";
	public static final String ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY = "etl.destination.path.topic.sub.dir";
	public static final String ETL_DEFAULT_PARTITIONER_CLASS = "etl.partitioner.class";

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

	public static final String KAFKA_CLIENT_BUFFER_SIZE = "kafka.client.buffer.size";
	public static final String KAFKA_CLIENT_SO_TIMEOUT = "kafka.client.so.timeout";
	public static final String KAFKA_MAX_PULL_HRS = "kafka.max.pull.hrs";
	public static final String KAFKA_MAX_PULL_MINUTES_PER_TASK = "kafka.max.pull.minutes.per.task";
	public static final String KAFKA_MAX_HISTORICAL_DAYS = "kafka.max.historical.days";

	public static final String KAFKA_MONITOR_TIME_GRANULARITY_MS = "kafka.monitor.time.granularity";

	public static final String ETL_IGNORE_SCHEMA_ERRORS = "etl.ignore.schema.errors";
	public static final String PRINT_MAX_DECODER_EXCEPTIONS = "max.decoder.exceptions.to.print";

	public static final String ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST = "etl.audit.ignore.service.topic.list";

	public static final String OFFSET_PREFIX = "offsets";
	public static final String ERRORS_PREFIX = "errors";
	public static final String COUNTS_PREFIX = "counts";

	public static final String REQUESTS_FILE = "requests.previous";

	public static final String JOB_EXCUTION_STAMP = "job.excution.stamp";

	public static final String MAPRED_MAP_TASKS = "mapreduce.job.maps";

	public static String getJobName(JobContext job) {
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

	public static boolean getEtlIgnoreSchemaErrors(JobContext job) {
		return job.getConfiguration()
				.getBoolean(ETL_IGNORE_SCHEMA_ERRORS, true);
	}

	public static int getKafkaMaxPullHrs(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_MAX_PULL_HRS, -1);
	}

	public static int getKafkaMaxPullMinutesPerTask(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_MAX_PULL_MINUTES_PER_TASK,
				-1);
	}

	public static int getKafkaMaxHistoricalDays(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_MAX_HISTORICAL_DAYS, -1);
	}

	public static int getMaximumDecoderExceptionsToPrint(JobContext job) {
		return job.getConfiguration().getInt(PRINT_MAX_DECODER_EXCEPTIONS, 10);
	}

	@SuppressWarnings("unchecked")
	public static Class<MessageDecoderFactory> getMessageDecoderFactoryClass(
			JobContext job) {
		return (Class<MessageDecoderFactory>) job.getConfiguration().getClass(
				CAMUS_MESSAGE_DECODER_FACTORY_CLASS,
				DefaultMessageDecoderFactory.class);
	}

	public static Partitioner getDefaultPartitioner(JobContext job) {
		List<Partitioner> partitioners = job.getConfiguration().getInstances(
				ETL_DEFAULT_PARTITIONER_CLASS, Partitioner.class);
		if (partitioners.isEmpty()) {
			return new DefaultPartitioner();
		} else {
			return partitioners.get(0);
		}
	}

	public static List<Partitioner> getPartitioner(JobContext job,
			String topicName) throws IOException {
		String customPartitionerProperty = ETL_DEFAULT_PARTITIONER_CLASS + "."
				+ topicName;
		return job.getConfiguration().getInstances(customPartitionerProperty,
				Partitioner.class);
	}

	@SuppressWarnings("unchecked")
	public static Class<RecordWriterProvider> getRecordWriterProviderClass(
			JobContext job) {
		return (Class<RecordWriterProvider>) job.getConfiguration().getClass(
				ETL_RECORD_WRITER_PROVIDER_CLASS,
				StringRecordWriterProvider.class);
	}

	public static int getMonitorTimeGranularityMins(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_MONITOR_TIME_GRANULARITY_MS,
				10);
	}

	public static long getJobExcutionStamp(JobContext job) {
		return job.getConfiguration().getLong(JOB_EXCUTION_STAMP,
				System.currentTimeMillis());
	}

	public static int getMapredMapTasks(JobContext job) {
		return job.getConfiguration().getInt(MAPRED_MAP_TASKS, 30);
	}
}
