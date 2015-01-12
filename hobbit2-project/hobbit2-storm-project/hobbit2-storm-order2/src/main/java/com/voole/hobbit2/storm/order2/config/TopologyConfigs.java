package com.voole.hobbit2.storm.order2.config;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Splitter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class TopologyConfigs {
	public static final SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	public static final String SYNC_START_FROM = "sync.start.from";
	public static final String SYNC_HDFS_NOEND_POINT = "sync.hdfs.noend.point";
	public static final String SYNC_KAFKA_WHITE_TOPICS = "sync.kafka.white.topics";

	public static SyncStartFrom DEFAULT_SYNCSTARTFROM = SyncStartFrom.KAFKA_HISTORY;

	public static void initTopologyConfig(Map config, Properties props) {
		setSyncStartFrom(config, props.getProperty(SYNC_START_FROM));
		setSyncHdfsNoendPoint(config, props.getProperty(SYNC_HDFS_NOEND_POINT));
		setWhiteTopics(config, props.getProperty(SYNC_KAFKA_WHITE_TOPICS));
	}

	public static List<String> getWhiteTopics(Map config) {
		return (List<String>) config.get(SYNC_KAFKA_WHITE_TOPICS);
	}

	public static void setWhiteTopics(Map config, String value) {
		if (value == null || value.trim().length() == 0) {
			throw new IllegalArgumentException(
					"sync.kafka.white.topics can't be empty!");
		}
		config.put(SYNC_KAFKA_WHITE_TOPICS,
				Splitter.on(',').splitToList(value.trim()));
	}

	public static SyncStartFrom getSyncStartFrom(Map config) {
		return (SyncStartFrom) config.get(SYNC_START_FROM);
	}

	public static void setSyncStartFrom(Map config, String fromConfig) {
		if (fromConfig == null) {
			fromConfig = DEFAULT_SYNCSTARTFROM.getConfig();
		}
		config.put(SYNC_START_FROM,
				SyncStartFrom.getTypeFromConfig(fromConfig.trim()));
	}

	public static void setSyncHdfsNoendPoint(Map config, String hdfsNoendPoint) {
		if (hdfsNoendPoint == null) {
			return;
		}
		try {
			Date date = df.parse(hdfsNoendPoint.trim());
			config.put(SYNC_HDFS_NOEND_POINT, date.getTime());
		} catch (ParseException e) {
			throw new IllegalArgumentException(hdfsNoendPoint
					+ " is wrong HdfsNoendPoint formate", e);
		}

	}

	public static Date getSyncHdfsNoendPoint(Map config) {
		Long l = (Long) config.get(SYNC_HDFS_NOEND_POINT);
		if (l == null) {
			return null;
		}
		return new Date(l);
	}

	public static enum SyncStartFrom {
		HDFS_NOEND("hdfs-noend"), KAFKA_HISTORY("kafka-history");
		private final String config;

		SyncStartFrom(String config) {
			this.config = config;
		}

		public String getConfig() {
			return config;
		}

		public static SyncStartFrom getTypeFromConfig(String config) {
			if (HDFS_NOEND.getConfig().equals(config)) {
				return HDFS_NOEND;
			} else if (KAFKA_HISTORY.getConfig().equals(config)) {
				return KAFKA_HISTORY;
			}
			throw new IllegalArgumentException(config
					+ " not found in SyncStartFrom!");
		}
	}

}
