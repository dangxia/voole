/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.Schema;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.voole.hobbit2.camus.OrderTopicsUtils;
import com.voole.hobbit2.common.Hobbit2Configuration;
import com.voole.hobbit2.common.config.KafkaMetaConfigs;
import com.voole.hobbit2.common.config.ZookeeperMetaConfigs;
import com.voole.hobbit2.tools.kafka.ZookeeperUtils;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class StormOrderMetaConfigs {
	private static final Logger logger = LoggerFactory
			.getLogger(StormOrderMetaConfigs.class);
	private static CompositeConfiguration hobbit2Configuration;
	static {
		try {
			hobbit2Configuration = Hobbit2Configuration.getInstance();
		} catch (Exception e) {
			logger.error("config init failed");
			Throwables.propagate(e);
		}
	}

	public static final String CAMUS_OFFSET_PREFIX = "offsets";
	public static final String CAMUS_REQUESTS_FILE = "previous.partition.states";
	public static final String CAMUS_EXEC_HISTORY_PATH = "camus.exec.history.path";
	public static final String HIVE_ORDER_NOEND_PREFIX = "noend_";
	public static final String HIVE_ORDER_EXEC_HISTORY_PATH = "hive.order.exec.history.path";

	public static final String STORM_ORDER_WHITELIST_TOPICS = "storm.order.whitelist.topics";

	public static Path getCamusExecHistoryPath() {
		return new Path(hobbit2Configuration.getString(CAMUS_EXEC_HISTORY_PATH));
	}

	public static Path getHiveOrderExecHistoryPath() {
		return new Path(
				hobbit2Configuration.getString(HIVE_ORDER_EXEC_HISTORY_PATH));
	}

	public static Schema getOrderUnionSchema() {
		List<String> topics = getWhiteTopics();
		List<Schema> schemas = new ArrayList<Schema>();
		for (String topic : topics) {
			schemas.add(OrderTopicsUtils.topicBiSchema.get(topic));
		}
		return Schema.createUnion(schemas);
	}

	public static List<String> getWhiteTopics() {
		String topicArrayStr = hobbit2Configuration
				.getString(STORM_ORDER_WHITELIST_TOPICS);
		return Splitter.on(',').splitToList(topicArrayStr);
	}

	public static ZkClient createZKClient() {
		return ZookeeperUtils.createZKClient(hobbit2Configuration
				.getString(KafkaMetaConfigs.KAFKA_ZOOKEEPER_CONNECT),
				hobbit2Configuration.getInteger(
						ZookeeperMetaConfigs.ZOOKEEPER_ROOT_SESSION_TIMEOUT_MS,
						40000), hobbit2Configuration.getInteger(
						ZookeeperMetaConfigs.ZOOKEEPER_ROOT_SESSION_TIMEOUT_MS,
						40000));

	}

}