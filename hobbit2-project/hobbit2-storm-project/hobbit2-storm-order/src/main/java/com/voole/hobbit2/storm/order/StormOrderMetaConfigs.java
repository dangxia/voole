/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import kafka.javaapi.consumer.SimpleConsumer;

import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.Schema;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.voole.hobbit2.camus.api.TopicMeta;
import com.voole.hobbit2.camus.api.TopicMetaManager;
import com.voole.hobbit2.camus.api.TopicMetaRegister;
import com.voole.hobbit2.camus.api.transform.TransformException;
import com.voole.hobbit2.common.Hobbit2Configuration;
import com.voole.hobbit2.common.config.KafkaMetaConfigs;
import com.voole.hobbit2.common.config.ZookeeperMetaConfigs;
import com.voole.hobbit2.tools.kafka.KafkaUtils;
import com.voole.hobbit2.tools.kafka.ZookeeperUtils;
import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.PartitionState;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class StormOrderMetaConfigs {
	private static final Logger log = LoggerFactory
			.getLogger(StormOrderMetaConfigs.class);
	private static CompositeConfiguration hobbit2Configuration;
	static {
		try {
			hobbit2Configuration = Hobbit2Configuration.getInstance();
		} catch (Exception e) {
			log.error("config init failed");
			Throwables.propagate(e);
		}
	}

	public static final String CAMUS_REQUESTS_FILE = "previous.partition.states";
	public static final String HIVE_ORDER_NOEND_PREFIX = "noend_";
	public static final String HIVE_ORDER_EXEC_HISTORY_PATH = "hive.order.exec.history.path";

	public static final String STORM_ORDER_WHITELIST_TOPICS = "storm.order.whitelist.topics";
	public static final String STORM_ORDER_PARTITIONS_META_CACHE_TIMEOUT = "storm.order.partitions.meta.cache.timeout";
	public static final String STORM_ORDER_NOEND_PARALLEL = "storm.order.noend.parallel";

	public static final String HIVE_ORDER_LAST_EXEC_PATH = "hive.order.last.exec.path";

	public static final String KAFKA_BUFFER_SIZE_BYTES = "storm.kafka.buffer.size.bytes";

	public static Map<Broker, List<PartitionState>> fetchPartitionState() {
		ZkClient client = ZookeeperUtils.createZKClient(hobbit2Configuration
				.getString(KafkaMetaConfigs.KAFKA_ZOOKEEPER_CONNECT),
				hobbit2Configuration.getInt(
						ZookeeperMetaConfigs.ZOOKEEPER_ROOT_SESSION_TIMEOUT_MS,
						40000), hobbit2Configuration.getInt(
						ZookeeperMetaConfigs.ZOOKEEPER_ROOT_SESSION_TIMEOUT_MS,
						40000));

		Map<Broker, List<PartitionState>> kafkaPartitionStatesMap = KafkaUtils
				.getPartitionState(client,
						getWhiteTopics().toArray(new String[] {}));
		client.close();

		return kafkaPartitionStatesMap;
	}

	public static Optional<Path> getHiveOrderLastExecPath(
			@SuppressWarnings("rawtypes") Map conf) {
		String lastPath = (String) conf.get(HIVE_ORDER_LAST_EXEC_PATH);
		if (lastPath == null || lastPath.length() == 0) {
			return Optional.absent();
		} else {
			return Optional.of(new Path(lastPath));
		}
	}

	@SuppressWarnings("unchecked")
	public static void setHiveOrderLastExecPath(
			@SuppressWarnings("rawtypes") Map conf, String lastPath) {
		conf.put(HIVE_ORDER_LAST_EXEC_PATH, lastPath);
	}

	public static Path getHiveOrderExecHistoryPath() {
		return new Path(
				hobbit2Configuration.getString(HIVE_ORDER_EXEC_HISTORY_PATH));
	}

	public static final String TOPIC_MEATA_REGISTERS = "storm.order.topic.meta.registers";

	private volatile static TopicMetaManager topicManager;

	public static TopicMetaManager getTopicMetaManager() {
		if (topicManager == null) {
			try {
				createTopicMetaManager();
			} catch (Exception e) {
				log.error(StormOrderMetaConfigs.class
						+ " getTopicMetaManager error", e);
				Throwables.propagate(e);
			}
		}
		return topicManager;
	}

	private synchronized static void createTopicMetaManager()
			throws TransformException {
		if (topicManager != null) {
			return;
		}
		TopicMetaManager _topicManager = new TopicMetaManager();
		_topicManager.register(getRegisters().toArray(
				new TopicMetaRegister[] {}));
		topicManager = _topicManager;

	}

	public static List<TopicMetaRegister> getRegisters() {
		List<String> registerStrs = Lists.newArrayList(Splitter.on(',').split(
				hobbit2Configuration.getString(TOPIC_MEATA_REGISTERS)));
		List<TopicMetaRegister> result = new ArrayList<TopicMetaRegister>();
		for (String registerStr : registerStrs) {
			try {
				Class<?> clazz = Class.forName(registerStr);
				result.add((TopicMetaRegister) clazz.newInstance());
			} catch (ClassNotFoundException e) {
				log.error("not found class:" + registerStr, e);
				Throwables.propagate(e);
			} catch (InstantiationException e) {
				log.error("InstantiationException:" + registerStr, e);
				Throwables.propagate(e);
			} catch (IllegalAccessException e) {
				log.error("IllegalAccessException:" + registerStr, e);
				Throwables.propagate(e);
			}
		}
		return result;

	}

	public static Schema getOrderUnionSchema() {
		List<String> topics = getWhiteTopics();
		List<Schema> schemas = new ArrayList<Schema>();
		TopicMetaManager topicMetaManager = getTopicMetaManager();
		for (String topic : topics) {
			TopicMeta topicMeta = topicMetaManager.findTopicMeta(topic);
			if (topicMeta != null) {
				schemas.add(topicMeta.getSchema());
			}
		}
		return Schema.createUnion(schemas);
	}

	public static Map<Class<?>, String> getClassToTopic() {
		TopicMetaManager topicMetaManager = getTopicMetaManager();
		Map<String, TopicMeta> topicToTopicMeta = topicMetaManager
				.getTopicToMeta();
		Map<Class<?>, String> classToTopic = new HashMap<Class<?>, String>();
		for (Entry<String, TopicMeta> entry : topicToTopicMeta.entrySet()) {
			TopicMeta topicMeta = entry.getValue();
			classToTopic.put(topicMeta.getClazz(), topicMeta.getTopic());
		}
		return classToTopic;
	}

	public static Map<String, Schema> getTopicToSchema() {
		TopicMetaManager topicMetaManager = getTopicMetaManager();
		Map<String, TopicMeta> topicToTopicMeta = topicMetaManager
				.getTopicToMeta();
		Map<String, Schema> topicToSchema = new HashMap<String, Schema>();
		for (Entry<String, TopicMeta> entry : topicToTopicMeta.entrySet()) {
			TopicMeta topicMeta = entry.getValue();
			topicToSchema.put(topicMeta.getTopic(), topicMeta.getSchema());
		}
		return topicToSchema;
	}

	public static List<String> getWhiteTopics() {
		String topicArrayStr = hobbit2Configuration
				.getString(STORM_ORDER_WHITELIST_TOPICS);
		return Lists.newArrayList(Splitter.on(',').split(topicArrayStr));
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

	public static SimpleConsumer createSimpleConsumer(Broker broker) {
		return new SimpleConsumer(broker.host(), broker.port(),
				hobbit2Configuration.getInt(KafkaMetaConfigs.KAFKA_TIME_OUT_MS,
						40000), getKafkafetchSize(), "");
	}

	volatile static Integer fetchSize = null;

	public static int getKafkafetchSize() {
		if (fetchSize == null) {
			fetchSize = hobbit2Configuration.getInt(KAFKA_BUFFER_SIZE_BYTES,
					10240);
		}
		return fetchSize;
	}

	public static int getPartitionsMetaCacheTimeout() {
		return hobbit2Configuration.getInt(
				STORM_ORDER_PARTITIONS_META_CACHE_TIMEOUT, 120000);
	}

	public static int getNoendParallel() {
		return hobbit2Configuration.getInt(STORM_ORDER_NOEND_PARALLEL, 4000);
	}

}
