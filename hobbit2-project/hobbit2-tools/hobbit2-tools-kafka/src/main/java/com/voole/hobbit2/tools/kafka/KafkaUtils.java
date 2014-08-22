/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.kafka;

import static com.voole.hobbit2.tools.kafka.ZookeeperUtils.getChildrenParentMayNotExist;
import static com.voole.hobbit2.tools.kafka.ZookeeperUtils.readDataMaybeNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;

import com.voole.hobbit2.config.props.Hobbit2Configuration;
import com.voole.hobbit2.config.props.KafkaConfigKeys;
import com.voole.hobbit2.tools.kafka.KafkaJsonUtils.PartitionsInfo;
import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.KafkaPartition;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
public class KafkaUtils {
	private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);

	public static ByteBufferMessageSet fetch(SimpleConsumer consumer,
			KafkaPartition partition, long offset, int fetchSize) {
		return fetch(consumer, partition.getTopic(), partition.getPartition(),
				offset, fetchSize);
	}

	public static ByteBufferMessageSet fetch(SimpleConsumer consumer,
			String topic, int partition, long offset, int fetchSize) {
		FetchRequestBuilder requestBuilder = new FetchRequestBuilder();
		kafka.api.FetchRequest fetchRequest = requestBuilder.addFetch(topic,
				partition, offset, fetchSize).build();
		FetchResponse fetchResponse = consumer.fetch(fetchRequest);
		return fetchResponse.messageSet(topic, partition);

	}

	public static List<Broker> getBrokerInfosInCluster(ZkClient zkClient) {
		List<String> borkerIds = getBrokerIdsInCluster(zkClient);
		List<Broker> list = new ArrayList<Broker>();
		for (String brokerId : borkerIds) {
			Broker broker = getBrokerInfo(zkClient, brokerId);
			if (broker != null) {
				list.add(broker);
			}
		}

		return list;
	}

	public static List<String> getBrokerIdsInCluster(ZkClient zkClient) {
		return getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath());
	}

	public static Broker getBrokerInfo(ZkClient zkClient, String brokerId) {
		String data = readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath() + "/"
				+ brokerId);
		if (data != null && data.length() > 0) {
			Broker broker = KafkaJsonUtils.toBroker(data);
			broker.setId(Integer.parseInt(brokerId));
			return broker;
		}

		return null;
	}

	public static Map<Integer, List<Integer>> readTopicPartitionToBrokerId(
			ZkClient zkClient, String topic) {
		Set<String> topics = new HashSet<String>();
		topics.add(topic);
		Map<String, Map<Integer, List<Integer>>> map = readTopicsPartitionToBrokerId(
				zkClient, topics);
		return map.get(topic);
	}

	public static Map<String, Map<Integer, List<Integer>>> readTopicsPartitionToBrokerId(
			ZkClient zkClient, Collection<String> topics) {
		Map<String, Map<Integer, List<Integer>>> result = new HashMap<String, Map<Integer, List<Integer>>>();
		for (String topic : topics) {
			String topicPath = ZkUtils.getTopicPath(topic);
			String data = readDataMaybeNull(zkClient, topicPath);
			if (data != null && data.length() > 0) {
				PartitionsInfo info = KafkaJsonUtils.toPartitionsInfo(data);
				result.put(topic, info.partitions);
			}
		}
		return result;
	}

	public static Map<Broker, List<KafkaPartition>> getPartitionMeta(
			ZkClient zkClient, String... topics) {
		List<KafkaPartition> partitions = getPartitions2(zkClient, topics);
		Map<Broker, List<KafkaPartition>> map = new HashMap<Broker, List<KafkaPartition>>();
		for (KafkaPartition kafkaPartition : partitions) {
			Broker broker = kafkaPartition.getBroker();
			List<KafkaPartition> brokerPartitions = null;
			if (!map.containsKey(broker)) {
				brokerPartitions = new ArrayList<KafkaPartition>();
				map.put(broker, brokerPartitions);
			} else {
				brokerPartitions = map.get(broker);
			}
			brokerPartitions.add(kafkaPartition);
		}
		for (Entry<Broker, List<KafkaPartition>> entry : map.entrySet()) {
			fillKafkaPartitionOffsets(entry.getKey(), entry.getValue());
		}
		return map;
	}

	public static void fillKafkaPartitionOffsets(Broker broker,
			List<KafkaPartition> partitions) {
		Map<TopicAndPartition, PartitionOffsetRequestInfo> latestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		Map<TopicAndPartition, PartitionOffsetRequestInfo> earliestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		// Latest Offset
		PartitionOffsetRequestInfo partitionLatestOffsetRequestInfo = new PartitionOffsetRequestInfo(
				kafka.api.OffsetRequest.LatestTime(), 1);
		// Earliest Offset
		PartitionOffsetRequestInfo partitionEarliestOffsetRequestInfo = new PartitionOffsetRequestInfo(
				kafka.api.OffsetRequest.EarliestTime(), 1);
		for (KafkaPartition kafkaPartition : partitions) {
			TopicAndPartition topicAndPartition = kafkaPartition
					.getTopicAndPartition();
			latestOffsetInfo.put(topicAndPartition,
					partitionLatestOffsetRequestInfo);
			earliestOffsetInfo.put(topicAndPartition,
					partitionEarliestOffsetRequestInfo);
		}

		SimpleConsumer consumer = new SimpleConsumer(broker.host(),
				broker.port(), 10000, 1024 * 1024,
				kafka.api.OffsetRequest.DefaultClientId());
		OffsetResponse latestOffsetResponse = consumer
				.getOffsetsBefore(new OffsetRequest(latestOffsetInfo,
						kafka.api.OffsetRequest.CurrentVersion(),
						kafka.api.OffsetRequest.DefaultClientId()));
		OffsetResponse earliestOffsetResponse = consumer
				.getOffsetsBefore(new OffsetRequest(earliestOffsetInfo,
						kafka.api.OffsetRequest.CurrentVersion(),
						kafka.api.OffsetRequest.DefaultClientId()));
		consumer.close();

		for (KafkaPartition kafkaPartition : partitions) {
			long[] latestOffsets = latestOffsetResponse.offsets(
					kafkaPartition.getTopic(), kafkaPartition.getPartition());
			long[] earliestOffsets = earliestOffsetResponse.offsets(
					kafkaPartition.getTopic(), kafkaPartition.getPartition());
			if (latestOffsets != null && latestOffsets.length > 0) {
				kafkaPartition.setLatestOffset(latestOffsets[0]);
			}
			if (earliestOffsets != null && earliestOffsets.length > 0) {
				kafkaPartition.setEarliestOffset(earliestOffsets[0]);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static List<KafkaPartition> getPartitions(ZkClient zkClient,
			String... topics) {
		// topic=>{partition=>[broker]}
		Map<String, Map<Integer, List<Integer>>> partitionToBrokerId = readTopicsPartitionToBrokerId(
				zkClient, Arrays.asList(topics));
		// id=>broker
		Map<Integer, Broker> idToBroker = new HashMap<Integer, Broker>();
		for (Broker broker : getBrokerInfosInCluster(zkClient)) {
			idToBroker.put(broker.id(), broker);
		}
		List<KafkaPartition> list = new ArrayList<KafkaPartition>();
		for (Entry<String, Map<Integer, List<Integer>>> entry : partitionToBrokerId
				.entrySet()) {
			String topic = entry.getKey();
			Map<Integer, List<Integer>> topicPartitionMap = entry.getValue();

			for (Entry<Integer, List<Integer>> topicEntry : topicPartitionMap
					.entrySet()) {
				int partition = topicEntry.getKey();
				List<Integer> brokerIds = topicEntry.getValue();
				for (Integer brokerId : brokerIds) {
					Broker broker = idToBroker.get(brokerId);
					if (broker != null) {
						list.add(new KafkaPartition(broker, topic, partition));
					}
				}
			}

		}
		return list;
	}

	public static List<KafkaPartition> getPartitions2(ZkClient zkClient,
			String... topics) {
		List<TopicMetadata> topicMetadatas = getKafkaMetadata(zkClient);
		List<KafkaPartition> partitions = new ArrayList<KafkaPartition>();
		@SuppressWarnings("unchecked")
		Set<String> queryTopics = new HashSet<String>(Arrays.asList(topics));
		for (TopicMetadata topicMetadata : topicMetadatas) {
			String topic = topicMetadata.topic();
			if (!queryTopics.contains(topic)) {
				continue;
			}
			List<PartitionMetadata> partitionMetadatas = topicMetadata
					.partitionsMetadata();
			for (PartitionMetadata partitionMetadata : partitionMetadatas) {
				if (partitionMetadata.errorCode() != ErrorMapping.NoError()) {
					log.info("Skipping the creation of ETL request for Topic : "
							+ topicMetadata.topic()
							+ " and Partition : "
							+ partitionMetadata.partitionId()
							+ " Exception : "
							+ ErrorMapping.exceptionFor(partitionMetadata
									.errorCode()));
					continue;
				}
				kafka.cluster.Broker kafkaBroker = partitionMetadata.leader();
				Broker broker = new Broker(kafkaBroker.host(),
						kafkaBroker.port(), kafkaBroker.id());
				KafkaPartition partition = new KafkaPartition();
				partition.setBroker(broker);
				partition.setTopic(topic);
				partition.setPartition(partitionMetadata.partitionId());
				partitions.add(partition);
			}
		}

		return partitions;
	}

	public static List<TopicMetadata> getKafkaMetadata(ZkClient zkClient) {
		List<Broker> brokers = getBrokerInfosInCluster(zkClient);
		boolean fetchMetaDataSucceeded = false;
		int i = 0;
		List<TopicMetadata> topicMetadataList = null;
		Exception savedException = null;
		ArrayList<String> metaRequestTopics = new ArrayList<String>();
		while (i < brokers.size() && !fetchMetaDataSucceeded) {
			Broker broker = brokers.get(i);
			SimpleConsumer consumer = new SimpleConsumer(broker.host(),
					broker.port(), 10000, 1024 * 1024,
					kafka.api.OffsetRequest.DefaultClientId());
			log.info(String
					.format("Fetching metadata from broker %s with client id %s for %d topic(s) %s",
							brokers.get(i), consumer.clientId(),
							metaRequestTopics.size(), metaRequestTopics));
			try {
				topicMetadataList = consumer.send(
						new TopicMetadataRequest(metaRequestTopics))
						.topicsMetadata();
				fetchMetaDataSucceeded = true;
			} catch (Exception e) {
				savedException = e;
				log.warn(
						String.format(
								"Fetching topic metadata with client id %s for topics [%s] from broker [%s] failed",
								consumer.clientId(), metaRequestTopics,
								brokers.get(i)), e);
			} finally {
				consumer.close();
				i++;
			}
		}
		if (!fetchMetaDataSucceeded) {
			throw new RuntimeException("Failed to obtain metadata!",
					savedException);
		}
		return topicMetadataList;
	}

	public static void main(String[] args) {
		Hobbit2Configuration conf = new Hobbit2Configuration();
		ZkClient client = ZookeeperUtils.createZKClient(
				conf.getString(KafkaConfigKeys.KAFKA_ZOOKEEPER_CONNECT),
				conf.getInt(KafkaConfigKeys.KAFKA_TIME_OUT_MS),
				conf.getInt(KafkaConfigKeys.KAFKA_TIME_OUT_MS));
		Map<Broker, List<KafkaPartition>> map = getPartitionMeta(client,
				"t_playbgn_v2", "t_playbgn_v3");
		for (Entry<Broker, List<KafkaPartition>> entry : map.entrySet()) {
			List<KafkaPartition> list = entry.getValue();
			for (KafkaPartition kafkaPartition : list) {
				System.out.println(kafkaPartition);
			}
		}
		// ByteBufferMessageSet
		long start = System.currentTimeMillis();
		long offset = 36416803;
		SimpleConsumer consumer = new SimpleConsumer("data-slave1.voole.com",
				9092, 10000, 1024 * 1024, "");
		long total = 0;
		for (int i = 0; i < 10; i++) {
			ByteBufferMessageSet byteBufferMessageSet = fetch(consumer,
					"t_playbgn_v3", 2, offset, 1024 * 1024);
			offset += 2000;
			Iterator<MessageAndOffset> iterator = byteBufferMessageSet
					.iterator();
			while (iterator.hasNext()) {
				iterator.next();
				total++;
			}
		}
		consumer.close();
		System.out.println("total:" + total);
		System.out.println(System.currentTimeMillis() - start);

		start = System.currentTimeMillis();
		total = 0;
		consumer = new SimpleConsumer("data-slave1.voole.com", 9092, 10000,
				10 * 1024 * 1024, "");
		ByteBufferMessageSet byteBufferMessageSet = fetch(consumer,
				"t_playbgn_v3", 2, 36416803, 10 * 1024 * 1024);
		Iterator<MessageAndOffset> iterator = byteBufferMessageSet.iterator();
		while (iterator.hasNext()) {
			iterator.next();
			total++;
		}
		consumer.close();
		System.out.println("total:" + total);
		System.out.println(System.currentTimeMillis() - start);

		// Iterator<MessageAndOffset> iterator =
		// byteBufferMessageSet.iterator();
		// int i = 0;
		// while (iterator.hasNext()) {
		// MessageAndOffset messageAndOffset = iterator.next();
		// // System.out.println(messageAndOffset.offset());
		// // break;
		// i++;
		// }
		// System.out.println(i);

	}
}
