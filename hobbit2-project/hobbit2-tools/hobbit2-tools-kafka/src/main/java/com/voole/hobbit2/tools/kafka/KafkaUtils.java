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
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.voole.hobbit2.tools.kafka.KafkaJsonUtils.BrokerShadow;
import com.voole.hobbit2.tools.kafka.KafkaJsonUtils.PartitionsInfo;
import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;
import com.voole.hobbit2.tools.kafka.partition.PartitionState;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
public class KafkaUtils {
	private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);

	public static ByteBufferMessageSet fetch(SimpleConsumer consumer,
			TopicPartition partition, long offset, int fetchSize) {
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
		List<Integer> borkerIds = getBrokerIdsInCluster(zkClient);
		List<Broker> list = new ArrayList<Broker>();
		for (Integer brokerId : borkerIds) {
			Optional<Broker> broker = getBrokerInfo(zkClient, brokerId);
			if (broker.isPresent()) {
				list.add(broker.get());
			} else {
				log.warn("broker info not found for broker id:" + brokerId);
			}
		}
		return list;
	}

	public static List<Integer> getBrokerIdsInCluster(ZkClient zkClient) {
		Optional<List<String>> children = getChildrenParentMayNotExist(
				zkClient, ZkUtils.BrokerIdsPath());
		if (!children.isPresent()) {
			throw new RuntimeException("kafka brokers not found in zookeeper");
		}
		return Lists.newArrayList(Iterators.transform(
				children.get().iterator(), new Function<String, Integer>() {
					@Override
					public Integer apply(String input) {
						return Integer.parseInt(input);
					}
				}));
	}

	public static Optional<Broker> getBrokerInfo(ZkClient zkClient,
			Integer brokerId) {
		Optional<String> data = readDataMaybeNull(zkClient,
				ZkUtils.BrokerIdsPath() + "/" + brokerId);
		if (data.isPresent()) {
			BrokerShadow brokerShadow = KafkaJsonUtils.toBrokerShadow(data
					.get());
			return Optional.of(new Broker(brokerShadow.host, brokerShadow.port,
					brokerId));
		}
		return Optional.absent();
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
			Optional<String> data = readDataMaybeNull(zkClient, topicPath);
			if (data.isPresent()) {
				PartitionsInfo info = KafkaJsonUtils.toPartitionsInfo(data
						.get());
				result.put(topic, info.partitions);
			} else {
				log.warn("not found topic partition info for topic:" + topic);
			}
		}
		return result;
	}

	public static Map<Broker, List<PartitionState>> getPartitionState(
			ZkClient zkClient, String... topics) {
		List<BrokerAndTopicPartition> partitions = getPartitions2(zkClient,
				topics);
		Map<Broker, List<BrokerAndTopicPartition>> map = new HashMap<Broker, List<BrokerAndTopicPartition>>();
		for (BrokerAndTopicPartition kafkaPartition : partitions) {
			Broker broker = kafkaPartition.getBroker();
			List<BrokerAndTopicPartition> brokerPartitions = null;
			if (!map.containsKey(broker)) {
				brokerPartitions = new ArrayList<BrokerAndTopicPartition>();
				map.put(broker, brokerPartitions);
			} else {
				brokerPartitions = map.get(broker);
			}
			brokerPartitions.add(kafkaPartition);
		}
		Map<Broker, List<PartitionState>> result = new HashMap<Broker, List<PartitionState>>();
		for (Entry<Broker, List<BrokerAndTopicPartition>> entry : map
				.entrySet()) {
			result.put(entry.getKey(),
					fillKafkaPartitionOffsets(entry.getKey(), entry.getValue()));
		}
		return result;
	}

	public static List<PartitionState> fillKafkaPartitionOffsets(Broker broker,
			List<BrokerAndTopicPartition> partitions) {
		Map<TopicAndPartition, PartitionOffsetRequestInfo> latestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		Map<TopicAndPartition, PartitionOffsetRequestInfo> earliestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		// Latest Offset
		PartitionOffsetRequestInfo partitionLatestOffsetRequestInfo = new PartitionOffsetRequestInfo(
				kafka.api.OffsetRequest.LatestTime(), 1);
		// Earliest Offset
		PartitionOffsetRequestInfo partitionEarliestOffsetRequestInfo = new PartitionOffsetRequestInfo(
				kafka.api.OffsetRequest.EarliestTime(), 1);
		for (BrokerAndTopicPartition kafkaPartition : partitions) {
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
		List<PartitionState> result = new ArrayList<PartitionState>();
		try {
			OffsetResponse latestOffsetResponse = consumer
					.getOffsetsBefore(new OffsetRequest(latestOffsetInfo,
							kafka.api.OffsetRequest.CurrentVersion(),
							kafka.api.OffsetRequest.DefaultClientId()));
			OffsetResponse earliestOffsetResponse = consumer
					.getOffsetsBefore(new OffsetRequest(earliestOffsetInfo,
							kafka.api.OffsetRequest.CurrentVersion(),
							kafka.api.OffsetRequest.DefaultClientId()));

			for (BrokerAndTopicPartition kafkaPartition : partitions) {
				long[] latestOffsets = latestOffsetResponse.offsets(
						kafkaPartition.getPartition().getTopic(),
						kafkaPartition.getPartition().getPartition());
				long[] earliestOffsets = earliestOffsetResponse.offsets(
						kafkaPartition.getPartition().getTopic(),
						kafkaPartition.getPartition().getPartition());
				if ((latestOffsets == null || latestOffsets.length == 0)
						&& (earliestOffsets == null || earliestOffsets.length == 0)) {
					continue;
				}
				PartitionState state = kafkaPartition.createState();
				if (latestOffsets != null && latestOffsets.length > 0) {
					state.setLatestOffset(latestOffsets[0]);
				}
				if (earliestOffsets != null && earliestOffsets.length > 0) {
					state.setEarliestOffset(earliestOffsets[0]);
				}
				// TODO
				// if (state.getLatestOffset() > 1000) {
				// state.setOffset(state.getLatestOffset() - 1000);
				// }
				state.setOffset(state.getEarliestOffset() - 1);
				result.add(state);
			}
		} catch (Exception e) {
			Throwables.propagate(e);
		} finally {
			consumer.close();
		}

		return result;

	}

	@SuppressWarnings("unchecked")
	@Deprecated
	public static List<BrokerAndTopicPartition> getPartitions(
			ZkClient zkClient, String... topics) {
		// topic=>{partition=>[broker]}
		Map<String, Map<Integer, List<Integer>>> partitionToBrokerId = readTopicsPartitionToBrokerId(
				zkClient, Arrays.asList(topics));
		// id=>broker
		Map<Integer, Broker> idToBroker = new HashMap<Integer, Broker>();
		for (Broker broker : getBrokerInfosInCluster(zkClient)) {
			idToBroker.put(broker.id(), broker);
		}
		List<BrokerAndTopicPartition> list = new ArrayList<BrokerAndTopicPartition>();
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
						list.add(new BrokerAndTopicPartition(broker, topic,
								partition));
					}
				}
			}

		}
		return list;
	}

	public static List<BrokerAndTopicPartition> getPartitions2(
			ZkClient zkClient, String... topics) {
		List<TopicMetadata> topicMetadatas = getKafkaMetadata(zkClient);
		List<BrokerAndTopicPartition> partitions = new ArrayList<BrokerAndTopicPartition>();
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
				BrokerAndTopicPartition partition = new BrokerAndTopicPartition(
						broker, topic, partitionMetadata.partitionId());
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

}
