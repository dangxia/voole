/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.voole.hibbit.storm.kafka.partition.HostPort;
import com.voole.hibbit.storm.kafka.partition.KafkaPartition;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
public class KafkaUtils {
	private static Gson gson;
	static {
		GsonBuilder builder = new GsonBuilder();
		gson = builder.create();
	}

	public static String readDataMaybeNull(ZkClient zkClient, String path) {
		try {
			return zkClient.readData(path);
		} catch (Exception e) {
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static List<String> getChildrenParentMayNotExist(ZkClient zkClient,
			String path) {
		try {
			return zkClient.getChildren(path);
		} catch (Exception e) {
		}
		return Collections.EMPTY_LIST;
	}

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

	public static long getOffsetsBefore(SimpleConsumer consumer,
			KafkaPartition partition, long time) {
		return getOffsetsBefore(consumer, partition.getTopic(),
				partition.getPartition(), time);
	}

	public static long getOffsetsBefore(SimpleConsumer consumer, String topic,
			int partition, long time) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(time,
				1));
		OffsetRequest request = new OffsetRequest(requestInfo,
				kafka.api.OffsetRequest.CurrentVersion(),
				kafka.api.OffsetRequest.DefaultClientId());
		OffsetResponse response = consumer.getOffsetsBefore(request);
		long[] offsets = response.offsets(topic, partition);
		if (offsets != null && offsets.length > 0) {
			return offsets[0];
		}
		return 0;

	}

	public static Map<Integer, Broker> getAllBrokerMapInCluster(
			ZkClient zkClient) {
		Map<Integer, Broker> map = new HashMap<Integer, Broker>();
		for (Broker broker : getAllBrokersInCluster(zkClient)) {
			map.put(broker.id(), broker);
		}
		return map;
	}

	public static List<Broker> getAllBrokersInCluster(ZkClient zkClient) {
		List<String> borkerIds = getChildrenParentMayNotExist(zkClient,
				ZkUtils.BrokerIdsPath());
		List<Broker> list = new ArrayList<Broker>();
		for (String brokerId : borkerIds) {
			Broker broker = getBrokerInfo(zkClient, brokerId);
			if (broker != null) {
				list.add(broker);
			}
		}

		return list;
	}

	public static Broker getBrokerInfo(ZkClient zkClient, String brokerId) {
		String data = readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath() + "/"
				+ brokerId);
		if (data != null && data.length() > 0) {
			HostPort hostport = gson.fromJson(data, HostPort.class);
			return new Broker(Integer.parseInt(brokerId), hostport.getHost(),
					hostport.getPort());
		}

		return null;
	}

	public static Map<Integer, List<Integer>> getPartitionAssignmentForTopic(
			ZkClient zkClient, String topic) {
		Set<String> topics = new HashSet<String>();
		topics.add(topic);
		Map<String, Map<Integer, List<Integer>>> map = getPartitionAssignmentForTopics(
				zkClient, topics);
		return map.get(topic);
	}

	public static Map<String, Map<Integer, List<Integer>>> getPartitionAssignmentForTopics(
			ZkClient zkClient, Collection<String> topics) {
		Map<String, Map<Integer, List<Integer>>> result = new HashMap<String, Map<Integer, List<Integer>>>();
		for (String topic : topics) {
			String topicPath = ZkUtils.getTopicPath(topic);
			String data = readDataMaybeNull(zkClient, topicPath);
			if (data != null && data.length() > 0) {
				PartitionsInfo info = gson.fromJson(data, PartitionsInfo.class);
				result.put(topic, info.partitions);
			}
		}
		return result;
	}

	public static List<KafkaPartition> getPartitions(ZkClient zkClient,
			String topic) {
		Map<Integer, List<Integer>> partitionToBrokerId = getPartitionAssignmentForTopic(
				zkClient, topic);
		Map<Integer, Broker> idToBroker = getAllBrokerMapInCluster(zkClient);
		List<KafkaPartition> list = new ArrayList<KafkaPartition>();
		for (Entry<Integer, List<Integer>> entry : partitionToBrokerId
				.entrySet()) {
			int partition = entry.getKey();
			List<Integer> brokerIds = entry.getValue();
			for (Integer brokerId : brokerIds) {
				Broker broker = idToBroker.get(brokerId);
				if (broker != null) {
					list.add(new KafkaPartition(broker.host(), broker.port(),
							topic, partition));
				}
			}
		}
		Collections.sort(list, new Comparator<KafkaPartition>() {
			@Override
			public int compare(KafkaPartition o1, KafkaPartition o2) {
				return o1.getPartition() - o2.getPartition();
			}
		});
		return list;
	}

	public static class PartitionsInfo implements Serializable {
		public Map<Integer, List<Integer>> partitions;
	}

}
