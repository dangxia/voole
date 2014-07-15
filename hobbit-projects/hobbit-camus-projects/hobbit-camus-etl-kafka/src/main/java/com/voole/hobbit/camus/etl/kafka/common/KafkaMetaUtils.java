/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.camus.etl.kafka.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

import com.voole.hobbit.camus.etl.kafka.CamusConfigs;
import com.voole.hobbit.camus.etl.kafka.CamusJob;
import com.voole.hobbit.camus.etl.kafka.coders.MessageDecoderFactory;

/**
 * @author XuehuiHe
 * @date 2014年7月11日
 */
public class KafkaMetaUtils {
	private final static Logger log = Logger.getLogger(KafkaMetaUtils.class);

	/**
	 * Gets the latest offsets and create the requests as needed
	 * 
	 * @param context
	 * @param offsetRequestInfo
	 * @return
	 */
	public static ArrayList<EtlRequest> fetchLatestOffsetAndCreateEtlRequests(
			JobContext context,
			HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo) {

		ArrayList<EtlRequest> finalRequests = new ArrayList<EtlRequest>();
		for (LeaderInfo leader : offsetRequestInfo.keySet()) {
			SimpleConsumer consumer = createConsumer(context, leader);
			// Latest Offset
			PartitionOffsetRequestInfo partitionLatestOffsetRequestInfo = new PartitionOffsetRequestInfo(
					kafka.api.OffsetRequest.LatestTime(), 1);
			// Earliest Offset
			PartitionOffsetRequestInfo partitionEarliestOffsetRequestInfo = new PartitionOffsetRequestInfo(
					kafka.api.OffsetRequest.EarliestTime(), 1);
			Map<TopicAndPartition, PartitionOffsetRequestInfo> latestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			Map<TopicAndPartition, PartitionOffsetRequestInfo> earliestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo
					.get(leader);
			for (TopicAndPartition topicAndPartition : topicAndPartitions) {
				latestOffsetInfo.put(topicAndPartition,
						partitionLatestOffsetRequestInfo);
				earliestOffsetInfo.put(topicAndPartition,
						partitionEarliestOffsetRequestInfo);
			}

			OffsetResponse latestOffsetResponse = consumer
					.getOffsetsBefore(new OffsetRequest(latestOffsetInfo,
							kafka.api.OffsetRequest.CurrentVersion(),
							CamusConfigs.getKafkaClientName(context)));
			OffsetResponse earliestOffsetResponse = consumer
					.getOffsetsBefore(new OffsetRequest(earliestOffsetInfo,
							kafka.api.OffsetRequest.CurrentVersion(),
							CamusConfigs.getKafkaClientName(context)));
			consumer.close();
			for (TopicAndPartition topicAndPartition : topicAndPartitions) {
				long latestOffset = latestOffsetResponse.offsets(
						topicAndPartition.topic(),
						topicAndPartition.partition())[0];
				long earliestOffset = earliestOffsetResponse.offsets(
						topicAndPartition.topic(),
						topicAndPartition.partition())[0];
				EtlRequest etlRequest = new EtlRequest(context,
						topicAndPartition.topic(), Integer.toString(leader
								.getLeaderId()), topicAndPartition.partition(),
						leader.getUri());
				etlRequest.setLatestOffset(latestOffset);
				etlRequest.setEarliestOffset(earliestOffset);
				finalRequests.add(etlRequest);
			}
		}
		Collections.sort(finalRequests, new Comparator<EtlRequest>() {
			public int compare(EtlRequest r1, EtlRequest r2) {
				return r1.getTopic().compareTo(r2.getTopic());
			}
		});
		return finalRequests;

	}

	/**
	 * Gets the metadata from Kafka
	 * 
	 * @param context
	 * @return
	 */
	public static List<TopicMetadata> getKafkaMetadata(JobContext context) {
		CamusJob.startTiming("kafkaSetupTime");
		ArrayList<String> metaRequestTopics = new ArrayList<String>();
		String brokerString = CamusConfigs.getKafkaBrokers(context);
		List<String> brokers = Arrays.asList(brokerString.split("\\s*,\\s*"));
		Collections.shuffle(brokers);
		boolean fetchMetaDataSucceeded = false;
		int i = 0;
		List<TopicMetadata> topicMetadataList = null;
		Exception savedException = null;
		while (i < brokers.size() && !fetchMetaDataSucceeded) {
			SimpleConsumer consumer = createConsumer(context, brokers.get(i));
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
		CamusJob.stopTiming("kafkaSetupTime");
		return filterTopics(context, topicMetadataList);
	}

	private static List<TopicMetadata> filterTopics(JobContext context,
			List<TopicMetadata> topicMetadataList) {
		topicMetadataList = filterWhitelistTopics(context, topicMetadataList);
		topicMetadataList = filterBlacklistTopics(context, topicMetadataList);
		topicMetadataList = filterNoDecoderTopics(context, topicMetadataList);
		return topicMetadataList;
	}

	// Filter all blacklist topics
	private static List<TopicMetadata> filterBlacklistTopics(
			JobContext context, List<TopicMetadata> topicMetadataList) {
		ArrayList<TopicMetadata> filteredTopics = new ArrayList<TopicMetadata>();
		HashSet<String> blackListTopics = new HashSet<String>(
				Arrays.asList(CamusConfigs.getKafkaBlacklistTopic(context)));
		String regex = "";
		if (!blackListTopics.isEmpty()) {
			regex = createTopicRegEx(blackListTopics);
		}
		for (TopicMetadata topicMetadata : topicMetadataList) {
			if (Pattern.matches(regex, topicMetadata.topic())) {
				log.info("Discarding topic (blacklisted): "
						+ topicMetadata.topic());
			} else {
				filteredTopics.add(topicMetadata);
			}
		}
		return filteredTopics;
	}

	private static List<TopicMetadata> filterNoDecoderTopics(
			JobContext context, List<TopicMetadata> topicMetadataList) {
		ArrayList<TopicMetadata> filteredTopics = new ArrayList<TopicMetadata>();
		for (TopicMetadata topicMetadata : topicMetadataList) {
			if (!createMessageDecoder(context, topicMetadata.topic())) {
				log.info("Discarding topic (Decoder generation failed) : "
						+ topicMetadata.topic());
			} else {
				filteredTopics.add(topicMetadata);
			}
		}
		return filteredTopics;
	}

	private static boolean createMessageDecoder(JobContext context, String topic) {
		try {
			MessageDecoderFactory.createMessageDecoder(context, topic);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Filter any white list topics
	 * 
	 * @param context
	 * @param topicMetadataList
	 * @return
	 */
	private static List<TopicMetadata> filterWhitelistTopics(
			JobContext context, List<TopicMetadata> topicMetadataList) {
		HashSet<String> whiteListTopics = new HashSet<String>(
				Arrays.asList(CamusConfigs.getKafkaWhitelistTopic(context)));
		if (whiteListTopics.isEmpty()) {
			return topicMetadataList;
		}
		ArrayList<TopicMetadata> filteredTopics = new ArrayList<TopicMetadata>();
		String regex = createTopicRegEx(whiteListTopics);
		for (TopicMetadata topicMetadata : topicMetadataList) {
			if (Pattern.matches(regex, topicMetadata.topic())) {
				filteredTopics.add(topicMetadata);
			} else {
				log.info("Discrading no white topic : " + topicMetadata.topic());
			}
		}
		return filteredTopics;
	}

	private static String createTopicRegEx(HashSet<String> topicsSet) {
		String regex = "";
		StringBuilder stringbuilder = new StringBuilder();
		for (String whiteList : topicsSet) {
			stringbuilder.append(whiteList);
			stringbuilder.append("|");
		}
		regex = "(" + stringbuilder.substring(0, stringbuilder.length() - 1)
				+ ")";
		Pattern.compile(regex);
		return regex;
	}

	private static SimpleConsumer createConsumer(JobContext context,
			LeaderInfo leader) {
		return new SimpleConsumer(leader.getUri().getHost(), leader.getUri()
				.getPort(), CamusConfigs.getKafkaTimeoutValue(context),
				CamusConfigs.getKafkaBufferSize(context),
				CamusConfigs.getKafkaClientName(context));
	}

	private static SimpleConsumer createConsumer(JobContext context,
			String broker) {
		String[] hostPort = broker.split(":");
		SimpleConsumer consumer = new SimpleConsumer(hostPort[0],
				Integer.valueOf(hostPort[1]),
				CamusConfigs.getKafkaTimeoutValue(context),
				CamusConfigs.getKafkaBufferSize(context),
				CamusConfigs.getKafkaClientName(context));
		return consumer;
	}
}
