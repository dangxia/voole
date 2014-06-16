/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.voole.hobbit.kafka.TopicProtoClassUtils;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

/**
 * @author XuehuiHe
 * @date 2014年5月29日
 */
public class TestKafka {
	public static void main2(String[] args) {
		System.out.println(kafka.api.OffsetRequest.LatestTime());
	}

	public static void main(String[] args) throws UnsupportedEncodingException {
		String topic = TopicProtoClassUtils.ORDER_BGN_V2;
		int partition = 2;
		SimpleConsumer consumer = new SimpleConsumer("data-slave1.voole.com",
				9092, 1000, 10000, "sdsdsd");
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				kafka.api.OffsetRequest.LatestTime(), 1));
		OffsetRequest request = new OffsetRequest(requestInfo,
				kafka.api.OffsetRequest.CurrentVersion(),
				kafka.api.OffsetRequest.DefaultClientId());
		OffsetResponse response = consumer.getOffsetsBefore(request);
		long[] offsets = response.offsets(topic, partition);
		for (long l : offsets) {
			System.out.println(l);
		}
		FetchRequestBuilder requestBuilder = new FetchRequestBuilder();
		kafka.api.FetchRequest fetchRequest = requestBuilder.addFetch(topic,
				partition, 1, 5000).build();
		FetchResponse fetchResponse = consumer.fetch(fetchRequest);
		Iterator<MessageAndOffset> iterator = fetchResponse.messageSet(topic,
				partition).iterator();
		while (iterator.hasNext()) {
			MessageAndOffset msg = iterator.next();
			System.out.println("offset:" + msg.offset());

			ByteBuffer payload = msg.message().payload();

			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);

			System.out.println("msg:" + new String(bytes, "UTF-8"));
		}
	}
}
