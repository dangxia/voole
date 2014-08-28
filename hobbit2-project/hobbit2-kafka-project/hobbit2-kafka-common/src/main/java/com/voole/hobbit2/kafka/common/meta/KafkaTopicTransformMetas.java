/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.common.meta;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.voole.hobbit2.kafka.common.KafkaTransformer;
import com.voole.hobbit2.kafka.common.exception.KafkaTransformException;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public class KafkaTopicTransformMetas {

	private final Map<String, KafkaTopicTransformMeta<?, ?>> topicToTransformMetaMap;
	private final Map<String, KafkaTransformer<?>> topicToTransformerMap;

	public KafkaTopicTransformMetas() {
		topicToTransformMetaMap = new ConcurrentHashMap<String, KafkaTopicTransformMeta<?, ?>>();
		topicToTransformerMap = new ConcurrentHashMap<String, KafkaTransformer<?>>();
	}

	public KafkaTopicTransformMetas(
			KafkaTopicTransformMetaInitiator... initiators) {
		this();
		for (KafkaTopicTransformMetaInitiator kafkaTopicTransformMetaInitiator : initiators) {
			kafkaTopicTransformMetaInitiator.initialize(this);
		}
	}

	public void register(KafkaTopicTransformMeta<?, ?> meta) {
		Preconditions.checkNotNull(meta, "meta is null");
		Preconditions.checkArgument(!Strings.isNullOrEmpty(meta.getTopic()),
				"topic is empty");
		Preconditions.checkNotNull(meta.getTransformerClass(),
				"TransformerClass is null");
		topicToTransformMetaMap.put(meta.getTopic(), meta);
		try {
			topicToTransformerMap
					.put(meta.getTopic(), meta.createTransformer());
		} catch (KafkaTransformException e) {
			Throwables.propagate(e);
		}
	}

	public void unregister(String topic) {
		topicToTransformMetaMap.remove(topic);
		topicToTransformerMap.remove(topic);
	}

	public KafkaTopicTransformMeta<?, ?> get(String topic) {
		if (topicToTransformMetaMap.containsKey(topic)) {
			return topicToTransformMetaMap.get(topic);
		} else {
			throw new RuntimeException("topic:" + topic
					+ " KafkaTopicTransformMeta not found");
		}
	}

	public KafkaTransformer<?> getTransformer(String topic) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(topic),
				"topic is empty");
		if (topicToTransformerMap.containsKey(topic)) {
			return topicToTransformerMap.get(topic);
		} else {
			throw new RuntimeException("topic:" + topic
					+ " KafkaTransformer not found");
		}
	}

}
