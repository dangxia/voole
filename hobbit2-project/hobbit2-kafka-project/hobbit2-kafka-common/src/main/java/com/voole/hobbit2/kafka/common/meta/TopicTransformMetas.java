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
public class TopicTransformMetas {

	private final Map<String, TopicTransformMeta<?, ?>> topicToTransformMetaMap;
	private final Map<String, KafkaTransformer<?>> topicToTransformerMap;

	public TopicTransformMetas() {
		topicToTransformMetaMap = new ConcurrentHashMap<String, TopicTransformMeta<?, ?>>();
		topicToTransformerMap = new ConcurrentHashMap<String, KafkaTransformer<?>>();
	}

	public TopicTransformMetas(
			TopicTransformMetaRegister... initiators) {
		this();
		for (TopicTransformMetaRegister kafkaTopicTransformMetaInitiator : initiators) {
			kafkaTopicTransformMetaInitiator.register(this);
		}
	}

	public void register(TopicTransformMeta<?, ?> meta) {
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

	public TopicTransformMeta<?, ?> get(String topic) {
		if (topicToTransformMetaMap.containsKey(topic)) {
			return topicToTransformMetaMap.get(topic);
		} else {
			throw new RuntimeException("topic:" + topic
					+ " TopicTransformMeta not found");
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
