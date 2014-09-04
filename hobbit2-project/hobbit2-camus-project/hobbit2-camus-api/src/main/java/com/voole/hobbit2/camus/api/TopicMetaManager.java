/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.voole.hobbit2.camus.api.transform.TransformException;

/**
 * @author XuehuiHe
 * @date 2014年9月4日
 */
public class TopicMetaManager {
	private Map<String, TopicMeta> topicToMeta;

	public TopicMetaManager() {
		topicToMeta = new HashMap<String, TopicMeta>();
	}

	public TopicMeta findTopicMeta(String topic) {
		Preconditions.checkNotNull(topic);
		if (topicToMeta.containsKey(topic)) {
			return topicToMeta.get(topic);
		}
		throw new UnsupportedOperationException(
				"TopicMeta not found for topic:" + topic);
	}

	public void register(TopicMetaRegister... metaRegisters)
			throws TransformException {
		for (TopicMetaRegister topicMetaRegister : metaRegisters) {
			for (TopicMeta meta : topicMetaRegister.getTopicMetas()) {
				register(meta);
			}
		}
		encapsulate();
	}

	protected void register(TopicMeta meta) {
		Preconditions.checkNotNull(meta);
		Preconditions.checkNotNull(meta.getTopic());
		topicToMeta.put(meta.getTopic(), meta);
	}

	protected void encapsulate() {
		topicToMeta = ImmutableMap.copyOf(topicToMeta);
	}
}
