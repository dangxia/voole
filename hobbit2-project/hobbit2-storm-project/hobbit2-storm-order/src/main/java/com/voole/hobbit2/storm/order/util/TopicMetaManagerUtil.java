/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.voole.hobbit2.camus.OrderTopicMetaRegister;
import com.voole.hobbit2.camus.api.TopicMeta;
import com.voole.hobbit2.camus.api.TopicMetaManager;
import com.voole.hobbit2.camus.api.TopicMetaRegister;
import com.voole.hobbit2.camus.api.transform.TransformException;
import com.voole.hobbit2.storm.order.StormOrderMetaConfigs;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class TopicMetaManagerUtil {
	private static volatile TopicMetaManager _inst;

	public static TopicMetaManager get() throws TransformException {
		if (_inst == null) {
			create();
		}
		return _inst;
	}

	private synchronized static void create() throws TransformException {
		if (_inst != null) {
			return;
		}
		TopicMetaManager inst = new TopicMetaManager();
		Set<String> topics = new HashSet<String>(
				StormOrderMetaConfigs.getWhiteTopics());
		final List<TopicMeta> topicMetas = new ArrayList<TopicMeta>();
		for (TopicMeta topicMeta : new OrderTopicMetaRegister().getTopicMetas()) {
			if (topics.contains(topicMeta.getTopic())) {
				topicMetas.add(topicMeta);
			}
		}
		inst.register(new TopicMetaRegister() {
			@Override
			public List<TopicMeta> getTopicMetas() throws TransformException {
				return topicMetas;
			}
		});

		_inst = inst;
	}

}
