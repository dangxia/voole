/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.transformer;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit.transformer.KafkaProtoBuffTransformer;
import com.voole.hobbit.transformer.KafkaProtoBuffTransformerFactory;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public class OrderTopicsTransformer {
	private final Map<String, KafkaProtoBuffTransformer<?>> transformerMap;
	private final static Logger logger = LoggerFactory
			.getLogger(OrderTopicsTransformer.class);

	public OrderTopicsTransformer(String... topics) {
		transformerMap = new HashMap<String, KafkaProtoBuffTransformer<?>>();
		try {
			for (String topic : topics) {
				transformerMap.put(topic,
						KafkaProtoBuffTransformerFactory.getTransformer(topic));
			}
		} catch (Exception e) {
			logger.error("KafkaTerminalProtoBuffTransformerFunction init fail",
					e);
		}
	}

	public Object transformer(String topic, byte[] bytes) {
		try {
			return transformerMap.get(topic).transform(bytes);
		} catch (RuntimeException e) {
			logger.warn("transformer error:topic\t" + topic + "\tmsg\t"
					+ e.getMessage());
		} catch (Exception e) {
			logger.warn("transformer error", e);
		}
		return null;
	}
}
