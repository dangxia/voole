/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus;

import org.apache.avro.Schema;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV2;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV3;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public class OrderTopicsUtils {
	public static final BiMap<String, Schema> topicBiSchema;
	public static final BiMap<String, Class<?>> topicBiClazz;

	public static final String TOPIC_ORDER_BGN_V2 = "t_playbgn_v2";
	public static final String TOPIC_ORDER_BGN_V3 = "t_playbgn_v3";
	public static final String TOPIC_ORDER_ALIVE_V2 = "t_playalive_v2";
	public static final String TOPIC_ORDER_ALIVE_V3 = "t_playalive_v3";
	public static final String TOPIC_ORDER_END_V2 = "t_playend_v2";
	public static final String TOPIC_ORDER_END_V3 = "t_playend_v3";

	static {
		BiMap<String, Schema> topicToSchema = HashBiMap.create(6);
		BiMap<String, Class<?>> topicToClazz = HashBiMap.create(6);

		add(topicToSchema, topicToClazz, TOPIC_ORDER_BGN_V2,
				OrderPlayBgnReqV2.class, OrderPlayBgnReqV2.getClassSchema());
		add(topicToSchema, topicToClazz, TOPIC_ORDER_BGN_V3,
				OrderPlayBgnReqV3.class, OrderPlayBgnReqV3.getClassSchema());

		add(topicToSchema, topicToClazz, TOPIC_ORDER_END_V2,
				OrderPlayEndReqV2.class, OrderPlayEndReqV2.getClassSchema());
		add(topicToSchema, topicToClazz, TOPIC_ORDER_END_V3,
				OrderPlayEndReqV3.class, OrderPlayEndReqV3.getClassSchema());

		add(topicToSchema, topicToClazz, TOPIC_ORDER_ALIVE_V2,
				OrderPlayAliveReqV2.class, OrderPlayAliveReqV2.getClassSchema());
		add(topicToSchema, topicToClazz, TOPIC_ORDER_ALIVE_V3,
				OrderPlayAliveReqV3.class, OrderPlayAliveReqV3.getClassSchema());

		topicBiSchema = ImmutableBiMap.copyOf(topicToSchema);
		topicBiClazz = ImmutableBiMap.copyOf(topicToClazz);
	}

	private static void add(BiMap<String, Schema> topicToSchema,
			BiMap<String, Class<?>> topicToClazz, String topic, Class<?> clazz,
			Schema schema) {
		topicToSchema.put(topic, schema);
		topicToClazz.put(topic, clazz);
	}

	public static boolean containsTopic(String topic) {
		return topicBiSchema.keySet().contains(topic);
	}
}
