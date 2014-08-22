/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.avro.order.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;

import com.voole.hobbit2.kafka.avro.AvroCtypeKafkaTransformer;
import com.voole.hobbit2.kafka.avro.AvroTopicTransformMeta;
import com.voole.hobbit2.kafka.avro.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.kafka.avro.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV3;
import com.voole.hobbit2.kafka.common.meta.KafkaTopicTransformMetas;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public class OrderTopicsUtils {
	private static final Map<String, Schema> topicToSchema = new HashMap<String, Schema>();

	static {
		topicToSchema.put("t_playbgn_v2", OrderPlayBgnReqV2.getClassSchema());
		topicToSchema.put("t_playbgn_v3", OrderPlayBgnReqV3.getClassSchema());

		topicToSchema.put("t_playend_v2", OrderPlayEndReqV2.getClassSchema());
		topicToSchema.put("t_playend_v3", OrderPlayEndReqV3.getClassSchema());

		topicToSchema.put("t_playalive_v2",
				OrderPlayAliveReqV2.getClassSchema());
		topicToSchema.put("t_playalive_v3",
				OrderPlayAliveReqV3.getClassSchema());
	}

	public static void registerTransformMetas() {
		for (Entry<String, Schema> entry : topicToSchema.entrySet()) {
			AvroTopicTransformMeta<AvroCtypeKafkaTransformer> meta = new AvroTopicTransformMeta<AvroCtypeKafkaTransformer>();
			meta.setTopic(entry.getKey());
			meta.setTransformerClass(AvroCtypeKafkaTransformer.class);
			meta.setSchema(entry.getValue());
			KafkaTopicTransformMetas.register(meta);
		}
	}
}
