/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.avro.order.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;

import com.voole.hobbit2.kafka.avro.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.kafka.avro.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV3;
import com.voole.hobbit2.kafka.common.partition.Partitioner;
import com.voole.hobbit2.kafka.common.partition.PartitionerInitiator.DefaultPartitionerInitiator;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public class OrderTopicsUtils {
	public static final Map<String, Schema> topicToSchema = new HashMap<String, Schema>();
	public static final Set<String> topics = new HashSet<String>();
	public static final String TOPIC_ORDER_BGN_V2 = "t_playbgn_v2";
	public static final String TOPIC_ORDER_BGN_V3 = "t_playbgn_v3";
	public static final String TOPIC_ORDER_ALIVE_V2 = "t_playalive_v2";
	public static final String TOPIC_ORDER_ALIVE_V3 = "t_playalive_v3";
	public static final String TOPIC_ORDER_END_V2 = "t_playend_v2";
	public static final String TOPIC_ORDER_END_V3 = "t_playend_v3";

	static {
		topicToSchema.put(TOPIC_ORDER_BGN_V2,
				OrderPlayBgnReqV2.getClassSchema());
		topicToSchema.put(TOPIC_ORDER_BGN_V3,
				OrderPlayBgnReqV3.getClassSchema());

		topicToSchema.put(TOPIC_ORDER_END_V2,
				OrderPlayEndReqV2.getClassSchema());
		topicToSchema.put(TOPIC_ORDER_END_V3,
				OrderPlayEndReqV3.getClassSchema());

		topicToSchema.put(TOPIC_ORDER_ALIVE_V2,
				OrderPlayAliveReqV2.getClassSchema());
		topicToSchema.put(TOPIC_ORDER_ALIVE_V3,
				OrderPlayAliveReqV3.getClassSchema());

		topics.add(TOPIC_ORDER_BGN_V2);
		topics.add(TOPIC_ORDER_BGN_V3);
		topics.add(TOPIC_ORDER_END_V2);
		topics.add(TOPIC_ORDER_END_V3);
		topics.add(TOPIC_ORDER_ALIVE_V2);
		topics.add(TOPIC_ORDER_ALIVE_V3);
	}

	public static class OrderPartitionerInitiator extends
			DefaultPartitionerInitiator {

		@Override
		protected Map<String, Partitioner<?, ?, ?>> getTopicToPartitionerMap() {
			// TODO Auto-generated method stub
			return null;
		}

	}

}
