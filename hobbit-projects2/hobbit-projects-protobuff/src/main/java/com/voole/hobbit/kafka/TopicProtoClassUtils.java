/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV3;
import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV3;
import com.voole.hobbit.proto.TerminalPB.OrderPlayEndReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayEndReqV3;

/**
 * @author XuehuiHe
 * @date 2014年6月4日
 */
public class TopicProtoClassUtils {
	// order topics
	public static final String ORDER_BGN_V2 = "t_playbgn_v2";
	public static final String ORDER_BGN_V3 = "t_playbgn_v3";
	public static final String ORDER_END_V2 = "t_playend_v2";
	public static final String ORDER_END_V3 = "t_playend_v3";
	public static final String ORDER_ALIVE_V2 = "t_playalive_v2";
	public static final String ORDER_ALIVE_V3 = "t_playalive_v3";

	public static final Set<String> ORDER_TOPICS = new HashSet<String>();
	static {
		ORDER_TOPICS.add(ORDER_BGN_V2);
		ORDER_TOPICS.add(ORDER_BGN_V3);
		ORDER_TOPICS.add(ORDER_END_V2);
		ORDER_TOPICS.add(ORDER_END_V3);
		ORDER_TOPICS.add(ORDER_ALIVE_V2);
		ORDER_TOPICS.add(ORDER_ALIVE_V3);
	}

	public static final Map<String, Class<?>> topicMapProtoClass = new HashMap<String, Class<?>>();

	static {
		topicMapProtoClass.put(ORDER_BGN_V2, OrderPlayBgnReqV2.class);
		topicMapProtoClass.put(ORDER_BGN_V3, OrderPlayBgnReqV3.class);

		topicMapProtoClass.put(ORDER_END_V2, OrderPlayEndReqV2.class);
		topicMapProtoClass.put(ORDER_END_V3, OrderPlayEndReqV3.class);

		topicMapProtoClass.put(ORDER_ALIVE_V2, OrderPlayAliveReqV2.class);
		topicMapProtoClass.put(ORDER_ALIVE_V3, OrderPlayAliveReqV3.class);
	}

	public static OrderLogType getType(String topic) {
		if (topic.equals(ORDER_BGN_V2) || topic.equals(ORDER_BGN_V3)) {
			return OrderLogType.BGN;
		} else if (topic.equals(ORDER_END_V2) || topic.equals(ORDER_END_V3)) {
			return OrderLogType.END;
		} else if (topic.equals(ORDER_ALIVE_V2) || topic.equals(ORDER_ALIVE_V3)) {
			return OrderLogType.ALIVE;
		}
		return null;
	}

	public static enum OrderLogType {
		BGN, END, ALIVE;
	}

}
