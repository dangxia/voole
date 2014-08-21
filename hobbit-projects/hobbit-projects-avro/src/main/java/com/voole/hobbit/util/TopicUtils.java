/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.util;

import java.util.HashSet;
import java.util.Set;

/**
 * @author XuehuiHe
 * @date 2014年8月18日
 */
public class TopicUtils {
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
}
