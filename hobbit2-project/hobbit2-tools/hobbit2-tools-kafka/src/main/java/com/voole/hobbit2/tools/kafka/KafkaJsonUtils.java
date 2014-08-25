/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.kafka;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public class KafkaJsonUtils {
	private static Gson gson;
	static {
		GsonBuilder builder = new GsonBuilder();
		gson = builder.create();
	}

	public static BrokerShadow toBrokerShadow(String data) {
		return gson.fromJson(data, BrokerShadow.class);
	}

	public static PartitionsInfo toPartitionsInfo(String data) {
		return gson.fromJson(data, PartitionsInfo.class);
	}

	public static class PartitionsInfo implements Serializable {
		public Map<Integer, List<Integer>> partitions;
	}

	public static class BrokerShadow implements Serializable {
		public String host;
		public int port;
	}
}
