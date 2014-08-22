/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.kafka;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.voole.hobbit2.tools.kafka.partition.Broker;

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

	public static Broker toBroker(String data) {
		return gson.fromJson(data, Broker.class);
	}

	public static PartitionsInfo toPartitionsInfo(String data) {
		return gson.fromJson(data, PartitionsInfo.class);
	}

	public static class PartitionsInfo implements Serializable {
		public Map<Integer, List<Integer>> partitions;
	}
}
