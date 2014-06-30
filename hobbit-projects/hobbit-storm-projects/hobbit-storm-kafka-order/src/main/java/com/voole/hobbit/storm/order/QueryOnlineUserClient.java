/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.voole.hobbit.storm.order.module.OnlineUser;

/**
 * @author XuehuiHe
 * @date 2014年6月29日
 */
public class QueryOnlineUserClient {
	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws TException,
			DRPCExecutionException {

		GsonBuilder gb = new GsonBuilder();
		gb.setPrettyPrinting();
		Gson gson = gb.create();

		DRPCClient client = new DRPCClient("data-master.voole.com", 3772);
		String json = client.execute("query", "sd");
		List list = gson.fromJson(json, List.class);
		List<OnlineUser2> results = new ArrayList<OnlineUser2>();
		for (Object object : list) {
			List l = (List) object;
			for (Object object2 : l) {
				String json2 = (String) object2;
				json2 = json2.replace("\\", "");
				Map<String, OnlineUser> map = gson.fromJson(json2,
						OnlineUserMap.class);
				for (Entry<String, OnlineUser> entry : map.entrySet()) {
					OnlineUser2 item = new OnlineUser2();
					item.setKey(entry.getKey());
					item.setOnlineUser(entry.getValue());
					results.add(item);
				}
			}
		}
		Collections.sort(results);
		System.out.println(gson.toJson(results));
	}

	public static class OnlineUserMap extends HashMap<String, OnlineUser> {

	}

	public static class OnlineUser2 implements Comparable<OnlineUser2> {
		private String key;
		private OnlineUser onlineUser;

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public OnlineUser getOnlineUser() {
			return onlineUser;
		}

		public void setOnlineUser(OnlineUser onlineUser) {
			this.onlineUser = onlineUser;
		}

		@Override
		public int compareTo(OnlineUser2 o) {
			return (int) (this.onlineUser.getUserNum() - o.onlineUser
					.getUserNum());
		}

	}
}
