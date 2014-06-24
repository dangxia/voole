/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.module;

import com.voole.hobbit.utils.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月16日
 */
public class OnlineUserModifier extends OnlineUser {
	private Tuple<String, Long> key;

	public OnlineUserModifier() {
	}

	public OnlineUserModifier(String spid, Long oemid) {
		Tuple<String, Long> key = new Tuple<String, Long>(spid, oemid);
		setKey(key);
	}

	public Tuple<String, Long> getKey() {
		return key;
	}

	public String getSpid() {
		return getKey().getA();
	}

	public Long getOemid() {
		return getKey().getB();
	}

	public void setKey(Tuple<String, Long> key) {
		this.key = key;
	}

}
