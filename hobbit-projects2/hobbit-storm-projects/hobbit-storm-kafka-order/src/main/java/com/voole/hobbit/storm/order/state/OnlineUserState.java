/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state;

import java.util.Map;

import storm.trident.state.State;

import com.voole.hobbit.storm.order.module.OnlineUser;
import com.voole.hobbit.utils.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月24日
 */
public interface OnlineUserState extends State {
	public OnlineUser update(Map<Tuple<String, Long>, OnlineUser> map);

	public String query();
}
