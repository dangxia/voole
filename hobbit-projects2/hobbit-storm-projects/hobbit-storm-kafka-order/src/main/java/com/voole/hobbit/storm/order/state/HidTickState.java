/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state;

import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.State;

import com.voole.hobbit.storm.order.module.session.SessionTick;

/**
 * @author XuehuiHe
 * @date 2014年6月24日
 */
public interface HidTickState extends State {
	public void update(List<SessionTick> ticks, TridentCollector collector);
}
