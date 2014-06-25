/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state;

import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

/**
 * @author XuehuiHe
 * @date 2014年6月25日
 */
public interface PlayExtraState extends State{

	/**
	 * @param tuples
	 * @param collector
	 */
	void updateByPlayExtra(List<TridentTuple> tuples, TridentCollector collector);

}
