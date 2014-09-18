/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.partition;

import java.io.Serializable;

import storm.trident.spout.ISpoutPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月18日
 */
public class GCSpoutPartition implements ISpoutPartition, Serializable {

	@Override
	public String getId() {
		return "storm-order-gc";
	}

}
