/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.partition;

import java.io.Serializable;

import com.google.common.base.Objects;

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

	@Override
	public String toString() {
		return Objects.toStringHelper(this).addValue(getId()).toString();
	}

	@Override
	public int hashCode() {
		return -1;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof GCSpoutPartition) {
			return true;
		}
		return false;
	}

}
