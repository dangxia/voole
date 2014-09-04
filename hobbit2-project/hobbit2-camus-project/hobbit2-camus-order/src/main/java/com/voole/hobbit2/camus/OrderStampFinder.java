/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus;

import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.camus.api.IStampFinder;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV2;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV3;

/**
 * @author XuehuiHe
 * @date 2014年9月4日
 */
public class OrderStampFinder implements IStampFinder {

	@Override
	public <Value> Long findStamp(Value value) throws IllegalArgumentException {
		Long stamp = null;
		if (value instanceof OrderPlayBgnReqV2
				|| value instanceof OrderPlayBgnReqV3) {
			stamp = (Long) ((SpecificRecordBase) value).get("playTick");
		} else if (value instanceof OrderPlayAliveReqV2
				|| value instanceof OrderPlayAliveReqV3) {
			stamp = (Long) ((SpecificRecordBase) value).get("aliveTick");
		} else if (value instanceof OrderPlayEndReqV2
				|| value instanceof OrderPlayEndReqV3) {
			stamp = (Long) ((SpecificRecordBase) value).get("endTick");
		} else {
			throw new IllegalArgumentException("Don't support class:"
					+ value.getClass().getName());
		}
		if (stamp == null) {
			return null;
		} else {
			return stamp * 1000;
		}
	}

}
