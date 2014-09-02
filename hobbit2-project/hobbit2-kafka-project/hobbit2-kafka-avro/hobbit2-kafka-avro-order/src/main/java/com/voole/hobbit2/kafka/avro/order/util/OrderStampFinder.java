package com.voole.hobbit2.kafka.avro.order.util;

import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.kafka.avro.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.kafka.avro.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV3;

public class OrderStampFinder {
	public static long getStamp(SpecificRecordBase value) {
		long stamp = 0;
		if (value instanceof OrderPlayBgnReqV2
				|| value instanceof OrderPlayBgnReqV3) {
			stamp = (Long) value.get("playTick");
		} else if (value instanceof OrderPlayAliveReqV2
				|| value instanceof OrderPlayAliveReqV3) {
			stamp = (Long) value.get("aliveTick");
		} else if (value instanceof OrderPlayEndReqV2
				|| value instanceof OrderPlayEndReqV3) {
			stamp = (Long) value.get("endTick");
		} else {
			throw new RuntimeException("Don't support class:"
					+ value.getClass().getName());
		}
		return stamp * 1000;
	}
}
