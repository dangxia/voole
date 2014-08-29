/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.avro.order.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.kafka.avro.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.kafka.avro.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2;
import com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV3;
import com.voole.hobbit2.kafka.common.partition.Partitioner;
import com.voole.hobbit2.kafka.common.partition.PartitionerRegister.DefaultPartitionerRegister;

public abstract class AbstractOrderPartitionerRegister extends
		DefaultPartitionerRegister {

	@Override
	protected Map<String, Partitioner<?, ?, ?>> getTopicToPartitionerMap() {
		Map<String, Partitioner<?, ?, ?>> map = new HashMap<String, Partitioner<?, ?, ?>>();
		AbstractOrderPartitioner<?, ?> partitioner = getOrderPartitioner();
		for (Entry<String, Schema> entry : OrderTopicsUtils.topicToSchema
				.entrySet()) {
			map.put(entry.getKey(), partitioner);
		}
		return map;
	}

	protected abstract AbstractOrderPartitioner<?, ?> getOrderPartitioner();

	public abstract static class AbstractOrderPartitioner<P, K> implements
			Partitioner<P, K, SpecificRecordBase> {

		protected long getStamp(SpecificRecordBase value) {
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

}