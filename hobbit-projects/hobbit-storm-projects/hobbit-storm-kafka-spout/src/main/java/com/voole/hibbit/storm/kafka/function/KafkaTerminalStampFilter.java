/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.function;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;

import com.voole.hobbit.kafka.TopicProtoClassUtils.OrderLogType;
import com.voole.hobbit.proto.simple.TermialSimplePB.OrderPlayAliveSimple;
import com.voole.hobbit.proto.simple.TermialSimplePB.OrderPlayBgnSimple;
import com.voole.hobbit.proto.simple.TermialSimplePB.OrderPlayEndSimple;

/**
 * @author XuehuiHe
 * @date 2014年6月4日
 */
public class KafkaTerminalStampFilter extends BaseFilter {
	public static final Fields INPUT_FIELDS = KafkaTermialOrginToSimpleFunction.OUTPUT_FIELDS;

	@Override
	public boolean isKeep(TridentTuple tuple) {
		OrderLogType type = (OrderLogType) tuple.get(0);
		Object t = tuple.get(1);
		Long stamp = null;
		switch (type) {
		case BGN:
			OrderPlayBgnSimple bgn = (OrderPlayBgnSimple) t;
			stamp = bgn.getStamp();
			break;
		case END:
			OrderPlayEndSimple end = (OrderPlayEndSimple) t;
			stamp = end.getStamp();
			break;
		case ALIVE:
			OrderPlayAliveSimple alive = (OrderPlayAliveSimple) t;
			stamp = alive.getStamp();
			break;
		}
		if (stamp != null && stamp > System.currentTimeMillis() - 60 * 60) {
			return true;
		}
		return false;
	}
}
