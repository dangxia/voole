/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.function;

import static com.voole.hobbit.kafka.TopicProtoClassUtils.ORDER_ALIVE_V2;
import static com.voole.hobbit.kafka.TopicProtoClassUtils.ORDER_ALIVE_V3;
import static com.voole.hobbit.kafka.TopicProtoClassUtils.ORDER_BGN_V2;
import static com.voole.hobbit.kafka.TopicProtoClassUtils.ORDER_BGN_V3;
import static com.voole.hobbit.kafka.TopicProtoClassUtils.ORDER_END_V2;
import static com.voole.hobbit.kafka.TopicProtoClassUtils.ORDER_END_V3;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.kafka.TopicProtoClassUtils;
import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV3;
import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV3;
import com.voole.hobbit.proto.TerminalPB.OrderPlayEndReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayEndReqV3;
import com.voole.hobbit.proto.simple.TermialSimplePB.OrderPlayAliveSimple;
import com.voole.hobbit.proto.simple.TermialSimplePB.OrderPlayBgnSimple;
import com.voole.hobbit.proto.simple.TermialSimplePB.OrderPlayEndSimple;

/**
 * @author XuehuiHe
 * @date 2014年6月4日
 */
public class KafkaTermialOrginToSimpleFunction extends BaseFunction {
	public static final Fields INPUT_FIELDS = new Fields("topic", "origin");
	public static final Fields OUTPUT_FIELDS = new Fields("type", "simple");

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String topic = tuple.getString(0);
		Object t = tuple.get(1);
		Object simple = null;
		if (ORDER_BGN_V2.equals(topic)) {
			OrderPlayBgnReqV2 v = (OrderPlayBgnReqV2) t;
			simple = OrderPlayBgnSimple.newBuilder()
					.setFID(regluerFid(v.getFID()))
					.setHID(regluerHid(v.getHID())).setOEMID(v.getOEMID())
					.setSessID(v.getSessID()).setStamp(v.getPlayTick())
					.setURL(v.getURL()).build();
		} else if (ORDER_BGN_V3.equals(topic)) {
			OrderPlayBgnReqV3 v = (OrderPlayBgnReqV3) t;
			simple = OrderPlayBgnSimple.newBuilder()
					.setFID(regluerFid(v.getFID()))
					.setHID(regluerHid(v.getHID())).setOEMID(v.getOEMID())
					.setSessID(v.getSessID()).setStamp(v.getPlayTick())
					.setURL(v.getURL()).build();
		} else if (ORDER_END_V2.equals(topic)) {
			OrderPlayEndReqV2 v = (OrderPlayEndReqV2) t;
			simple = OrderPlayEndSimple.newBuilder()
					.setHID(regluerHid(v.getHID()))
					.setSessAvgSpeed(v.getSessAvgSpeed())
					.setSessID(v.getSessID()).setStamp(v.getEndTick()).build();
		} else if (ORDER_END_V3.equals(topic)) {
			OrderPlayEndReqV3 v = (OrderPlayEndReqV3) t;
			simple = OrderPlayEndSimple.newBuilder()
					.setHID(regluerHid(v.getHID()))
					.setSessAvgSpeed(v.getSessAvgSpeed())
					.setSessID(v.getSessID()).setStamp(v.getEndTick()).build();
		} else if (ORDER_ALIVE_V2.equals(topic)) {
			OrderPlayAliveReqV2 v = (OrderPlayAliveReqV2) t;
			simple = OrderPlayAliveSimple.newBuilder()
					.setSessAvgSpeed(v.getSessAvgSpeed())
					.setSessID(v.getSessID()).setStamp(v.getAliveTick())
					.build();
		} else if (ORDER_ALIVE_V3.equals(topic)) {
			OrderPlayAliveReqV3 v = (OrderPlayAliveReqV3) t;
			simple = OrderPlayAliveSimple.newBuilder()
					.setSessAvgSpeed(v.getSessAvgSpeed())
					.setSessID(v.getSessID()).setStamp(v.getAliveTick())
					.build();
		}
		if (simple != null) {
			collector.emit(new Values(TopicProtoClassUtils.getType(topic),
					simple));
		}
	}

	public static String regluerFid(String fid) {
		if (fid != null && fid.length() > 0) {
			return fid.toUpperCase();
		}
		return null;
	}

	public static String regluerHid(String hid) {
		if (hid != null && hid.length() > 0) {
			hid = hid.toUpperCase();
		}
		if (hid != null && hid.length() > 12) {
			return hid.substring(0, 12);
		}
		return hid;
	}

}
