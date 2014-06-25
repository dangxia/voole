/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.filter;

import storm.trident.Stream;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;

import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV3;
import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV3;
import com.voole.hobbit.proto.TerminalPB.OrderPlayEndReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayEndReqV3;
import com.voole.hobbit.storm.order.function.transformer.TransformerFunction;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public class TickFilter extends BaseFilter {
	public static final Fields INPUT_FIELDS = TransformerFunction.OUTPUT_FIELDS;

	public static Stream filte(Stream stream) {
		return stream.each(INPUT_FIELDS, new TickFilter());
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		Long tick = getTick(tuple.get(0));
		if (tick != null && tick > System.currentTimeMillis() - 60 * 60) {
			return true;
		}
		return true;
		// return false;
	}

	protected Long getTick(Object proto) {
		if (proto instanceof OrderPlayBgnReqV2) {
			return ((OrderPlayBgnReqV2) proto).getPlayTick();
		} else if (proto instanceof OrderPlayBgnReqV3) {
			return ((OrderPlayBgnReqV3) proto).getPlayTick();
		} else if (proto instanceof OrderPlayAliveReqV2) {
			return ((OrderPlayAliveReqV2) proto).getAliveTick();
		} else if (proto instanceof OrderPlayAliveReqV3) {
			return ((OrderPlayAliveReqV3) proto).getAliveTick();
		} else if (proto instanceof OrderPlayEndReqV2) {
			return ((OrderPlayEndReqV2) proto).getEndTick();
		} else if (proto instanceof OrderPlayEndReqV3) {
			return ((OrderPlayEndReqV3) proto).getEndTick();
		}
		return 0l;
	}

}
