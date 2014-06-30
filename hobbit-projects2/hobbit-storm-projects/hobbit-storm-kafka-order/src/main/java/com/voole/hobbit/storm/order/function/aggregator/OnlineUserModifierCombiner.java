/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.function.aggregator;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.fluent.GlobalAggregationScheme;
import storm.trident.fluent.IAggregatableStream;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.impl.GlobalBatchToPartition;
import storm.trident.operation.impl.SingleEmitAggregator.BatchToPartition;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;

import com.voole.hobbit.storm.order.module.OnlineUser;
import com.voole.hobbit.storm.order.module.OnlineUserModifier;
import com.voole.hobbit.storm.order.state.OnlineUserStateImpl.OnlineUserStateFactory;
import com.voole.hobbit.storm.order.state.updater.HidTickStateUpdater;
import com.voole.hobbit.storm.order.state.updater.OnlineUserStateQueryUpdater;
import com.voole.hobbit.utils.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月16日
 */
public class OnlineUserModifierCombiner implements
		CombinerAggregator<Map<Tuple<String, Long>, OnlineUser>> {
	public static final Fields INPUT_FIELDS = HidTickStateUpdater.OUTPUT_FIELDS;
	public static final Fields OUTPUT_FIELDS = new Fields("modifier_combiner");

	public static TridentState combine(Stream stream) {
		OnlineUserModifierCombiner combiner = new OnlineUserModifierCombiner();
		return stream.aggregate(INPUT_FIELDS, combiner, OUTPUT_FIELDS)
				.partitionPersist(new OnlineUserStateFactory(), OUTPUT_FIELDS,
						new OnlineUserStateQueryUpdater(),
						OnlineUserStateQueryUpdater.OUTPUT_FIELDS);

		// return new ChainedAggregatorDeclarer(stream, new GlobalAggScheme())
		// .aggregate(INPUT_FIELDS, combiner, OUTPUT_FIELDS)
		// .chainEnd()
		// .partitionPersist(new StateSpec(new OnlineUserStateFactory()),
		// OUTPUT_FIELDS, new OnlineUserStateQueryUpdater(),
		// OnlineUserStateQueryUpdater.OUTPUT_FIELDS);

	}

	@Override
	public Map<Tuple<String, Long>, OnlineUser> init(TridentTuple tuple) {
		OnlineUserModifier modifier = (OnlineUserModifier) tuple.get(0);
		Map<Tuple<String, Long>, OnlineUser> map = new HashMap<Tuple<String, Long>, OnlineUser>();
		map.put(modifier.getKey(), modifier);
		return map;
	}

	@Override
	public Map<Tuple<String, Long>, OnlineUser> combine(
			Map<Tuple<String, Long>, OnlineUser> val1,
			Map<Tuple<String, Long>, OnlineUser> val2) {
		for (Entry<Tuple<String, Long>, OnlineUser> entry : val2.entrySet()) {
			Tuple<String, Long> key = entry.getKey();
			OnlineUser v = entry.getValue();
			if (val1.containsKey(key)) {
				val1.get(key).update(v);
			} else {
				val1.put(key, v);
			}
		}
		return val1;
	}

	@Override
	public Map<Tuple<String, Long>, OnlineUser> zero() {
		return new HashMap<Tuple<String, Long>, OnlineUser>();
	}

	static class GlobalAggScheme implements GlobalAggregationScheme<Stream> {

		@Override
		public IAggregatableStream aggPartition(Stream s) {
			return s.global();
		}

		@Override
		public BatchToPartition singleEmitPartitioner() {
			return new GlobalBatchToPartition();
		}

	}

}
