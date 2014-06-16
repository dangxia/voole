/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.builder.ToStringBuilder;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.storm.order.function.OrderOnlineUserModifierCombinerAggregator;
import com.voole.hobbit.storm.order.module.OrderOnlineUser;
import com.voole.hobbit.utils.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月16日
 */
public class OrderOnlineUserState implements State {
	private Long _currTx;
	private final Tuple<String, Long> globalKey;
	// (spid,oemid)=>OrderOnlineUser
	// oemid == 0 > spid info
	private final Map<Tuple<String, Long>, OpaqueValue<OrderOnlineUser>> snapshotMap;

	public OrderOnlineUserState() {
		snapshotMap = new HashMap<Tuple<String, Long>, OpaqueValue<OrderOnlineUser>>();
		globalKey = new Tuple<String, Long>("", 0l);
	}

	@Override
	public void beginCommit(Long txid) {
		_currTx = txid;
	}

	@Override
	public void commit(Long txid) {
		_currTx = null;
	}

	public OrderOnlineUser update(Map<Tuple<String, Long>, OrderOnlineUser> map) {
		Map<String, OrderOnlineUser> spMap = new HashMap<String, OrderOnlineUser>();
		OrderOnlineUser global = new OrderOnlineUser();
		for (Entry<Tuple<String, Long>, OrderOnlineUser> entry : map.entrySet()) {
			Tuple<String, Long> key = entry.getKey();
			OrderOnlineUser curr = entry.getValue();
			// UPDATE (SPID,OEMID)
			update(key, curr);
			OrderOnlineUser spUser = null;
			if (spMap.containsKey(key.getA())) {
				spUser = spMap.get(key.getA());
			} else {
				spUser = new OrderOnlineUser();
				spMap.put(key.getA(), spUser);
			}
			spUser.update(curr);
			global.update(curr);
		}
		// update sp
		for (Entry<String, OrderOnlineUser> entry : spMap.entrySet()) {
			update(new Tuple<String, Long>(entry.getKey(), 0l),
					entry.getValue());
		}
		// update global
		return update(globalKey, global);
	}

	protected OrderOnlineUser update(Tuple<String, Long> key,
			OrderOnlineUser curr) {
		OpaqueValue<OrderOnlineUser> opa = null;
		if (snapshotMap.containsKey(key)) {
			opa = snapshotMap.get(key);
		} else {
			opa = new OpaqueValue<OrderOnlineUser>(0l, new OrderOnlineUser());
			snapshotMap.put(key, opa);
		}
		return update(opa, curr);
	}

	protected OrderOnlineUser update(OpaqueValue<OrderOnlineUser> opa,
			OrderOnlineUser curr) {
		OrderOnlineUser prev = opa.get(_currTx);
		if (prev != null) {
			curr.update(prev);
		}
		opa.currTxid = _currTx;
		opa.curr = curr;
		opa.prev = prev;
		return curr;
	}

	public static class OrderOnlineUserStateUpdateQueryFunction extends
			BaseQueryFunction<OrderOnlineUserState, OrderOnlineUser> {
		public static final Fields INPUT_FIELDS = OrderOnlineUserModifierCombinerAggregator.OUTPUT_FIELDS;
		public static final Fields OUTPUT_FIELDS = new Fields("global");

		@SuppressWarnings("unchecked")
		@Override
		public List<OrderOnlineUser> batchRetrieve(OrderOnlineUserState state,
				List<TridentTuple> args) {
			List<OrderOnlineUser> list = new ArrayList<OrderOnlineUser>();
			for (TridentTuple tuple : args) {
				list.add(state
						.update((Map<Tuple<String, Long>, OrderOnlineUser>) tuple
								.get(0)));

			}
			return list;
		}

		@Override
		public void execute(TridentTuple tuple, OrderOnlineUser result,
				TridentCollector collector) {
			collector.emit(new Values(result));
		}

	}

	public static class OrderOnlineUserStateFactory implements StateFactory {

		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new OrderOnlineUserState();
		}

	}

	public static class OpaqueValue<T> {
		Long currTxid;
		T prev;
		T curr;

		public OpaqueValue(Long currTxid, T val, T prev) {
			this.curr = val;
			this.currTxid = currTxid;
			this.prev = prev;
		}

		public OpaqueValue(Long currTxid, T val) {
			this(currTxid, val, null);
		}

		public OpaqueValue<T> update(Long batchTxid, T newVal) {
			T prev;
			if (batchTxid == null || (this.currTxid < batchTxid)) {
				prev = this.curr;
			} else if (batchTxid.equals(this.currTxid)) {
				prev = this.prev;
			} else {
				throw new RuntimeException("Current batch (" + batchTxid
						+ ") is behind state's batch: " + this.toString());
			}
			return new OpaqueValue<T>(batchTxid, newVal, prev);
		}

		public T get(Long batchTxid) {
			if (batchTxid == null || (this.currTxid < batchTxid)) {
				return curr;
			} else if (batchTxid.equals(this.currTxid)) {
				return prev;
			} else {
				throw new RuntimeException("Current batch (" + batchTxid
						+ ") is behind state's batch: " + this.toString());
			}
		}

		public T getCurr() {
			return curr;
		}

		public Long getCurrTxid() {
			return currTxid;
		}

		public T getPrev() {
			return prev;
		}

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this);
		}
	}
}
