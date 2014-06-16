/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.storm.order.module.OrderBgnSessionInfo;
import com.voole.hobbit.storm.order.module.OrderOnlineUserModifier;
import com.voole.hobbit.storm.order.module.OrderSessionInfo;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class OrderSessionState implements State {
	private Long _currTx;
	private final TreeMap<Long, Set<String>> timeoutTree;
	private final Map<String, OpaqueValue<OrderSessionInfo>> db;

	public OrderSessionState() {
		timeoutTree = new TreeMap<Long, Set<String>>();
		db = new HashMap<String, OpaqueValue<OrderSessionInfo>>();
	}

	@Override
	public void beginCommit(Long txid) {
		_currTx = txid;
	}

	@Override
	public void commit(Long txid) {
		_currTx = null;
	}

	public void updateByOrderPlayBgnSession(List<TridentTuple> tuples,
			TridentCollector collector) {
		for (TridentTuple tuple : tuples) {
			String sessid = tuple.getString(0);
			OrderBgnSessionInfo curr = (OrderBgnSessionInfo) tuple.get(1);
			OpaqueValue<OrderSessionInfo> val = db.get(sessid);
			OrderSessionInfo prev = null;
			if (val != null) {
				prev = val.get(_currTx);
			}
			if (prev == null
					|| (prev.isDead() && prev.lastStamp() < curr.lastStamp())) {// 新建
				put(sessid, prev, curr);
				collector.emit(new Values(increaseModifier(curr)));
			} else if (prev.isDead() && prev.lastStamp() >= curr.lastStamp()) {// end先与bgn到达,丢弃
			} else {// throw exception

			}
		}
	}

	public List<OrderOnlineUserModifier> gc() {
		Map<Long, Set<String>> gcMap = timeoutTree.headMap(
				System.currentTimeMillis() / 1000 - 600, true);
		LinkedList<Long> gcTreeKeys = new LinkedList<Long>();
		gcTreeKeys.addAll(gcMap.keySet());
		for (Long long1 : gcTreeKeys) {
			timeoutTree.remove(long1);
		}
		LinkedList<String> gcSessids = new LinkedList<String>();
		for (Set<String> gcSessidsTmp : gcMap.values()) {
			gcSessids.addAll(gcSessidsTmp);
		}
		List<OrderOnlineUserModifier> modifiers = new ArrayList<OrderOnlineUserModifier>();
		for (String gcSessid : gcSessids) {
			OpaqueValue<OrderSessionInfo> opa = db.remove(gcSessid);
			if (opa != null) {
				OrderSessionInfo info = opa.getCurr();
				if (info != null && info instanceof OrderBgnSessionInfo) {
					modifiers.add(createModifier(false,
							(OrderBgnSessionInfo) info, -1));
				}
			}
		}
		return modifiers;
	}

	protected OrderOnlineUserModifier increaseModifier(
			OrderBgnSessionInfo sessionInfo) {
		return createModifier(false, sessionInfo, 1);
	}

	protected OrderOnlineUserModifier createModifier(boolean isLow,
			OrderBgnSessionInfo sessionInfo, long change) {
		OrderOnlineUserModifier modifier = new OrderOnlineUserModifier(
				sessionInfo.getSpid(), sessionInfo.getExtra().getOemid());
		modifier.setUserNum(change);
		if (isLow) {
			modifier.setUserNum_l(change);
		}
		if (sessionInfo.isVip()) {
			modifier.setVipNum(change);
			if (isLow) {
				modifier.setVipNum_l(change);
			}
		}
		return modifier;
	}

	public void putEndOrderSession(List<TridentTuple> tuples) {
		for (TridentTuple tuple : tuples) {
			String sessid = tuple.getString(0);
			OrderSessionInfo curr = (OrderSessionInfo) tuple.get(1);
			OpaqueValue<OrderSessionInfo> val = db.get(sessid);
			OrderSessionInfo prev = null;
			if (val != null) {
				prev = val.get(_currTx);
			}
			if (prev == null) {// end先与bgn到达,先行标记

			} else if (prev.isDead()) {
				if (prev.lastStamp() < curr.lastStamp()) {// end先与bgn到达,先行标记

				} else if (prev.lastStamp() == curr.lastStamp()) {// 可能重复发送

				} else if (prev.lastStamp() > curr.lastStamp()) {// 异常

				}
			} else {
				if (prev.lastStamp() <= curr.lastStamp()) {// 正常结束

				} else {// 异常

				}
			}
			if (prev == null
					|| (prev.isDead() && prev.lastStamp() < curr.lastStamp())) {// 新建
				put(sessid, prev, curr);
			} else if (prev.isDead() && prev.lastStamp() >= curr.lastStamp()) {// end先与bgn到达,不变
			} else {// throw exception

			}
		}
	}

	protected void put(String sessid, OrderSessionInfo prev,
			OrderSessionInfo curr) {
		if (prev != null) {
			Long oldStamp = prev.lastStamp();
			Set<String> keys = timeoutTree.get(oldStamp);
			keys.remove(sessid);
			if (keys.isEmpty()) {
				timeoutTree.remove(oldStamp);
			}
		}
		Long newStamp = curr.lastStamp();
		Set<String> keys = null;
		if (!timeoutTree.containsKey(newStamp)) {
			keys = new HashSet<String>();
			timeoutTree.put(newStamp, keys);
		} else {
			keys = timeoutTree.get(newStamp);
		}
		keys.add(sessid);
		db.put(sessid, new OpaqueValue<OrderSessionInfo>(_currTx, curr));
	}

	public static class OrderSessionStateUpdater implements
			StateUpdater<OrderSessionState> {
		public static final Fields OUTPUT_FIELDS = new Fields("modifier");

		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map conf,
				TridentOperationContext context) {
		}

		@Override
		public void cleanup() {
		}

		@Override
		public void updateState(OrderSessionState state,
				List<TridentTuple> tuples, TridentCollector collector) {
			state.updateByOrderPlayBgnSession(tuples, collector);
		}
	}

	public static class OrderSessionStateFactory implements StateFactory {

		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new OrderSessionState();
		}

	}

	public static enum OrderAction {
		INCREASE, DECREASE, TO_LOW, TO_HIGH;
	}

}
