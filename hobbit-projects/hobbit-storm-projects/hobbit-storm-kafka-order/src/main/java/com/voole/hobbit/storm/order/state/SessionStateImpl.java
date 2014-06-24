/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import com.voole.hobbit.storm.order.module.session.AliveSessionInfo;
import com.voole.hobbit.storm.order.module.session.SessionInfo;
import com.voole.hobbit.storm.order.module.session.SessionTick;
import com.voole.hobbit.storm.order.module.session.SessionTick.OrderSessionTickType;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class SessionStateImpl implements SessionState {
	private Long _currTx;
	private final TreeMap<Long, Set<String>> timeoutTree;
	private final Map<String, OpaqueValue<SessionInfo>> db;

	public SessionStateImpl() {
		timeoutTree = new TreeMap<Long, Set<String>>();
		db = new HashMap<String, OpaqueValue<SessionInfo>>();
	}

	@Override
	public void beginCommit(Long txid) {
		_currTx = txid;
	}

	@Override
	public void commit(Long txid) {
		_currTx = null;
	}

	@Override
	public void updateByOrderPlayBgn(
			Map<String, AliveSessionInfo> idToInfo,
			TridentCollector collector) {
		for (Entry<String, AliveSessionInfo> entry : idToInfo.entrySet()) {
			String sessid = entry.getKey();
			AliveSessionInfo curr = entry.getValue();

			OpaqueValue<SessionInfo> opa = db.get(sessid);
			SessionInfo prev = null;
			if (opa != null) {
				prev = opa.get(_currTx);
			}
			if (prev == null
					|| (prev.isDead() && prev.lastStamp() < curr.lastStamp())) {// 新建
				put(sessid, opa, prev, curr);
				collector.emit(new Values(curr.getExtra().getHid(),
						createOrderBgnSessionTick(curr)));
			} else if (prev.isDead() && prev.lastStamp() >= curr.lastStamp()) {// end先与bgn到达,丢弃
			} else {// throw exception
				throw new RuntimeException("has the same sessionid:" + sessid);
			}
		}
	}

	protected SessionTick createOrderBgnSessionTick(
			AliveSessionInfo sessionInfo) {
		return createOrderSessionTick(OrderSessionTickType.BGN, sessionInfo);
	}

	protected SessionTick createOrderSessionTick(
			OrderSessionTickType type, AliveSessionInfo sessionInfo) {
		SessionTick tick = new SessionTick(type);
		tick.setLastStamp(sessionInfo.lastStamp());
		tick.setLow(sessionInfo.isLow());
		tick.setOemid(sessionInfo.getExtra().getOemid());
		tick.setSessionId(sessionInfo.getSessionId());
		tick.setSpid(sessionInfo.getSpid());
		tick.setVip(sessionInfo.isVip());
		return tick;
	}

	// public List<OrderOnlineUserModifier> gc() {
	// Map<Long, Set<String>> gcMap = timeoutTree.headMap(
	// System.currentTimeMillis() / 1000 - 600, true);
	// LinkedList<String> gcSessids = new LinkedList<String>();
	// for (Set<String> gcSessidsTmp : gcMap.values()) {
	// gcSessids.addAll(gcSessidsTmp);
	// }
	// LinkedList<Long> gcTreeKeys = new LinkedList<Long>();
	// gcTreeKeys.addAll(gcMap.keySet());
	// for (Long long1 : gcTreeKeys) {
	// timeoutTree.remove(long1);
	// }
	//
	// List<OrderOnlineUserModifier> modifiers = new
	// ArrayList<OrderOnlineUserModifier>();
	// for (String gcSessid : gcSessids) {
	// OpaqueValue<OrderSessionInfo> opa = db.remove(gcSessid);
	// if (opa != null) {
	// OrderSessionInfo info = opa.getCurr();
	// if (info != null && info instanceof OrderAliveSessionInfo) {
	// modifiers.add(createModifier(
	// ((OrderAliveSessionInfo) info).isLow(),
	// (OrderAliveSessionInfo) info, -1));
	// }
	// }
	// }
	// return modifiers;
	// }

	public void putEndOrderSession(List<TridentTuple> tuples) {
		for (TridentTuple tuple : tuples) {
			String sessid = tuple.getString(0);
			SessionInfo curr = (SessionInfo) tuple.get(1);
			OpaqueValue<SessionInfo> opa = db.get(sessid);
			SessionInfo prev = null;
			if (opa != null) {
				prev = opa.get(_currTx);
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
				put(sessid, opa, prev, curr);
			} else if (prev.isDead() && prev.lastStamp() >= curr.lastStamp()) {// end先与bgn到达,不变
			} else {// throw exception

			}
		}
	}

	protected void put(String sessid, OpaqueValue<SessionInfo> opa,
			SessionInfo prev, SessionInfo curr) {
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
		if (opa == null) {
			db.put(sessid, new OpaqueValue<SessionInfo>(_currTx, curr));
		} else {
			opa.curr = curr;
			opa.currTxid = _currTx;
			opa.prev = prev;
		}
	}

	public static class SessionStateFactory implements StateFactory {

		private static final long serialVersionUID = -2131811832766425515L;

		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new SessionStateImpl();
		}

	}

}
