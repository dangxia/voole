/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.voole.hobbit.storm.order.module.OnlineUser;
import com.voole.hobbit.utils.Tuple;

/**
 * @author XuehuiHe
 * @date 2014年6月16日
 */
public class OnlineUserStateImpl implements OnlineUserState {
	private Long _currTx;
	private final Tuple<String, Long> globalKey;
	// (spid,oemid)=>OrderOnlineUser
	// oemid == 0 > spid info
	private final Map<Tuple<String, Long>, OpaqueValue<OnlineUser>> snapshotMap;
	private final Gson gson;

	public OnlineUserStateImpl() {
		snapshotMap = new HashMap<Tuple<String, Long>, OpaqueValue<OnlineUser>>();
		globalKey = new Tuple<String, Long>("", 0l);

		GsonBuilder gb = new GsonBuilder();
		gb.setPrettyPrinting();
		gson = gb.create();
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
	public String query() {
		Map<String, OnlineUser> map = new HashMap<String, OnlineUser>();
		for (Entry<Tuple<String, Long>, OpaqueValue<OnlineUser>> entry : snapshotMap
				.entrySet()) {
			Tuple<String, Long> key = entry.getKey();
			map.put(key.getA() + "|" + key.getB(), entry.getValue().curr);
		}
		return gson.toJson(map);
	}

	public OnlineUser update(Map<Tuple<String, Long>, OnlineUser> map) {
		Map<String, OnlineUser> spMap = new HashMap<String, OnlineUser>();
		OnlineUser global = new OnlineUser();
		for (Entry<Tuple<String, Long>, OnlineUser> entry : map.entrySet()) {
			Tuple<String, Long> key = entry.getKey();
			OnlineUser curr = entry.getValue();
			// UPDATE (SPID,OEMID)
			update(key, curr);
			OnlineUser spUser = null;
			if (spMap.containsKey(key.getA())) {
				spUser = spMap.get(key.getA());
			} else {
				spUser = new OnlineUser();
				spMap.put(key.getA(), spUser);
			}
			spUser.update(curr);
			global.update(curr);
		}
		// update sp
		for (Entry<String, OnlineUser> entry : spMap.entrySet()) {
			update(new Tuple<String, Long>(entry.getKey(), 0l),
					entry.getValue());
		}
		// update global
		return update(globalKey, global);
	}

	protected OnlineUser update(Tuple<String, Long> key, OnlineUser curr) {
		OpaqueValue<OnlineUser> opa = null;
		if (snapshotMap.containsKey(key)) {
			opa = snapshotMap.get(key);
		} else {
			opa = new OpaqueValue<OnlineUser>(0l, new OnlineUser());
			snapshotMap.put(key, opa);
		}
		return update(opa, curr);
	}

	protected OnlineUser update(OpaqueValue<OnlineUser> opa, OnlineUser curr) {
		OnlineUser prev = opa.get(_currTx);
		OnlineUser v = new OnlineUser();
		if (prev != null) {
			v.update(prev);
		}
		v.update(curr);
		opa.currTxid = _currTx;
		opa.curr = v;
		opa.prev = prev;
		return v;
	}

	public static class OnlineUserStateFactory implements StateFactory {

		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new OnlineUserStateImpl();
		}

	}

}
