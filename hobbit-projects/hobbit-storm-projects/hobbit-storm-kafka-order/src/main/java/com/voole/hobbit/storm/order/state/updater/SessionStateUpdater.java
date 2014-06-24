/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state.updater;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;

import com.voole.hobbit.storm.order.module.session.AliveSessionInfo;
import com.voole.hobbit.storm.order.state.SessionState;

public class SessionStateUpdater implements StateUpdater<SessionState> {
	private static final long serialVersionUID = -1212053827092489966L;
	public static final Fields OUTPUT_FIELDS = new Fields("hid", "tick");

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf,
			TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void updateState(SessionState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		// sessinId to sessionInfo
		Map<String, AliveSessionInfo> idToInfo = new HashMap<String, AliveSessionInfo>();
		for (TridentTuple tuple : tuples) {
			String sessid = tuple.getString(0);
			AliveSessionInfo curr = (AliveSessionInfo) tuple.get(1);
			if (idToInfo.containsKey(sessid)) {
				AliveSessionInfo old = idToInfo.get(sessid);
				if (old.lastStamp() < curr.lastStamp()) {
					idToInfo.put(sessid, curr);
				}
			} else {
				idToInfo.put(sessid, curr);
			}
		}
		state.updateByOrderPlayBgn(idToInfo, collector);
	}
}