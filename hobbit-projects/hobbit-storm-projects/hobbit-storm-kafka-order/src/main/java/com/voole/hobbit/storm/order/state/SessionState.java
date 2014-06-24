package com.voole.hobbit.storm.order.state;

import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.state.State;

import com.voole.hobbit.storm.order.module.session.AliveSessionInfo;

public interface SessionState extends State {
	public void updateByOrderPlayBgn(
			Map<String, AliveSessionInfo> idToInfo,
			TridentCollector collector);
}
