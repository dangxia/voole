package com.voole.hobbit.storm.order.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import com.voole.hobbit.storm.order.module.OnlineUserModifier;
import com.voole.hobbit.storm.order.module.session.SessionTick;

public class HidTickStateImpl implements HidTickState {
	private Long _currTx;
	private final Map<String, OpaqueValue<SessionTick>> hidToSessionId;

	public HidTickStateImpl() {
		hidToSessionId = new HashMap<String, OpaqueValue<SessionTick>>();
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
	public void update(Map<String, SessionTick> hidToTick,
			TridentCollector collector) {
		for (Entry<String, SessionTick> entry : hidToTick.entrySet()) {
			String hid = entry.getKey();
			SessionTick curr = entry.getValue();
			OpaqueValue<SessionTick> opa = hidToSessionId.get(hid);
			SessionTick prev = null;
			if (opa != null) {
				prev = opa.get(_currTx);
			}
			if (prev == null) {
				if (curr.isAlive()) {
					opa = new OpaqueValue<SessionTick>(_currTx, curr);
					hidToSessionId.put(hid, opa);
					collector.emit(new Values(createModifier(curr, 1)));
				}
			} else {
				if (prev.getLastStamp() < curr.getLastStamp()) {
					if (curr.isAlive()) {
						if (!prev.equals(curr)) {
							opa.prev = prev;
							opa.curr = curr;
							opa.currTxid = _currTx;
							collector
									.emit(new Values(createModifier(prev, -1)));
							collector.emit(new Values(createModifier(curr, 1)));
						}
					} else if (prev.getSessionId().equals(curr.getSessionId())) {
						hidToSessionId.remove(hid);
						collector.emit(new Values(createModifier(prev, -1)));
					}
				}
			}
		}

	}

	protected OnlineUserModifier createModifier(SessionTick tick,
			long num) {
		OnlineUserModifier modifier = new OnlineUserModifier(
				tick.getSpid(), tick.getOemid());
		modifier.setUserNum(num);
		if (tick.isLow()) {
			modifier.setUserNum_l(num);
		}
		if (tick.isVip()) {
			modifier.setVipNum(num);
			if (tick.isLow()) {
				modifier.setVipNum(num);
			}
		}
		return modifier;
	}

	public static class HidTickStateFactory implements StateFactory {
		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new HidTickStateImpl();
		}

	}

}
