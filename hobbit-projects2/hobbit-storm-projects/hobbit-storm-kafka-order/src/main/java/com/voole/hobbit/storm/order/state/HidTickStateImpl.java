package com.voole.hobbit.storm.order.state;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import com.voole.hobbit.storm.order.module.OnlineUserModifier;
import com.voole.hobbit.storm.order.module.session.SessionTick;

public class HidTickStateImpl implements HidTickState {
	private final Map<String, SessionTick> db;
	private final AtomicLong total;

	public HidTickStateImpl() {
		db = new HashMap<String, SessionTick>();
		total = new AtomicLong(0l);
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	@Override
	public void update(List<SessionTick> ticks, TridentCollector collector) {
		for (SessionTick curr : ticks) {
			String hid = curr.getHid();
			SessionTick prev = db.get(hid);
			if (prev == null) {
				updateNoPrev(hid, curr, collector);
			} else {
				if (curr.getSessionId().equals(prev.getSessionId())) {
					updateSameSessionId(hid, prev, curr, collector);
				} else {
					updateDifferentSessionId(hid, prev, curr, collector);
				}
			}
		}
	}

	private void updateNoPrev(String hid, SessionTick curr,
			TridentCollector collector) {
		if (!curr.getType().isEnd()) {
			db.put(hid, curr);
			collector.emit(new Values(createModifier(curr, 1)));
		}
	}

	private void updateSameSessionId(String hid, SessionTick prev,
			SessionTick curr, TridentCollector collector) {
		if (curr.getType().isEnd()) {
			db.remove(hid);
			collector.emit(new Values(createModifier(prev, -1)));
		} else {
			db.put(hid, curr);
			if (!prev.equals(curr)) {
				collector.emit(new Values(createModifier(prev, -1)));
				collector.emit(new Values(createModifier(curr, 1)));
			}
		}
	}

	private void updateDifferentSessionId(String hid, SessionTick prev,
			SessionTick curr, TridentCollector collector) {
		if (!curr.getType().isEnd()
				&& curr.getLastStamp() > prev.getLastStamp()) {
			db.put(hid, curr);
			if (!prev.equals(curr)) {
				collector.emit(new Values(createModifier(prev, -1)));
				collector.emit(new Values(createModifier(curr, 1)));
			}
		}
	}

	protected OnlineUserModifier createModifier(SessionTick tick, long num) {
		total.addAndGet(num);
		OnlineUserModifier modifier = new OnlineUserModifier(tick.getSpid(),
				tick.getOemid());
		modifier.setUserNum(num);
		if (tick.isLow()) {
			modifier.setUserNum_l(num);
		}
		if (tick.isVip()) {
			modifier.setVipNum(num);
			if (tick.isLow()) {
				modifier.setVipNum_l(num);
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
