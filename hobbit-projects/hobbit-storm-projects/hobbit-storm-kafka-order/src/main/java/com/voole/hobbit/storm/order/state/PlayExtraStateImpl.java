/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit.storm.order.module.extra.PlayAliveExtra;
import com.voole.hobbit.storm.order.module.extra.PlayBgnExtra;
import com.voole.hobbit.storm.order.module.extra.PlayEndExtra;
import com.voole.hobbit.storm.order.module.extra.PlayExtra;
import com.voole.hobbit.storm.order.module.extra.PlayExtra.PlayType;
import com.voole.hobbit.storm.order.module.session.SessionTick;
import com.voole.hobbit.storm.order.state.OrderExtraInfoQueryState.OrderExtraInfoQuery;

/**
 * @author XuehuiHe
 * @date 2014年6月25日
 */
public class PlayExtraStateImpl implements PlayExtraState {
	private Long _currTx;
	private final TreeMap<Long, Set<String>> timeoutTree;
	private final Map<String, OpaqueValue<PlayExtra>> db;

	public PlayExtraStateImpl() {
		timeoutTree = new TreeMap<Long, Set<String>>();
		db = new HashMap<String, OpaqueValue<PlayExtra>>();
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
	public void updateByPlayExtra(List<TridentTuple> tuples,
			TridentCollector collector) {
		for (TridentTuple tuple : tuples) {
			PlayExtra extra = (PlayExtra) tuple.get(1);
			String sessionId = extra.getSessionId();
			OpaqueValue<PlayExtra> opa = db.get(sessionId);
			if (opa == null) {
				_new(sessionId, extra, collector);
			} else {
				_update(opa, sessionId, extra, collector);
			}
		}
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayExtra curr, TridentCollector collector) {
		if (_currTx.equals(opa.currTxid)) {
			rollbackTimeoutTree(sessionId, opa);
		}
		PlayExtra prev = opa.get(_currTx);
		if (prev == null) {
			_new(sessionId, curr, collector);
		} else {
			if (prev.getPlayType().isBgn()) {
				if (curr.getPlayType().isBgn()) {
					_update(opa, sessionId, (PlayBgnExtra) prev,
							(PlayBgnExtra) curr, collector);
				} else if (curr.getPlayType().isAlive()) {
					_update(opa, sessionId, (PlayBgnExtra) prev,
							(PlayAliveExtra) curr, collector);
				} else {
					_update(opa, sessionId, (PlayBgnExtra) prev,
							(PlayEndExtra) curr, collector);
				}
			} else {
				if (curr.getPlayType().isBgn()) {
					_update(opa, sessionId, (PlayEndExtra) prev,
							(PlayBgnExtra) curr, collector);
				} else if (curr.getPlayType().isAlive()) {
					_update(opa, sessionId, (PlayEndExtra) prev,
							(PlayAliveExtra) curr, collector);
				} else {
					_update(opa, sessionId, (PlayEndExtra) prev,
							(PlayEndExtra) curr, collector);
				}
			}
		}
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayEndExtra prev, PlayEndExtra curr, TridentCollector collector) {
		if (curr.lastStamp() > prev.lastStamp()) {
			updateTimeoutTree(sessionId, curr, prev);
			updateDb(opa, sessionId, curr, prev);
		}
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayEndExtra prev, PlayAliveExtra curr, TridentCollector collector) {
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayEndExtra prev, PlayBgnExtra curr, TridentCollector collector) {
		if (prev.lastStamp() > curr.lastStamp()) {
			removeFromTimeOutTree(sessionId, prev.lastStamp());
			db.remove(sessionId);
		} else {
			updateTimeoutTree(sessionId, curr, prev);
			updateDb(opa, sessionId, curr, prev);

			_emitNew(collector, curr);
		}
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayBgnExtra prev, PlayEndExtra curr, TridentCollector collector) {
		if (curr.lastStamp() > prev.lastStamp()) {
			removeFromTimeOutTree(sessionId, prev.lastStamp());
			db.remove(sessionId);

			_emitEnd(collector, prev);
		}
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayBgnExtra prev, PlayAliveExtra curr, TridentCollector collector) {
		if (curr.lastStamp() > prev.lastStamp()) {
			updateTimeoutTree(sessionId, curr, prev);
			prev.update(curr);

			_emitAlive(collector, prev);
		}
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayBgnExtra prev, PlayBgnExtra curr, TridentCollector collector) {
		if (curr.lastStamp() > prev.lastStamp()) {
			updateTimeoutTree(sessionId, curr, prev);
			updateDb(opa, sessionId, curr, prev);

			_emitNew(collector, curr);
		}
	}

	private void updateDb(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayExtra curr, PlayExtra prev) {
		if (opa == null) {
			db.put(sessionId, new OpaqueValue<PlayExtra>(_currTx, curr, prev));
		} else {
			opa.prev = prev;
			opa.curr = curr;
			opa.currTxid = _currTx;
		}
	}

	private void rollbackTimeoutTree(String sessionId,
			OpaqueValue<PlayExtra> opa) {
		PlayExtra _old = opa.curr;
		PlayExtra _new = opa.prev;
		if (_old != null) {
			removeFromTimeOutTree(sessionId, _old.lastStamp());
		}
		if (_new != null) {
			addToTimeOutTree(sessionId, _new.lastStamp());
		}
	}

	private void _new(String sessionId, PlayExtra curr,
			TridentCollector collector) {
		if (curr.getPlayType().isAlive()) {
			return;
		}
		updateTimeoutTree(sessionId, curr, null);
		updateDb(null, sessionId, curr, null);
		if (curr.getPlayType().isBgn()) {
			_emitNew(collector, (PlayBgnExtra) curr);
		}
	}

	private void _emitNew(TridentCollector collector, PlayBgnExtra extra) {
		collector.emit(new Values(extra.getHid(), createBgnSessionTick(extra)));
	}

	private void _emitEnd(TridentCollector collector, PlayBgnExtra extra) {
		collector.emit(new Values(extra.getHid(), createEndSessionTick(extra)));
	}

	private void _emitAlive(TridentCollector collector, PlayBgnExtra extra) {
		collector
				.emit(new Values(extra.getHid(), createAliveSessionTick(extra)));
	}

	private void updateTimeoutTree(String sessionId, PlayExtra curr,
			PlayExtra prev) {
		if (prev != null) {
			removeFromTimeOutTree(sessionId, prev.lastStamp());
		}
		if (curr != null) {
			addToTimeOutTree(sessionId, curr.lastStamp());
		}
	}

	private void removeFromTimeOutTree(String sessionId, Long stamp) {
		if (!timeoutTree.containsKey(stamp)) {
			return;
		}
		Set<String> keys = timeoutTree.get(stamp);
		keys.remove(sessionId);
		if (keys.size() == 0) {
			timeoutTree.remove(stamp);
		}
	}

	private void addToTimeOutTree(String sessionId, Long stamp) {
		Set<String> keys = null;
		if (timeoutTree.containsKey(stamp)) {
			keys = timeoutTree.get(stamp);
		} else {
			keys = new HashSet<String>();
			timeoutTree.put(stamp, keys);
		}
		keys.add(sessionId);
	}

	protected SessionTick createBgnSessionTick(PlayBgnExtra playBgnExtra) {
		return createOrderSessionTick(PlayType.BGN, playBgnExtra);
	}

	protected SessionTick createAliveSessionTick(PlayBgnExtra playBgnExtra) {
		return createOrderSessionTick(PlayType.ALIVE, playBgnExtra);
	}

	protected SessionTick createEndSessionTick(PlayBgnExtra playBgnExtra) {
		return createOrderSessionTick(PlayType.END, playBgnExtra);
	}

	protected SessionTick createOrderSessionTick(PlayType type,
			PlayBgnExtra playBgnExtra) {
		SessionTick tick = new SessionTick(type, playBgnExtra.getHid());
		tick.setLastStamp(playBgnExtra.lastStamp());
		tick.setLow(playBgnExtra.isLow());
		tick.setOemid(playBgnExtra.getOemid());
		tick.setSessionId(playBgnExtra.getSessionId());
		tick.setSpid(playBgnExtra.getSpid());
		tick.setVip(playBgnExtra.isVip());
		return tick;
	}

	public static class PlayExtraUpdater implements
			StateUpdater<PlayExtraState> {

		public static final Fields INPUT_FIELDS = OrderExtraInfoQuery.OUTPUT_FIELDS;
		public static final Fields OUTPUT_FIELDS = new Fields("hid", "tick");

		public static TridentState partitionPersist(Stream stream) {
			return stream.partitionBy(new Fields("s")).partitionPersist(
					new PlayExtraStateFactory(), INPUT_FIELDS,
					new PlayExtraUpdater(), OUTPUT_FIELDS);
		}

		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map conf,
				TridentOperationContext context) {
		}

		@Override
		public void cleanup() {
		}

		@Override
		public void updateState(PlayExtraState state,
				List<TridentTuple> tuples, TridentCollector collector) {
			state.updateByPlayExtra(tuples, collector);
		}
	}

	public static class PlayExtraStateFactory implements StateFactory {
		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new PlayExtraStateImpl();
		}

	}

}
