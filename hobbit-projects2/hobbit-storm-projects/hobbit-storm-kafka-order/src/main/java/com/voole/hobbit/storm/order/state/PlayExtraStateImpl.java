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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private static Logger logger = LoggerFactory
			.getLogger(PlayExtraStateImpl.class);

	private long prev = 0;

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

	public Long get_currTx() {
		return _currTx;
	}

	public void set_currTx(Long _currTx) {
		this._currTx = _currTx;
	}

	public TreeMap<Long, Set<String>> getTimeoutTree() {
		return timeoutTree;
	}

	public Map<String, OpaqueValue<PlayExtra>> getDb() {
		return db;
	}

	@Override
	public void gc(TridentCollector collector) {
		long now = System.currentTimeMillis();
		if (now - prev > 2 * 60 * 1000) {
			prev = now;
		} else {
			return;
		}
		logger.info("session state gc start");
		long max = System.currentTimeMillis() / 1000 - 60 * 10;
		Map<Long, Set<String>> timeout = timeoutTree
				.subMap(0l, true, max, true);
		if (timeout == null) {
			return;
		}
		Set<Long> timeoutKeys = new HashSet<Long>();
		timeoutKeys.addAll(timeout.keySet());
		Set<String> timeoutSessionIds = new HashSet<String>();
		for (Long k : timeoutKeys) {
			timeoutSessionIds.addAll(timeoutTree.remove(k));
		}
		long gc = 0;
		for (String sessionId : timeoutSessionIds) {
			gc++;
			OpaqueValue<PlayExtra> opa = db.remove(sessionId);
			if (opa != null && opa.curr != null
					&& opa.curr.getPlayType().isBgn()) {
				_emitEnd(collector, (PlayBgnExtra) opa.curr);
			}
		}
		logger.info("session state gc:" + gc);
	}

	@Override
	public void updateByPlayExtra(List<TridentTuple> tuples,
			TridentCollector collector) {
		for (TridentTuple tuple : tuples) {
			update((PlayExtra) tuple.get(1), collector);
		}
		gc(collector);
	}

	protected void update(PlayExtra extra, TridentCollector collector) {
		String sessionId = extra.getSessionId();
		OpaqueValue<PlayExtra> opa = db.get(sessionId);
		if (opa == null) {
			_new(opa, sessionId, extra, collector);
		} else {
			_update(opa, sessionId, extra, collector);
		}
	}

	protected void _new(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayExtra curr, TridentCollector collector) {
		update(opa, sessionId, curr, null);
		if (curr.getPlayType().isBgn()) {
			_emitNew(collector, (PlayBgnExtra) curr);
		}
	}

	protected void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayExtra curr, TridentCollector collector) {
		if (_currTx != null && _currTx.equals(opa.currTxid)) {
			rollbackTimeoutTree(sessionId, opa);
		}
		PlayExtra prev = opa.get(_currTx);
		if (prev == null) {
			_new(opa, sessionId, curr, collector);
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
			} else if (prev.getPlayType().isEnd()) {
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
			} else {
				if (curr.getPlayType().isBgn()) {
					_update(opa, sessionId, (PlayAliveExtra) prev,
							(PlayBgnExtra) curr, collector);
				} else if (curr.getPlayType().isAlive()) {
					_update(opa, sessionId, (PlayAliveExtra) prev,
							(PlayAliveExtra) curr, collector);
				} else {
					_update(opa, sessionId, (PlayAliveExtra) prev,
							(PlayEndExtra) curr, collector);
				}
			}
		}
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayAliveExtra prev, PlayEndExtra curr, TridentCollector collector) {
		if (curr.lastStamp() > prev.lastStamp()) {
			update(opa, sessionId, curr, prev);
		}
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayAliveExtra prev, PlayAliveExtra curr, TridentCollector collector) {
		if (curr.lastStamp() > prev.lastStamp()) {
			update(opa, sessionId, curr, prev);
		}
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayAliveExtra prev, PlayBgnExtra curr, TridentCollector collector) {
		if (curr.lastStamp() > prev.lastStamp()) {
			update(opa, sessionId, curr, prev);

			_emitNew(collector, curr);
		} else {
			curr.update(prev);
			updateDb(opa, sessionId, curr, prev);

			_emitNew(collector, curr);
		}
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayEndExtra prev, PlayEndExtra curr, TridentCollector collector) {
		if (curr.lastStamp() > prev.lastStamp()) {
			update(opa, sessionId, curr, prev);
		}
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayEndExtra prev, PlayAliveExtra curr, TridentCollector collector) {
		if (curr.lastStamp() > prev.lastStamp()) {
			update(opa, sessionId, curr, prev);
		}
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayEndExtra prev, PlayBgnExtra curr, TridentCollector collector) {
		if (prev.lastStamp() > curr.lastStamp()) {
			removePrev(sessionId, prev);
		} else {
			update(opa, sessionId, curr, prev);

			_emitNew(collector, curr);
		}
	}

	private void _update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayBgnExtra prev, PlayEndExtra curr, TridentCollector collector) {
		if (curr.lastStamp() > prev.lastStamp()) {
			removePrev(sessionId, prev);

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
			update(opa, sessionId, curr, prev);
			_emitNew(collector, curr);
		}
	}

	private void update(OpaqueValue<PlayExtra> opa, String sessionId,
			PlayExtra curr, PlayExtra prev) {
		updateTimeoutTree(sessionId, curr, prev);
		updateDb(opa, sessionId, curr, prev);
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

	private void updateTimeoutTree(String sessionId, PlayExtra curr,
			PlayExtra prev) {
		if (prev != null) {
			removeFromTimeOutTree(sessionId, prev.lastStamp());
		}
		if (curr != null) {
			addToTimeOutTree(sessionId, curr.lastStamp());
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

	private void removePrev(String sessionId, PlayExtra prev) {
		removeFromTimeOutTree(sessionId, prev.lastStamp());
		db.remove(sessionId);
	}

	private void rollbackTimeoutTree(String sessionId,
			OpaqueValue<PlayExtra> opa) {
		PlayExtra _old = opa.curr;
		PlayExtra _new = opa.prev;
		updateTimeoutTree(sessionId, _new, _old);
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

	protected static SessionTick createBgnSessionTick(PlayBgnExtra playBgnExtra) {
		return createOrderSessionTick(PlayType.BGN, playBgnExtra);
	}

	protected static SessionTick createAliveSessionTick(
			PlayBgnExtra playBgnExtra) {
		return createOrderSessionTick(PlayType.ALIVE, playBgnExtra);
	}

	protected static SessionTick createEndSessionTick(PlayBgnExtra playBgnExtra) {
		return createOrderSessionTick(PlayType.END, playBgnExtra);
	}

	protected static SessionTick createOrderSessionTick(PlayType type,
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
