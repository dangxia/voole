/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;

import com.voole.hobbit.storm.order.module.extra.PlayAliveExtra;
import com.voole.hobbit.storm.order.module.extra.PlayBgnExtra;
import com.voole.hobbit.storm.order.module.extra.PlayEndExtra;
import com.voole.hobbit.storm.order.module.extra.PlayExtra;
import com.voole.hobbit.storm.order.module.extra.PlayExtra.PlayType;

import storm.trident.operation.TridentCollector;

/**
 * @author XuehuiHe
 * @date 2014年6月29日
 */
public class PlayExtraStateTestBase {
	public final static String SID = "SID";
	public PlayExtraStateImpl state;
	public TridentCollectorTest collector;
	public List<List<Object>> list;

	public final static long PREV_STAMP = 2;

	public final static long CURR_STAMP_GT = 3;
	public final static long CURR_STAMP_LT = 1;

	@Before
	public void before() {
		state = new PlayExtraStateImpl();
		list = new ArrayList<List<Object>>();
		collector = new TridentCollectorTest(list);
	}

	protected PlayBgnExtra createBgn(long bgn) {
		PlayBgnExtra curr = new PlayBgnExtra();
		curr.setPlayBgn(bgn);
		curr.setSessionId(SID);
		curr.setBitrate(0);
		return curr;
	}

	protected PlayAliveExtra createAlivelong(long alive) {
		PlayAliveExtra curr = new PlayAliveExtra();
		curr.setAvgSpeed(0l);
		curr.setPlayAlive(alive);
		curr.setSessionId(SID);
		return curr;
	}

	protected PlayEndExtra createEnd(long end) {
		PlayEndExtra curr = new PlayEndExtra();
		curr.setEndTick(end);
		curr.setSessionId(SID);
		return curr;
	}

	protected void collectorIsEmpty() {
		checkCollector(0, null, null);
	}

	protected void checkCollector(int size, PlayBgnExtra curr, PlayType type) {
		Assert.assertEquals(size, list.size());
		if (curr != null) {
			Assert.assertEquals(PlayExtraStateImpl.createOrderSessionTick(type,
					curr), list.get(0).get(1));
		}
		list.clear();
	}

	protected void dbIsEmpty() {
		checkDb(0, null);
	}

	protected void checkDb(int size, PlayExtra curr) {
		Assert.assertEquals(size, state.getDb().size());
		if (curr != null) {
			Assert.assertTrue(state.getDb().containsKey(SID));
			Assert.assertEquals(state.getDb().get(SID).curr, curr);
		}
	}

	protected void timeoutTreeIsEmpty() {
		checkTimeoutTree(0, null);
	}

	protected void checkTimeoutTree(int size, PlayExtra curr) {
		Assert.assertEquals(size, state.getTimeoutTree().size());
		if (curr != null) {
			long stamp = curr.lastStamp();
			Assert.assertTrue(state.getTimeoutTree().containsKey(stamp));
			Assert.assertEquals(1, state.getTimeoutTree().get(stamp).size());
			Assert.assertEquals(true, state.getTimeoutTree().get(stamp)
					.contains(SID));
		}
	}

	public static class TridentCollectorTest implements TridentCollector {
		public final List<List<Object>> list;

		public TridentCollectorTest(List<List<Object>> list) {
			this.list = list;
		}

		@Override
		public void emit(List<Object> values) {
			list.add(values);
		}

		@Override
		public void reportError(Throwable t) {
		}

	}
}
