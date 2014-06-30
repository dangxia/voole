/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state;

import org.junit.Before;
import org.junit.Test;

import com.voole.hobbit.storm.order.module.extra.PlayAliveExtra;
import com.voole.hobbit.storm.order.module.extra.PlayBgnExtra;
import com.voole.hobbit.storm.order.module.extra.PlayEndExtra;
import com.voole.hobbit.storm.order.module.extra.PlayExtra.PlayType;

/**
 * @author XuehuiHe
 * @date 2014年6月29日
 */
public class PlayExtraStateTestEndThenElse extends PlayExtraStateTestBase {
	private PlayEndExtra prev;

	@Before
	public void before() {
		super.before();

		prev = createEnd(PREV_STAMP);
		state.update(prev, collector);
		list.clear();
	}

	@Test
	public void thenBgnGt() {
		PlayBgnExtra curr = createBgn(CURR_STAMP_GT);
		state.update(curr, collector);

		checkDb(1, curr);
		checkTimeoutTree(1, curr);

		checkCollector(1, curr, PlayType.BGN);
	}

	@Test
	public void thenBgnLt() {
		PlayBgnExtra curr = createBgn(CURR_STAMP_LT);
		state.update(curr, collector);

		dbIsEmpty();
		timeoutTreeIsEmpty();
		collectorIsEmpty();
	}

	@Test
	public void thenAliveGt() {
		PlayAliveExtra curr = createAlivelong(CURR_STAMP_GT);
		state.update(curr, collector);

		checkDb(1, curr);
		checkTimeoutTree(1, curr);

		collectorIsEmpty();
	}

	@Test
	public void thenAliveLt() {
		PlayAliveExtra curr = createAlivelong(CURR_STAMP_LT);
		state.update(curr, collector);

		checkDb(1, prev);
		checkTimeoutTree(1, prev);

		collectorIsEmpty();
	}

	@Test
	public void thenEndGt() {
		PlayEndExtra curr = createEnd(CURR_STAMP_GT);
		state.update(curr, collector);

		checkDb(1, curr);
		checkTimeoutTree(1, curr);

		collectorIsEmpty();
	}

	@Test
	public void thenEndLt() {
		PlayEndExtra curr = createEnd(CURR_STAMP_LT);
		state.update(curr, collector);

		checkDb(1, prev);
		checkTimeoutTree(1, prev);

		collectorIsEmpty();
	}
}
