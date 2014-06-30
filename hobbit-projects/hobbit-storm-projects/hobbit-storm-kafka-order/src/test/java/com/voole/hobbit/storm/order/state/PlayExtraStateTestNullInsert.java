/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state;

import org.junit.Test;

import com.voole.hobbit.storm.order.module.extra.PlayAliveExtra;
import com.voole.hobbit.storm.order.module.extra.PlayBgnExtra;
import com.voole.hobbit.storm.order.module.extra.PlayEndExtra;
import com.voole.hobbit.storm.order.module.extra.PlayExtra;
import com.voole.hobbit.storm.order.module.extra.PlayExtra.PlayType;

/**
 * @author XuehuiHe
 * @date 2014年6月29日
 */
public class PlayExtraStateTestNullInsert extends PlayExtraStateTestBase {

	@Test
	public void testNullBgn() {
		long stamp = 11l;
		PlayBgnExtra curr = createBgn(stamp);
		state.update(curr, collector);

		testNullInsert(curr);

		checkCollector(1, curr, PlayType.BGN);
	}

	@Test
	public void testNullAlive() {
		long stamp = 11l;
		PlayAliveExtra curr = createAlivelong(stamp);
		state.update(curr, collector);

		testNullInsert(curr);

		collectorIsEmpty();
	}

	@Test
	public void testNullEnd() {
		long stamp = 11l;
		PlayEndExtra curr = createEnd(stamp);
		state.update(curr, collector);

		testNullInsert(curr);

		collectorIsEmpty();
	}

	protected void testNullInsert(PlayExtra curr) {
		checkDb(1, curr);
		checkTimeoutTree(1, curr);
	}

}
