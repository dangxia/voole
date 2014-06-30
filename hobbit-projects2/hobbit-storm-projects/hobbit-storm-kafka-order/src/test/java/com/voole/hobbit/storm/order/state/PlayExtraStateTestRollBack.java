/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.state;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.voole.hobbit.storm.order.module.extra.PlayBgnExtra;
import com.voole.hobbit.storm.order.module.extra.PlayExtra;
import com.voole.hobbit.storm.order.module.extra.PlayExtra.PlayType;

/**
 * @author XuehuiHe
 * @date 2014年6月29日
 */
public class PlayExtraStateTestRollBack extends PlayExtraStateTestBase {
	@Before
	public void before() {
		super.before();
	}

	@Test
	public void testInsertBgnRightTx() {
		PlayBgnExtra bgn1 = createBgn(PREV_STAMP);
		state.update(bgn1, collector);

		checkDb(1, bgn1);
		checkTimeoutTree(1, bgn1);

		checkCollector(1, bgn1, PlayType.BGN);

		PlayBgnExtra bgn2 = createBgn(CURR_STAMP_GT);
		state.update(bgn2, collector);
		checkDb(1, bgn2);
		checkTimeoutTree(1, bgn2);
		checkCollector(1, bgn2, PlayType.BGN);

		OpaqueValue<PlayExtra> opa = state.getDb().get(SID);
		Assert.assertEquals(bgn1, opa.prev);
		Assert.assertEquals(bgn2, opa.curr);
	}

	@Test
	public void testInsertBgnWrongTx() {
		state.set_currTx(1l);
		PlayBgnExtra bgn1 = createBgn(PREV_STAMP);
		state.update(bgn1, collector);

		checkDb(1, bgn1);
		checkTimeoutTree(1, bgn1);

		checkCollector(1, bgn1, PlayType.BGN);

		PlayBgnExtra bgn2 = createBgn(CURR_STAMP_GT);
		state.update(bgn2, collector);
		checkDb(1, bgn2);
		checkTimeoutTree(1, bgn2);
		checkCollector(1, bgn2, PlayType.BGN);

		OpaqueValue<PlayExtra> opa = state.getDb().get(SID);
		Assert.assertEquals(null, opa.prev);
		Assert.assertEquals(bgn2, opa.curr);
	}

	@Test
	public void testInsertBgnWrongTx2() {
		state.set_currTx(1l);
		PlayBgnExtra bgn1 = createBgn(PREV_STAMP);
		state.update(bgn1, collector);

		checkDb(1, bgn1);
		checkTimeoutTree(1, bgn1);

		checkCollector(1, bgn1, PlayType.BGN);
		state.set_currTx(2l);
		PlayBgnExtra bgn2 = createBgn(CURR_STAMP_GT);
		state.update(bgn2, collector);
		checkDb(1, bgn2);
		checkTimeoutTree(1, bgn2);
		checkCollector(1, bgn2, PlayType.BGN);

		OpaqueValue<PlayExtra> opa = state.getDb().get(SID);
		Assert.assertEquals(bgn1, opa.prev);
		Assert.assertEquals(bgn2, opa.curr);

		PlayBgnExtra bgn3 = createBgn(CURR_STAMP_GT + 1);
		state.update(bgn3, collector);
		checkDb(1, bgn3);
		checkTimeoutTree(1, bgn3);
		checkCollector(1, bgn3, PlayType.BGN);

		opa = state.getDb().get(SID);
		Assert.assertEquals(bgn1, opa.prev);
		Assert.assertEquals(bgn3, opa.curr);
	}
}
