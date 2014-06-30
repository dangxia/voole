/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.module.extra;

/**
 * @author XuehuiHe
 * @date 2014年6月29日
 */
public class PlayEmptyExtra implements PlayExtra {

	@Override
	public long lastStamp() {
		return 0;
	}

	@Override
	public String getSessionId() {
		return null;
	}

	@Override
	public PlayType getPlayType() {
		return PlayType.EMPTY;
	}

}
