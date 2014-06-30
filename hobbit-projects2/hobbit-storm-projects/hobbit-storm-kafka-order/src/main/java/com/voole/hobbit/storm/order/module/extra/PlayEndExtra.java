/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.module.extra;

import com.voole.hobbit.proto.TerminalPB.OrderPlayEndReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayEndReqV3;

/**
 * @author XuehuiHe
 * @date 2014年6月24日
 */
public class PlayEndExtra implements PlayExtra {
	private String sessionId;
	private long endTick;

	@Override
	public long lastStamp() {
		return endTick;
	}

	@Override
	public String getSessionId() {
		return sessionId;
	}

	@Override
	public PlayType getPlayType() {
		return PlayType.END;
	}

	public long getEndTick() {
		return endTick;
	}

	public void setEndTick(long endTick) {
		this.endTick = endTick;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public void fillWith(OrderPlayEndReqV2 v) {
		setSessionId(v.getSessID());
		setEndTick(v.getEndTick());
	}

	public void fillWith(OrderPlayEndReqV3 v) {
		setSessionId(v.getSessID());
		setEndTick(v.getEndTick());
	}

	public static PlayEndExtra getExtra(Object proto) {
		if (proto == null) {
			return null;
		}
		PlayEndExtra extra = null;
		if (proto instanceof OrderPlayEndReqV2) {
			extra = new PlayEndExtra();
			extra.fillWith((OrderPlayEndReqV2) proto);
		} else if (proto instanceof OrderPlayEndReqV2) {
			extra = new PlayEndExtra();
			extra.fillWith((OrderPlayEndReqV3) proto);
		}
		return extra;
	}
}
