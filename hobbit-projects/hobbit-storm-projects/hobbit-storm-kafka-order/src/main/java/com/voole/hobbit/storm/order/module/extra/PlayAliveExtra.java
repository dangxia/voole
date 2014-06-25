/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.module.extra;

import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV3;

/**
 * @author XuehuiHe
 * @date 2014年6月24日
 */
public class PlayAliveExtra implements PlayExtra {
	private String sessionId;
	private long playAlive;// 播放心跳时间(单位:second)
	private long avgSpeed;// 平均速度

	public void fillWith(OrderPlayAliveReqV2 v) {
		setSessionId(v.getSessID());
		setPlayAlive(v.getAliveTick());
		setAvgSpeed(v.getSessAvgSpeed());
	}

	public void fillWith(OrderPlayAliveReqV3 v) {
		setSessionId(v.getSessID());
		setPlayAlive(v.getAliveTick());
		setAvgSpeed(v.getSessAvgSpeed());
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public long getPlayAlive() {
		return playAlive;
	}

	public void setPlayAlive(long playAlive) {
		this.playAlive = playAlive;
	}

	public long getAvgSpeed() {
		return avgSpeed;
	}

	public void setAvgSpeed(long avgSpeed) {
		this.avgSpeed = avgSpeed;
	}

	@Override
	public long lastStamp() {
		return getPlayAlive();
	}

	@Override
	public PlayType getPlayType() {
		return PlayType.ALIVE;
	}

	public static PlayAliveExtra getExtra(Object proto) {
		if (proto == null) {
			return null;
		}
		if (proto instanceof OrderPlayAliveReqV2) {
			PlayAliveExtra extra = new PlayAliveExtra();
			extra.fillWith((OrderPlayAliveReqV2) proto);
			return extra;
		} else if (proto instanceof OrderPlayAliveReqV3) {
			PlayAliveExtra extra = new PlayAliveExtra();
			extra.fillWith((OrderPlayAliveReqV3) proto);
			return extra;
		}
		return null;
	}

}
