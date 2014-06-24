/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.module.session;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class EndSessionInfo implements SessionInfo {
	private String sessionId;
	private Long playEnd;

	@Override
	public String getSessionId() {
		return this.sessionId;
	}

	@Override
	public Long lastStamp() {
		return getPlayEnd();
	}

	@Override
	public boolean isDead() {
		return true;
	}

	public Long getPlayEnd() {
		return playEnd;
	}

	public void setPlayEnd(Long playEnd) {
		this.playEnd = playEnd;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

}
