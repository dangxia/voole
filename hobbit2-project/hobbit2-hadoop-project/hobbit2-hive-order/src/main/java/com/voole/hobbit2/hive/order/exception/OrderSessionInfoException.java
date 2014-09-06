/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.hive.order.exception;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class OrderSessionInfoException extends Exception {
	private final CharSequence sessionId;

	public CharSequence getSessionId() {
		return sessionId;
	}

	public OrderSessionInfoException(CharSequence sessionId) {
		super();
		this.sessionId = sessionId;
	}

	public OrderSessionInfoException(CharSequence sessionId, String message) {
		super(message);
		this.sessionId = sessionId;
	}

	public OrderSessionInfoException(CharSequence sessionId, String message,
			Throwable cause) {
		super(message, cause);
		this.sessionId = sessionId;
	}

	public OrderSessionInfoException(CharSequence sessionId, Throwable cause) {
		super(cause);
		this.sessionId = sessionId;
	}
}
