/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.hive.order.exception;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class OrderSessionInfoBgnNullException extends OrderSessionInfoException {
	public OrderSessionInfoBgnNullException(CharSequence sessionId) {
		super(sessionId);
	}

	public OrderSessionInfoBgnNullException(CharSequence sessionId,
			String message) {
		super(sessionId, message);
	}

	public OrderSessionInfoBgnNullException(CharSequence sessionId,
			String message, Throwable cause) {
		super(sessionId, message, cause);
	}

	public OrderSessionInfoBgnNullException(CharSequence sessionId,
			Throwable cause) {
		super(sessionId, cause);
	}
}
