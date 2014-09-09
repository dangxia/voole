/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache.exception;

/**
 * @author XuehuiHe
 * @date 2014年9月9日
 */
public class CacheRefreshException extends Exception {
	public CacheRefreshException() {
		super();
	}

	public CacheRefreshException(String message) {
		super(message);
	}

	public CacheRefreshException(String message, Throwable cause) {
		super(message, cause);
	}

	public CacheRefreshException(Throwable cause) {
		super(cause);
	}
}
