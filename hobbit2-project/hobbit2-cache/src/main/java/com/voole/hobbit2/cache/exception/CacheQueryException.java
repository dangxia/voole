/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache.exception;

/**
 * @author XuehuiHe
 * @date 2014年9月9日
 */
public class CacheQueryException extends Exception {
	public CacheQueryException() {
		super();
	}

	public CacheQueryException(String message) {
		super(message);
	}

	public CacheQueryException(String message, Throwable cause) {
		super(message, cause);
	}

	public CacheQueryException(Throwable cause) {
		super(cause);
	}
}
