/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.common.convert;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public class ConvertDontSupportException extends Exception {
	public ConvertDontSupportException() {
		super();
	}

	public ConvertDontSupportException(String message) {
		super(message);
	}

	public ConvertDontSupportException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConvertDontSupportException(Throwable cause) {
		super(cause);
	}
}
