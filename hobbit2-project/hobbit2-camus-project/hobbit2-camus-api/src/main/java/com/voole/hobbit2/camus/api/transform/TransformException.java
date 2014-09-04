/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.transform;

/**
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public class TransformException extends Exception {
	public TransformException() {
		super();
	}

	public TransformException(String message) {
		super(message);
	}

	public TransformException(String message, Throwable cause) {
		super(message, cause);
	}

	public TransformException(Throwable cause) {
		super(cause);
	}
}
