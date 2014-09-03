/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.transform;

/**
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public class CamusTransformException extends Exception {
	public CamusTransformException() {
		super();
	}

	public CamusTransformException(String message) {
		super(message);
	}

	public CamusTransformException(String message, Throwable cause) {
		super(message, cause);
	}

	public CamusTransformException(Throwable cause) {
		super(cause);
	}
}
