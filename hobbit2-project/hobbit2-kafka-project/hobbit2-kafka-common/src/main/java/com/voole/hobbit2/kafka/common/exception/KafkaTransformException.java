/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.kafka.common.exception;

/**
 * @author XuehuiHe
 * @date 2014年6月3日
 */
public class KafkaTransformException extends Exception {

	public KafkaTransformException() {
		super();
	}

	public KafkaTransformException(String message) {
		super(message);
	}

	public KafkaTransformException(String message, Throwable cause) {
		super(message, cause);
	}

	public KafkaTransformException(Throwable cause) {
		super(cause);
	}

}
