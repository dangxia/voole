/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.transformer;

/**
 * @author XuehuiHe
 * @date 2014年6月3日
 */
public class KafkaTransformException extends RuntimeException {

	private Class<?> target;
	private String source;

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

	public Class<?> getTarget() {
		return target;
	}

	public void setTarget(Class<?> target) {
		this.target = target;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

}
