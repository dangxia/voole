/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.common.convert;

import com.voole.hobbit2.tools.common.Hobbit2Utils;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public class ConvertException extends Exception {
	private Class<?> target;
	private String source;

	public ConvertException() {
		super();
	}

	public ConvertException(String message) {
		super(message);
	}

	public ConvertException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConvertException(Throwable cause) {
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

	@Override
	public String toString() {
		return "targetClass:" + target + "\tsource:"
				+ Hobbit2Utils.toString(source);
	}

}
