/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.exception;

/**
 * @author XuehuiHe
 * @date 2014年5月29日
 */
public class FailedFetchException extends RuntimeException {
	public FailedFetchException(Exception e) {
		super(e);
	}
}
