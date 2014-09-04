/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api;

/**
 * @author XuehuiHe
 * @date 2014年9月4日
 */
public interface IStampFinder {
	<Value> Long findStamp(Value value) throws IllegalArgumentException;

}
