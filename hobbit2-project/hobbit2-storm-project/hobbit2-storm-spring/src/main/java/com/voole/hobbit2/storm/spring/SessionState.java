/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.spring;

import java.util.List;

import storm.trident.state.State;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public interface SessionState extends State {
	public void update(List<String> data);
}
