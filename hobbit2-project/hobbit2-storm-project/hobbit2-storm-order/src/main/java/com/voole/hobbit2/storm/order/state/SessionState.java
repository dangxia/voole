/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.state;

import java.util.List;

import org.apache.avro.specific.SpecificRecordBase;

import storm.trident.state.State;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public interface SessionState extends State {
	public void update(List<SpecificRecordBase> data);

	public void close();
}
