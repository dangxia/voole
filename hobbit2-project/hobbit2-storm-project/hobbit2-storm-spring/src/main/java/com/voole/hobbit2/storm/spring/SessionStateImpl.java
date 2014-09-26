/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.spring;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class SessionStateImpl implements SessionState {
	private static final Logger log = LoggerFactory
			.getLogger(SessionState.class);
	public SessionStateImpl() {

	}

	@Override
	public void beginCommit(Long arg0) {
	}

	@Override
	public void commit(Long arg0) {
	}

	@Override
	public void update(List<String> data) {
		for (String string : data) {
			log.info("update:" + string);
		}
	}

	public static class SessionStateFactory implements StateFactory {
		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new SessionStateImpl();
		}

	}

}
