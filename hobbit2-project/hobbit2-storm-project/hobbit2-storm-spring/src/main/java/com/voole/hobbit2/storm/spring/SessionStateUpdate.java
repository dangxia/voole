/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.spring;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class SessionStateUpdate implements StateUpdater<SessionState> {
	private static final Logger log = LoggerFactory
			.getLogger(SessionState.class);

	private ClassPathXmlApplicationContext cxt;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf,
			TridentOperationContext context) {
		cxt = new ClassPathXmlApplicationContext("test-spring.xml");
		TestBean dao = cxt.getBean(TestBean.class);
		log.info(dao.getRealtimeJt().toString());
	}

	@Override
	public void cleanup() {
		cxt.close();
	}

	@Override
	public void updateState(SessionState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		List<String> list = new ArrayList<String>();
		for (TridentTuple tridentTuple : tuples) {
			list.add((String) tridentTuple.get(0));
		}
		state.update(list);

	}

}
