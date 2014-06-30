/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cachestate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author XuehuiHe
 * @date 2014年6月9日
 */
public class RefreshCmdSender implements IBatchSpout {
	public static final Fields OUTPUT_FIELDS = new Fields("cmds");
	private ArrayBlockingQueue<String> blockingQueue;
	private ClassPathXmlApplicationContext cxt;

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context) {
		cxt = new ClassPathXmlApplicationContext("refresh-cache-spring.xml");
		RefreshCmdAdder adder = cxt.getBean(RefreshCmdAdder.class);
		blockingQueue = adder.getBlockingQueue();
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		try {
			List<String> cmds = new ArrayList<String>();
			String cmd = blockingQueue.poll(200, TimeUnit.MILLISECONDS);
			while (cmd != null) {
				cmds.add(cmd);
				cmd = blockingQueue.poll(200, TimeUnit.MILLISECONDS);
			}
			if (cmds.size() > 0) {
				collector.emit(new Values(cmds));
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void ack(long batchId) {

	}

	@Override
	public void close() {
		if (cxt != null) {
			cxt.close();
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return OUTPUT_FIELDS;
	}

}
