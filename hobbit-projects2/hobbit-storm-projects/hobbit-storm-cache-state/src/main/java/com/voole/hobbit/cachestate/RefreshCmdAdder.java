/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cachestate;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author XuehuiHe
 * @date 2014年6月9日
 */
public class RefreshCmdAdder {
	private ArrayBlockingQueue<String> blockingQueue;

	public void add(String... cmds) {
		for (String cmd : cmds) {
			blockingQueue.add(cmd);
		}
	}

	public ArrayBlockingQueue<String> getBlockingQueue() {
		return blockingQueue;
	}

	public void setBlockingQueue(ArrayBlockingQueue<String> blockingQueue) {
		this.blockingQueue = blockingQueue;
	}

}
