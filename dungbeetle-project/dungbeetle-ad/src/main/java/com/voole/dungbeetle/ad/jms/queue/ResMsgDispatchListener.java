package com.voole.dungbeetle.ad.jms.queue;

import org.apache.log4j.Logger;

public abstract class ResMsgDispatchListener extends Thread {

	ResDataQueue<Message> msgQueue;
	Logger log = Logger.getLogger(getClass());

	public ResMsgDispatchListener() {
		msgQueue = new ResDataQueue<Message>();
	}

	public abstract void handleMsg(Message msg);

	public void process(Message msg) {
		// TODO Auto-generated method stub
		msgQueue.writeData(msg);

	}

	public void run() {
		while (true) {
			try {
				Message msg = msgQueue.readData();
				if (msg != null) {
					handleMsg(msg);
				}
			} catch (InterruptedException e) {
				break;
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
				log.error(e);
			}
		}
	}

}