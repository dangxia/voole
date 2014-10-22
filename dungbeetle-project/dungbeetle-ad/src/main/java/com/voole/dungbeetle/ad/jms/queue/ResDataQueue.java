package com.voole.dungbeetle.ad.jms.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.voole.dungbeetle.ad.util.GlobalProperties;

@Component
public class ResDataQueue<T>{
	Logger log = Logger.getLogger(getClass());
	protected BlockingQueue<T> queue;
	private int max = Integer.parseInt(GlobalProperties.getProperties("message.queue.size"));// 阻塞队列大小
	private int clear = 0;

	public ResDataQueue(){
		clear = max > 10 ? max / 10 : max / 2;
		queue = new ArrayBlockingQueue<T>(max);
	}

	public T readData() throws InterruptedException{
		return queue.take();
	}


	public T readDataNoWait() throws InterruptedException {
		return queue.poll();
	}

	
	public List<T> readAllDataNoWait() throws InterruptedException{
		List<T> t_l = new ArrayList<T>();
		queue.drainTo(t_l);
		if(t_l.size()> 0){
			return t_l;
		}
		return null;
	}


	public  boolean writeData(T data) {
		if(data == null) {
			log.error("Do not write null to the DataPipe");
			return false;
		}
		checkFull();
		queue.offer(data);
		return true;
	}
	
	private synchronized void checkFull(){
		if(queue.size() >= max){
			log.info("DataPipe exceeds upper limit");
			for (int i=0; i<clear; i++) 
				queue.poll();
		}
	}
}
