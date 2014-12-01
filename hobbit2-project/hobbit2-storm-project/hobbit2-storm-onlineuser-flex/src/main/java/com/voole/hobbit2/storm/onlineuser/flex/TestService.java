package com.voole.hobbit2.storm.onlineuser.flex;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.voole.hobbit2.storm.onlineuser.flex.service.SyncHistoryData;

public class TestService {
	public static void main(String[] args) {
		ClassPathXmlApplicationContext cxt = new ClassPathXmlApplicationContext(
				"spring/spring-storm-onlineuser-calc.xml");
		SyncHistoryData syncHistoryData = cxt.getBean(SyncHistoryData.class);
		syncHistoryData.sync();
		cxt.close();
	}
}
