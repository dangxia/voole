package com.voole.dungbeetle.ad.main;

import java.util.List;
import java.util.Map;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.voole.dungbeetle.ad.dao.IPlatFormDao;

public class StartUp {

	public static ClassPathXmlApplicationContext getSpringContext() {
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
				new String[] { "classpath:/configfile/applicationContext.xml" });
		return ctx;
	}

	public static void main(String[] args) {
		ClassPathXmlApplicationContext ctx = StartUp.getSpringContext();
//		IPlatFormDao platFormDao = (IPlatFormDao) ctx
//				.getBean("platFormDaoImpl");

//		List<Map<String, String>> allMovieTypeListTemp = platFormDao
//				.findAllMovieTypes();
//		System.out
//				.println("start successful" + allMovieTypeListTemp != null ? allMovieTypeListTemp
//						.size() : allMovieTypeListTemp);

		/*
		 * for(Map<String,Object> map : alladInfoListTemp){ Set<Entry<String,
		 * Object>> set = map.entrySet(); Iterator<Entry<String, Object>> iter =
		 * set.iterator(); while(iter.hasNext()){ Entry<String, Object> entry =
		 * iter.next(); System.out.println(entry.getKey() + " " +
		 * entry.getValue()); } }
		 */
		
		ctx.close();
	}
}
