/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache.db;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author XuehuiHe
 * @date 2014年6月11日
 */
public class CacheDaoUtil {
	private static ClassPathXmlApplicationContext cxt;
	private static AtomicInteger times = new AtomicInteger(0);

	private static class CacheDaoHolder {
		static CacheDao cacheDao;
		static {
			cxt = new ClassPathXmlApplicationContext("cache-dao.xml");
			cacheDao = cxt.getBean(CacheDao.class);
		}
	}

	public synchronized static CacheDao getCacheDao() {
		times.incrementAndGet();
		return CacheDaoHolder.cacheDao;
	}

	public synchronized static void close() {
		int t = times.decrementAndGet();
		if (t == 0 && cxt != null) {
			cxt.close();
		}
	}
}
