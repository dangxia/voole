/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.spring;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.google.common.base.Throwables;
import com.voole.hobbit2.cache.AreaInfoCache;
import com.voole.hobbit2.cache.AreaInfoCacheImpl;
import com.voole.hobbit2.cache.OemInfoCache;
import com.voole.hobbit2.cache.OemInfoCacheImpl;
import com.voole.hobbit2.cache.ResourceInfoCache;
import com.voole.hobbit2.cache.ResourceInfoCacheImpl;
import com.voole.hobbit2.cache.db.CacheDao;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class SessionStateImpl implements SessionState {
	private static final Logger log = LoggerFactory
			.getLogger(SessionState.class);

	private final AreaInfoCache areaInfoCache;
	private final OemInfoCache oemInfoCache;
	private final ResourceInfoCache resourceInfoCache;

	public SessionStateImpl(CacheDao dao) {
		areaInfoCache = new AreaInfoCacheImpl(dao);
		oemInfoCache = new OemInfoCacheImpl(dao);
		resourceInfoCache = new ResourceInfoCacheImpl(dao);
		try {
			areaInfoCache.refresh();
			oemInfoCache.refresh();
			resourceInfoCache.refresh();
		} catch (Exception e) {
			Throwables.propagate(e);
		}
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
			ClassPathXmlApplicationContext cxt = new ClassPathXmlApplicationContext(
					"cache-dao.xml");
			return new SessionStateImpl(cxt.getBean(CacheDao.class));
		}

	}

}
