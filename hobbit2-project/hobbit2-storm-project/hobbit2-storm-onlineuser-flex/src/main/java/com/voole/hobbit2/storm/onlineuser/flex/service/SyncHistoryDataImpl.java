package com.voole.hobbit2.storm.onlineuser.flex.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit2.storm.onlineuser.flex.dao.PhoenixDao;

public class SyncHistoryDataImpl implements SyncHistoryData {

	private static Logger log = LoggerFactory
			.getLogger(SyncHistoryDataImpl.class);
	private PhoenixDao phoenixDao;

	@Override
	public void sync() {
		long start = System.currentTimeMillis();
		log.info("sync history started");
		phoenixDao.sync();
		log.info("sync history end,using time:"
				+ (System.currentTimeMillis() - start) / 1000 + " seconds");
	}

	public PhoenixDao getPhoenixDao() {
		return phoenixDao;
	}

	public void setPhoenixDao(PhoenixDao phoenixDao) {
		this.phoenixDao = phoenixDao;
	}

}
