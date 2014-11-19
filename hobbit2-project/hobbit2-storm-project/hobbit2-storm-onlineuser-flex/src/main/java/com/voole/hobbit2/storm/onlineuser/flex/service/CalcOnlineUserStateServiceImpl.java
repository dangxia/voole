package com.voole.hobbit2.storm.onlineuser.flex.service;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.voole.hobbit2.cache.OemInfoCache;
import com.voole.hobbit2.cache.entity.OemInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;
import com.voole.hobbit2.common.enums.ProductType;
import com.voole.hobbit2.storm.onlineuser.flex.OnlineUserConfigs;
import com.voole.hobbit2.storm.onlineuser.flex.dao.PhoenixDao;
import com.voole.hobbit2.storm.onlineuser.flex.model.calc.CalcOemOnlineUserState;
import com.voole.hobbit2.storm.onlineuser.flex.model.calc.CalcSpOnlineUserState;
import com.voole.hobbit2.storm.onlineuser.flex.model.calc.PhoenixOnlineUserState;

public class CalcOnlineUserStateServiceImpl implements
		CalcOnlineUserStateService {
	private static Logger log = LoggerFactory
			.getLogger(CalcOnlineUserStateServiceImpl.class);

	private PhoenixDao phoenixDao;
	private OemInfoCache oemInfoCache;

	public void calc() {

		long stamp = calcStamp();
		log.info("calc online user start");
		long start = System.currentTimeMillis();
		long queryStart = start;

		List<PhoenixOnlineUserState> list = getPhoenixDao()
				.queryOnlineUserState();
		log.info("query from phoenix used time:"
				+ (System.currentTimeMillis() - queryStart));

		CalcSpOnlineUserState global = new CalcSpOnlineUserState();
		global.setSpid(OnlineUserConfigs.GLOBAL_SPID);
		global.setStamp(stamp);

		Map<String, CalcSpOnlineUserState> spidToRecord = new HashMap<String, CalcSpOnlineUserState>();
		spidToRecord.put(OnlineUserConfigs.GLOBAL_SPID, global);
		Map<Long, CalcOemOnlineUserState> oemidToRecord = new HashMap<Long, CalcOemOnlineUserState>();
		for (PhoenixOnlineUserState queryItem : list) {
			long oemid = queryItem.getOemid();
			String spid = null;
			try {
				spid = findSpid(oemid);
			} catch (Exception e) {
				spid = "-1";
			}
			CalcOemOnlineUserState oemRecord = null;
			if (oemidToRecord.containsKey(oemid)) {
				oemRecord = oemidToRecord.get(oemid);
			} else {
				oemRecord = new CalcOemOnlineUserState();
				oemRecord.setOemid(oemid);
				oemRecord.setSpid(spid);
				oemRecord.setStamp(stamp);
				oemidToRecord.put(oemid, oemRecord);
			}

			CalcSpOnlineUserState spRecord = null;
			if (spidToRecord.containsKey(spid)) {
				spRecord = spidToRecord.get(spid);
			} else {
				spRecord = new CalcSpOnlineUserState();
				spRecord.setSpid(spid);
				spRecord.setStamp(stamp);
				spidToRecord.put(spid, spRecord);
			}

			oemRecord.append(queryItem.getTotal(), queryItem.getLow());
			spRecord.append(queryItem.getTotal(), queryItem.getLow());
			global.append(queryItem.getTotal(), queryItem.getLow());
		}

		log.info("insert into phoenix start");
		long hbaseInsertStart = System.currentTimeMillis();
		getPhoenixDao().updateOnlineUserState(
				Lists.newArrayList(oemidToRecord.values()),
				Lists.newArrayList(spidToRecord.values()));
		log.info("insert into phoenix end,used time:"
				+ (System.currentTimeMillis() - hbaseInsertStart));
		log.info("calc online user end,used time:"
				+ (System.currentTimeMillis() - start));
	}

	public String findSpid(Long oemid) throws CacheRefreshException,
			CacheQueryException {
		Optional<OemInfo> oemInfoOpt = getOemInfoCache().getOemInfo(oemid);
		if (!oemInfoOpt.isPresent()) {
			return ProductType.VOOLE_SPID.toString();
		} else {
			return oemInfoOpt.get().getSpid();
		}
	}

	public static long calcStamp() {
		Calendar c = Calendar.getInstance();
		c.set(Calendar.MILLISECOND, 0);
		c.set(Calendar.SECOND, 0);
		int minute = c.get(Calendar.MINUTE);
		if (minute % 2 == 1) {
			c.add(Calendar.MINUTE, 1);
		}
		return c.getTimeInMillis() / 1000;
	}

	public PhoenixDao getPhoenixDao() {
		return phoenixDao;
	}

	public void setPhoenixDao(PhoenixDao phoenixDao) {
		this.phoenixDao = phoenixDao;
	}

	public OemInfoCache getOemInfoCache() {
		return oemInfoCache;
	}

	public void setOemInfoCache(OemInfoCache oemInfoCache) {
		this.oemInfoCache = oemInfoCache;
	}

}
