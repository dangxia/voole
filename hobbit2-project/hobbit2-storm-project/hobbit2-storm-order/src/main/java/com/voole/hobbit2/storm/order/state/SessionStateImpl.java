/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.voole.hobbit2.cache.AreaInfoCache;
import com.voole.hobbit2.cache.AreaInfoCacheImpl;
import com.voole.hobbit2.cache.OemInfoCache;
import com.voole.hobbit2.cache.OemInfoCacheImpl;
import com.voole.hobbit2.cache.ResourceInfoCache;
import com.voole.hobbit2.cache.ResourceInfoCacheImpl;
import com.voole.hobbit2.cache.db.CacheDao;
import com.voole.hobbit2.cache.db.CacheDaoUtil;
import com.voole.hobbit2.cache.entity.AreaInfo;
import com.voole.hobbit2.cache.entity.OemInfo;
import com.voole.hobbit2.cache.entity.ResourceInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;
import com.voole.hobbit2.camus.order.dry.PlayBgnDryRecord;
import com.voole.hobbit2.common.enums.ProductType;
import com.voole.hobbit2.storm.order.util.DryGenerator;
import com.voole.hobbit2.storm.order.util.PutGenerator;

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
	private HConnection hConnection;

	public SessionStateImpl(CacheDao dao) {
		areaInfoCache = new AreaInfoCacheImpl(dao);
		oemInfoCache = new OemInfoCacheImpl(dao);
		resourceInfoCache = new ResourceInfoCacheImpl(dao);
		try {
			areaInfoCache.refresh();
			oemInfoCache.refresh();
			resourceInfoCache.refresh();
			hConnection = HConnectionManager
					.createConnection(HBaseConfiguration.create());
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
	public void update(List<SpecificRecordBase> data) {
		List<SpecificRecordBase> result = new ArrayList<SpecificRecordBase>();
		for (SpecificRecordBase base : data) {
			try {
				SpecificRecordBase dry = (SpecificRecordBase) DryGenerator
						.dry(base);
				if (dry == null) {
					continue;
				}
				if (dry instanceof PlayBgnDryRecord) {
					dealBgn((PlayBgnDryRecord) dry);
				}
				result.add(dry);
			} catch (Exception e) {
				log.warn("record dry failed", e);
			}

		}
		if (result.size() == 0) {
			return;
		}
		try {
			List<Put> sessionPuts = new ArrayList<Put>();
			for (SpecificRecordBase specificRecordBase : result) {
				try {
					Put put = PutGenerator.generateSession(specificRecordBase);
					if (put != null) {
						sessionPuts.add(put);
					}
				} catch (Exception e) {
					log.warn("session put generate failed");
					continue;
				}
			}
			if (sessionPuts.size() > 0) {
				HTableInterface sessionTable = hConnection
						.getTable("storm_order_session");
				sessionTable.put(sessionPuts);
				sessionTable.close();
			}

			// List<Put> hidPuts = new ArrayList<Put>();
			// for (SpecificRecordBase specificRecordBase : result) {
			// try {
			// hidPuts.add(PutGenerator
			// .generateSession(specificRecordBase));
			// } catch (Exception e) {
			// log.warn("hid put generate failed");
			// continue;
			// }
			//
			// }
			// if (hidPuts.size() > 0) {
			// HTableInterface hidTable = hConnection
			// .getTable("storm_order_hid");
			// hidTable.put(hidPuts);
			// hidTable.close();
			// }
		} catch (Exception e) {
			log.warn("insert hbase failed", e);
		}
	}

	private void dealBgn(PlayBgnDryRecord record) {
		try {
			String spid = getSpid(record.getOEMID());
			Optional<AreaInfo> areaInfo = getAreaInfo(
					record.getHID() != null ? record.getHID().toString() : null,
					record.getOEMID() != null ? record.getOEMID().toString()
							: null, spid, record.getNatip());
			Optional<ResourceInfo> resourceInfo = getResourceInfo(spid,
					record.getFID() != null ? record.getFID().toString() : null);
			record.setSpid(spid);
			if (areaInfo.isPresent()) {
				record.setAreaid(areaInfo.get().getAreaid());
			}
			if (resourceInfo.isPresent()) {
				Integer bitrate = resourceInfo.get().getBitrate();
				if (bitrate != null) {
					record.setBitrate((long) bitrate);
				} else {
					record.setBitrate(null);
				}
			}
		} catch (Exception e) {
			Throwables.propagate(e);
		}
	}

	protected String getSpid(Long oemid) throws CacheRefreshException,
			CacheQueryException {
		Optional<OemInfo> oemInfo = getOemInfo(oemid);
		if (!oemInfo.isPresent()) {
			return ProductType.VOOLE_SPID.toString();
		} else {
			return oemInfo.get().getSpid();
		}
	}

	public Optional<AreaInfo> getAreaInfo(String hid, String oemid,
			String spid, long ip) throws CacheQueryException,
			CacheRefreshException {
		return areaInfoCache.getAreaInfo(hid, oemid, spid, ip);
	}

	public Optional<OemInfo> getOemInfo(Long oemid)
			throws CacheRefreshException, CacheQueryException {
		return oemInfoCache.getOemInfo(oemid);
	}

	public Optional<ResourceInfo> getResourceInfo(String spid, String fid)
			throws CacheRefreshException, CacheQueryException {
		return resourceInfoCache.getResourceInfo(spid, fid);
	}

	public static class SessionStateFactory implements StateFactory {
		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			ClassPathXmlApplicationContext cxt = new ClassPathXmlApplicationContext("cache-dao.xml");
			return new SessionStateImpl(cxt.getBean(CacheDao.class));
		}

	}

}
