package com.voole.hobbit2.storm.order.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
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

public class ExtraInfoQueryStateImpl implements ExtraInfoQueryState {
	private static final Logger log = LoggerFactory
			.getLogger(ExtraInfoQueryState.class);

	private final AreaInfoCache areaInfoCache;
	private final OemInfoCache oemInfoCache;
	private final ResourceInfoCache resourceInfoCache;

	public ExtraInfoQueryStateImpl(AreaInfoCache areaInfoCache,
			OemInfoCache oemInfoCache, ResourceInfoCache resourceInfoCache) {
		this.areaInfoCache = areaInfoCache;
		this.oemInfoCache = oemInfoCache;
		this.resourceInfoCache = resourceInfoCache;
	}

	@Override
	public void beginCommit(Long arg0) {

	}

	@Override
	public void commit(Long arg0) {

	}

	public static class ExtraInfoQueryStateFactory implements StateFactory {
		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			CacheDao dao = CacheDaoUtil.getCacheDao();
			AreaInfoCache areaInfoCache = new AreaInfoCacheImpl(dao);
			OemInfoCache oemInfoCache = new OemInfoCacheImpl(dao);
			ResourceInfoCache resourceInfoCache = new ResourceInfoCacheImpl(dao);
			try {
				areaInfoCache.refresh();
				oemInfoCache.refresh();
				resourceInfoCache.refresh();
			} catch (Exception e) {
				log.warn("init ExtraInfoQueryStateImpl error:", e);
				Throwables.propagate(e);
			}
			return new ExtraInfoQueryStateImpl(areaInfoCache, oemInfoCache,
					resourceInfoCache);
		}

	}

	@Override
	public List<SpecificRecordBase> query(List<TridentTuple> tuples) {
		long start = System.currentTimeMillis();
		List<SpecificRecordBase> result = new ArrayList<SpecificRecordBase>();
		for (TridentTuple tuple : tuples) {
			SpecificRecordBase base = (SpecificRecordBase) tuple.get(0);
			try {
				if (base instanceof PlayBgnDryRecord) {
					dealBgn((PlayBgnDryRecord) base);
				}
			} catch (Exception e) {
				log.warn("record add extra info failed", e);
			}
			result.add(base);

		}
		log.info("query size:" + tuples.size() + ",used time:"
				+ ((System.currentTimeMillis() - start) / 1000));
		return result;
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

}
