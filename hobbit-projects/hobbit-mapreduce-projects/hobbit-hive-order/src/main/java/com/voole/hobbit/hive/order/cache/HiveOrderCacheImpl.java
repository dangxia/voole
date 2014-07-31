/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order.cache;

import com.voole.hobbit.avro.hive.HiveOrderRecord;
import com.voole.hobbit.cache.AreaInfoCache;
import com.voole.hobbit.cache.AreaInfoCacheImpl;
import com.voole.hobbit.cache.OemInfoCache;
import com.voole.hobbit.cache.OemInfoCacheImpl;
import com.voole.hobbit.cache.ResourceInfoCache;
import com.voole.hobbit.cache.ResourceInfoCacheImpl;
import com.voole.hobbit.cache.db.CacheDao;
import com.voole.hobbit.cache.db.CacheDaoUtil;
import com.voole.hobbit.cache.entity.AreaInfo;
import com.voole.hobbit.cache.entity.OemInfo;
import com.voole.hobbit.cache.entity.ResourceInfo;
import com.voole.hobbit.utils.ProductUtils;

/**
 * @author XuehuiHe
 * @date 2014年7月31日
 */
public class HiveOrderCacheImpl implements HiveOrderCache {
	private AreaInfoCache areaInfoCache;
	private OemInfoCache oemInfoCache;
	private ResourceInfoCache resourceInfoCache;

	@Override
	public void open() {
		CacheDao dao = CacheDaoUtil.getCacheDao();
		areaInfoCache = new AreaInfoCacheImpl(dao);
		oemInfoCache = new OemInfoCacheImpl(dao);
		resourceInfoCache = new ResourceInfoCacheImpl(dao);
	}

	@Override
	public void close() {
		CacheDaoUtil.close();
	}

	@Override
	public void deal(HiveOrderRecord record) {
		String spid = getSpid(record.getOEMID());
		AreaInfo areaInfo = getAreaInfo(record.getHID() != null ? record
				.getHID().toString() : null, record.getOEMID() != null ? record
				.getOEMID().toString() : null, spid, record.getNatip());
		ResourceInfo resourceInfo = getResourceInfo(spid,
				record.getFID() != null ? record.getFID().toString() : null);
		record.setSpid(spid);
		if (areaInfo != null) {
			record.setAreaid(areaInfo.getAreaid());
		}
		if (resourceInfo != null) {
			Integer bitrate = resourceInfo.getBitrate();
			if (bitrate != null) {
				record.setBitrate((long) bitrate);
			} else {
				record.setBitrate(null);
			}

		}

	}

	protected String getSpid(Long oemid) {
		OemInfo oemInfo = null;
		if (oemid != null) {
			oemInfo = getOemInfo(oemid);
		}
		if (oemInfo == null) {
			return ProductUtils.VOOLE_SPID.toString();
		} else {
			return oemInfo.getSpid();
		}
	}

	@Override
	public AreaInfo getAreaInfoNormal(long ip) {
		return areaInfoCache.getAreaInfoNormal(ip);
	}

	@Override
	public AreaInfo getAreaInfoFromBoxStore(String oemid, String hid) {
		return areaInfoCache.getAreaInfoFromBoxStore(oemid, hid);
	}

	@Override
	public AreaInfo getAreaInfoFromSp(String spid, long ip) {
		return areaInfoCache.getAreaInfoFromSp(spid, ip);
	}

	@Override
	public AreaInfo getAreaInfo(String hid, String oemid, String spid, long ip) {
		return areaInfoCache.getAreaInfo(hid, oemid, spid, ip);
	}

	@Override
	public void refreshDelay() {
	}

	@Override
	public void refreshImmediately() {

	}

	@Override
	public String getName() {
		return null;
	}

	@Override
	public OemInfo getOemInfo(Long oemid) {
		return oemInfoCache.getOemInfo(oemid);
	}

	@Override
	public ResourceInfo getResourceInfo(String spid, String fid) {
		return resourceInfoCache.getResourceInfo(spid, fid);
	}

}
