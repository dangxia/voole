/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.dungbeetle.order.record;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;
import com.voole.dungbeetle.api.DumgBeetleTransformException;
import com.voole.dungbeetle.api.IDumgBeetleTransformer;
import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.order.record.avro.HiveOrderDetailRecord;
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
import com.voole.hobbit2.common.enums.ProductType;
import com.voole.hobbit2.hive.order.avro.HiveOrderDryRecord;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class OrderDetailDumgBeetleTransformer implements
		IDumgBeetleTransformer<HiveOrderDryRecord> {
	private AreaInfoCache areaInfoCache;
	private OemInfoCache oemInfoCache;
	private ResourceInfoCache resourceInfoCache;
	private final Map<String, HiveTable> partitionCache;

	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

	public OrderDetailDumgBeetleTransformer() {
		partitionCache = new HashMap<String, HiveTable>();
	}

	@Override
	public void setup(TaskAttemptContext context) throws IOException,
			InterruptedException {
		CacheDao dao = CacheDaoUtil.getCacheDao();
		areaInfoCache = new AreaInfoCacheImpl(dao);
		oemInfoCache = new OemInfoCacheImpl(dao);
		resourceInfoCache = new ResourceInfoCacheImpl(dao);

	}

	@Override
	public void cleanup(TaskAttemptContext context) throws IOException,
			InterruptedException {
		CacheDaoUtil.close();

	}

	@Override
	public Map<HiveTable, List<SpecificRecordBase>> transform(
			HiveOrderDryRecord dry) throws DumgBeetleTransformException {
		HiveOrderDetailRecord record = new HiveOrderDetailRecord();
		fillDetail(record, dry);
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
		String partition = getDayPartition(record.getPlayBgnTime());
		Map<HiveTable, List<SpecificRecordBase>> result = new HashMap<HiveTable, List<SpecificRecordBase>>();
		result.put(getTable(partition),
				Lists.newArrayList((SpecificRecordBase) record));
		return result;
	}

	public HiveTable getTable(String partition) {
		if (!partitionCache.containsKey(partition)) {
			createHiveTable(partition);
		}
		return partitionCache.get(partition);
	}

	private synchronized void createHiveTable(String partition) {
		if (partitionCache.containsKey(partition)) {
			return;
		}
		partitionCache.put(partition,
				OrderDetailHiveTableCreator.create(partition));
	}

	private String getDayPartition(long stamp) {
		return df.format(new Date(stamp));
	}

	private void fillDetail(HiveOrderDetailRecord record, HiveOrderDryRecord dry) {
		record.setAvgspeed(dry.getAvgspeed());
		record.setEpgid(dry.getEpgid());
		record.setFID(dry.getFID());
		record.setHID(dry.getHID());
		record.setIsAdMod(dry.getIsAdMod());
		record.setIsRepeatMod(dry.getIsRepeatMod());
		record.setNatip(dry.getNatip());
		record.setOEMID(dry.getOEMID());
		record.setPid(dry.getPid());
		record.setPlayAliveTime(dry.getPlayAliveTime());
		record.setPlayBgnTime(dry.getPlayBgnTime());
		record.setPlayDurationTime(dry.getPlayDurationTime());
		record.setPlayEndTime(dry.getPlayEndTime());
		record.setSecid(dry.getSecid());
		record.setSessID(dry.getSessID());
		record.setUID(dry.getUID());
	}

	protected String getSpid(Long oemid) {
		OemInfo oemInfo = null;
		if (oemid != null) {
			oemInfo = getOemInfo(oemid);
		}
		if (oemInfo == null) {
			return ProductType.VOOLE_SPID.toString();
		} else {
			return oemInfo.getSpid();
		}
	}

	public AreaInfo getAreaInfoNormal(long ip) {
		return areaInfoCache.getAreaInfoNormal(ip);
	}

	public AreaInfo getAreaInfoFromBoxStore(String oemid, String hid) {
		return areaInfoCache.getAreaInfoFromBoxStore(oemid, hid);
	}

	public AreaInfo getAreaInfoFromSp(String spid, long ip) {
		return areaInfoCache.getAreaInfoFromSp(spid, ip);
	}

	public AreaInfo getAreaInfo(String hid, String oemid, String spid, long ip) {
		return areaInfoCache.getAreaInfo(hid, oemid, spid, ip);
	}

	public OemInfo getOemInfo(Long oemid) {
		return oemInfoCache.getOemInfo(oemid);
	}

	public ResourceInfo getResourceInfo(String spid, String fid) {
		return resourceInfoCache.getResourceInfo(spid, fid);
	}
}
