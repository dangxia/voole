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

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
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
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;
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
		try {
			areaInfoCache.refresh();
			oemInfoCache.refresh();
			resourceInfoCache.refresh();
		} catch (Exception e) {
			Throwables.propagate(e);
		}
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
			throw new DumgBeetleTransformException(e);
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
		return df.format(new Date(stamp * 1000));
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

	public static void main(String[] args) throws IOException, InterruptedException {
		OrderDetailDumgBeetleTransformer transformer = new OrderDetailDumgBeetleTransformer();
		transformer.setup(null);
	}
}
