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
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.voole.dungbeetle.api.DumgBeetleTransformException;
import com.voole.dungbeetle.api.IDumgBeetleTransformer;
import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.order.record.avro.BsPvPlayDetailInfo;
import com.voole.dungbeetle.util.IPtoLong;
import com.voole.hobbit2.cache.AreaInfoCache;
import com.voole.hobbit2.cache.OemInfoCache;
import com.voole.hobbit2.cache.ParentAreaInfoCache;
import com.voole.hobbit2.cache.ParentSectionInfoCache;
import com.voole.hobbit2.cache.entity.AreaInfo;
import com.voole.hobbit2.cache.entity.OemInfo;
import com.voole.hobbit2.cache.entity.ParentAreaInfo;
import com.voole.hobbit2.cache.entity.ParentSectionInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;
import com.voole.hobbit2.hive.order.avro.BsPvPlayDryInfo;
import com.voole.hobbit2.common.enums.ProductType;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class BsPvEpgDetailDumgBeetleTransformer implements
		IDumgBeetleTransformer<BsPvPlayDryInfo> {
	private AreaInfoCache areaInfoCache;
	private OemInfoCache oemInfoCache;
	private ParentAreaInfoCache parentAreaInfoCache;
	private ParentSectionInfoCache parentSectionInfoCache;
	private final Map<String, HiveTable> partitionCache;

	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	private static SimpleDateFormat df2 = new SimpleDateFormat("HH");

	public static final String IS_AUTO_REFRESH_CACHE = "order.detail.transformer.is.auto.refresh.cache";

	public BsPvEpgDetailDumgBeetleTransformer() {
		partitionCache = new HashMap<String, HiveTable>();
	}

	protected boolean getIsAutoRefreshCache(TaskAttemptContext context) {
		if (context == null || context.getConfiguration() == null) {
			return false;
		}
		return context.getConfiguration().getBoolean(IS_AUTO_REFRESH_CACHE,
				false);
	}

	@Override
	public void setup(TaskAttemptContext context) throws IOException,
			InterruptedException {
		ClassPathXmlApplicationContext cxt = ApplicationContextUtil
				.createCxt(getIsAutoRefreshCache(context));
		areaInfoCache = cxt.getBean(AreaInfoCache.class);
		oemInfoCache = cxt.getBean(OemInfoCache.class);
		parentAreaInfoCache = cxt.getBean(ParentAreaInfoCache.class);
		parentSectionInfoCache = cxt.getBean(ParentSectionInfoCache.class);
	}

	@Override
	public void cleanup(TaskAttemptContext context) throws IOException,
			InterruptedException {
		ApplicationContextUtil.closeCxt();
	}

	@Override
	public Map<HiveTable, List<SpecificRecordBase>> transform(
			BsPvPlayDryInfo dry) throws DumgBeetleTransformException {
		BsPvPlayDetailInfo record = new BsPvPlayDetailInfo();
		fillDetail(record, dry);
		try {
			String spid = getSpid(record.getDimOemId());
			record.setDimIspId(Integer.parseInt(spid));
			// area
			Optional<AreaInfo> areaInfo = getAreaInfo(
					record.getDimUserHid() != null ? record.getDimUserHid()
							.toString() : null,
					record.getDimOemId() != null ? record.getDimOemId()
							.toString() : null, spid, record.getUserip());
			record.setDimIspId(Integer.parseInt(spid));
			if (areaInfo.isPresent()) {
				record.setDimAreaId(areaInfo.get().getAreaid());
				record.setDimNettypeId(areaInfo.get().getNettype());
			} else {
				record.setDimAreaId(0);
				record.setDimNettypeId(0);
			}

			// 省份
			Optional<ParentAreaInfo> parentAreaInfo = getParentAreaInfo(record
					.getDimAreaId());
			if (parentAreaInfo.isPresent()) {
				record.setDimAreaParentid(parentAreaInfo.get().getParentid());
			} else {
				record.setDimAreaParentid(record.getDimAreaId());
			}
			// 栏目
			Optional<ParentSectionInfo> parentSectionInfo = getParentSectionInfo(record
					.getDimSectionId() + "");
			if (parentSectionInfo.isPresent()) {
				record.setDimSectionParentid(parentSectionInfo.get().getCode());
			} else {
				record.setDimSectionParentid(record.getDimSectionId() + "");
			}

			// 时段
			record.setDimDateHour(getDayHour(record.getAccesstime()));

		} catch (Exception e) {
			throw new DumgBeetleTransformException(e);
		}

		String partition = getDayPartition(record.getAccesstime());
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
				BsPvEpgDetailHiveTableCreator.create(partition));
	}

	private String getDayPartition(long stamp) {
		return df.format(new Date(stamp * 1000));
	}

	private String getDayHour(long stamp) {
		return df2.format(new Date(stamp * 1000));
	}

	private void fillDetail(BsPvPlayDetailInfo record, BsPvPlayDryInfo dry) {
		record.setSessid(dry.getSessID());
		record.setStamp(System.currentTimeMillis());
		record.setAccesstime(dry.getAccesstime());
		record.setDimUserHid(dry.getHid());
		record.setDimOemId(dry.getOemid());
		record.setDimUserUid(dry.getUid());
		record.setUserip(IPtoLong.ipToLong(dry.getUserip() + ""));
		record.setDimEpgId(IPtoLong.StrToLong(dry.getEpgid() + ""));
		record.setUrl(dry.getUrl());
		record.setDimKindCode(dry.getKind());
		record.setDimSectionId(dry.getChannelCD());
		record.setPage(dry.getPage());
		record.setDimMovieMid(IPtoLong.StrToLong(dry.getMid() + ""));
		record.setUsertype(dry.getUsertype());
		record.setSearchword(dry.getSearchword());
		record.setMidlist(dry.getMidlist());
		record.setButton(dry.getButton());
		record.setPerfip(dry.getPerfip());
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

	public Optional<ParentAreaInfo> getParentAreaInfo(Integer areaid)
			throws CacheRefreshException, CacheQueryException {
		return parentAreaInfoCache.getParentAreaInfo(areaid);
	}

	public Optional<ParentSectionInfo> getParentSectionInfo(String sectionid)
			throws CacheRefreshException, CacheQueryException {
		return parentSectionInfoCache.getParentSectionInfo(sectionid);
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		BsPvEpgDetailDumgBeetleTransformer transformer = new BsPvEpgDetailDumgBeetleTransformer();
		transformer.setup(null);
	}
}
