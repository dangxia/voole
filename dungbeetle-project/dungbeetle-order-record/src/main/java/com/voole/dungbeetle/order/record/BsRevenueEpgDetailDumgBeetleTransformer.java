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
import com.voole.dungbeetle.order.record.avro.BsRevenueDetailInfo;
import com.voole.dungbeetle.util.IPtoLong;
import com.voole.hobbit2.cache.AreaInfoCache;
import com.voole.hobbit2.cache.OemInfoCache;
import com.voole.hobbit2.cache.ParentAreaInfoCache;
import com.voole.hobbit2.cache.ProductInfoCache;
import com.voole.hobbit2.cache.entity.AreaInfo;
import com.voole.hobbit2.cache.entity.OemInfo;
import com.voole.hobbit2.cache.entity.ParentAreaInfo;
import com.voole.hobbit2.cache.entity.ProductInfo;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;
import com.voole.hobbit2.common.enums.ProductType;
import com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class BsRevenueEpgDetailDumgBeetleTransformer implements
		IDumgBeetleTransformer<BsRevenueDryInfo> {
	private AreaInfoCache areaInfoCache;
	private OemInfoCache oemInfoCache;
	private ParentAreaInfoCache parentAreaInfoCache;
	private ProductInfoCache productInfoCache;
	private final Map<String, HiveTable> partitionCache;

	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

	public static final String IS_AUTO_REFRESH_CACHE = "order.detail.transformer.is.auto.refresh.cache";

	public BsRevenueEpgDetailDumgBeetleTransformer() {
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
		productInfoCache = cxt.getBean(ProductInfoCache.class);
	}

	@Override
	public void cleanup(TaskAttemptContext context) throws IOException,
			InterruptedException {
		ApplicationContextUtil.closeCxt();
	}

	@Override
	public Map<HiveTable, List<SpecificRecordBase>> transform(
			BsRevenueDryInfo dry) throws DumgBeetleTransformException {
		BsRevenueDetailInfo record = new BsRevenueDetailInfo();
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

			// 产品类型
			Optional<ProductInfo> productInfo = getProductInfo(spid,
					record.getDimProductPid() + "");
			if (productInfo.isPresent()) {
				record.setDimProductPtype(productInfo.get().getPtype());
				record.setDimProductFee(productInfo.get().getFee());
			} else {
				record.setDimProductPtype(0);
				record.setDimProductFee(0);
			}

			record.setPerfip(record.getPerfip());

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
				BsRevenueEpgDetailHiveTableCreator.create(partition));
	}

	private String getDayPartition(long stamp) {
		return df.format(new Date(stamp * 1000));
	}

	private void fillDetail(BsRevenueDetailInfo record, BsRevenueDryInfo dry) {
		record.setSessid(dry.getSessID());
		record.setDatasource(dry.getDatasource());
		record.setStamp(System.currentTimeMillis());
		record.setAccesstime(IPtoLong.StrToLong(dry.getAccesstime() + ""));
		record.setDimUserHid(dry.getHid());
		record.setDimOemId(IPtoLong.StrToLong(dry.getOemid() + ""));
		record.setDimUserUid(dry.getUid());
		record.setUserip(IPtoLong.ipToLong(dry.getUserip() + ""));
		record.setDimEpgId(IPtoLong.StrToLong(dry.getEpgid() + ""));
		record.setDimProductPid(dry.getPerfip());
		record.setResult(dry.getResult());
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
		return areaInfoCache.getAreaInfo(spid, ip);
	}

	public Optional<OemInfo> getOemInfo(Long oemid)
			throws CacheRefreshException, CacheQueryException {
		return oemInfoCache.getOemInfo(oemid);
	}

	public Optional<ParentAreaInfo> getParentAreaInfo(Integer areaid)
			throws CacheRefreshException, CacheQueryException {
		return parentAreaInfoCache.getParentAreaInfo(areaid);
	}

	public Optional<ProductInfo> getProductInfo(String dim_po_id,
			String dim_product_pid) throws CacheRefreshException,
			CacheQueryException {
		return productInfoCache.getProductInfo(dim_po_id, dim_product_pid);
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		BsRevenueEpgDetailDumgBeetleTransformer transformer = new BsRevenueEpgDetailDumgBeetleTransformer();
		transformer.setup(null);
	}
}
