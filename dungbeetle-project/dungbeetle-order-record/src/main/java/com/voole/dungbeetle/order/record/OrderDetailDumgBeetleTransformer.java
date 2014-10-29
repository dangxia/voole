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
import com.voole.hobbit2.cache.MovieInfoCache;
import com.voole.hobbit2.cache.MovieInfoCacheImpl;
import com.voole.hobbit2.cache.OemInfoCache;
import com.voole.hobbit2.cache.OemInfoCacheImpl;
import com.voole.hobbit2.cache.ParentAreaInfoCache;
import com.voole.hobbit2.cache.ParentAreaInfoCacheImpl;
import com.voole.hobbit2.cache.ParentSectionInfoCache;
import com.voole.hobbit2.cache.ParentSectionInfoCacheImpl;
import com.voole.hobbit2.cache.ProductInfoCache;
import com.voole.hobbit2.cache.ProductInfoCacheImpl;
import com.voole.hobbit2.cache.ResourceInfoCache;
import com.voole.hobbit2.cache.ResourceInfoCacheImpl;
import com.voole.hobbit2.cache.db.CacheDao;
import com.voole.hobbit2.cache.db.CacheDaoUtil;
import com.voole.hobbit2.cache.entity.AreaInfo;
import com.voole.hobbit2.cache.entity.MovieInfo;
import com.voole.hobbit2.cache.entity.OemInfo;
import com.voole.hobbit2.cache.entity.ParentAreaInfo;
import com.voole.hobbit2.cache.entity.ParentSectionInfo;
import com.voole.hobbit2.cache.entity.ProductInfo;
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
	private MovieInfoCache movieInfoCache;
	private ParentAreaInfoCache parentAreaInfoCache;
	private ParentSectionInfoCache parentSectionInfoCache;
	private ProductInfoCache productInfoCache;
	private final Map<String, HiveTable> partitionCache;

	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	private static SimpleDateFormat df2 = new SimpleDateFormat("HH");

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
		movieInfoCache = new MovieInfoCacheImpl(dao);
		parentAreaInfoCache = new ParentAreaInfoCacheImpl(dao);
		productInfoCache = new ProductInfoCacheImpl(dao);
		parentSectionInfoCache = new ParentSectionInfoCacheImpl(dao);
		try {
			areaInfoCache.refresh();
			oemInfoCache.refresh();
			resourceInfoCache.refresh();
			movieInfoCache.refresh();
			parentAreaInfoCache.refresh();
			parentSectionInfoCache.refresh();
			productInfoCache.refresh();
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
			// 过滤异常时长
			if (record.getMetricDurationtime() > 10800) {
				record.setMetricDurationtime((long) 10800);
			}

			String spid = getSpid(record.getDimOemId());
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
			// resource
			Optional<ResourceInfo> resourceInfo = getResourceInfo(spid,
					record.getDimMediaFid() != null ? record.getDimMediaFid()
							.toString() : null);
			if (resourceInfo.isPresent()) {
				Long mid = resourceInfo.get().getMid();
				int series = resourceInfo.get().getSeries();
				int mimeid = resourceInfo.get().getMimeid();
				if (mid != null) {
					record.setDimMovieMid(mid);
					record.setDimMediaSeries(series);
					record.setDimMediaMimeid(mimeid);
				} else {
					record.setDimMovieMid((long) 0);
					record.setDimMediaSeries(0);
					record.setDimMediaMimeid(0);
				}
			}
			// movie
			Optional<MovieInfo> movieInfo = getMovieInfo(record
					.getDimMovieMid());
			if (movieInfo.isPresent()) {
				record.setDimCpId(movieInfo.get().getDim_cp_id());
				record.setDimMovieCategory(movieInfo.get().getCategory());
			} else {
				record.setDimCpId(100010);
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
			// 产品类型
			Optional<ProductInfo> productInfo = getProductInfo(
					record.getDimPoId() + "", record.getDimProductPid() + "");
			if (productInfo.isPresent()) {
				record.setDimProductPtype(productInfo.get().getPptype());
			} else {
				record.setDimProductPtype(0);
			}

			// 时段
			record.setDimDateHour(getDayHour(record.getMetricPlaybgntime()));
		} catch (Exception e) {
			throw new DumgBeetleTransformException(e);
		}

		String partition = getDayPartition(record.getMetricPlaybgntime());
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

	private String getDayHour(long stamp) {
		return df2.format(new Date(stamp * 1000));
	}

	private void fillDetail(HiveOrderDetailRecord record, HiveOrderDryRecord dry) {
		record.setSessid(dry.getSessID());
		record.setStamp(System.currentTimeMillis());
		record.setUserip(dry.getNatip());
		record.setDatasorce(dry.getDatasorce());
		record.setPlayurl(null);
		record.setVersion(dry.getApkVersion());
		record.setDimUserUid(dry.getUID());
		record.setDimUserHid(dry.getHID());
		record.setDimOemId(dry.getOEMID());
		record.setDimMediaFid(dry.getFID());
		record.setDimProductPid(dry.getPid());
		record.setDimPoId(dry.getDimPoId());
		record.setDimEpgId(dry.getEpgid());
		record.setDimSectionId(dry.getSecid());
		record.setMetricPlaybgntime(dry.getPlayBgnTime());
		record.setMetricPlayendtime(dry.getPlayEndTime());
		record.setMetricPlayalivetime(dry.getPlayAliveTime());
		record.setMetricDurationtime(dry.getPlayDurationTime());
		record.setMetricAvgspeed(dry.getAvgspeed());
		record.setMetricIsad((dry.getIsAdMod()) ? 1 : 0);
		record.setMetricIsrepeatmod((dry.getIsRepeatMod()) ? 1 : 0);
		record.setMetricStatus(dry.getMetricStatus());
		record.setMetricTechtype(dry.getMetricTechtype());
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

	public Optional<MovieInfo> getMovieInfo(Long mid)
			throws CacheRefreshException, CacheQueryException {
		return movieInfoCache.getMovieInfo(mid);
	}

	public Optional<ParentAreaInfo> getParentAreaInfo(Integer areaid)
			throws CacheRefreshException, CacheQueryException {
		return parentAreaInfoCache.getParentAreaInfo(areaid);
	}

	public Optional<ParentSectionInfo> getParentSectionInfo(String sectionid)
			throws CacheRefreshException, CacheQueryException {
		return parentSectionInfoCache.getParentSectionInfo(sectionid);
	}

	public Optional<ProductInfo> getProductInfo(String dim_po_id,
			String dim_product_pid) throws CacheRefreshException,
			CacheQueryException {
		return productInfoCache.getProductInfo(dim_po_id, dim_product_pid);
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		OrderDetailDumgBeetleTransformer transformer = new OrderDetailDumgBeetleTransformer();
		transformer.setup(null);
	}
}
