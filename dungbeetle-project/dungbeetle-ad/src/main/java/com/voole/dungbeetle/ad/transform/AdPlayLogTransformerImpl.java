package com.voole.dungbeetle.ad.transform;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.voole.dungbeetle.ad.dao.IPlatFormDao;
import com.voole.dungbeetle.ad.dao.IPlayLogDao;
import com.voole.dungbeetle.ad.exception.AdUrlTransformException;
import com.voole.dungbeetle.ad.jms.queue.MessageQueue;
import com.voole.dungbeetle.ad.main.StartUp;
import com.voole.dungbeetle.ad.model.PlayUrlAdEntity;
import com.voole.dungbeetle.ad.model.PlayUrlAdInfos;
import com.voole.dungbeetle.ad.model.PlayUrlAdInfos.PlayUrlAdItem;
import com.voole.dungbeetle.ad.record.PlayLogHiveTableCreator;
import com.voole.dungbeetle.ad.record.avro.InterfacePlayLogDry;
import com.voole.dungbeetle.ad.record.avro.PlayLog;
import com.voole.dungbeetle.ad.util.AdUrlTools;
import com.voole.dungbeetle.ad.util.DateUtil;
import com.voole.dungbeetle.ad.util.GlobalProperties;
import com.voole.dungbeetle.api.DumgBeetleTransformException;
import com.voole.dungbeetle.api.IDumgBeetleTransformer;
import com.voole.dungbeetle.api.model.HiveTable;

public class AdPlayLogTransformerImpl implements
		IDumgBeetleTransformer<InterfacePlayLogDry> {
	private Logger log = LoggerFactory
			.getLogger(AdPlayLogTransformerImpl.class);

	private final Map<String, HiveTable> partitionCache;

	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

	private static SimpleDateFormat startTimeDf = new SimpleDateFormat(
			"EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);

	private int ispushnielsen;

	private IPlatFormDao platFormDao;

	private IPlayLogDao playLogDao;

	private MessageQueue messageQueue;

	public ClassPathXmlApplicationContext ctx = null;

	// 全局缓存数据(广告信息通过planId_adfid可以唯一确定)
	public Map<String, Map<String, Object>> alladInfoMap = new HashMap<String, Map<String, Object>>();

	public Map<String, Map<String, Object>> alladPosMap = new HashMap<String, Map<String, Object>>();
	// 从播控平台获取所有的栏目信息
	public Map<String, String> allChannelProgramByPlatformMap = new HashMap<String, String>();
	// 广告所有的栏目信息
	public Map<String, Map<String, Object>> allChannelProgramByAdMap = new HashMap<String, Map<String, Object>>();
	// 获取所有的区域信息
	public Map<String, Map<String, Object>> allAreaInfoMap = new HashMap<String, Map<String, Object>>();
	// 根据mid(影片id)反查影片类型的列表
	public Map<String, Map<String, Object>> allMovieTypesMap = new HashMap<String, Map<String, Object>>();
	// 获取常用频道信息的列表
	public Map<String, Map<String, Object>> allChannelInfoMap = new HashMap<String, Map<String, Object>>();

	public SimpleDateFormat sdf = new SimpleDateFormat("HH");

	public AdPlayLogTransformerImpl() {
		partitionCache = new HashMap<String, HiveTable>();
	}

	// public static void main(String[] args) throws IOException,
	// InterruptedException, DumgBeetleTransformException {
	// AdPlayLogTransformerImpl2 test = new AdPlayLogTransformerImpl2();
	// test.setup(null);
	// test.cleanup(null);
	// }

	@Override
	public void setup(TaskAttemptContext context) throws IOException,
			InterruptedException, DumgBeetleTransformException {
		try {
			// 初始化spring容器
			ctx = StartUp.getSpringContext();

			ispushnielsen = GlobalProperties.getInteger("ispushnielsen");// 是否推送尼尔森播放数据

			platFormDao = ctx.getBean(IPlatFormDao.class);
			playLogDao = ctx.getBean(IPlayLogDao.class);
			messageQueue = ctx.getBean(MessageQueue.class);

			// 缓存广告信息数据
			List<Map<String, Object>> alladInfoListTemp = playLogDao
					.findAllAdinfo();
			for (Map<String, Object> map : alladInfoListTemp) {
				alladInfoMap.put(
						map.get("seqno").toString() + "_"
								+ map.get("fid").toString(), map);
			}

			// 缓存广告位置信息
			List<Map<String, Object>> alladPosListTemp = playLogDao
					.findAllAdPos();
			for (Map<String, Object> map : alladPosListTemp) {
				alladPosMap.put(map.get("seqno").toString(), map);
			}

			// 缓存播控平台的所有栏目信息(以后通过sectionid获取)
			List<Map<String, Object>> allprogramnameListTemp = platFormDao
					.findAllChannelProgramInfoAccessPlatform();
			for (Map<String, Object> map : allprogramnameListTemp) {
				allChannelProgramByPlatformMap.put(map.get("progaramcode")
						.toString(), map.get("channelname").toString() + "_"
						+ map.get("programname").toString());
			}

			// 缓存广告的所有栏目信息
			List<Map<String, Object>> allChannelProgramByAdListTemp = playLogDao
					.findAllChannelProgramInfoAccessAd();
			for (Map<String, Object> map : allChannelProgramByAdListTemp) {
				allChannelProgramByAdMap.put(map.get("adChannelName")
						.toString()
						+ "_"
						+ map.get("adCategoryName").toString(), map);
			}

			// 缓存区域信息
			List<Map<String, Object>> allAreaInfoListTemp = playLogDao
					.findAllAreaInfos();
			for (Map<String, Object> map : allAreaInfoListTemp) {
				allAreaInfoMap.put(map.get("code").toString(), map);
			}

			// 缓存所有节目类型的列表
			List<Map<String, Object>> allMovieTypeListTemp = platFormDao
					.findAllMovieTypes();
			for (Map<String, Object> map : allMovieTypeListTemp) {
				allMovieTypesMap.put(map.get("mid").toString(), map);
			}

			// 缓存常用频道的信息列表
			List<Map<String, Object>> allChannelInfoListTemp = playLogDao
					.findAllChannelInfos();
			for (Map<String, Object> map : allChannelInfoListTemp) {
				allChannelInfoMap.put(map.get("mtype").toString(), map);
			}
		} catch (Exception e) {
			log.error("加载spring配置文件失败！", e);
			Throwables.propagate(e);
		}

	}

	@Override
	public void cleanup(TaskAttemptContext context) throws IOException,
			InterruptedException {
		// 清空缓存数据
		alladInfoMap.clear();
		alladPosMap.clear();
		allChannelProgramByPlatformMap.clear();
		allChannelProgramByAdMap.clear();
		allAreaInfoMap.clear();
		allMovieTypesMap.clear();
		allChannelInfoMap.clear();

		// 关闭spring容器
		ctx.close();

	}

	protected Date secondsToDate(Long time) {
		if (time == null) {
			return null;
		}
		return new Date(time * 1000);
	}

	protected int calcPlayTime(Date startTime, Date lastAliveTime, Date endTime) {
		Date _endTime = null;
		if (endTime != null) {
			_endTime = endTime;
		} else if (_endTime == null && lastAliveTime != null) {
			_endTime = lastAliveTime;
		} else {
			_endTime = new Date();
		}
		return (int) Math.abs(_endTime.getTime() - startTime.getTime()) / 1000;
	}

	protected PlayUrlAdInfos processPlayUrl(InterfacePlayLogDry dry)
			throws DumgBeetleTransformException, AdUrlTransformException {
		if (dry.getPlayurl() == null || dry.getPlayurl().length() == 0) {
			throw new DumgBeetleTransformException("playurl is empty!");
		}

		Optional<PlayUrlAdInfos> playUrlAdInfosOpt = AdUrlTools
				.parsePlayUrl(dry.getPlayurl().toString());
		if (!playUrlAdInfosOpt.isPresent()) {
			throw new DumgBeetleTransformException("playurl:"
					+ dry.getPlayurl() + " is a illegal url!");
		}
		return playUrlAdInfosOpt.get();
	}

	protected void fillRecordWithDry(PlayLog record, InterfacePlayLogDry dry) {
		String sessionid = fromCharsToString(dry.getSessionid());
		String oemidStr = fromCharsToString(dry.getOemid());
		String spidStr = fromCharsToString(dry.getSpid());
		String speedStr = fromCharsToString(dry.getSpeed());
		String hid = fromCharsToString(dry.getHid());
		record.setSessionid(sessionid);
		if (oemidStr != null) {
			record.setOemid(Integer.parseInt(oemidStr));
		} else {
			record.setOemid(-1);
		}

		if (spidStr != null) {
			record.setSpid(Integer.parseInt(spidStr));
		} else {
			record.setSpid(-1);
		}

		if (speedStr != null) {
			record.setSpeed(Integer.parseInt(speedStr));
		} else {
			record.setSpeed(-1);
		}
		// hid格式化
		record.setHid(hid);

		this.setCodeInfo(dry, record);

	}

	/**
	 * 设置区域信息：省、市 有区域编码查询省市信息
	 * 
	 * @param dry
	 * @param record
	 */
	private void setCodeInfo(InterfacePlayLogDry dry, PlayLog record) {
		// TODO 根据区域编号查询省份和城市信息
		String areaStr = fromCharsToString(dry.getArea());
		Map<String, Object> areamap = allAreaInfoMap.get(areaStr);
		if (areamap != null) {
			if (areamap.get("provinceid") != null) {
				record.setProvinceid(Integer.valueOf(areamap.get("provinceid")
						.toString()));
			}
			if (areamap.get("cityid") != null) {
				record.setCityid(Integer.valueOf(areamap.get("cityid")
						.toString()));
			}
		} else {
			record.setProvinceid(0);
			record.setCityid(0);
		}
	}

	protected void processChannel(InterfacePlayLogDry dry, PlayLog record,
			PlayUrlAdInfos playUrlAdInfos) {
		String sectionid = fromCharsToString(dry.getSectionid());
		// 根据栏目code查询播控栏目名称
		String channelname_programname = allChannelProgramByPlatformMap
				.get(sectionid);

		Map<String, Object> channelInfoMap = allChannelProgramByAdMap
				.get(channelname_programname);
		if (channelInfoMap != null && channelInfoMap.size() > 0) {
			Object channelid = channelInfoMap.get("adChannelCode");
			record.setChannelid(channelid != null ? Integer.valueOf(channelid
					.toString()) : GlobalProperties
					.getInteger("other.channelid"));
			Object pragramid = channelInfoMap.get("adCategoryCode");
			record.setProgramid(pragramid != null ? Integer.valueOf(pragramid
					.toString()) : GlobalProperties
					.getInteger("other.channelid"));
		} else {
			// playurl中解析出mid(影片节目id) 去播控平台查询获取影片节目类型
			Map<String, Object> mtypeMap = allMovieTypesMap.get(playUrlAdInfos
					.getMid());
			String mtype = (String) mtypeMap.get("mtype");
			if (StringUtils.isNotBlank(mtype)) {
				Map<String, Object> channemap = allChannelInfoMap.get(mtype);
				if (channemap != null && channemap.size() > 0) {
					String channelid = (String) channemap.get("adChannelCode");
					record.setChannelid(channelid != null ? Integer
							.valueOf(channelid.toString()) : GlobalProperties
							.getInteger("other.channelid"));
					record.setProgramid(GlobalProperties
							.getInteger("other.channelid"));
				}
			}
		}
		if (record.getChannelid() == null) {
			record.setChannelid(GlobalProperties.getInteger("other.channelid"));
			record.setProgramid(GlobalProperties.getInteger("other.channelid"));
		}
	}

	public static String fromCharsToString(CharSequence chars) {
		if (chars == null) {
			return null;
		}
		return chars.toString();
	}

	@Override
	public Map<HiveTable, List<SpecificRecordBase>> transform(
			InterfacePlayLogDry dry) throws DumgBeetleTransformException {

		List<PlayLog> records = new ArrayList<PlayLog>();
		try {
			PlayLog record = new PlayLog();

			Date startTime = secondsToDate(dry.getStarttime());
			if (startTime == null) {
				throw new DumgBeetleTransformException("传输的数据有异常，播放开始为空");
			}
			Date lastAliveTime = secondsToDate(dry.getLastalivetime());
			Date endTime = secondsToDate(dry.getEndtime());

			Integer playTime = calcPlayTime(startTime, lastAliveTime, endTime);
			PlayUrlAdInfos playUrlAdInfos = processPlayUrl(dry);
			int listSize = playUrlAdInfos.getItems().size();
			for (int i = 0; i < listSize; i++) {
				PlayUrlAdItem adItem = playUrlAdInfos.getItems().get(i);
				// if ("10".equals(playUrlAdEntity.getAdmergetype())
				// && StringUtils.isBlank(playUrlAdEntity.getFid())) {
				// this.lookbackPlay(dry, playUrlAdEntity, i);
				// }
				// 播放时间小于广告起始时间(无需新增修改)
				if (playTime < adItem.getStartTime()) {
					break;
				}

				fillRecordWithDry(record, dry);
				record.setStamp(System.currentTimeMillis());
				record.setLogid(playUrlAdInfos.getLogid());
				record.setFid(adItem.getFid());
				processChannel(dry, record, playUrlAdInfos);

				// 时间设置

				// 播放时间以及完整性设置
				setPlayTime(record, adItem.getPlayTime(),
						adItem.getStartTime(), playTime, startTime);
				// 反查介质信息
				List<String> plnidList = playUrlAdInfos.getPlnidList(); // 排期id列表

				// TODO 这种方式必须保证接口中发送的pln顺序和播放视频的介质id保持一致顺序
				String planid = "";
				if (plnidList != null && plnidList.size() >= i) {
					planid = plnidList.get(i);
				}
				String adfid = adItem.getFid();
				// 根据fid和排期id查询节目信息能够保证唯一
				Map<String, Object> adInfoMap = alladInfoMap.get(planid + "_"
						+ adfid);
				this.fillRecord(adInfoMap, record);
				// 排期反查广告位置
				Map<String, Object> adposmap = alladPosMap.get(planid);
				if (adposmap != null && adposmap.get("adposid") != null) {
					record.setAdposid(Integer.valueOf(adposmap.get("adposid")
							.toString()));
				}
				// 设置是在哪个小时发送的一条记录
				String hour = record.getStarttime().toString()
						.substring(10, 13);
				if (hour.equals("00")) {
					record.setPlaycnttime00(1);
				} else if (hour.equals("01")) {
					record.setPlaycnttime01(1);
				} else if (hour.equals("02")) {
					record.setPlaycnttime02(1);
				} else if (hour.equals("03")) {
					record.setPlaycnttime03(1);
				} else if (hour.equals("04")) {
					record.setPlaycnttime04(1);
				} else if (hour.equals("05")) {
					record.setPlaycnttime05(1);
				} else if (hour.equals("06")) {
					record.setPlaycnttime06(1);
				} else if (hour.equals("07")) {
					record.setPlaycnttime07(1);
				} else if (hour.equals("08")) {
					record.setPlaycnttime08(1);
				} else if (hour.equals("09")) {
					record.setPlaycnttime09(1);
				} else if (hour.equals("10")) {
					record.setPlaycnttime10(1);
				} else if (hour.equals("11")) {
					record.setPlaycnttime11(1);
				} else if (hour.equals("12")) {
					record.setPlaycnttime12(1);
				} else if (hour.equals("13")) {
					record.setPlaycnttime13(1);
				} else if (hour.equals("14")) {
					record.setPlaycnttime14(1);
				} else if (hour.equals("15")) {
					record.setPlaycnttime15(1);
				} else if (hour.equals("16")) {
					record.setPlaycnttime16(1);
				} else if (hour.equals("17")) {
					record.setPlaycnttime17(1);
				} else if (hour.equals("18")) {
					record.setPlaycnttime18(1);
				} else if (hour.equals("19")) {
					record.setPlaycnttime19(1);
				} else if (hour.equals("20")) {
					record.setPlaycnttime20(1);
				} else if (hour.equals("21")) {
					record.setPlaycnttime21(1);
				} else if (hour.equals("22")) {
					record.setPlaycnttime22(1);
				} else if (hour.equals("23")) {
					record.setPlaycnttime23(1);
				}

				// add 推送播放数据给尼尔森
				// if (ispushnielsen == 1) {
				// record.setIp(dry.getIp().toString());
				// Message msg = new Message();
				// msg.setProducer(2);
				// msg.setMessage(record);
				// messageQueue.process(msg);
				// }

				records.add(record);

			}
		} catch (Exception e) {
			throw new DumgBeetleTransformException(e);
		}
		Map<HiveTable, List<SpecificRecordBase>> result = new HashMap<HiveTable, List<SpecificRecordBase>>();
		for (PlayLog record : records) {
			String partition = getDayPartition(record.getStarttime());
			result.put(getTable(partition),
					Lists.newArrayList((SpecificRecordBase) record));
		}
		return result;
	}

	private String getDayPartition(CharSequence startTime) {
		try {
			return df.format(startTimeDf.parse(startTime.toString()));
		} catch (Exception e) {
		}
		return null;
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
		partitionCache
				.put(partition, PlayLogHiveTableCreator.create(partition));
	}

	private void setPlayTime(PlayLog playlog, Integer adPlayTime,
			Integer adStartTime, Integer playtime, Date starttime) {
		playlog.setStarttime(DateUtil.dateDiff(starttime, Calendar.SECOND,
				adStartTime).toString());
		if (playtime != null && playtime >= adStartTime + adPlayTime) {// 完整播放
			playlog.setEndtime(DateUtil.dateDiff(starttime, Calendar.SECOND,
					adStartTime + adPlayTime).toString());
			playlog.setPlaytime(adPlayTime);
			playlog.setFullplay(1);
		} else {// 非完整播放
			playlog.setEndtime(DateUtil.dateDiff(starttime, Calendar.SECOND,
					playtime).toString());
			playlog.setPlaytime(playtime - adStartTime);
			playlog.setFullplay(0);
		}
	}

	/**
	 * 
	 * 根据排期，广告介质id查询广告节目信息
	 * 
	 * @param planId
	 * @param adfid
	 * @param playlog
	 */
	private void fillRecord(Map<String, Object> aminfomap, PlayLog playlog) {
		if (aminfomap != null && aminfomap.size() > 0) {
			if (aminfomap.get("amid") != null) {
				playlog.setAmid(Integer.valueOf(aminfomap.get("amid")
						.toString()));
			}
			if (aminfomap.get("adverno") != null) {
				playlog.setAdverno(Integer.parseInt(aminfomap.get("adverno")
						.toString()));
			}
			if (aminfomap.get("agentno") != null) {
				playlog.setAgentno(Integer.parseInt(aminfomap.get("agentno")
						.toString()));
			}
			if (aminfomap.get("sid") != null) {
				playlog.setSid(Integer
						.parseInt(aminfomap.get("sid").toString()));
			}
		} else {
			playlog.setAmid(-1);
		}
	}

	public void lookbackPlay(InterfacePlayLogDry dry,
			PlayUrlAdEntity playUrlAdEntity, int i) {
		// 直播回看暂时不处理，潘玉涛说比较简单，不用现在的逻辑
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, DumgBeetleTransformException {
		AdPlayLogTransformerImpl t = new AdPlayLogTransformerImpl();
		t.setup(null);
		t.cleanup(null);
	}

}
