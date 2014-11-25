package com.voole.hobbit2.order.common;

import java.util.Map;

import com.voole.hobbit2.camus.bsepg.BsEpgPlayInfo;
import com.voole.hobbit2.common.Hobbit2Utils;
import com.voole.hobbit2.hive.order.avro.HiveOrderDryRecord;
import com.voole.monitor2.playurl.PlayurlAnalyzer;

public class BsEpgHiveOrderDryRecordGenerator {
	public static HiveOrderDryRecord generate(
			BsEpgOrderSessionInfo orderSessionInfo) {
		HiveOrderDryRecord record = new HiveOrderDryRecord();
		BsEpgPlayInfo playInfo = orderSessionInfo.getPlayInfo();
		record.setPlayBgnTime(orderSessionInfo.getPlayBgnTime());
		fill(record, playInfo);

		Long endTime = orderSessionInfo.getPlayEndTime();
		Long startTime = orderSessionInfo.getPlayBgnTime();
		if (startTime != null && endTime != null) {
			record.setPlayDurationTime(endTime - startTime);
		} else {
			record.setPlayDurationTime(0l);
		}

		return record;
	}

	public static long getIp(CharSequence ipStr) {
		try {
			return Hobbit2Utils.ipToLong(String.valueOf(ipStr));
		} catch (Exception e) {
			return -1l;
		}

	}

	private static void fill(HiveOrderDryRecord record, BsEpgPlayInfo playInfo) {
		CharSequence url = playInfo.getPlayurl();

		record.setSessID(String.valueOf(playInfo.getSessID()));
		record.setHID(playInfo.getHid());
		record.setOEMID(playInfo.getOemid());
		record.setNatip(getIp(playInfo.getUserip()));
		record.setPlayurl(url);
		record.setPerfip(getIp(playInfo.getPerfip()));

		record.setPlayEndTime(playInfo.getPlayendtime());
		record.setPlayAliveTime(playInfo.getPlayalivetime());

		record.setDatasorce(1);
		record.setMetricTechtype(1);

		record.setMetricStatus(0);
		record.setApkVersion("0");
		record.setMetricPartnerinfo(null);
		record.setVssip(null);
		record.setExtinfo(null);
		record.setIsAdMod(false);
		record.setIsRepeatMod(false);

		if (url != null) {
			processUrl(record, url.toString());
		}
		fidHidToUperCase(record);
	}

	protected static void processUrl(HiveOrderDryRecord record, String url) {
		if (url == null || url.length() == 0) {
			return;
		}
		processUrlMap(record, PlayurlAnalyzer.analyze(url));
	}

	protected static void fidHidToUperCase(HiveOrderDryRecord record) {
		CharSequence fid = record.getFID();
		if (fid != null) {
			record.setFID(fid.toString().toUpperCase());
		}
		CharSequence hid = record.getHID();
		if (hid != null && hid.length() > 12) {
			hid = hid.toString().substring(0, 12);
		}
		if (hid != null) {
			record.setHID(hid.toString().toUpperCase());
		}
	}

	public static void processUrlMap(HiveOrderDryRecord record,
			Map<String, String> pars) {
		record.setPid(pars.get("pid"));
		record.setSecid(pars.get("secid"));
		String egpid = pars.get("epgid");
		if (egpid != null && egpid.length() > 0) {
			try {
				record.setEpgid(Long.parseLong(egpid));
			} catch (Exception e) {
			}
		}
		String oemid = pars.get("oemid");
		try {
			if (oemid != null && oemid.length() > 0) {
				record.setOEMID(Long.parseLong(oemid));
			}
		} catch (Exception e) {
		}
		// FID
		if (pars.containsKey("fid")) {
			record.setFID(pars.get("fid").toLowerCase());
		}

		if (pars.containsKey("po")) {
			try {
				record.setDimPoId(Integer.parseInt(pars.get("po")));
			} catch (Exception e) {
			}
		}
		String uid = pars.get("uid");
		if (uid != null && uid.length() > 0) {
			record.setUID(uid);
		}

		String mid = pars.get("mid");
		if (mid != null && mid.length() > 0) {
			try {
				record.setMid(Long.parseLong(mid));
			} catch (Exception e) {
			}
		}

		String sid = pars.get("sid");
		if (sid != null && sid.length() > 0) {
			try {
				record.setSid(Long.parseLong(sid));
			} catch (Exception e) {
			}
		}

	}
}
