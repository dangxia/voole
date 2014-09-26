/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.util;

import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.camus.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV2;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV3;
import com.voole.hobbit2.camus.order.dry.PlayAliveDryRecord;
import com.voole.hobbit2.camus.order.dry.PlayBgnDryRecord;
import com.voole.hobbit2.camus.order.dry.PlayEndDryRecord;
import com.voole.monitor2.playurl.PlayurlAnalyzer;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class DryGenerator {

	public static SpecificRecordBase dry(SpecificRecordBase base) {
		if (base == null) {
			return null;
		}
		if (base instanceof OrderPlayBgnReqV2) {
			return dryBgn((OrderPlayBgnReqV2) base);
		} else if (base instanceof OrderPlayBgnReqV3) {
			return dryBgn((OrderPlayBgnReqV3) base);
		} else if (base instanceof OrderPlayAliveReqV2) {
			return dryAlive((OrderPlayAliveReqV2) base);
		} else if (base instanceof OrderPlayAliveReqV3) {
			return dryAlive((OrderPlayAliveReqV3) base);
		} else if (base instanceof OrderPlayEndReqV2) {
			return dryEnd((OrderPlayEndReqV2) base);
		} else if (base instanceof OrderPlayEndReqV3) {
			return dryEnd((OrderPlayEndReqV3) base);
		}
		return null;
	}

	public static PlayBgnDryRecord dryBgn(OrderPlayBgnReqV2 bgn) {
		PlayBgnDryRecord record = new PlayBgnDryRecord();
		fillBgn(record, bgn);
		return record;
	}

	public static PlayBgnDryRecord dryBgn(OrderPlayBgnReqV3 bgn) {
		PlayBgnDryRecord record = new PlayBgnDryRecord();
		fillBgn(record, bgn);
		return record;
	}

	public static PlayAliveDryRecord dryAlive(OrderPlayAliveReqV2 alive) {
		PlayAliveDryRecord record = new PlayAliveDryRecord();
		record.setAvgspeed(alive.getSessAvgSpeed());
		record.setNatip(alive.getNatip());
		record.setPlayAliveTime(alive.getAliveTick());
		record.setSessID(alive.getSessID());
		return record;
	}

	public static PlayAliveDryRecord dryAlive(OrderPlayAliveReqV3 alive) {
		PlayAliveDryRecord record = new PlayAliveDryRecord();
		record.setAvgspeed(alive.getSessAvgSpeed());
		record.setNatip(alive.getNatip());
		record.setPlayAliveTime(alive.getAliveTick());
		record.setSessID(alive.getSessID());
		return record;
	}

	public static PlayEndDryRecord dryEnd(OrderPlayEndReqV2 end) {
		PlayEndDryRecord record = new PlayEndDryRecord();
		record.setAvgspeed(end.getSessAvgSpeed());
		record.setNatip(end.getNatip());
		record.setPlayEndTime(end.getEndTick());
		record.setSessID(end.getSessID());
		return record;
	}

	public static PlayEndDryRecord dryEnd(OrderPlayEndReqV3 end) {
		PlayEndDryRecord record = new PlayEndDryRecord();
		record.setAvgspeed(end.getSessAvgSpeed());
		record.setNatip(end.getNatip());
		record.setPlayEndTime(end.getEndTick());
		record.setSessID(end.getSessID());
		return record;
	}

	private static void fillBgn(PlayBgnDryRecord record, OrderPlayBgnReqV2 bgn) {
		fillBgn(record, bgn.getSessID(), bgn.getUID(), bgn.getHID(),
				bgn.getOEMID(), bgn.getNatip(), bgn.getFID(),
				bgn.getPlayTick(), bgn.getURL());

	}

	private static void fillBgn(PlayBgnDryRecord record, OrderPlayBgnReqV3 bgn) {
		fillBgn(record, bgn.getSessID(), bgn.getUID(), bgn.getHID(),
				bgn.getOEMID(), bgn.getNatip(), bgn.getFID(),
				bgn.getPlayTick(), bgn.getURL());
	}

	private static void fillBgn(PlayBgnDryRecord record,
			CharSequence sessionId, CharSequence uid, CharSequence hid,
			Long oemid, Long natip, CharSequence fid, Long playTick,
			CharSequence url) {
		record.setSessID(sessionId);
		record.setUID(uid);
		record.setHID(hid);
		record.setOEMID(oemid);
		record.setNatip(natip);
		record.setFID(fid);
		record.setPlayBgnTime(playTick);
		afterFill(record, url.toString());
	}

	protected static void afterFill(PlayBgnDryRecord record, String url) {
		processUrl(record, url);
		fidHidToUperCase(record);
	}

	protected static void processUrl(PlayBgnDryRecord record, String url) {
		if (url == null || url.length() == 0) {
			return;
		}
		processUrlMap(record, PlayurlAnalyzer.analyze(url));
	}

	protected static void fidHidToUperCase(PlayBgnDryRecord record) {
		CharSequence hid = record.getHID();
		if (hid != null && hid.length() > 12) {
			hid = hid.toString().substring(0, 12);
		}
		if (hid != null) {
			record.setHID(hid.toString().toUpperCase());
		}
	}

	public static void processUrlMap(PlayBgnDryRecord record,
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
		// setChannelId(pars.get("chid"));
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
		// AD
		record.setIsAdMod(pars.containsKey("adInfo"));
		// QTYPE
		record.setIsRepeatMod("500".equals(pars.get("qtype")));
	}
}
