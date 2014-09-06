/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.hive.order;

import java.util.Map;

import com.voole.hobbit2.camus.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV2;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV3;
import com.voole.hobbit2.hive.order.avro.HiveOrderDryRecord;
import com.voole.monitor2.playurl.PlayurlAnalyzer;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class HiveOrderDryRecordGenerator {
	public static HiveOrderDryRecord generate(String sessionId,
			OrderSessionInfo orderSessionInfo) {
		HiveOrderDryRecord record = new HiveOrderDryRecord();
		record.setSessID(sessionId);
		fillBgn(record, orderSessionInfo._bgn);

		if (orderSessionInfo._lastAlive != null) {
			fillAlive(record, orderSessionInfo._lastAlive);
		}

		if (orderSessionInfo._end != null) {
			fillEnd(record, orderSessionInfo._end);
		}

		if (orderSessionInfo._end != null) {
			record.setPlayDurationTime(orderSessionInfo._endTime
					- orderSessionInfo._bgnTime);
		} else if (orderSessionInfo._lastAlive != null) {
			record.setPlayDurationTime(orderSessionInfo._lastAliveTime
					- orderSessionInfo._bgnTime);
		}
		return record;

	}

	private static void fillAlive(HiveOrderDryRecord record, Object alive) {
		if (alive instanceof OrderPlayAliveReqV2) {
			_fillAlive(record, (OrderPlayAliveReqV2) alive);
		} else {
			_fillAlive(record, (OrderPlayAliveReqV3) alive);
		}
	}

	private static void _fillAlive(HiveOrderDryRecord record,
			OrderPlayAliveReqV2 alive) {
		_fillAlive(record, alive.getAliveTick(), alive.getSessAvgSpeed());
	}

	private static void _fillAlive(HiveOrderDryRecord record,
			OrderPlayAliveReqV3 alive) {
		_fillAlive(record, alive.getAliveTick(), alive.getSessAvgSpeed());
	}

	private static void _fillAlive(HiveOrderDryRecord record, Long aliveTick,
			Long sessAvgSpeed) {
		record.setPlayAliveTime(aliveTick);
		record.setAvgspeed(sessAvgSpeed);
	}

	private static void fillEnd(HiveOrderDryRecord record, Object end) {
		if (end instanceof OrderPlayEndReqV2) {
			_fillEnd(record, (OrderPlayEndReqV2) end);
		} else {
			_fillEnd(record, (OrderPlayEndReqV3) end);
		}
	}

	private static void _fillEnd(HiveOrderDryRecord record,
			OrderPlayEndReqV3 end) {
		_fillEnd(record, end.getEndTick(), end.getSessAvgSpeed());
	}

	private static void _fillEnd(HiveOrderDryRecord record,
			OrderPlayEndReqV2 end) {
		_fillEnd(record, end.getEndTick(), end.getSessAvgSpeed());
	}

	private static void _fillEnd(HiveOrderDryRecord record, Long endTick,
			Long sessAvgSpeed) {
		record.setPlayEndTime(endTick);
		record.setAvgspeed(sessAvgSpeed);
	}

	private static void fillBgn(HiveOrderDryRecord record, Object bgn) {
		if (bgn instanceof OrderPlayBgnReqV2) {
			_fillBgn(record, (OrderPlayBgnReqV2) bgn);
		} else {
			_fillBgn(record, (OrderPlayBgnReqV3) bgn);
		}
	}

	private static void _fillBgn(HiveOrderDryRecord record,
			OrderPlayBgnReqV2 bgn) {
		_fillBgn(record, bgn.getUID(), bgn.getHID(), bgn.getOEMID(),
				bgn.getNatip(), bgn.getFID(), bgn.getPlayTick(), bgn.getURL());

	}

	private static void _fillBgn(HiveOrderDryRecord record,
			OrderPlayBgnReqV3 bgn) {
		_fillBgn(record, bgn.getUID(), bgn.getHID(), bgn.getOEMID(),
				bgn.getNatip(), bgn.getFID(), bgn.getPlayTick(), bgn.getURL());
	}

	private static void _fillBgn(HiveOrderDryRecord record, CharSequence uid,
			CharSequence hid, Long oemid, Long natip, CharSequence fid,
			Long playTick, CharSequence url) {
		record.setUID(uid);
		record.setHID(hid);
		record.setOEMID(oemid);
		record.setNatip(natip);
		record.setFID(fid);
		record.setPlayBgnTime(playTick);
		afterFill(record, url.toString());
	}

	protected static void afterFill(HiveOrderDryRecord record, String url) {
		processUrl(record, url);
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
