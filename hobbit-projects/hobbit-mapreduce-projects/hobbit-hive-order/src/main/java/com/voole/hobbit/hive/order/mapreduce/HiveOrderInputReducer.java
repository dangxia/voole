/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.voole.hobbit.avro.hive.HiveOrderRecord;
import com.voole.hobbit.avro.termial.OrderPlayAliveReqV2;
import com.voole.hobbit.avro.termial.OrderPlayAliveReqV3;
import com.voole.hobbit.avro.termial.OrderPlayBgnReqV2;
import com.voole.hobbit.avro.termial.OrderPlayBgnReqV3;
import com.voole.hobbit.avro.termial.OrderPlayEndReqV2;
import com.voole.hobbit.avro.termial.OrderPlayEndReqV3;
import com.voole.hobbit.hive.order.cache.HiveOrderCache;
import com.voole.hobbit.hive.order.cache.HiveOrderCacheImpl;
import com.voole.monitor2.playurl.PlayurlAnalyzer;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputReducer
		extends
		Reducer<Text, AvroValue<SpecificRecordBase>, HiveOrderRecord, NullWritable> {
	private SessionInfo sessionInfo = new SessionInfo();
	private List<Object> srvs = new ArrayList<Object>();
	private HiveOrderCache hiveOrderCache;
	NullWritable outValue = NullWritable.get();
	AvroKey<HiveOrderRecord> outkey = new AvroKey<HiveOrderRecord>();

	private AvroMultipleOutputs ass;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		hiveOrderCache = new HiveOrderCacheImpl();
		hiveOrderCache.open();

		ass = new AvroMultipleOutputs(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		if (hiveOrderCache != null) {
			hiveOrderCache.close();
		}
		ass.close();
	}

	@Override
	protected void reduce(Text sessionId,
			Iterable<AvroValue<SpecificRecordBase>> iterable, Context context)
			throws IOException, InterruptedException {
		srvs.clear();
		sessionInfo.clear();

		HiveOrderRecord noendRecord = null;

		for (AvroValue<SpecificRecordBase> avroValue : iterable) {
			SpecificRecordBase record = avroValue.datum();
			if (record instanceof OrderPlayBgnReqV2) {
				sessionInfo.setBgn((OrderPlayBgnReqV2) record);
			} else if (record instanceof OrderPlayBgnReqV3) {
				sessionInfo.setBgn((OrderPlayBgnReqV3) record);
			} else if (record instanceof OrderPlayEndReqV2) {
				sessionInfo.setEnd((OrderPlayEndReqV2) record);
			} else if (record instanceof OrderPlayEndReqV3) {
				sessionInfo.setEnd((OrderPlayEndReqV3) record);
			} else if (record instanceof OrderPlayAliveReqV2) {
				sessionInfo.setAlive((OrderPlayAliveReqV2) record);
			} else if (record instanceof OrderPlayAliveReqV3) {
				sessionInfo.setAlive((OrderPlayAliveReqV3) record);
			} else if (record instanceof HiveOrderRecord) {
				noendRecord = (HiveOrderRecord) record;
			} else {
				// TODO
			}
		}
		HiveOrderRecord orderRecord = sessionInfo.generateHiveOrderRecord(
				sessionId.toString(), noendRecord);
		if (orderRecord != null) {
			outkey.datum(orderRecord);
			ass.write("record", outkey);
		}
	}

	// <!---fill alive bgn

	private void fillAlive(HiveOrderRecord record, Object alive) {
		if (alive instanceof OrderPlayAliveReqV2) {
			_fillAlive(record, (OrderPlayAliveReqV2) alive);
		} else {
			_fillAlive(record, (OrderPlayAliveReqV3) alive);
		}
	}

	private void _fillAlive(HiveOrderRecord record, OrderPlayAliveReqV2 alive) {
		_fillAlive(record, alive.getAliveTick(), alive.getSessAvgSpeed());
	}

	private void _fillAlive(HiveOrderRecord record, OrderPlayAliveReqV3 alive) {
		_fillAlive(record, alive.getAliveTick(), alive.getSessAvgSpeed());
	}

	private void _fillAlive(HiveOrderRecord record, Long aliveTick,
			Long sessAvgSpeed) {
		if (record.getPlayAliveTime() == null
				|| record.getPlayAliveTime() < aliveTick) {
			record.setPlayAliveTime(aliveTick);
			record.setAvgspeed(sessAvgSpeed);
		}
	}

	// --->fill alive end

	// <!---fill end bgn
	private void fillEnd(HiveOrderRecord record, Object end) {
		if (end instanceof OrderPlayEndReqV2) {
			_fillEnd(record, (OrderPlayEndReqV2) end);
		} else {
			_fillEnd(record, (OrderPlayEndReqV3) end);
		}
	}

	private void _fillEnd(HiveOrderRecord record, OrderPlayEndReqV3 end) {
		_fillEnd(record, end.getEndTick(), end.getSessAvgSpeed());
	}

	private void _fillEnd(HiveOrderRecord record, OrderPlayEndReqV2 end) {
		_fillEnd(record, end.getEndTick(), end.getSessAvgSpeed());
	}

	private void _fillEnd(HiveOrderRecord record, Long endTick,
			Long sessAvgSpeed) {
		record.setPlayEndTime(endTick);
		record.setAvgspeed(sessAvgSpeed);
	}

	// --->fill end end

	// <!---fill bgn bgn
	private void fillBgn(HiveOrderRecord record, Object bgn) {
		if (bgn instanceof OrderPlayBgnReqV2) {
			_fillBgn(record, (OrderPlayBgnReqV2) bgn);
		} else {
			_fillBgn(record, (OrderPlayBgnReqV3) bgn);
		}
	}

	private void _fillBgn(HiveOrderRecord record, OrderPlayBgnReqV2 bgn) {
		_fillBgn(record, bgn.getUID(), bgn.getHID(), bgn.getOEMID(),
				bgn.getNatip(), bgn.getFID(), bgn.getPlayTick(), bgn.getURL());

	}

	private void _fillBgn(HiveOrderRecord record, OrderPlayBgnReqV3 bgn) {
		_fillBgn(record, bgn.getUID(), bgn.getHID(), bgn.getOEMID(),
				bgn.getNatip(), bgn.getFID(), bgn.getPlayTick(), bgn.getURL());
	}

	private void _fillBgn(HiveOrderRecord record, CharSequence uid,
			CharSequence hid, Long oemid, Long natip, CharSequence fid,
			Long playTick, CharSequence url) {
		if (record.getPlayBgnTime() == null
				|| record.getPlayBgnTime() < playTick) {
			record.setUID(uid);
			record.setHID(hid);
			record.setOEMID(oemid);
			record.setNatip(natip);
			record.setFID(fid);
			record.setPlayBgnTime(playTick);
			afterFill(record, url.toString());
		}
	}

	protected void afterFill(HiveOrderRecord record, String url) {
		processUrl(record, url);
		fidHidToUperCase(record);
		hiveOrderCache.deal(record);
	}

	protected void processUrl(HiveOrderRecord record, String url) {
		if (url == null || url.length() == 0) {
			return;
		}
		processUrlMap(record, PlayurlAnalyzer.analyze(url));
	}

	protected void fidHidToUperCase(HiveOrderRecord record) {
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

	public void processUrlMap(HiveOrderRecord record, Map<String, String> pars) {
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

	// --->fill bgn end
	private class SessionInfo {
		public Object _bgn;
		public long _bgnTime;

		public Object _end;
		public long _endTime;

		public Object _lastAlive;
		public long _lastAliveTime;

		public void clear() {
			_bgn = null;
			_bgnTime = 0l;

			_end = null;
			_endTime = 0l;

			_lastAlive = null;
			_lastAliveTime = 0l;
		}

		public HiveOrderRecord generateHiveOrderRecord(String sessionId,
				HiveOrderRecord noendRecord) {
			HiveOrderRecord record = null;
			if (noendRecord == null) {
				if (_bgn == null) {
					return null;
				}
				wash(_bgnTime);
				record = new HiveOrderRecord();
				record.setSessID(sessionId);
				fillBgn(record, sessionInfo._bgn);
			} else {
				if (sessionInfo._bgn != null) {
					fillBgn(noendRecord, sessionInfo._bgn);
				}
				wash(noendRecord.getPlayBgnTime());
				washNoEndRecordAliveTime(noendRecord);
				record = noendRecord;
			}

			if (sessionInfo._end != null) {
				fillEnd(record, sessionInfo._end);
			}
			if (sessionInfo._lastAlive != null) {
				fillAlive(record, sessionInfo._lastAlive);
			}
			if (sessionInfo._end != null) {
				record.setPlayDurationTime(sessionInfo._endTime
						- sessionInfo._bgnTime);
			} else if (sessionInfo._lastAlive != null) {
				record.setPlayDurationTime(sessionInfo._lastAliveTime
						- sessionInfo._bgnTime);
			}
			return record;
		}

		private void washNoEndRecordAliveTime(HiveOrderRecord noendRecord) {
			if (noendRecord.getPlayAliveTime() != null
					&& noendRecord.getPlayBgnTime() > noendRecord
							.getPlayAliveTime()) {
				noendRecord.setPlayAliveTime(null);
				noendRecord.setAvgspeed(null);
			}
		}

		private void wash(long bgnTime) {
			if (_end != null && bgnTime > _endTime) {
				_end = null;
				_endTime = 0l;
			}
			if (_lastAlive != null && bgnTime > _lastAliveTime) {
				_lastAlive = null;
				_lastAliveTime = 0l;
			}

			if (_lastAlive != null && _end != null && _endTime < _lastAliveTime) {
				_lastAlive = null;
				_lastAliveTime = 0l;
			}
		}

		public void setBgn(OrderPlayBgnReqV2 bgn) {
			setBgn(bgn, bgn.getPlayTick());
		}

		public void setBgn(OrderPlayBgnReqV3 bgn) {
			setBgn(bgn, bgn.getPlayTick());
		}

		public void setBgn(Object bgn, Long bgnTime) {
			if (bgnTime == null) {
				return;
			}
			if (_bgn == null || bgnTime > _bgnTime) {
				_bgn = bgn;
				_bgnTime = bgnTime;
			}
			// TODO record wrong record
		}

		public void setEnd(OrderPlayEndReqV2 end) {
			setEnd(end, end.getEndTick());
		}

		public void setEnd(OrderPlayEndReqV3 end) {
			setEnd(end, end.getEndTick());
		}

		public void setEnd(Object end, Long endTime) {
			if (endTime == null) {
				return;
			}
			if (_end == null || endTime > _endTime) {
				_end = end;
				_endTime = endTime;
			}
			// TODO record wrong record
		}

		public void setAlive(OrderPlayAliveReqV2 alive) {
			setAlive(alive, alive.getAliveTick());
		}

		public void setAlive(OrderPlayAliveReqV3 alive) {
			setAlive(alive, alive.getAliveTick());
		}

		public void setAlive(Object alive, Long aliveTime) {
			if (aliveTime == null) {
				return;
			}
			srvs.add(alive);
			if (_lastAlive == null || aliveTime > _lastAliveTime) {
				_lastAlive = alive;
				_lastAliveTime = aliveTime;
			}
		}
	}
}
