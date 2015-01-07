package com.voole.hobbit2.order.common;

import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.camus.bsepg.BsEpgPlayInfo;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV2;
import com.voole.hobbit2.hive.order.avro.HiveOrderDryRecord;

public class BsEpgOrderSessionInfo {

	private BsEpgPlayInfo _bgn;

	private BsEpgPlayInfo _end;

	public void clear() {
		_bgn = null;

		_end = null;
	}

	public boolean isEnd(long currCamusExecTime) {
		if (_end != null) {
			return true;
		}
		// 超时时间为2个小时
		return _bgn.getPlaybgntime() == null
				|| _bgn.getPlaybgntime() < currCamusExecTime - 120 * 60;
	}

	public void setPlayInfo(BsEpgPlayInfo playInfo) {
		if (playInfo.getPlayendtime() == null
				|| playInfo.getPlayendtime() == 0l) {
			set_bgn(playInfo);
		} else {
			set_end(playInfo);
		}
	}

	public Long getPlayBgnTime() {
		if (_bgn != null) {
			return _bgn.getPlaybgntime();
		}
		return null;
	}

	public Long getPlayEndTime() {
		if (_end != null) {
			return _end.getPlayendtime();
		}
		return null;
	}

	public BsEpgPlayInfo getPlayInfo() {
		if (_end != null) {
			return _end;
		}
		return _bgn;
	}

	public BsEpgPlayInfo get_bgn() {
		return _bgn;
	}

	public void set_bgn(BsEpgPlayInfo _bgn) {
		this._bgn = _bgn;
	}

	public BsEpgPlayInfo get_end() {
		return _end;
	}

	public void set_end(BsEpgPlayInfo _end) {
		this._end = _end;
	}

	public HiveOrderDryRecord getMrRecord() {
		return BsEpgHiveOrderDryRecordGenerator.generate(this);
	}

	public SpecificRecordBase getStormRecord() {
		if (get_bgn() != null) {
			return getMrRecord();
		}

		OrderPlayEndReqV2 record = new OrderPlayEndReqV2();

		record.setSessID(String.valueOf(_end.getSessID()));
		record.setHID(_end.getHid());

		record.setSessAvgSpeed(0l);
		record.setNatip(BsEpgHiveOrderDryRecordGenerator.getIp(_end.getUserip()));
		record.setPerfIp(BsEpgHiveOrderDryRecordGenerator.getIp(_end
				.getPerfip()));
		record.setEndTick(_end.getPlayendtime());

		return record;
	}
}
