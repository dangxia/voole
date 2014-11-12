package com.voole.hobbit2.order.common;

import com.voole.hobbit2.camus.bsepg.BsEpgPlayInfo;

public class BsEpgOrderSessionInfo {

	public BsEpgPlayInfo _bgn;

	public BsEpgPlayInfo _end;

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

}
