package com.voole.hobbit2.cache.entity.live;

import java.util.Date;

public class LiveProgramInfo {
	private Integer seqno;
	private Date starttime;
	private Date endtime;

	public LiveProgramInfo() {
	}

	public Integer getSeqno() {
		return seqno;
	}

	public void setSeqno(Integer seqno) {
		this.seqno = seqno;
	}

	public Date getStarttime() {
		return starttime;
	}

	public void setStarttime(Date starttime) {
		this.starttime = starttime;
	}

	public Date getEndtime() {
		return endtime;
	}

	public void setEndtime(Date endtime) {
		this.endtime = endtime;
	}

}
