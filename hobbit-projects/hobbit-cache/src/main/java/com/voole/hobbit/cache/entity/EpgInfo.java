package com.voole.hobbit.cache.entity;

import java.io.Serializable;

public class EpgInfo implements Serializable, Cloneable {
	private static final long serialVersionUID = 1129206330931367658L;
	private String ispid;
	private long tid;
	private String pname;

	public long getTid() {
		return tid;
	}

	public void setTid(long tid) {
		this.tid = tid;
	}

	public String getPname() {
		return pname;
	}

	public String getIspid() {
		return ispid;
	}

	public void setIspid(String ispid) {
		this.ispid = ispid;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	@Override
	public EpgInfo clone() throws CloneNotSupportedException {
		return (EpgInfo) super.clone();
	}

}