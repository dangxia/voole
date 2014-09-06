/*
 * Copyright (C) 2013 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache.entity.live;

import java.io.Serializable;
import java.util.Date;

import com.voole.hobbit2.common.Tuple;


/**
 * @author XuehuiHe
 * @date 2013年10月25日
 */
public class LiveChannelConfig implements Serializable {
	private static final long serialVersionUID = -1257229043861865493L;
	private String channelcode;
	private String programname;
	private Integer ctype;
	private Date starttime;

	public LiveChannelConfig() {
	}

	public String getChannelcode() {
		return channelcode;
	}

	public String getProgramname() {
		return programname;
	}

	public Integer getCtype() {
		return ctype;
	}

	public Date getStarttime() {
		return starttime;
	}

	public void setChannelcode(String channelcode) {
		this.channelcode = channelcode;
	}

	public void setProgramname(String programname) {
		this.programname = programname;
	}

	public void setCtype(Integer ctype) {
		this.ctype = ctype;
	}

	public void setStarttime(Date starttime) {
		this.starttime = starttime;
	}

	public Tuple<Integer, String> getKey() {
		return new Tuple<Integer, String>(getCtype(), getChannelcode());
	}

}
