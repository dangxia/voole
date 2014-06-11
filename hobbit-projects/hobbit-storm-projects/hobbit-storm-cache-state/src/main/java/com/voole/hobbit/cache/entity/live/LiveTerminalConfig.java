/*
 * Copyright (C) 2013 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache.entity.live;

import java.io.Serializable;

import com.voole.hobbit.config.Tuple;

/**
 * @author XuehuiHe
 * @date 2013年10月25日
 */
public class LiveTerminalConfig implements Serializable {

	private static final long serialVersionUID = -8438727139815417740L;

	private String channelcode;
	private String tid;
	private String localcode;
	private Integer ctype;

	public LiveTerminalConfig() {
	}

	public String getChannelcode() {
		return channelcode;
	}

	public String getTid() {
		return tid;
	}

	public String getLocalcode() {
		return localcode;
	}

	public Integer getCtype() {
		return ctype;
	}

	public void setChannelcode(String channelcode) {
		this.channelcode = channelcode;
	}

	public void setTid(String tid) {
		this.tid = tid;
	}

	public void setLocalcode(String localcode) {
		this.localcode = localcode;
	}

	public void setCtype(Integer ctype) {
		this.ctype = ctype;
	}

	public Tuple<Tuple<String, Integer>, String> getKey() {
		return new Tuple<Tuple<String, Integer>, String>(
				new Tuple<String, Integer>(getLocalcode(), getCtype()),
				getTid());
	}

}
