/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.module.extra;

import java.io.Serializable;
import java.util.Map;

import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV3;
import com.voole.monitor2.playurl.PlayurlAnalyzer;

/**
 * @author XuehuiHe
 * @date 2014年6月9日
 */
public class OrderPlayBgnExtra implements Serializable {
	private String sessionId;
	private String bandwidth;// 用户带宽
	private String thirdpartyUid;// 第三方Uid
	private String productId;// 产品ID
	private String sectionId; // 点播所在的栏目id
	private String epgId;
	private String channelId;// 点播所在的频道id
	private Long logId;// epg日志id：本条日志标志着用户从哪个页面点击后产生的点播
	private Long oemid;
	private String uid;// 用户ID
	private String tvType;// 电视机型号
	private Integer protocol;// 播放协议 0 rtsp 1PC 2TV 3mobile
	private boolean isTest; // 是否是测试数据
	private String hid;
	private String natip;// 公网IP
	// private Long realtimeSpeed;// 实时速度
	private boolean isPlayedWithRightStamp;
	private Long playBgn;// 播放开始时间(单位:second)
	private String fid;
	private Integer policyId;// 产品策略ID（来源于播放串po参数）
	private boolean isAdMode;// 是否为广告模式
	private boolean isLookBack;// 是否为回看模式
	private Integer lookBackChannelId;// 回看频道ID
	private Integer lookBackStartTime;// 回看开始时间
	private Integer lookBackEndTime;// 回看结束时间
	private Integer lookBackProgramId;// 回看节目ID

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getBandwidth() {
		return bandwidth;
	}

	public void setBandwidth(String bandwidth) {
		this.bandwidth = bandwidth;
	}

	public String getThirdpartyUid() {
		return thirdpartyUid;
	}

	public void setThirdpartyUid(String thirdpartyUid) {
		this.thirdpartyUid = thirdpartyUid;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public String getSectionId() {
		return sectionId;
	}

	public void setSectionId(String sectionId) {
		this.sectionId = sectionId;
	}

	public String getEpgId() {
		return epgId;
	}

	public void setEpgId(String epgId) {
		this.epgId = epgId;
	}

	public String getChannelId() {
		return channelId;
	}

	public void setChannelId(String channelId) {
		this.channelId = channelId;
	}

	public Long getLogId() {
		return logId;
	}

	public void setLogId(Long logId) {
		this.logId = logId;
	}

	public Long getOemid() {
		return oemid;
	}

	public void setOemid(Long oemid) {
		this.oemid = oemid;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getTvType() {
		return tvType;
	}

	public void setTvType(String tvType) {
		this.tvType = tvType;
	}

	public boolean isTest() {
		return isTest;
	}

	public void setTest(boolean isTest) {
		this.isTest = isTest;
	}

	public String getHid() {
		return hid;
	}

	public void setHid(String hid) {
		this.hid = hid;
	}

	public String getNatip() {
		return natip;
	}

	public void setNatip(String natip) {
		this.natip = natip;
	}

	public boolean isPlayedWithRightStamp() {
		return isPlayedWithRightStamp;
	}

	public void setPlayedWithRightStamp(boolean isPlayedWithRightStamp) {
		this.isPlayedWithRightStamp = isPlayedWithRightStamp;
	}

	public Long getPlayBgn() {
		return playBgn;
	}

	public void setPlayBgn(Long playBgn) {
		this.playBgn = playBgn;
	}

	public String getFid() {
		return this.fid;
	}

	public void setFid(String fid) {
		this.fid = fid;
	}

	public Integer getPolicyId() {
		return policyId;
	}

	public void setPolicyId(Integer policyId) {
		this.policyId = policyId;
	}

	public boolean isAdMode() {
		return isAdMode;
	}

	public void setAdMode(boolean isAdMode) {
		this.isAdMode = isAdMode;
	}

	public boolean isLookBack() {
		return isLookBack;
	}

	public void setLookBack(boolean isLookBack) {
		this.isLookBack = isLookBack;
	}

	public Integer getLookBackChannelId() {
		return lookBackChannelId;
	}

	public void setLookBackChannelId(Integer lookBackChannelId) {
		this.lookBackChannelId = lookBackChannelId;
	}

	public Integer getLookBackStartTime() {
		return lookBackStartTime;
	}

	public void setLookBackStartTime(Integer lookBackStartTime) {
		this.lookBackStartTime = lookBackStartTime;
	}

	public Integer getLookBackEndTime() {
		return lookBackEndTime;
	}

	public void setLookBackEndTime(Integer lookBackEndTime) {
		this.lookBackEndTime = lookBackEndTime;
	}

	public Integer getLookBackProgramId() {
		return lookBackProgramId;
	}

	public void setLookBackProgramId(Integer lookBackProgramId) {
		this.lookBackProgramId = lookBackProgramId;
	}

	public Integer getProtocol() {
		return protocol;
	}

	public void setProtocol(Integer protocol) {
		this.protocol = protocol;
	}

	public void fillWith(OrderPlayBgnReqV2 v) {
		setFid(v.getFID());
		setHid(v.getHID());
		setOemid(v.getOEMID());
		setPlayBgn(v.getPlayTick());
		setSessionId(v.getSessID());
		setUid(v.getUID());
		// TODO 外网IP

		afterFill(v.getURL());
	}

	public void fillWith(OrderPlayBgnReqV3 v) {
		setFid(v.getFID());
		setHid(v.getHID());
		setOemid(v.getOEMID());
		setPlayBgn(v.getPlayTick());
		setSessionId(v.getSessID());
		setUid(v.getUID());
		// TODO 外网IP

		afterFill(v.getURL());
	}

	protected void afterFill(String url) {
		processUrl(url);
		fidHidToLowerCase();
		processProtocol();
	}

	protected void processProtocol() {
		Long oemid = this.getOemid();
		// 协议
		int protocol = 0;// rtsp
		if (oemid == null) {
		} else if (oemid == 100) {// pc客户端官方版
			protocol = 1;
		} else if (oemid > 100 && oemid < 500) {// 互联网电视
			protocol = 2;
		} else if (oemid >= 500 && oemid < 800) {// PC客户端
			protocol = 1;
		} else if (oemid >= 800 && oemid < 999) {// 手机客户端
			protocol = 3;
		}
		setProtocol(protocol);
	}

	protected void fidHidToLowerCase() {
		String fid = getFid();
		if (fid != null) {
			setFid(fid.toLowerCase());
		}
		String hid = getHid();
		if (hid != null && hid.length() > 12) {
			hid = hid.substring(0, 12);
		}
		if (hid != null) {
			setHid(hid.toLowerCase());
		}
	}

	protected void processUrl(String url) {
		if (url == null || url.length() == 0) {
			return;
		}
		processUrlMap(PlayurlAnalyzer.analyze(url));
	}

	public void processUrlMap(Map<String, String> pars) {
		setBandwidth(pars.get("speed"));
		setThirdpartyUid(pars.get("uid3"));
		setProductId(pars.get("pid"));
		setSectionId(pars.get("secid"));
		setEpgId(pars.get("epgid"));
		setChannelId(pars.get("chid"));
		String logId = pars.get("logid");
		if (logId != null) {
			try {
				setLogId(Long.parseLong(logId));
			} catch (Exception e) {
			}
		}
		String oemid = pars.get("oemid");
		try {
			if (oemid != null && oemid.length() > 0) {
				setOemid(Long.parseLong(oemid));
			}
		} catch (Exception e) {
		}
		String uid = pars.get("uid");
		setUid(uid);
		String tvType = "";
		String tvTypeFromUrl = pars.get("tvid");
		if (tvTypeFromUrl != null) {
			int pos = tvTypeFromUrl.indexOf('-');
			if (pos != -1) {
				tvType = tvTypeFromUrl.substring(0, pos);
			}
		}
		setTvType(tvType);
		boolean isTest = false;
		if ("202.106.92.98".equals(getNatip())
				|| "202.106.92.99".equals(getNatip())
				|| "1".equals(pars.get("test"))) {
			isTest = true;
		}

		if (getOemid() != null && (getOemid() == 998 || getOemid() == 999)) {
			isTest = true;
		}
		setTest(isTest);
		// // 即时速度 KB
		// if (realtimeSpeed == null) {
		// realtimeSpeed = 0l;
		// } else {
		// realtimeSpeed = new Long(Math.round((realtimeSpeed / 1024.0)));
		// }
		setPlayedWithRightStamp(isPlayedWithRightStamp(pars.get("stamp"),
				getPlayBgn()));
		// FID
		if (pars.containsKey("fid")) {
			setFid(pars.get("fid").toLowerCase());
		}
		// PO
		if (pars.containsKey("po")) {
			try {
				setPolicyId(Integer.parseInt(pars.get("po")));
			} catch (Exception e) {
			}
		}
		// AD
		setAdMode(pars.containsKey("adInfo"));
		// QTYPE
		setLookBack("500".equals(pars.get("qtype")));
		if (isLookBack()) {
			// // SUBLEVEL
			try {
				setLookBackChannelId(pars.containsKey("sublevel") ? Integer
						.parseInt(pars.get("sublevel")) : 0);
			} catch (Exception e) {
			}
			// STARTTIME
			try {
				setLookBackStartTime(pars.containsKey("starttime") ? Integer
						.parseInt(pars.get("starttime")) : 0);
			} catch (Exception e) {
			}
			// ENDTIME
			try {
				setLookBackEndTime(pars.containsKey("endtime") ? Integer
						.parseInt(pars.get("endtime")) : 0);
			} catch (Exception e) {
			}
		}
	}

	public boolean isPlayedWithRightStamp(String stamp, Long playbgn) {
		if (stamp == null || stamp.length() == 0) {
			return true;
		}
		if (!stamp.matches("/^1[0-9]{9,}$/")) {
			return true;
		}
		if (stamp.length() > 10) {
			stamp = stamp.substring(0, 10);
		}
		int stampInt = Integer.parseInt(stamp);
		if (stampInt < 1356969600) {
			return true;
		}
		if (playbgn == null) {
			return false;
		}
		double diff = Math.abs(stampInt - playbgn * 1000);
		return diff > 1 * 60 * 60 ? false : true;
	}
}
