package com.voole.dungbeetle.ad.model;

import java.util.Date;

/**
 * 广告播放详细信息
 */
public class AdPlayStat extends ValueObject implements java.io.Serializable {

	private static final long serialVersionUID = -483714330701436238L;
	public static final Short FULLPLAY_YES = 1;
	public static final Short FULLPLAY_NO = 0;
	private String logid;
	// sessionid
	private String sessionid;
	// 广告节目id
	private Integer amid;
	// 广告节目名称
	private String adname;
	// 广告主编号
	private Integer adverno;
	// 广告主名称
	private String advername;
	// 代理商编号
	private Integer agentno;
	//代理商名称
	private String agentname;
	// 运营商编号
	private Integer spid;
	//运营商名称
	private String spname;
	// oem编号
	private Integer oemid;
	// oem名称
	private String oemname;
	// 终端编号
	private String hid;
	// 投放区域
	private Integer provinceid;
	//区域名称
	private String provincename;
	// 投放区域
	private Integer cityid;
	//区域名称
	private String cityname;
	// 投放地段
	// 播放频道id
	private Integer channelid;
	//播放频道名称
	private String channelname;
	//播放栏目id
	private Integer programid;
	//播放栏目名称
	private String programname;
	// 播放节目
	private String movname;
	// 播放集数
	private Integer sid;
	// 广告长度
	private Integer adlength;
	// 介质码流
	private Integer coderate;
	// 介质分辨率
	private String resolution;
	// 播放开始时间
	private Date starttime;
	// 播放结束时间
	private Date endtime;
	// 播放时长
	private Integer playtime;
	// 播放速度
	private Integer speed;
	// 是否播放完整 0-不完整 1-完整
	private Short fullplay;
	// 广告介质fid
	private String fid;
	//广告位置id
	private Integer adposid;
	//广告位置名称
	private String adposname;
	//请求者ip
	private String ip;
	
	public String getSessionid() {
		return sessionid;
	}
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}
	public Integer getAmid() {
		return amid;
	}
	public void setAmid(Integer amid) {
		this.amid = amid;
	}
	public String getAdname() {
		return adname;
	}
	public void setAdname(String adname) {
		this.adname = adname;
	}
	public Integer getAdverno() {
		return adverno;
	}
	public void setAdverno(Integer adverno) {
		this.adverno = adverno;
	}
	public String getAdvername() {
		return advername;
	}
	public void setAdvername(String advername) {
		this.advername = advername;
	}
	public Integer getAgentno() {
		return agentno;
	}
	public void setAgentno(Integer agentno) {
		this.agentno = agentno;
	}
	public String getAgentname() {
		return agentname;
	}
	public void setAgentname(String agentname) {
		this.agentname = agentname;
	}
	public Integer getSpid() {
		return spid;
	}
	public void setSpid(Integer spid) {
		this.spid = spid;
	}
	public String getSpname() {
		return spname;
	}
	public void setSpname(String spname) {
		this.spname = spname;
	}
	public Integer getOemid() {
		return oemid;
	}
	public void setOemid(Integer oemid) {
		this.oemid = oemid;
	}
	public String getOemname() {
		return oemname;
	}
	public void setOemname(String oemname) {
		this.oemname = oemname;
	}
	public String getHid() {
		return hid;
	}
	public void setHid(String hid) {
		this.hid = hid;
	}
	public Integer getProvinceid() {
		return provinceid;
	}
	public void setProvinceid(Integer provinceid) {
		this.provinceid = provinceid;
	}
	public String getProvincename() {
		return provincename;
	}
	public void setProvincename(String provincename) {
		this.provincename = provincename;
	}
	public Integer getCityid() {
		return cityid;
	}
	public void setCityid(Integer cityid) {
		this.cityid = cityid;
	}
	public String getCityname() {
		return cityname;
	}
	public void setCityname(String cityname) {
		this.cityname = cityname;
	}
	public Integer getChannelid() {
		return channelid;
	}
	public void setChannelid(Integer channelid) {
		this.channelid = channelid;
	}
	public String getChannelname() {
		return channelname;
	}
	public void setChannelname(String channelname) {
		this.channelname = channelname;
	}
	public Integer getProgramid() {
		return programid;
	}
	public void setProgramid(Integer programid) {
		this.programid = programid;
	}
	public String getProgramname() {
		return programname;
	}
	public void setProgramname(String programname) {
		this.programname = programname;
	}
	public String getMovname() {
		return movname;
	}
	public void setMovname(String movname) {
		this.movname = movname;
	}
	public Integer getSid() {
		return sid;
	}
	public void setSid(Integer sid) {
		this.sid = sid;
	}
	public Integer getAdlength() {
		return adlength;
	}
	public void setAdlength(Integer adlength) {
		this.adlength = adlength;
	}
	public Integer getCoderate() {
		return coderate;
	}
	public void setCoderate(Integer coderate) {
		this.coderate = coderate;
	}
	public String getResolution() {
		return resolution;
	}
	public void setResolution(String resolution) {
		this.resolution = resolution;
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
	public Integer getPlaytime() {
		return playtime;
	}
	public void setPlaytime(Integer playtime) {
		this.playtime = playtime;
	}
	public Integer getSpeed() {
		return speed;
	}
	public void setSpeed(Integer speed) {
		this.speed = speed;
	}
	public Short getFullplay() {
		return fullplay;
	}
	public void setFullplay(Short fullplay) {
		this.fullplay = fullplay;
	}
	public String getFid() {
		return fid;
	}
	public void setFid(String fid) {
		this.fid = fid;
	}
	public Integer getAdposid() {
		return adposid;
	}
	public void setAdposid(Integer adposid) {
		this.adposid = adposid;
	}
	public String getAdposname() {
		return adposname;
	}
	public void setAdposname(String adposname) {
		this.adposname = adposname;
	}
	public String getLogid() {
		return logid;
	}
	public void setLogid(String logid) {
		this.logid = logid;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	
}
