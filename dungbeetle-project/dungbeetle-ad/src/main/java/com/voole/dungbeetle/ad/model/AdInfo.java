package com.voole.dungbeetle.ad.model;
/**
 * 广告信息实体
 * SELECT am.amid,am.adname,am.runtime,am.adverno,vs.advername, am.agentno,gs.agentname 
		FROM voole_ad.ad_movieinfo am,voole_ad.advertisers vs,voole_ad.ad_agents gs
		WHERE  am.adverno=vs.adverno AND am.agentno=gs.agentno
 * @author Administrator
 *
 */
public class AdInfo {
	private String amid;
	private String adname;
	private String runtime;
	private String adverno;
	private String advername;
	private String agentno;
	public String getAmid() {
		return amid;
	}
	public void setAmid(String amid) {
		this.amid = amid;
	}
	public String getAdname() {
		return adname;
	}
	public void setAdname(String adname) {
		this.adname = adname;
	}
	public String getRuntime() {
		return runtime;
	}
	public void setRuntime(String runtime) {
		this.runtime = runtime;
	}
	public String getAdverno() {
		return adverno;
	}
	public void setAdverno(String adverno) {
		this.adverno = adverno;
	}
	public String getAdvername() {
		return advername;
	}
	public void setAdvername(String advername) {
		this.advername = advername;
	}
	public String getAgentno() {
		return agentno;
	}
	public void setAgentno(String agentno) {
		this.agentno = agentno;
	}
	
}
