package com.voole.dungbeetle.ad.model;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;

public class PlayUrlAdInfos {

	public static final int REPEAT_MERGE_TYPE = 10;

	private int mergetype;// 广告merge类型
	private String logid;// 投放日志id
	private String mid;//节目id
	private List<String> plnidList; // 广告排期id列表
	private final List<PlayUrlAdItem> items;

	public PlayUrlAdInfos() {
		items = new ArrayList<PlayUrlAdInfos.PlayUrlAdItem>();
	}

	public void addItem(PlayUrlAdItem item) {
		if (item != null) {
			items.add(item);
		}
	}

	public int getMergetype() {
		return mergetype;
	}

	public void setMergetype(int mergetype) {
		this.mergetype = mergetype;
	}

	public String getLogid() {
		return logid;
	}

	public void setLogid(String logid) {
		this.logid = logid;
	}

	public List<PlayUrlAdItem> getItems() {
		return items;
	}

	public boolean isRepeatType() {
		return getMergetype() == REPEAT_MERGE_TYPE;
	}

	public List<String> getPlnidList() {
		return plnidList;
	}

	public void setPlnidList(List<String> plnidList) {
		this.plnidList = plnidList;
	}

	public String getMid() {
		return mid;
	}

	public void setMid(String mid) {
		this.mid = mid;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("mergetype", this.mergetype)
				.add("plnidList", this.plnidList).add("logid", this.logid).add("mid", this.mid)
				.add("items", this.items)
				.add("isRepeatType", this.isRepeatType()).toString();
	}

	public static class PlayUrlAdItem {
		private String fid;// 广告介质id
		private Integer startTime;// 开始时间
		private Integer playTime;// 播放时长
		private String adSize;// 广告大小 (m3u8 特有)

		public String getFid() {
			return fid;
		}

		public void setFid(String fid) {
			this.fid = fid;
		}

		public Integer getStartTime() {
			return startTime;
		}

		public void setStartTime(Integer startTime) {
			this.startTime = startTime;
		}

		public Integer getPlayTime() {
			return playTime;
		}

		public void setPlayTime(Integer playTime) {
			this.playTime = playTime;
		}

		public String getAdSize() {
			return adSize;
		}

		public void setAdSize(String adSize) {
			this.adSize = adSize;
		}

		@Override
		public String toString() {
			return Objects.toStringHelper(this).add("fid", fid)
					.add("startTime", this.startTime)
					.add("playTime", this.playTime).add("adSize", this.adSize)
					.toString();
		}

	}
}
