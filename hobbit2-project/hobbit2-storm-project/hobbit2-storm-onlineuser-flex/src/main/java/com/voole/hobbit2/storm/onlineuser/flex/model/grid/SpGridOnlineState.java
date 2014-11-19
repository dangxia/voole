package com.voole.hobbit2.storm.onlineuser.flex.model.grid;

import com.google.common.base.Objects;
import com.voole.hobbit2.flex.base.model.grid.FlexGridParentState;
import com.voole.hobbit2.storm.onlineuser.flex.model.OnlineState;

public class SpGridOnlineState extends
		FlexGridParentState<OnlineState, OemGridOnlineState> {
	private static final long serialVersionUID = -6081827543591053057L;
	private String spid;

	public String getSpid() {
		return spid;
	}

	public void setSpid(String spid) {
		this.spid = spid;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("spid", spid)
				.add("state", getState()).add("children", getChildren())
				.add("name", getName()).toString();
	}

}
