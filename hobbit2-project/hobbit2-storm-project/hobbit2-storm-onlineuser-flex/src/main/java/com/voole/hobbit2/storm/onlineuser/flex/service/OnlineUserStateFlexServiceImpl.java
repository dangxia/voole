package com.voole.hobbit2.storm.onlineuser.flex.service;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.voole.hobbit2.cache.OemInfoCache;
import com.voole.hobbit2.cache.SpInfoCache;
import com.voole.hobbit2.cache.entity.OemInfo;
import com.voole.hobbit2.cache.entity.SpInfo;
import com.voole.hobbit2.flex.base.model.curve.FlexCurveStampState;
import com.voole.hobbit2.flex.base.model.grid.FlexGridHasState;
import com.voole.hobbit2.flex.base.service.AbstractFlexBaseService;
import com.voole.hobbit2.storm.onlineuser.flex.model.OnlineState;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.OemGridOnlineState;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.OemTrait;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.SpGridOnlineState;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.SpTrait;

public class OnlineUserStateFlexServiceImpl
		extends
		AbstractFlexBaseService<OnlineState, OemGridOnlineState, SpGridOnlineState, FlexCurveStampState<OnlineState>, SpTrait, OemTrait>
		implements OnlineUserStateFlexService {

	private OemInfoCache oemInfoCache;
	private SpInfoCache spInfoCache;

	public OnlineUserStateFlexServiceImpl() {
		setDataDays(4);
		setComparator(new Comparator<FlexGridHasState<OnlineState>>() {
			@Override
			public int compare(FlexGridHasState<OnlineState> o1,
					FlexGridHasState<OnlineState> o2) {
				return (int) (o2.getState().getUserNum() - o1.getState()
						.getUserNum());
			}
		});
	}

	@Override
	public List<SpGridOnlineState> getGridData(String spid) {
		List<SpGridOnlineState> list = getFlexDao().getGridData(spid, null);
		if (list == null || list.size() == 0) {
			return list;
		}
		for (SpGridOnlineState item : list) {
			item.setName(getName(item.getSpid()));
			for (OemGridOnlineState oemItem : item.getChildren()) {
				oemItem.setName(getName(oemItem.getOemid()));
			}

			Collections.sort(item.getChildren(), getComparator());
		}
		Collections.sort(list, getComparator());
		return list;
	}

	@Override
	protected List<SpGridOnlineState> fillEmpty(List<SpGridOnlineState> empty,
			List<SpGridOnlineState> list) {
		return list;
	}

	@Override
	protected List<SpGridOnlineState> getEmptyGridData(String arg0,
			List<String> arg1) {
		return null;
	}

	private String getName(String spid) {
		Optional<SpInfo> spInfoOpt = null;
		try {
			spInfoOpt = spInfoCache.getSpInfo(spid);
		} catch (Exception e) {
			Throwables.propagate(e);
		}
		if (spInfoOpt.isPresent()) {
			return spInfoOpt.get().getShortname();
		} else {
			return "未知";
		}
	}

	private String getName(Long oemid) {
		Optional<OemInfo> oemInfoOpt = null;
		try {
			oemInfoOpt = oemInfoCache.getOemInfo(oemid);
		} catch (Exception e) {
			Throwables.propagate(e);
		}
		if (!oemInfoOpt.isPresent()) {
			return "未知";
		}
		return oemInfoOpt.get().getOemid() + "—"
				+ oemInfoOpt.get().getShortname();
	}

	@Override
	protected String getName(SpTrait parentTrait) {
		return getName(parentTrait.getSpid()) + "—在线用户走势图";
	}

	@Override
	protected String getName(OemTrait childTrait) {
		return getName(childTrait.getOemid()) + "—在线用户走势图";
	}

	@Override
	protected String getTotalName() {
		return "全国—在线用户走势图";
	}

	@Override
	protected List<String> getSpids(String spid) {
		return null;
	}

	public OemInfoCache getOemInfoCache() {
		return oemInfoCache;
	}

	public void setOemInfoCache(OemInfoCache oemInfoCache) {
		this.oemInfoCache = oemInfoCache;
	}

	public SpInfoCache getSpInfoCache() {
		return spInfoCache;
	}

	public void setSpInfoCache(SpInfoCache spInfoCache) {
		this.spInfoCache = spInfoCache;
	}

}
