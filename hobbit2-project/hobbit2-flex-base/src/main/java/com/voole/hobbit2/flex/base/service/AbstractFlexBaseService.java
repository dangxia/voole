package com.voole.hobbit2.flex.base.service;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import com.voole.hobbit2.flex.base.dao.FlexBaseDao;
import com.voole.hobbit2.flex.base.model.ChildTrait;
import com.voole.hobbit2.flex.base.model.FlexBaseState;
import com.voole.hobbit2.flex.base.model.ParentTrait;
import com.voole.hobbit2.flex.base.model.curve.FlexChartData;
import com.voole.hobbit2.flex.base.model.curve.FlexCurveStampState;
import com.voole.hobbit2.flex.base.model.grid.FlexGridChildState;
import com.voole.hobbit2.flex.base.model.grid.FlexGridHasState;
import com.voole.hobbit2.flex.base.model.grid.FlexGridParentState;

public abstract class AbstractFlexBaseService<State extends FlexBaseState, GridChildState extends FlexGridChildState<State>, GridParentState extends FlexGridParentState<State, GridChildState>, CurveStampState extends FlexCurveStampState<State>, PT extends ParentTrait, CT extends ChildTrait>
		implements
		FlexBaseService<State, GridChildState, GridParentState, CurveStampState, PT, CT> {
	private FlexBaseDao<State, GridChildState, GridParentState, CurveStampState, PT, CT> flexDao;
	private Comparator<FlexGridHasState<State>> comparator;
	private int dataDays;

	@Override
	public List<GridParentState> getGridData(String spid) {
		List<String> spids = getSpids(spid);
		if (spids != null) {
			spid = null;
		}
		List<GridParentState> empty = getEmptyGridData(spid, spids);
		List<GridParentState> list = null;
		if (spids != null && spids.size() == 0) {
			list = new ArrayList<GridParentState>();
		} else {
			list = getFlexDao().getGridData(spid, spids);
		}
		if (list == null || list.size() == 0) {
			return empty;
		}
		empty = fillEmpty(empty, list);
		for (FlexGridParentState<State, ? extends FlexGridChildState<State>> item : empty) {
			Collections.sort(item.getChildren(), getComparator());
		}
		Collections.sort(empty, getComparator());
		return empty;
	}

	protected abstract List<GridParentState> fillEmpty(
			List<GridParentState> empty, List<GridParentState> list);

	protected abstract List<GridParentState> getEmptyGridData(String spid,
			List<String> spids);

	protected List<String> getSpids(String spid) {
		// Integer nettype = null;
		// if (spid != null && spid.startsWith("nettype:")) {
		// nettype = Integer.parseInt(spid.replace("nettype:", ""));
		// }
		// if (nettype == null) {
		// return null;
		// }
		// List<String> spids = new ArrayList<String>();
		// List<SpInfo> spInfos = spInfoCacher.getSpInfos(nettype);
		// for (SpInfo spInfo : spInfos) {
		// spids.add(spInfo.getSpid());
		// }
		// return spids;
		return null;
	}

	@Override
	public FlexChartData<CurveStampState> getTotalFlexChartData(String spid,
			Date stamp) {
		List<String> spids = getSpids(spid);
		if (spids != null) {
			spid = null;
		}
		List<CurveStampState> list = getFlexDao().getTotalCurveStampStates(
				spid, spids, getStamp(stamp));
		FlexChartData<CurveStampState> chartData = new FlexChartData<CurveStampState>();
		chartData.setData(list);
		chartData.setName(getTotalName());
		chartData.calcMaxDate();
		return chartData;
	}

	protected abstract String getTotalName();

	@Override
	public FlexChartData<CurveStampState> getParentFlexChartData(
			PT parentTrait, Date stamp) {
		List<CurveStampState> list = getFlexDao().getParentCurveStampStates(
				parentTrait, getStamp(stamp));
		FlexChartData<CurveStampState> chartData = new FlexChartData<CurveStampState>();
		chartData.setData(list);
		chartData.setName(getName(parentTrait));
		chartData.calcMaxDate();
		return chartData;
	}

	protected abstract String getName(PT parentTrait);

	protected abstract String getName(CT childTrait);

	@Override
	public FlexChartData<CurveStampState> getChildFlexChartData(CT childTrait,
			Date stamp) {
		List<CurveStampState> list = getFlexDao().getChildCurveStampStates(
				childTrait, getStamp(stamp));
		FlexChartData<CurveStampState> chartData = new FlexChartData<CurveStampState>();
		chartData.setData(list);
		chartData.setName(getName(childTrait));
		chartData.calcMaxDate();
		return chartData;
	}

	protected Date getStamp(Date stamp) {
		if (stamp == null || stamp.getTime() == 0l) {
			Calendar c = Calendar.getInstance();
			c.add(Calendar.DAY_OF_MONTH, -1 * getDataDays());
			return c.getTime();
		}
		return stamp;
	}

	public FlexBaseDao<State, GridChildState, GridParentState, CurveStampState, PT, CT> getFlexDao() {
		return flexDao;
	}

	public void setFlexDao(
			FlexBaseDao<State, GridChildState, GridParentState, CurveStampState, PT, CT> flexDao) {
		this.flexDao = flexDao;
	}

	public Comparator<FlexGridHasState<State>> getComparator() {
		return comparator;
	}

	public void setComparator(Comparator<FlexGridHasState<State>> comparator) {
		this.comparator = comparator;
	}

	public int getDataDays() {
		return dataDays;
	}

	public void setDataDays(int dataDays) {
		this.dataDays = dataDays;
	}

}
