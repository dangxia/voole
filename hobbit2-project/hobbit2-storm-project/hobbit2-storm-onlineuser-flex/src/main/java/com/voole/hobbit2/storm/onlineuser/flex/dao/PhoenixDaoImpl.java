package com.voole.hobbit2.storm.onlineuser.flex.dao;

import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.voole.hobbit2.flex.base.model.curve.FlexCurveStampState;
import com.voole.hobbit2.storm.onlineuser.flex.OnlineUserConfigs;
import com.voole.hobbit2.storm.onlineuser.flex.model.OnlineState;
import com.voole.hobbit2.storm.onlineuser.flex.model.calc.CalcOemOnlineUserState;
import com.voole.hobbit2.storm.onlineuser.flex.model.calc.CalcSpOnlineUserState;
import com.voole.hobbit2.storm.onlineuser.flex.model.calc.PhoenixOnlineUserState;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.OemGridOnlineState;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.OemTrait;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.SpGridOnlineState;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.SpTrait;

public class PhoenixDaoImpl implements DisposableBean, PhoenixDao {
	private static final Logger log = LoggerFactory
			.getLogger(PhoenixDaoImpl.class);
	private Connection connection;

	public PhoenixDaoImpl() {
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver", true,
					PhoenixDaoImpl.class.getClassLoader());
			connection = DriverManager
					.getConnection("jdbc:phoenix:data-slave2.voole.com,data-slave3.voole.com,data-slave4.voole.com");
			connection.setAutoCommit(false);
		} catch (Exception e) {
			log.warn("init PhoenixDao error:", e);
			Throwables.propagate(e);
		}
	}

	public static void main(String[] args) {
		PhoenixDaoImpl impl = new PhoenixDaoImpl();
		List<PhoenixOnlineUserState> list = impl.queryOnlineUserState();
		for (PhoenixOnlineUserState phoenixOnlineUserState : list) {
			System.out.println(phoenixOnlineUserState);
		}
	}

	private String getQueryPhoenixOnlineUserStateSql() {
		try {
			return CharStreams
					.toString(new InputStreamReader(
							PhoenixDaoImpl.class
									.getResourceAsStream("queryPhoenixOnlineUserState.sql")));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void destroy() throws Exception {
		connection.close();
	}

	@Override
	public List<FlexCurveStampState<OnlineState>> getTotalCurveStampStates(
			String spid, List<String> spids, Date stamp) {
		List<FlexCurveStampState<OnlineState>> list = new ArrayList<FlexCurveStampState<OnlineState>>();
		if (spid == null) {
			spid = OnlineUserConfigs.GLOBAL_SPID;
		}
		long ts = stamp.getTime() / 1000;
		String sql = "select stamp,total,low from sp_online_state where stamp >= ? and spid = ? ";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = connection.prepareStatement(sql);
			ps.setLong(1, ts);
			ps.setString(2, spid);

			rs = ps.executeQuery();
			while (rs.next()) {
				list.add(newCurveStampState(rs));
			}
		} catch (SQLException e) {
			Throwables.propagate(e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}
		}
		return list;
	}

	@Override
	public List<FlexCurveStampState<OnlineState>> getParentCurveStampStates(
			SpTrait spTrait, Date stamp) {
		return getTotalCurveStampStates(spTrait.getSpid(), null, stamp);
	}

	@Override
	public List<FlexCurveStampState<OnlineState>> getChildCurveStampStates(
			OemTrait oemTrait, Date stamp) {
		long oemid = oemTrait.getOemid();
		List<FlexCurveStampState<OnlineState>> list = new ArrayList<FlexCurveStampState<OnlineState>>();
		long ts = stamp.getTime() / 1000;
		String sql = "select stamp,total,low from oem_online_state where stamp >= ? and oemid = ? ";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = connection.prepareStatement(sql);
			ps.setLong(1, ts);
			ps.setLong(2, oemid);

			rs = ps.executeQuery();
			while (rs.next()) {
				list.add(newCurveStampState(rs));
			}
		} catch (SQLException e) {
			Throwables.propagate(e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}
		}
		return list;
	}

	@Override
	public List<SpGridOnlineState> getGridData(String spid, List<String> spids) {
		List<SpGridOnlineState> spGridOnlineStates = querySpGridOnlineState(spid);
		Map<String, SpGridOnlineState> cache = new HashMap<String, SpGridOnlineState>();
		for (SpGridOnlineState spGridOnlineState : spGridOnlineStates) {
			cache.put(spGridOnlineState.getSpid(), spGridOnlineState);
		}

		List<OemGridOnlineState> oemGridOnlineStates = queryOemGridOnlineState(spid);
		for (OemGridOnlineState oemGridOnlineState : oemGridOnlineStates) {
			String oemSpid = oemGridOnlineState.getSpid();
			if (cache.containsKey(oemSpid)) {
				cache.get(oemSpid).getChildren().add(oemGridOnlineState);
			}
		}
		return spGridOnlineStates;
	}

	private List<SpGridOnlineState> querySpGridOnlineState(String spid) {
		List<SpGridOnlineState> list = new ArrayList<SpGridOnlineState>();
		String sql = " select spid,total,low from sp_online_state_snapshot where 1= 1 ";
		if (spid == null) {
			sql += " and spid <> '" + OnlineUserConfigs.GLOBAL_SPID + "' ";
		} else {
			sql += " and spid = '" + spid + "' ";
		}
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				SpGridOnlineState item = new SpGridOnlineState();
				item.setSpid(rs.getString("spid"));
				item.setState(newOnlineState(rs));

				list.add(item);
			}
		} catch (SQLException e) {
			Throwables.propagate(e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}
		}
		return list;

	}

	private List<OemGridOnlineState> queryOemGridOnlineState(String spid) {
		List<OemGridOnlineState> list = new ArrayList<OemGridOnlineState>();
		String sql = " select oemid,spid,total,low from oem_online_state_snapshot where 1= 1 ";
		if (spid == null) {
			sql += " and spid <> '" + OnlineUserConfigs.GLOBAL_SPID + "' ";
		} else {
			sql += " and spid = '" + spid + "' ";
		}
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				OemGridOnlineState item = new OemGridOnlineState();
				item.setSpid(rs.getString("spid"));
				item.setOemid(rs.getLong("oemid"));
				item.setState(newOnlineState(rs));

				list.add(item);
			}
		} catch (SQLException e) {
			Throwables.propagate(e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}
		}
		return list;
	}

	private FlexCurveStampState<OnlineState> newCurveStampState(ResultSet rs)
			throws SQLException {
		OnlineState state = newOnlineState(rs);

		FlexCurveStampState<OnlineState> item = new FlexCurveStampState<OnlineState>();
		item.setState(state);
		item.setStamp(new Date(rs.getLong("stamp") * 1000));

		return item;
	}

	private OnlineState newOnlineState(ResultSet rs) throws SQLException {
		OnlineState state = new OnlineState();
		state.setLowspeedUserNum(rs.getLong("low"));
		state.setUserNum(rs.getLong("total"));
		state.calcPercentage();
		return state;
	}

	@Override
	public List<PhoenixOnlineUserState> queryOnlineUserState() {
		PreparedStatement statement = null;

		Map<Long, PhoenixOnlineUserState> result = new HashMap<Long, PhoenixOnlineUserState>();
		try {
			statement = connection
					.prepareStatement(getQueryPhoenixOnlineUserStateSql());
			ResultSet rset = statement.executeQuery();

			while (rset.next()) {
				Long oemid = rset.getLong("oemid");
				boolean isLow = rset.getInt("is_low") == 1 ? true : false;

				PhoenixOnlineUserState state = null;
				if (result.containsKey(oemid)) {
					state = result.get(oemid);
				} else {
					state = new PhoenixOnlineUserState();
					state.setOemid(oemid);
					result.put(oemid, state);
				}
				state.setTotal(state.getTotal() + 1);
				if (isLow) {
					state.setLow(state.getLow() + 1);
				}

			}
		} catch (SQLException e) {
			Throwables.propagate(e);
		} finally {
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}
		}
		return Lists.newArrayList(result.values());

	}

	@Override
	public void updateOnlineUserState(
			List<CalcOemOnlineUserState> oemOnlineUserStates,
			List<CalcSpOnlineUserState> spOnlineUserStates) {
		updateSpState(spOnlineUserStates);
		updateOemState(oemOnlineUserStates);

	}

	private void updateSpState(List<CalcSpOnlineUserState> spOnlineUserStates) {
		PreparedStatement psHistory = null;
		PreparedStatement psSnapSpot = null;
		try {
			psHistory = connection
					.prepareStatement("UPSERT INTO sp_online_state (stamp,spid,total,low) values (?,?,?,?) ");
			psSnapSpot = connection
					.prepareStatement("UPSERT INTO sp_online_state_snapshot (spid,total,low) values (?,?,?) ");
			for (CalcSpOnlineUserState item : spOnlineUserStates) {
				psHistory.setLong(1, item.getStamp());
				psHistory.setString(2, item.getSpid());
				psHistory.setLong(3, item.getTotal());
				psHistory.setLong(4, item.getLow());
				psHistory.addBatch();

				psSnapSpot.setString(1, item.getSpid());
				psSnapSpot.setLong(2, item.getTotal());
				psSnapSpot.setLong(3, item.getLow());
				psSnapSpot.addBatch();
			}

			psHistory.executeBatch();

			connection.createStatement().execute(
					"delete from sp_online_state_snapshot");
			connection.commit();

			psSnapSpot.executeBatch();
			connection.commit();

		} catch (SQLException e) {
			Throwables.propagate(e);
		} finally {
			if (psHistory != null) {
				try {
					psHistory.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}

			if (psSnapSpot != null) {
				try {
					psSnapSpot.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}

		}
	}

	private void updateOemState(List<CalcOemOnlineUserState> oemOnlineUserStates) {
		PreparedStatement psHistory = null;
		PreparedStatement psSnapSpot = null;
		try {
			psHistory = connection
					.prepareStatement("UPSERT INTO oem_online_state (stamp,spid,total,low,oemid) values (?,?,?,?,?) ");
			psSnapSpot = connection
					.prepareStatement("UPSERT INTO oem_online_state_snapshot (spid,total,low,oemid) values (?,?,?,?) ");
			for (CalcOemOnlineUserState item : oemOnlineUserStates) {
				psHistory.setLong(1, item.getStamp());
				psHistory.setString(2, item.getSpid());
				psHistory.setLong(3, item.getTotal());
				psHistory.setLong(4, item.getLow());
				psHistory.setLong(5, item.getOemid());
				psHistory.addBatch();

				psSnapSpot.setString(1, item.getSpid());
				psSnapSpot.setLong(2, item.getTotal());
				psSnapSpot.setLong(3, item.getLow());
				psSnapSpot.setLong(4, item.getOemid());
				psSnapSpot.addBatch();
			}

			psHistory.executeBatch();

			connection.createStatement().execute(
					"delete from oem_online_state_snapshot");
			connection.commit();

			psSnapSpot.executeBatch();
			connection.commit();

		} catch (SQLException e) {
			Throwables.propagate(e);
		} finally {
			if (psHistory != null) {
				try {
					psHistory.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}

			if (psSnapSpot != null) {
				try {
					psSnapSpot.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}

		}
	}

}
