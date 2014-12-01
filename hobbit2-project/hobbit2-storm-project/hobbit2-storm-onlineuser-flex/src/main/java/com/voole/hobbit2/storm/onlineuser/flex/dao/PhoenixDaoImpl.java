package com.voole.hobbit2.storm.onlineuser.flex.dao;

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
			connection.setAutoCommit(true);
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
		String sql = "";
		sql += "SELECT  ";
		sql += "  FIRST_VALUE (DIM_OEM_ID) WITHIN ";
		sql += "GROUP ( ";
		sql += "ORDER BY METRIC_PLAYBGNTIME DESC) AS oemid, ";
		sql += "FIRST_VALUE ( ";
		sql += "  CASE ";
		sql += "    WHEN METRIC_AVGSPEED IS NULL  ";
		sql += "    OR METRIC_AVGSPEED = 0  ";
		sql += "    OR BITRATE IS NULL  ";
		sql += "    OR BITRATE * 1024 <= METRIC_AVGSPEED * 8  ";
		sql += "    THEN 0  ";
		sql += "    ELSE 1  ";
		sql += "  END ";
		sql += ") WITHIN ";
		sql += "GROUP ( ";
		sql += "ORDER BY METRIC_PLAYBGNTIME DESC) AS is_low  ";
		sql += "FROM ";
		sql += "  HIVEORDERDETAILRECORD_PHOENIX ";
		sql += "WHERE METRIC_PLAYBGNTIME IS NOT NULL  ";
		sql += "  AND METRIC_PLAYBGNTIME > CAST(CURRENT_DATE() AS BIGINT) / 1000 - 10800  ";
		sql += "  AND  ";
		sql += "  CASE ";
		sql += "    WHEN METRIC_PLAYENDTIME IS NOT NULL  ";
		sql += "    AND METRIC_PLAYENDTIME > METRIC_PLAYBGNTIME  ";
		sql += "    THEN - 1  ";
		sql += "    ELSE  ";
		sql += "    CASE ";
		sql += "      WHEN METRIC_PLAYALIVETIME IS NOT NULL  ";
		sql += "      AND METRIC_PLAYALIVETIME > METRIC_PLAYBGNTIME  ";
		sql += "      THEN METRIC_PLAYALIVETIME  ";
		sql += "      ELSE METRIC_PLAYBGNTIME  ";
		sql += "    END  ";
		sql += "  END >  CAST(CURRENT_DATE() AS BIGINT) / 1000 - 600  ";
		sql += "GROUP BY DIM_USER_HID  ";
		// try {
		// return CharStreams.toString(new InputStreamReader(this.getClass()
		// .getResourceAsStream("queryPhoenixOnlineUserState.sql")));
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// return null;
		return sql;
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

	@Override
	public void sync() {
		String sql = "";
		sql+="UPSERT INTO fact_vod_history ( ";
		sql+="  DAY, sessid, stamp, userip, datasorce, playurl, VERSION, dim_date_hour, dim_isp_id, dim_user_uid, dim_user_hid, dim_oem_id, dim_area_id, dim_area_parentid, dim_nettype_id, dim_media_fid, dim_media_series, dim_media_mimeid, dim_movie_mid, dim_cp_id, dim_movie_category, dim_product_pid, dim_product_ptype, dim_po_id, dim_epg_id, dim_section_id, dim_section_parentid, metric_playbgntime, metric_playalivetime, metric_playendtime, metric_durationtime, metric_avgspeed, metric_isad, metric_isrepeatmod, metric_status, metric_techtype, metric_partnerinfo, extinfo, vssip, perfip, bitrate ";
		sql+=")  ";
		sql+="SELECT  ";
		sql+="  TO_CHAR ( ";
		sql+="    CAST( ";
		sql+="      METRIC_PLAYBGNTIME * 1000 AS TIMESTAMP ";
		sql+="    ), 'yyyy-MM-dd' ";
		sql+="  ) AS DATE, sessid, stamp, userip, datasorce, playurl, VERSION, dim_date_hour, dim_isp_id, dim_user_uid, dim_user_hid, dim_oem_id, dim_area_id, dim_area_parentid, dim_nettype_id, dim_media_fid, dim_media_series, dim_media_mimeid, dim_movie_mid, dim_cp_id, dim_movie_category, dim_product_pid, dim_product_ptype, dim_po_id, dim_epg_id, dim_section_id, dim_section_parentid, metric_playbgntime, metric_playalivetime, metric_playendtime, metric_durationtime, metric_avgspeed, metric_isad, metric_isrepeatmod, metric_status, metric_techtype, metric_partnerinfo, extinfo, vssip, perfip, bitrate  ";
		sql+="FROM ";
		sql+="  HIVEORDERDETAILRECORD_PHOENIX  ";
		sql+="WHERE METRIC_PLAYBGNTIME IS NOT NULL  ";
		sql+="  AND METRIC_PLAYBGNTIME <= CAST(CURRENT_DATE() AS BIGINT) / 1000 - 10800  ";

		String deleteOldSql = "";
		deleteOldSql += "DELETE  ";
		deleteOldSql += "FROM ";
		deleteOldSql += "  HIVEORDERDETAILRECORD_PHOENIX  ";
		deleteOldSql += "WHERE  ";
		deleteOldSql += "  CASE ";
		deleteOldSql += "    WHEN METRIC_PLAYALIVETIME IS NULL  ";
		deleteOldSql += "    OR METRIC_PLAYBGNTIME > METRIC_PLAYALIVETIME  ";
		deleteOldSql += "    THEN  ";
		deleteOldSql += "    CASE ";
		deleteOldSql += "      WHEN METRIC_PLAYENDTIME IS NULL  ";
		deleteOldSql += "      OR METRIC_PLAYBGNTIME > METRIC_PLAYENDTIME  ";
		deleteOldSql += "      THEN METRIC_PLAYBGNTIME  ";
		deleteOldSql += "      ELSE METRIC_PLAYENDTIME  ";
		deleteOldSql += "    END  ";
		deleteOldSql += "    ELSE  ";
		deleteOldSql += "    CASE ";
		deleteOldSql += "      WHEN METRIC_PLAYENDTIME IS NULL  ";
		deleteOldSql += "      OR METRIC_PLAYALIVETIME > METRIC_PLAYENDTIME  ";
		deleteOldSql += "      THEN METRIC_PLAYALIVETIME  ";
		deleteOldSql += "      ELSE METRIC_PLAYENDTIME  ";
		deleteOldSql += "    END  ";
		deleteOldSql += "  END <= CAST(CURRENT_DATE() AS BIGINT) / 1000 - 10800  ";
		PreparedStatement ps = null;
		PreparedStatement deleteps = null;
		try {
			ps = connection.prepareStatement(sql);
			ps.execute();
			
			log.info("copy real time data finished");

			deleteps = connection.prepareStatement(deleteOldSql);
			deleteps.execute();
			
			log.info("delete time out data finished");

		} catch (SQLException e) {
			Throwables.propagate(e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
					ps = null;
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}
			if (deleteps != null) {
				try {
					deleteps.close();
					deleteps = null;
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}
		}

	}

}
