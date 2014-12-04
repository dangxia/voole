package com.voole.hobbit2.storm.onlineuser.flex.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
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

	protected String fillInsertSql(ResultSet qs, PreparedStatement ps)
			throws SQLException {
		String sessid = qs.getString("sessid");
		long playBgnTime = qs.getLong(28);
		if (!qs.wasNull()) {// 有影片播放开始信息
			long playAliveTime = qs.getLong(29);
			long playendTime = qs.getLong(30);
			long endTime = Math.max(playAliveTime, playendTime);
			long durationtime = 0;
			if (endTime != 0) {
				durationtime = endTime - playBgnTime;
			}
			ps.setString(1, qs.getString(1));// day
			// sessid
			ps.setString(2, qs.getString(2));
			// stamp
			Long tmp3 = qs.getLong(3);
			if (qs.wasNull()) {
				ps.setNull(3, Types.BIGINT);
			} else {
				ps.setLong(3, tmp3);
			}
			// userip
			Long tmp4 = qs.getLong(4);
			if (qs.wasNull()) {
				ps.setNull(4, Types.BIGINT);
			} else {
				ps.setLong(4, tmp4);
			}
			// datasorce
			Integer tmp5 = qs.getInt(5);
			if (qs.wasNull()) {
				ps.setNull(5, Types.INTEGER);
			} else {
				ps.setInt(5, tmp5);
			}
			// playurl
			ps.setString(6, qs.getString(6));
			// version
			ps.setString(7, qs.getString(7));
			// dim_date_hour
			ps.setString(8, qs.getString(8));
			// dim_isp_id
			Integer tmp9 = qs.getInt(9);
			if (qs.wasNull()) {
				ps.setNull(9, Types.INTEGER);
			} else {
				ps.setInt(9, tmp9);
			}
			// dim_user_uid
			ps.setString(10, qs.getString(10));
			// dim_user_hid
			ps.setString(11, qs.getString(11));
			// dim_oem_id
			Long tmp12 = qs.getLong(12);
			if (qs.wasNull()) {
				ps.setNull(12, Types.BIGINT);
			} else {
				ps.setLong(12, tmp12);
			}
			// dim_area_id
			Integer tmp13 = qs.getInt(13);
			if (qs.wasNull()) {
				ps.setNull(13, Types.INTEGER);
			} else {
				ps.setInt(13, tmp13);
			}
			// dim_area_parentid
			Integer tmp14 = qs.getInt(14);
			if (qs.wasNull()) {
				ps.setNull(14, Types.INTEGER);
			} else {
				ps.setInt(14, tmp14);
			}
			// dim_nettype_id
			Integer tmp15 = qs.getInt(15);
			if (qs.wasNull()) {
				ps.setNull(15, Types.INTEGER);
			} else {
				ps.setInt(15, tmp15);
			}
			// dim_media_fid
			ps.setString(16, qs.getString(16));
			// dim_media_series
			Integer tmp17 = qs.getInt(17);
			if (qs.wasNull()) {
				ps.setNull(17, Types.INTEGER);
			} else {
				ps.setInt(17, tmp17);
			}
			// dim_media_mimeid
			Integer tmp18 = qs.getInt(18);
			if (qs.wasNull()) {
				ps.setNull(18, Types.INTEGER);
			} else {
				ps.setInt(18, tmp18);
			}
			// dim_movie_mid
			Long tmp19 = qs.getLong(19);
			if (qs.wasNull()) {
				ps.setNull(19, Types.BIGINT);
			} else {
				ps.setLong(19, tmp19);
			}
			// dim_cp_id
			Integer tmp20 = qs.getInt(20);
			if (qs.wasNull()) {
				ps.setNull(20, Types.INTEGER);
			} else {
				ps.setInt(20, tmp20);
			}
			// dim_movie_category
			ps.setString(21, qs.getString(21));
			// dim_product_pid
			ps.setString(22, qs.getString(22));
			// dim_product_ptype
			Integer tmp23 = qs.getInt(23);
			if (qs.wasNull()) {
				ps.setNull(23, Types.INTEGER);
			} else {
				ps.setInt(23, tmp23);
			}
			// dim_po_id
			Integer tmp24 = qs.getInt(24);
			if (qs.wasNull()) {
				ps.setNull(24, Types.INTEGER);
			} else {
				ps.setInt(24, tmp24);
			}
			// dim_epg_id
			Long tmp25 = qs.getLong(25);
			if (qs.wasNull()) {
				ps.setNull(25, Types.BIGINT);
			} else {
				ps.setLong(25, tmp25);
			}
			// dim_section_id
			ps.setString(26, qs.getString(26));
			// dim_section_parentid
			ps.setString(27, qs.getString(27));
			// metric_playbgntime
			Long tmp28 = qs.getLong(28);
			if (qs.wasNull()) {
				ps.setNull(28, Types.BIGINT);
			} else {
				ps.setLong(28, tmp28);
			}
			// metric_playalivetime
			Long tmp29 = qs.getLong(29);
			if (qs.wasNull()) {
				ps.setNull(29, Types.BIGINT);
			} else {
				ps.setLong(29, tmp29);
			}
			// metric_playendtime
			Long tmp30 = qs.getLong(30);
			if (qs.wasNull()) {
				ps.setNull(30, Types.BIGINT);
			} else {
				ps.setLong(30, tmp30);
			}
			// metric_durationtime
			ps.setLong(31, durationtime);
			// metric_avgspeed
			Long tmp32 = qs.getLong(32);
			if (qs.wasNull()) {
				ps.setNull(32, Types.BIGINT);
			} else {
				ps.setLong(32, tmp32);
			}
			// metric_isad
			Integer tmp33 = qs.getInt(33);
			if (qs.wasNull()) {
				ps.setNull(33, Types.INTEGER);
			} else {
				ps.setInt(33, tmp33);
			}
			// metric_isrepeatmod
			Integer tmp34 = qs.getInt(34);
			if (qs.wasNull()) {
				ps.setNull(34, Types.INTEGER);
			} else {
				ps.setInt(34, tmp34);
			}
			// metric_status
			Integer tmp35 = qs.getInt(35);
			if (qs.wasNull()) {
				ps.setNull(35, Types.INTEGER);
			} else {
				ps.setInt(35, tmp35);
			}
			// metric_techtype
			Integer tmp36 = qs.getInt(36);
			if (qs.wasNull()) {
				ps.setNull(36, Types.INTEGER);
			} else {
				ps.setInt(36, tmp36);
			}
			// metric_partnerinfo
			ps.setString(37, qs.getString(37));
			// extinfo
			ps.setString(38, qs.getString(38));
			// vssip
			Long tmp39 = qs.getLong(39);
			if (qs.wasNull()) {
				ps.setNull(39, Types.BIGINT);
			} else {
				ps.setLong(39, tmp39);
			}
			// perfip
			Long tmp40 = qs.getLong(40);
			if (qs.wasNull()) {
				ps.setNull(40, Types.BIGINT);
			} else {
				ps.setLong(40, tmp40);
			}
			// bitrate
			Integer tmp41 = qs.getInt(41);
			if (qs.wasNull()) {
				ps.setNull(41, Types.INTEGER);
			} else {
				ps.setInt(41, tmp41);
			}

			ps.addBatch();
		}
		return sessid;
	}

	@Override
	public void sync() {
		String querySql = "";
		querySql += "SELECT  ";
		querySql += "  TO_CHAR ( ";
		querySql += "    CAST( ";
		querySql += "      METRIC_PLAYBGNTIME * 1000 AS TIMESTAMP ";
		querySql += "    ), 'yyyy-MM-dd' ";
		querySql += "  ) AS DAY, sessid, stamp, userip, datasorce, playurl, VERSION, dim_date_hour, dim_isp_id, dim_user_uid, dim_user_hid, dim_oem_id, dim_area_id, dim_area_parentid, dim_nettype_id, dim_media_fid, dim_media_series, dim_media_mimeid, dim_movie_mid, dim_cp_id, dim_movie_category, dim_product_pid, dim_product_ptype, dim_po_id, dim_epg_id, dim_section_id, dim_section_parentid, metric_playbgntime, metric_playalivetime, metric_playendtime, metric_durationtime, metric_avgspeed, metric_isad, metric_isrepeatmod, metric_status, metric_techtype, metric_partnerinfo, extinfo, vssip, perfip, bitrate  ";
		querySql += "FROM ";
		querySql += "  HIVEORDERDETAILRECORD_PHOENIX  ";
		querySql += "WHERE  ";
		querySql += "  CASE ";
		querySql += "    WHEN METRIC_PLAYALIVETIME IS NULL  ";
		querySql += "    OR METRIC_PLAYBGNTIME > METRIC_PLAYALIVETIME  ";
		querySql += "    THEN  ";
		querySql += "    CASE ";
		querySql += "      WHEN METRIC_PLAYENDTIME IS NULL  ";
		querySql += "      OR METRIC_PLAYBGNTIME > METRIC_PLAYENDTIME  ";
		querySql += "      THEN METRIC_PLAYBGNTIME  ";
		querySql += "      ELSE METRIC_PLAYENDTIME  ";
		querySql += "    END  ";
		querySql += "    ELSE  ";
		querySql += "    CASE ";
		querySql += "      WHEN METRIC_PLAYENDTIME IS NULL  ";
		querySql += "      OR METRIC_PLAYALIVETIME > METRIC_PLAYENDTIME  ";
		querySql += "      THEN METRIC_PLAYALIVETIME  ";
		querySql += "      ELSE METRIC_PLAYENDTIME  ";
		querySql += "    END  ";
		querySql += "  END <= CAST(CURRENT_DATE() AS BIGINT) / 1000 - 10800  ";

		String insertSql = "";
		querySql += "UPSERT INTO fact_vod_history ( ";
		querySql += "  DAY, sessid, stamp, userip, datasorce, playurl, VERSION, dim_date_hour, dim_isp_id, dim_user_uid, dim_user_hid, dim_oem_id, dim_area_id, dim_area_parentid, dim_nettype_id, dim_media_fid, dim_media_series, dim_media_mimeid, dim_movie_mid, dim_cp_id, dim_movie_category, dim_product_pid, dim_product_ptype, dim_po_id, dim_epg_id, dim_section_id, dim_section_parentid, metric_playbgntime, metric_playalivetime, metric_playendtime, metric_durationtime, metric_avgspeed, metric_isad, metric_isrepeatmod, metric_status, metric_techtype, metric_partnerinfo, extinfo, vssip, perfip, bitrate ";
		querySql += ")  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

		String deleteSql = "DELETE FROM HIVEORDERDETAILRECORD_PHOENIX WHERE sessid = ? ";
		PreparedStatement queryPs = null;
		ResultSet queryResult = null;
		PreparedStatement insertPs = null;
		PreparedStatement deletePs = null;
		try {
			queryPs = connection.prepareStatement(querySql);
			insertPs = connection.prepareStatement(insertSql);
			deletePs = connection.prepareStatement(deleteSql);

			queryResult = queryPs.executeQuery();
			while (queryResult.next()) {
				String sessid = fillInsertSql(queryResult, insertPs);

				deletePs.setString(1, sessid);
				deletePs.addBatch();
			}
			insertPs.executeBatch();
			deletePs.executeBatch();
			connection.commit();
		} catch (Exception e) {
			Throwables.propagate(e);
		} finally {
			if (queryPs != null) {
				try {
					queryPs.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}
			if (queryResult != null) {
				try {
					queryResult.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}

			if (insertPs != null) {
				try {
					insertPs.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}

			if (deletePs != null) {
				try {
					deletePs.close();
				} catch (SQLException e) {
					Throwables.propagate(e);
				}
			}
		}

	}

}
