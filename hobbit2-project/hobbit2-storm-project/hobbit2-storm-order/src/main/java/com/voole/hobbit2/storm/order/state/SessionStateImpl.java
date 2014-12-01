/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.state;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.google.common.base.Throwables;
import com.voole.dungbeetle.order.record.avro.HiveOrderDetailRecord;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV2;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV3;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class SessionStateImpl implements SessionState {
	private static final Logger log = LoggerFactory
			.getLogger(SessionState.class);
	private static final String UPDATE_INSERT_SQL_BGN = "UPSERT INTO HiveOrderDetailRecord_phoenix(sessid,stamp,userip,datasorce,playurl,version,dim_date_hour,dim_isp_id,dim_user_uid,dim_user_hid,dim_oem_id,dim_area_id,dim_area_parentid,dim_nettype_id,dim_media_fid,dim_media_series,dim_media_mimeid,dim_movie_mid,dim_cp_id,dim_movie_category,dim_product_pid,dim_product_ptype,dim_po_id,dim_epg_id,dim_section_id,dim_section_parentid,metric_playbgntime,metric_durationtime,metric_isad,metric_isrepeatmod,metric_status,metric_techtype,metric_partnerinfo,extinfo,vssip,perfip,bitrate) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
	private static final String UPDATE_INSERT_SQL_BGN_BS = "UPSERT INTO HiveOrderDetailRecord_phoenix(sessid,stamp,userip,datasorce,playurl,version,dim_date_hour,dim_isp_id,dim_user_uid,dim_user_hid,dim_oem_id,dim_area_id,dim_area_parentid,dim_nettype_id,dim_media_fid,dim_media_series,dim_media_mimeid,dim_movie_mid,dim_cp_id,dim_movie_category,dim_product_pid,dim_product_ptype,dim_po_id,dim_epg_id,dim_section_id,dim_section_parentid,metric_playbgntime,metric_durationtime,metric_isad,metric_isrepeatmod,metric_status,metric_techtype,metric_partnerinfo,extinfo,vssip,perfip,bitrate,metric_playalivetime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
	private static final String UPDATE_INSERT_SQL_ALIVE = "UPSERT INTO HiveOrderDetailRecord_phoenix(sessid,metric_playalivetime,metric_avgspeed) VALUES (?,?,?) ";
	private static final String UPDATE_INSERT_SQL_END = "UPSERT INTO HiveOrderDetailRecord_phoenix(sessid,metric_playendtime,metric_avgspeed) VALUES (?,?,?) ";
	private Connection connection;

	public SessionStateImpl() {
		try {
			connection = DriverManager
					.getConnection("jdbc:phoenix:data-slave2.voole.com,data-slave3.voole.com,data-slave4.voole.com");
		} catch (Exception e) {
			log.warn("init SessionStateImpl error:", e);
			Throwables.propagate(e);
		}
	}

	@Override
	public void beginCommit(Long arg0) {
	}

	@Override
	public void commit(Long arg0) {
	}

	@Override
	public void close() {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void update(List<SpecificRecordBase> data) {
		if (data.size() == 0) {
			return;
		}
		PreparedStatement bgnPs = null;
		long bgnSize = 0;

		PreparedStatement alivePs = null;
		long aliveSize = 0;

		PreparedStatement endPs = null;
		long endSize = 0;

		PreparedStatement bgnBsPs = null;
		try {
			long total = 0l;
			long start = System.currentTimeMillis();
			bgnPs = connection.prepareStatement(UPDATE_INSERT_SQL_BGN);
			alivePs = connection.prepareStatement(UPDATE_INSERT_SQL_ALIVE);
			endPs = connection.prepareStatement(UPDATE_INSERT_SQL_END);

			bgnBsPs = connection.prepareStatement(UPDATE_INSERT_SQL_BGN_BS);

			for (SpecificRecordBase specificRecordBase : data) {
				if (specificRecordBase == null) {
					log.warn("record is empty!");
					continue;
				}
				total++;
				if (specificRecordBase instanceof HiveOrderDetailRecord) {
					HiveOrderDetailRecord record = (HiveOrderDetailRecord) specificRecordBase;
					if (record.getMetricPlayalivetime() == null) {
						noAlive(record, bgnPs);
						bgnPs.addBatch();
					} else {
						withAlive(record, bgnBsPs);
						bgnBsPs.addBatch();
					}
					bgnSize++;
				} else if (specificRecordBase instanceof OrderPlayAliveReqV2) {
					OrderPlayAliveReqV2 record = (OrderPlayAliveReqV2) specificRecordBase;

					alivePs.setString(1, String.valueOf(record.getSessID()));
					if (record.getAliveTick() == null) {
						alivePs.setNull(2, Types.BIGINT);
					} else {
						alivePs.setLong(2, record.getAliveTick());
					}
					if (record.getSessAvgSpeed() == null) {
						alivePs.setNull(3, Types.BIGINT);
					} else {
						alivePs.setLong(3, record.getSessAvgSpeed());
					}

					alivePs.addBatch();
					aliveSize++;
				} else if (specificRecordBase instanceof OrderPlayAliveReqV3) {
					OrderPlayAliveReqV3 record = (OrderPlayAliveReqV3) specificRecordBase;
					alivePs.setString(1, String.valueOf(record.getSessID()));
					if (record.getAliveTick() == null) {
						alivePs.setNull(2, Types.BIGINT);
					} else {
						alivePs.setLong(2, record.getAliveTick());
					}
					if (record.getSessAvgSpeed() == null) {
						alivePs.setNull(3, Types.BIGINT);
					} else {
						alivePs.setLong(3, record.getSessAvgSpeed());
					}

					alivePs.addBatch();
					aliveSize++;
				} else if (specificRecordBase instanceof OrderPlayEndReqV2) {
					OrderPlayEndReqV2 record = (OrderPlayEndReqV2) specificRecordBase;

					endPs.setString(1, String.valueOf(record.getSessID()));
					if (record.getEndTick() == null) {
						endPs.setNull(2, Types.BIGINT);
					} else {
						endPs.setLong(2, record.getEndTick());
					}
					if (record.getSessAvgSpeed() == null) {
						endPs.setNull(3, Types.BIGINT);
					} else {
						endPs.setLong(3, record.getSessAvgSpeed());
					}

					endPs.addBatch();
					endSize++;
				} else if (specificRecordBase instanceof OrderPlayEndReqV3) {
					OrderPlayEndReqV3 record = (OrderPlayEndReqV3) specificRecordBase;

					endPs.setString(1, String.valueOf(record.getSessID()));
					if (record.getEndTick() == null) {
						endPs.setNull(2, Types.BIGINT);
					} else {
						endPs.setLong(2, record.getEndTick());
					}
					if (record.getSessAvgSpeed() == null) {
						endPs.setNull(3, Types.BIGINT);
					} else {
						endPs.setLong(3, record.getSessAvgSpeed());
					}

					endPs.addBatch();
					endSize++;
				} else {
					log.warn("Don't support record,"
							+ specificRecordBase.getClass().getName());
				}
			}

			bgnPs.executeBatch();
			bgnBsPs.executeBatch();
			alivePs.executeBatch();
			endPs.executeBatch();

			connection.commit();

			log.info("session size:" + total + ",bgnSize:" + bgnSize
					+ ",aliveSize:" + aliveSize + ",endSize:" + endSize
					+ ",used time:"
					+ ((System.currentTimeMillis() - start) / 1000));

		} catch (Exception e) {
			log.warn("insert hbase failed", e);
		} finally {
			if (bgnPs != null) {
				try {
					bgnPs.close();
				} catch (SQLException e) {
					log.warn("bgnPs close error");
				}
			}

			if (alivePs != null) {
				try {
					alivePs.close();
				} catch (SQLException e) {
					log.warn("alivePs close error");
				}
			}

			if (endPs != null) {
				try {
					endPs.close();
				} catch (SQLException e) {
					log.warn("endPs close error");
				}
			}
			if (bgnBsPs != null) {
				try {
					bgnBsPs.close();
				} catch (SQLException e) {
					log.warn("endPs close error");
				}
			}
		}
	}

	protected void noAlive(HiveOrderDetailRecord record, PreparedStatement bgnPs)
			throws SQLException {
		bgnPs.setString(1, String.valueOf(record.getSessid()));
		if (record.getStamp() == null) {
			bgnPs.setNull(2, Types.BIGINT);
		} else {
			bgnPs.setLong(2, record.getStamp());
		}
		if (record.getUserip() == null) {
			bgnPs.setNull(3, Types.BIGINT);
		} else {
			bgnPs.setLong(3, record.getUserip());
		}
		if (record.getDatasorce() == null) {
			bgnPs.setNull(4, Types.INTEGER);
		} else {
			bgnPs.setInt(4, record.getDatasorce());
		}
		bgnPs.setString(5, String.valueOf(record.getPlayurl()));
		bgnPs.setString(6, String.valueOf(record.getVersion()));
		bgnPs.setString(7, String.valueOf(record.getDimDateHour()));
		if (record.getDimIspId() == null) {
			bgnPs.setNull(8, Types.INTEGER);
		} else {
			bgnPs.setInt(8, record.getDimIspId());
		}
		bgnPs.setString(9, String.valueOf(record.getDimUserUid()));
		bgnPs.setString(10, String.valueOf(record.getDimUserHid()));
		if (record.getDimOemId() == null) {
//			bgnPs.setNull(11, Types.BIGINT);
			bgnPs.setLong(11, -1l);
		} else {
			bgnPs.setLong(11, record.getDimOemId());
		}
		if (record.getDimAreaId() == null) {
			bgnPs.setNull(12, Types.INTEGER);
		} else {
			bgnPs.setInt(12, record.getDimAreaId());
		}
		if (record.getDimAreaParentid() == null) {
			bgnPs.setNull(13, Types.INTEGER);
		} else {
			bgnPs.setInt(13, record.getDimAreaParentid());
		}
		if (record.getDimNettypeId() == null) {
			bgnPs.setNull(14, Types.INTEGER);
		} else {
			bgnPs.setInt(14, record.getDimNettypeId());
		}
		bgnPs.setString(15, String.valueOf(record.getDimMediaFid()));
		if (record.getDimMediaSeries() == null) {
			bgnPs.setNull(16, Types.INTEGER);
		} else {
			bgnPs.setInt(16, record.getDimMediaSeries());
		}
		if (record.getDimMediaMimeid() == null) {
			bgnPs.setNull(17, Types.INTEGER);
		} else {
			bgnPs.setInt(17, record.getDimMediaMimeid());
		}
		if (record.getDimMovieMid() == null) {
			bgnPs.setNull(18, Types.BIGINT);
		} else {
			bgnPs.setLong(18, record.getDimMovieMid());
		}
		if (record.getDimCpId() == null) {
			bgnPs.setNull(19, Types.INTEGER);
		} else {
			bgnPs.setInt(19, record.getDimCpId());
		}
		bgnPs.setString(20, String.valueOf(record.getDimMovieCategory()));
		bgnPs.setString(21, String.valueOf(record.getDimProductPid()));
		if (record.getDimProductPtype() == null) {
			bgnPs.setNull(22, Types.INTEGER);
		} else {
			bgnPs.setInt(22, record.getDimProductPtype());
		}
		if (record.getDimPoId() == null) {
			bgnPs.setNull(23, Types.INTEGER);
		} else {
			bgnPs.setInt(23, record.getDimPoId());
		}
		if (record.getDimEpgId() == null) {
			bgnPs.setNull(24, Types.BIGINT);
		} else {
			bgnPs.setLong(24, record.getDimEpgId());
		}
		bgnPs.setString(25, String.valueOf(record.getDimSectionId()));
		bgnPs.setString(26, String.valueOf(record.getDimSectionParentid()));
		if (record.getMetricPlaybgntime() == null) {
			bgnPs.setNull(27, Types.BIGINT);
		} else {
			bgnPs.setLong(27, record.getMetricPlaybgntime());
		}
		if (record.getMetricDurationtime() == null) {
			bgnPs.setNull(28, Types.BIGINT);
		} else {
			bgnPs.setLong(28, record.getMetricDurationtime());
		}
		if (record.getMetricIsad() == null) {
			bgnPs.setNull(29, Types.INTEGER);
		} else {
			bgnPs.setInt(29, record.getMetricIsad());
		}
		if (record.getMetricIsrepeatmod() == null) {
			bgnPs.setNull(30, Types.INTEGER);
		} else {
			bgnPs.setInt(30, record.getMetricIsrepeatmod());
		}
		if (record.getMetricStatus() == null) {
			bgnPs.setNull(31, Types.INTEGER);
		} else {
			bgnPs.setInt(31, record.getMetricStatus());
		}
		if (record.getMetricTechtype() == null) {
			bgnPs.setNull(32, Types.INTEGER);
		} else {
			bgnPs.setInt(32, record.getMetricTechtype());
		}
		bgnPs.setString(33, String.valueOf(record.getMetricPartnerinfo()));
		bgnPs.setString(34, String.valueOf(record.getExtinfo()));
		if (record.getVssip() == null) {
			bgnPs.setNull(35, Types.BIGINT);
		} else {
			bgnPs.setLong(35, record.getVssip());
		}
		if (record.getPerfip() == null) {
			bgnPs.setNull(36, Types.BIGINT);
		} else {
			bgnPs.setLong(36, record.getPerfip());
		}
		if (record.getBitrate() == null) {
			bgnPs.setNull(37, Types.INTEGER);
		} else {
			bgnPs.setInt(37, record.getBitrate());
		}
	}

	protected void withAlive(HiveOrderDetailRecord record,
			PreparedStatement bgnPs) throws SQLException {
		noAlive(record, bgnPs);
		if (record.getMetricPlayalivetime() == null) {
			bgnPs.setNull(38, Types.BIGINT);
		} else {
			bgnPs.setLong(38, record.getMetricPlayalivetime());
		}
	}

	public static class SessionStateFactory implements StateFactory {
		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new SessionStateImpl();
		}

	}

}
