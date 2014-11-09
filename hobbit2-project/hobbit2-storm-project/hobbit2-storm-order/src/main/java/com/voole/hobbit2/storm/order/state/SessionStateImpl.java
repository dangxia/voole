/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.state;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
	private static final String UPDATE_INSERT_SQL_BGN = "UPSERT INTO HiveOrderDetailRecord_phoenix(id,sessid,stamp,userip,datasorce,playurl,version,dim_date_hour,dim_isp_id,dim_user_uid,dim_user_hid,dim_oem_id,dim_area_id,dim_area_parentid,dim_nettype_id,dim_media_fid,dim_media_series,dim_media_mimeid,dim_movie_mid,dim_cp_id,dim_movie_category,dim_product_pid,dim_product_ptype,dim_po_id,dim_epg_id,dim_section_id,dim_section_parentid,metric_playbgntime,metric_playalivetime,metric_playendtime,metric_durationtime,metric_avgspeed,metric_isad,metric_isrepeatmod,metric_status,metric_techtype) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
	private static final String UPDATE_INSERT_SQL_ALIVE = "UPSERT INTO HiveOrderDetailRecord_phoenix(id,metric_playalivetime,metric_avgspeed) VALUES (?,?,?) ";
	private static final String UPDATE_INSERT_SQL_END = "UPSERT INTO HiveOrderDetailRecord_phoenix(id,metric_playalivetime,metric_avgspeed) VALUES (?,?,?) ";
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
		try {
			long total = 0l;
			long start = System.currentTimeMillis();
			bgnPs = connection.prepareStatement(UPDATE_INSERT_SQL_BGN);
			alivePs = connection.prepareStatement(UPDATE_INSERT_SQL_ALIVE);
			endPs = connection.prepareStatement(UPDATE_INSERT_SQL_END);

			for (SpecificRecordBase specificRecordBase : data) {
				if (specificRecordBase == null) {
					log.warn("record is empty!");
					continue;
				}
				total++;
				if (specificRecordBase instanceof HiveOrderDetailRecord) {
					HiveOrderDetailRecord record = (HiveOrderDetailRecord) specificRecordBase;
					StringBuffer idSb = new StringBuffer();
					idSb.append(record.getSessid());
					idSb.append('-');
					idSb.append(record.getUserip());
					bgnPs.setString(1, String.valueOf(idSb.toString()));
					bgnPs.setString(2, String.valueOf(record.getSessid()));
					bgnPs.setLong(3, record.getStamp());
					bgnPs.setLong(4, record.getUserip());
					bgnPs.setInt(5, record.getDatasorce());
					bgnPs.setString(6, String.valueOf(record.getPlayurl()));
					bgnPs.setString(7, String.valueOf(record.getVersion()));
					bgnPs.setString(8, String.valueOf(record.getDimDateHour()));
					bgnPs.setInt(9, record.getDimIspId());
					bgnPs.setString(10, String.valueOf(record.getDimUserUid()));
					bgnPs.setString(11, String.valueOf(record.getDimUserHid()));
					bgnPs.setLong(12, record.getDimOemId());
					bgnPs.setInt(13, record.getDimAreaId());
					bgnPs.setInt(14, record.getDimAreaParentid());
					bgnPs.setInt(15, record.getDimNettypeId());
					bgnPs.setString(16, String.valueOf(record.getDimMediaFid()));
					bgnPs.setInt(17, record.getDimMediaSeries());
					bgnPs.setInt(18, record.getDimMediaMimeid());
					bgnPs.setLong(19, record.getDimMovieMid());
					bgnPs.setInt(20, record.getDimCpId());
					bgnPs.setString(21,
							String.valueOf(record.getDimMovieCategory()));
					bgnPs.setString(22,
							String.valueOf(record.getDimProductPid()));
					bgnPs.setInt(23, record.getDimProductPtype());
					bgnPs.setInt(24, record.getDimPoId());
					bgnPs.setLong(25, record.getDimEpgId());
					bgnPs.setString(26,
							String.valueOf(record.getDimSectionId()));
					bgnPs.setString(27,
							String.valueOf(record.getDimSectionParentid()));
					bgnPs.setLong(28, record.getMetricPlaybgntime());
					bgnPs.setLong(29, record.getMetricPlayalivetime());
					bgnPs.setLong(30, record.getMetricPlayendtime());
					bgnPs.setLong(31, record.getMetricDurationtime());
					bgnPs.setLong(32, record.getMetricAvgspeed());
					bgnPs.setInt(33, record.getMetricIsad());
					bgnPs.setInt(34, record.getMetricIsrepeatmod());
					bgnPs.setInt(35, record.getMetricStatus());
					bgnPs.setInt(36, record.getMetricTechtype());

					bgnPs.addBatch();
					bgnSize++;
				} else if (specificRecordBase instanceof OrderPlayAliveReqV2) {
					OrderPlayAliveReqV2 record = (OrderPlayAliveReqV2) specificRecordBase;
					StringBuffer idSb = new StringBuffer();
					idSb.append(record.getSessID());
					idSb.append('-');
					idSb.append(record.getNatip());
					alivePs.setString(1, String.valueOf(idSb.toString()));
					alivePs.setLong(2, record.getAliveTick());
					alivePs.setLong(3, record.getSessAvgSpeed());
					alivePs.addBatch();
					aliveSize++;
				} else if (specificRecordBase instanceof OrderPlayAliveReqV3) {
					OrderPlayAliveReqV3 record = (OrderPlayAliveReqV3) specificRecordBase;
					StringBuffer idSb = new StringBuffer();
					idSb.append(record.getSessID());
					idSb.append('-');
					idSb.append(record.getNatip());
					alivePs.setString(1, String.valueOf(idSb.toString()));
					alivePs.setLong(2, record.getAliveTick());
					alivePs.setLong(3, record.getSessAvgSpeed());
					alivePs.addBatch();
					aliveSize++;
				} else if (specificRecordBase instanceof OrderPlayEndReqV2) {
					OrderPlayEndReqV2 record = (OrderPlayEndReqV2) specificRecordBase;
					StringBuffer idSb = new StringBuffer();
					idSb.append(record.getSessID());
					idSb.append('-');
					idSb.append(record.getNatip());
					endPs.setString(1, String.valueOf(idSb.toString()));
					endPs.setLong(2, record.getEndTick());
					endPs.setLong(3, record.getSessAvgSpeed());
					endPs.addBatch();
					endSize++;
				} else if (specificRecordBase instanceof OrderPlayEndReqV3) {
					OrderPlayEndReqV3 record = (OrderPlayEndReqV3) specificRecordBase;
					StringBuffer idSb = new StringBuffer();
					idSb.append(record.getSessID());
					idSb.append('-');
					idSb.append(record.getNatip());
					endPs.setString(1, String.valueOf(idSb.toString()));
					endPs.setLong(2, record.getEndTick());
					endPs.setLong(3, record.getSessAvgSpeed());
					endPs.addBatch();
					endSize++;
				} else {
					log.warn("Don't support record,"
							+ specificRecordBase.getClass().getName());
				}
			}

			if (bgnSize > 0) {
				bgnPs.execute();
			} else {
				bgnPs.cancel();
			}
			if (aliveSize > 0) {
				alivePs.execute();
			} else {
				alivePs.cancel();
			}
			if (endSize > 0) {
				endPs.execute();
			} else {
				endPs.cancel();
			}

			connection.commit();

			log.warn("session size:" + total + ",bgnSize:" + bgnSize
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
