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
	private static final String UPDATE_INSERT_SQL_BGN = "UPSERT INTO HiveOrderDetailRecord_phoenix(id,sessid,stamp,userip,datasorce,playurl,version,dim_date_hour,dim_isp_id,dim_user_uid,dim_user_hid,dim_oem_id,dim_area_id,dim_area_parentid,dim_nettype_id,dim_media_fid,dim_media_series,dim_media_mimeid,dim_movie_mid,dim_cp_id,dim_movie_category,dim_product_pid,dim_product_ptype,dim_po_id,dim_epg_id,dim_section_id,dim_section_parentid,metric_playbgntime,metric_playalivetime,metric_playendtime,metric_durationtime,metric_avgspeed,metric_isad,metric_isrepeatmod,metric_status,metric_techtype,metric_partnerinfo,extinfo,vssip,perfip) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
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
					if (record.getStamp() == null) {
						bgnPs.setNull(3, Types.BIGINT);
					} else {
						bgnPs.setLong(3, record.getStamp());
					}
					if (record.getUserip() == null) {
						bgnPs.setNull(4, Types.BIGINT);
					} else {
						bgnPs.setLong(4, record.getUserip());
					}
					if (record.getDatasorce() == null) {
						bgnPs.setNull(5, Types.INTEGER);
					} else {
						bgnPs.setInt(5, record.getDatasorce());
					}
					bgnPs.setString(6, String.valueOf(record.getPlayurl()));
					bgnPs.setString(7, String.valueOf(record.getVersion()));
					bgnPs.setString(8, String.valueOf(record.getDimDateHour()));
					if (record.getDimIspId() == null) {
						bgnPs.setNull(9, Types.INTEGER);
					} else {
						bgnPs.setInt(9, record.getDimIspId());
					}
					bgnPs.setString(10, String.valueOf(record.getDimUserUid()));
					bgnPs.setString(11, String.valueOf(record.getDimUserHid()));
					if (record.getDimOemId() == null) {
						bgnPs.setNull(12, Types.BIGINT);
					} else {
						bgnPs.setLong(12, record.getDimOemId());
					}
					if (record.getDimAreaId() == null) {
						bgnPs.setNull(13, Types.INTEGER);
					} else {
						bgnPs.setInt(13, record.getDimAreaId());
					}
					if (record.getDimAreaParentid() == null) {
						bgnPs.setNull(14, Types.INTEGER);
					} else {
						bgnPs.setInt(14, record.getDimAreaParentid());
					}
					if (record.getDimNettypeId() == null) {
						bgnPs.setNull(15, Types.INTEGER);
					} else {
						bgnPs.setInt(15, record.getDimNettypeId());
					}
					bgnPs.setString(16, String.valueOf(record.getDimMediaFid()));
					if (record.getDimMediaSeries() == null) {
						bgnPs.setNull(17, Types.INTEGER);
					} else {
						bgnPs.setInt(17, record.getDimMediaSeries());
					}
					if (record.getDimMediaMimeid() == null) {
						bgnPs.setNull(18, Types.INTEGER);
					} else {
						bgnPs.setInt(18, record.getDimMediaMimeid());
					}
					if (record.getDimMovieMid() == null) {
						bgnPs.setNull(19, Types.BIGINT);
					} else {
						bgnPs.setLong(19, record.getDimMovieMid());
					}
					if (record.getDimCpId() == null) {
						bgnPs.setNull(20, Types.INTEGER);
					} else {
						bgnPs.setInt(20, record.getDimCpId());
					}
					bgnPs.setString(21,
							String.valueOf(record.getDimMovieCategory()));
					bgnPs.setString(22,
							String.valueOf(record.getDimProductPid()));
					if (record.getDimProductPtype() == null) {
						bgnPs.setNull(23, Types.INTEGER);
					} else {
						bgnPs.setInt(23, record.getDimProductPtype());
					}
					if (record.getDimPoId() == null) {
						bgnPs.setNull(24, Types.INTEGER);
					} else {
						bgnPs.setInt(24, record.getDimPoId());
					}
					if (record.getDimEpgId() == null) {
						bgnPs.setNull(25, Types.BIGINT);
					} else {
						bgnPs.setLong(25, record.getDimEpgId());
					}
					bgnPs.setString(26,
							String.valueOf(record.getDimSectionId()));
					bgnPs.setString(27,
							String.valueOf(record.getDimSectionParentid()));
					if (record.getMetricPlaybgntime() == null) {
						bgnPs.setNull(28, Types.BIGINT);
					} else {
						bgnPs.setLong(28, record.getMetricPlaybgntime());
					}
					if (record.getMetricPlayalivetime() == null) {
						bgnPs.setNull(29, Types.BIGINT);
					} else {
						bgnPs.setLong(29, record.getMetricPlayalivetime());
					}
					if (record.getMetricPlayendtime() == null) {
						bgnPs.setNull(30, Types.BIGINT);
					} else {
						bgnPs.setLong(30, record.getMetricPlayendtime());
					}
					if (record.getMetricDurationtime() == null) {
						bgnPs.setNull(31, Types.BIGINT);
					} else {
						bgnPs.setLong(31, record.getMetricDurationtime());
					}
					if (record.getMetricAvgspeed() == null) {
						bgnPs.setNull(32, Types.BIGINT);
					} else {
						bgnPs.setLong(32, record.getMetricAvgspeed());
					}
					if (record.getMetricIsad() == null) {
						bgnPs.setNull(33, Types.INTEGER);
					} else {
						bgnPs.setInt(33, record.getMetricIsad());
					}
					if (record.getMetricIsrepeatmod() == null) {
						bgnPs.setNull(34, Types.INTEGER);
					} else {
						bgnPs.setInt(34, record.getMetricIsrepeatmod());
					}
					if (record.getMetricStatus() == null) {
						bgnPs.setNull(35, Types.INTEGER);
					} else {
						bgnPs.setInt(35, record.getMetricStatus());
					}
					if (record.getMetricTechtype() == null) {
						bgnPs.setNull(36, Types.INTEGER);
					} else {
						bgnPs.setInt(36, record.getMetricTechtype());
					}
					bgnPs.setString(37,
							String.valueOf(record.getMetricPartnerinfo()));
					bgnPs.setString(38, String.valueOf(record.getExtinfo()));
					if (record.getVssip() == null) {
						bgnPs.setNull(39, Types.BIGINT);
					} else {
						bgnPs.setLong(39, record.getVssip());
					}
					if (record.getPerfip() == null) {
						bgnPs.setNull(40, Types.BIGINT);
					} else {
						bgnPs.setLong(40, record.getPerfip());
					}

					bgnPs.addBatch();
					bgnSize++;
				} else if (specificRecordBase instanceof OrderPlayAliveReqV2) {
					OrderPlayAliveReqV2 record = (OrderPlayAliveReqV2) specificRecordBase;
					StringBuffer idSb = new StringBuffer();
					idSb.append(record.getSessID());
					idSb.append('-');
					idSb.append(record.getNatip());
					alivePs.setString(1, String.valueOf(idSb.toString()));

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
					StringBuffer idSb = new StringBuffer();
					idSb.append(record.getSessID());
					idSb.append('-');
					idSb.append(record.getNatip());
					alivePs.setString(1, String.valueOf(idSb.toString()));

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
					StringBuffer idSb = new StringBuffer();
					idSb.append(record.getSessID());
					idSb.append('-');
					idSb.append(record.getNatip());
					endPs.setString(1, String.valueOf(idSb.toString()));

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
					StringBuffer idSb = new StringBuffer();
					idSb.append(record.getSessID());
					idSb.append('-');
					idSb.append(record.getNatip());
					endPs.setString(1, String.valueOf(idSb.toString()));

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

			if (bgnSize > 0) {
				bgnPs.execute();
			}
			if (aliveSize > 0) {
				alivePs.execute();
			}
			if (endSize > 0) {
				endPs.execute();
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
