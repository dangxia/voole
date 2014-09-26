/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.state;

import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.google.common.base.Throwables;
import com.voole.hobbit2.storm.order.util.PutGenerator;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class SessionStateImpl implements SessionState {
	private static final Logger log = LoggerFactory
			.getLogger(SessionState.class);
	private HConnection hConnection;
	private HTableInterface sessionTable;

	public SessionStateImpl() {
		try {
			hConnection = HConnectionManager
					.createConnection(HBaseConfiguration.create());
			sessionTable = hConnection.getTable("storm_order_session");
			sessionTable.setAutoFlush(true, true);
			sessionTable.setWriteBufferSize(1024 * 1024 * 1024);
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
		try {
			long total = 0l;
			long start = System.currentTimeMillis();
			long generateSessionTime = 0l;
			for (SpecificRecordBase specificRecordBase : data) {
				Put put = null;
				try {
					generateSessionTime -= System.currentTimeMillis();
					put = PutGenerator.generateSession(specificRecordBase);
				} catch (Exception e) {
					log.warn("session put generate failed", e);
					continue;
				} finally {
					generateSessionTime += System.currentTimeMillis();
				}

				try {
					if (put != null) {
						total++;
						sessionTable.put(put);
					}
				} catch (Exception e) {
					log.warn("session put insert hbase failed", e);
					continue;
				}
			}
			sessionTable.flushCommits();
			log.warn("session size:" + total + ",used time:"
					+ ((System.currentTimeMillis() - start) / 1000)
					+ ",generate put used time:" + (generateSessionTime / 1000));

		} catch (Exception e) {
			log.warn("insert hbase failed", e);
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
