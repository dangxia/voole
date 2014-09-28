/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.util;

import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit2.camus.order.dry.PlayAliveDryRecord;
import com.voole.hobbit2.camus.order.dry.PlayBgnDryRecord;
import com.voole.hobbit2.camus.order.dry.PlayEndDryRecord;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class PutGenerator {
	private static final Logger log = LoggerFactory
			.getLogger(PutGenerator.class);
	private static final byte[] family = "cf".getBytes();

	public static Put generateSession(SpecificRecordBase base) {
		if (base instanceof PlayBgnDryRecord) {
			return generateBgn((PlayBgnDryRecord) base);
		} else if (base instanceof PlayAliveDryRecord) {
			return generateAlive((PlayAliveDryRecord) base);
		} else if (base instanceof PlayEndDryRecord) {
			return generateEnd((PlayEndDryRecord) base);
		}
		return null;
	}

	public static Put generateBgn(PlayBgnDryRecord bgn) {
		StringBuffer sb = new StringBuffer();
		sb.append(bgn.getSessID());
		sb.append('-');
		sb.append(bgn.getNatip());
		Put put = new Put(sb.toString().getBytes());
		Long ts = bgn.getPlayBgnTime();
		if (ts == null) {
			log.warn("ts can't be null");
			return null;
		}
		fillPut(put, ts, bgn);
		return put;
	}

	public static Put generateAlive(PlayAliveDryRecord alive) {
		StringBuffer sb = new StringBuffer();
		sb.append(alive.getSessID());
		sb.append('-');
		sb.append(alive.getNatip());
		Put put = new Put(sb.toString().getBytes());
		Long ts = alive.getPlayAliveTime();
		if (ts == null) {
			log.warn("ts can't be null");
			return null;
		}
		fillPut(put, ts, alive);
		return put;
	}

	public static Put generateEnd(PlayEndDryRecord end) {
		StringBuffer sb = new StringBuffer();
		sb.append(end.getSessID());
		sb.append('-');
		sb.append(end.getNatip());
		Put put = new Put(sb.toString().getBytes());
		Long ts = end.getPlayEndTime();
		if (ts == null) {
			log.warn("ts can't be null");
			return null;
		}
		fillPut(put, ts, end);
		return put;
	}

	public static void fillPut(Put put, long ts, SpecificRecordBase base) {
		List<Field> fileds = base.getSchema().getFields();
		for (Field field : fileds) {
			Object o = base.get(field.pos());
			if (o != null) {
				put.add(family, field.name().getBytes(), ts * 1000, getBytes(o));
			}
		}
	}

	public static byte[] getBytes(Object o) {

		if (o instanceof Boolean) {
			return Bytes.toBytes((Boolean) o);
		} else if (o instanceof Integer || o instanceof Character) {
			return Bytes.toBytes((Integer) o);
		} else if (o instanceof Long) {
			return Bytes.toBytes((Long) o);
		} else if (o instanceof Float) {
			return Bytes.toBytes((Float) o);
		} else if (o instanceof Double) {
			return Bytes.toBytes((Double) o);
		} else if (o instanceof Short) {
			return Bytes.toBytes((Short) o);
		} else if (o instanceof String) {
			return Bytes.toBytes((String) o);
		} else if (o instanceof org.apache.avro.util.Utf8) {
			return Bytes.toBytes(((org.apache.avro.util.Utf8) o).toString());
		}
		throw new RuntimeException("don't support getBytes Type:"
				+ o.getClass().getName());
	}
}
