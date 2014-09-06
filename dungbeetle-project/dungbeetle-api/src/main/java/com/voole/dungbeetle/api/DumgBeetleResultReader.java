/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.dungbeetle.api;

import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;

import com.voole.dungbeetle.api.model.HiveTable;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class DumgBeetleResultReader {
	private DumgBeetleResult dumgBeetleResult;

	public DumgBeetleResultReader() {
	}

	public boolean isPartition() {
		return getTable().hasPartitions();
	}

	public boolean isEmpty() {
		return size() == 0;
	}

	public HiveTable getTable() {
		return getDumgBeetleResult().getTable();
	}

	public Map<String, Object> getPartitionValueMap() {
		return getDumgBeetleResult().getPartitionValueMap();
	}

	public List<SpecificRecordBase> getRecords() {
		return getDumgBeetleResult().getRecords();
	}

	public int size() {
		if (getDumgBeetleResult() != null
				&& getDumgBeetleResult().getRecords() != null) {
			return getDumgBeetleResult().getRecords().size();
		}
		return 0;
	}

	public void setDumgBeetleResult(DumgBeetleResult dumgBeetleResult) {
		this.dumgBeetleResult = dumgBeetleResult;
	}

	public DumgBeetleResult getDumgBeetleResult() {
		return dumgBeetleResult;
	}

}
