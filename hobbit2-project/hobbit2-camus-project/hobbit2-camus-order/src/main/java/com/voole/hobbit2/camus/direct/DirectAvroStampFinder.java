package com.voole.hobbit2.camus.direct;

import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.camus.api.IStampFinder;
import com.voole.hobbit2.hive.order.avro.BsPvPlayDryInfo;
import com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo;

public class DirectAvroStampFinder implements IStampFinder {

	@Override
	public <Value> Long findStamp(Value value) throws IllegalArgumentException {
		if (value == null) {
			return null;
		}
		Long stamp = null;
		if (value instanceof BsPvPlayDryInfo) {
			stamp = (Long) ((SpecificRecordBase) value).get("accesstime");
		} else if (value instanceof BsRevenueDryInfo) {
			stamp = (Long) ((SpecificRecordBase) value).get("accesstime");
		} else {
			throw new IllegalArgumentException("Don't support class:"
					+ value.getClass().getName());
		}
		if (stamp == null) {
			return null;
		} else {
			return stamp * 1000;
		}
	}

}
