package com.voole.hobbit2.camus;

import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.camus.api.IStampFinder;
import com.voole.hobbit2.camus.bsepg.BsEpgPlayInfo;

public class BsEpgOrderStampFinder implements IStampFinder {

	@Override
	public <Value> Long findStamp(Value value) throws IllegalArgumentException {
		if (value == null) {
			return null;
		}
		Long stamp = null;
		if (value instanceof BsEpgPlayInfo) {
			stamp = (Long) ((SpecificRecordBase) value).get("playbgntime");
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
