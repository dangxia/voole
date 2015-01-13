package com.voole.hobbit2.camus.v3a;

import com.voole.hobbit2.camus.api.IStampFinder;
import com.voole.hobbit2.hive.order.avro.V3aLogVersion1;

public class V3aAvroStampFinder implements IStampFinder {

	@Override
	public <Value> Long findStamp(Value value) throws IllegalArgumentException {
		if (value == null) {
			return null;
		}
		Long stamp = null;
		if (value instanceof V3aLogVersion1) {
			stamp = ((V3aLogVersion1) value).getStamp();
		} else {
			throw new IllegalArgumentException("Don't support class:"
					+ value.getClass().getName());
		}
		if (stamp == null) {
			return null;
		} else {
			return stamp;
		}
	}

}
