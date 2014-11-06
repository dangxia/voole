package com.voole.hobbit2.storm.order.util;

import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.camus.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV2;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV3;

public class KafkaRecordDehydration {
	public static void dry(SpecificRecordBase base) {
		if (base == null) {
			return;
		}
		if (base instanceof OrderPlayBgnReqV2) {
			((OrderPlayBgnReqV2) base).setSrvs$1(null);
		} else if (base instanceof OrderPlayBgnReqV3) {
			((OrderPlayBgnReqV3) base).setSrvs$1(null);
		} else if (base instanceof OrderPlayAliveReqV2) {
			((OrderPlayAliveReqV2) base).setSrvs$1(null);
		} else if (base instanceof OrderPlayAliveReqV3) {
			((OrderPlayAliveReqV3) base).setSrvs$1(null);
		} else if (base instanceof OrderPlayEndReqV2) {
			((OrderPlayEndReqV2) base).setSrvs$1(null);
		} else if (base instanceof OrderPlayEndReqV3) {
			((OrderPlayEndReqV3) base).setSrvs$1(null);
		}
	}
}
