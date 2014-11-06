package com.voole.hobbit2.storm.order.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.task.IMetricsContext;

import com.voole.dungbeetle.api.DummyTaskAttemptContext;
import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.order.record.OrderDetailDumgBeetleTransformer;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.hive.order.avro.HiveOrderDryRecord;
import com.voole.hobbit2.order.common.HiveOrderDryRecordGenerator;
import com.voole.hobbit2.order.common.OrderSessionInfo;
import com.voole.hobbit2.storm.order.StormOrderHDFSUtils;

public class ExtraInfoQueryStateImpl implements ExtraInfoQueryState {

	private static final Logger log = LoggerFactory
			.getLogger(ExtraInfoQueryState.class);
	private final OrderDetailDumgBeetleTransformer transformer;

	public ExtraInfoQueryStateImpl() {
		transformer = new OrderDetailDumgBeetleTransformer();
		DummyTaskAttemptContext context = new DummyTaskAttemptContext(
				StormOrderHDFSUtils.conf);
		try {
			transformer.setup(context);
		} catch (Exception e) {
			log.warn(getClass() + " init failed", e);
		}
	}

	@Override
	public void beginCommit(Long txid) {

	}

	@Override
	public void commit(Long txid) {

	}

	@Override
	public List<SpecificRecordBase> query(List<TridentTuple> tuples) {
		long start = System.currentTimeMillis();
		List<SpecificRecordBase> result = new ArrayList<SpecificRecordBase>();
		OrderSessionInfo sessionInfo = new OrderSessionInfo();
		for (TridentTuple tuple : tuples) {
			SpecificRecordBase base = (SpecificRecordBase) tuple.get(0);
			try {
				if (base instanceof OrderPlayBgnReqV2
						|| base instanceof OrderPlayBgnReqV3) {
					sessionInfo.clear();
					if (base instanceof OrderPlayBgnReqV2) {
						sessionInfo.setBgn((OrderPlayBgnReqV2) base);
					} else {
						sessionInfo.setBgn((OrderPlayBgnReqV3) base);
					}
					HiveOrderDryRecord dryRecord = HiveOrderDryRecordGenerator
							.generate(sessionInfo);
					if (dryRecord == null) {
						log.warn("dryRecord is null ,base record:"
								+ base.toString());
						result.add(null);
					} else {
						Map<HiveTable, List<SpecificRecordBase>> tableDetails = transformer
								.transform(dryRecord);
						if (tableDetails == null || tableDetails.size() == 0) {
							log.warn("transformer result is null || size == 0,dry record:"
									+ dryRecord.toString());
							result.add(null);
						} else {
							boolean isSet = false;
							for (Entry<HiveTable, List<SpecificRecordBase>> entry : tableDetails
									.entrySet()) {
								HiveTable table = entry.getKey();
								if ("fact_vod".equals(table.getName())
										&& entry.getValue() != null
										&& entry.getValue().size() > 0) {
									result.add(entry.getValue().get(0));
									isSet = true;
								}
							}
							if (!isSet) {
								result.add(null);
							}
						}
					}

				} else {
					result.add(base);
				}
			} catch (Exception e) {
				result.add(null);
				log.warn("record transform failed", e);
			}
		}
		log.info("query size:" + tuples.size() + ",used time:"
				+ ((System.currentTimeMillis() - start) / 1000));
		return result;
	}

	public static class ExtraInfoQueryStateFactory implements StateFactory {
		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new ExtraInfoQueryStateImpl();
		}

	}
}
