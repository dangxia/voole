package com.voole.hobbit2.storm.order.state;

import java.io.IOException;
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
import com.voole.hobbit2.camus.bsepg.BsEpgPlayInfo;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.hive.order.avro.HiveOrderDryRecord;
import com.voole.hobbit2.order.common.BsEpgOrderSessionInfo;
import com.voole.hobbit2.order.common.HiveOrderDryRecordGenerator;
import com.voole.hobbit2.order.common.OrderSessionInfo;
import com.voole.hobbit2.order.common.exception.OrderSessionInfoException;
import com.voole.hobbit2.storm.order.StormOrderHDFSUtils;

public class ExtraInfoQueryStateImpl implements ExtraInfoQueryState {

	private static final Logger log = LoggerFactory
			.getLogger(ExtraInfoQueryState.class);
	private final OrderDetailDumgBeetleTransformer transformer;
	private final OrderSessionInfo sessionInfo;
	private final BsEpgOrderSessionInfo bsSessionInfo;
	private final DummyTaskAttemptContext context;

	public ExtraInfoQueryStateImpl() {
		transformer = new OrderDetailDumgBeetleTransformer();
		sessionInfo = new OrderSessionInfo();
		bsSessionInfo = new BsEpgOrderSessionInfo();
		context = new DummyTaskAttemptContext(StormOrderHDFSUtils.conf);
		try {
			transformer.setup(context);
		} catch (Exception e) {
			log.warn(getClass() + " init failed", e);
		}
	}

	@Override
	public void close() throws IOException, InterruptedException {
		if (transformer != null) {
			transformer.cleanup(context);
		}

	}

	@Override
	public void beginCommit(Long txid) {

	}

	@Override
	public void commit(Long txid) {

	}

	@Override
	public List<SpecificRecordBase> queryRecords(List<SpecificRecordBase> inputs) {
		long start = System.currentTimeMillis();
		List<SpecificRecordBase> result = new ArrayList<SpecificRecordBase>();
		for (SpecificRecordBase base : inputs) {
			try {
				if (base instanceof OrderPlayBgnReqV2
						|| base instanceof OrderPlayBgnReqV3
						|| base instanceof BsEpgPlayInfo) {
					HiveOrderDryRecord dryRecord = null;
					if (base instanceof BsEpgPlayInfo) {
						SpecificRecordBase record = createBsEpgRecord((BsEpgPlayInfo) base);
						if (record instanceof HiveOrderDryRecord) {
							dryRecord = (HiveOrderDryRecord) record;
						} else {
							result.add(record);
							continue;
						}
					} else {
						dryRecord = createDryRecord(base);
					}

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
									break;
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
		log.info("query size:" + inputs.size() + ",used time:"
				+ ((System.currentTimeMillis() - start) / 1000));
		return result;
	}

	public HiveOrderDryRecord createDryRecord(Object base)
			throws OrderSessionInfoException {
		sessionInfo.clear();
		if (base instanceof OrderPlayBgnReqV2) {
			sessionInfo.setBgn((OrderPlayBgnReqV2) base);
		} else {
			sessionInfo.setBgn((OrderPlayBgnReqV3) base);
		}
		return HiveOrderDryRecordGenerator.generate(sessionInfo);
	}

	public SpecificRecordBase createBsEpgRecord(BsEpgPlayInfo base) {
		bsSessionInfo.clear();
		bsSessionInfo.setPlayInfo(base);
		return bsSessionInfo.getStormRecord();
	}

	@Override
	public List<SpecificRecordBase> query(List<TridentTuple> tuples) {
		List<SpecificRecordBase> result = new ArrayList<SpecificRecordBase>();
		for (TridentTuple tuple : tuples) {
			result.add((SpecificRecordBase) tuple.get(0));
		}
		return queryRecords(result);
	}

	public static class ExtraInfoQueryStateFactory implements StateFactory {
		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new ExtraInfoQueryStateImpl();
		}

	}
}
