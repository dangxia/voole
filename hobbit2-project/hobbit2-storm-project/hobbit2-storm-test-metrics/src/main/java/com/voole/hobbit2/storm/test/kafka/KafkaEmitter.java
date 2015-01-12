package com.voole.hobbit2.storm.test.kafka;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.tuple.Values;

import com.voole.hobbit2.storm.test.kafka.avro.PartitionInfo;
import com.voole.hobbit2.storm.test.kafka.avro.PartitionInfoType;
import com.voole.hobbit2.storm.test.kafka.avro.PlayAlive;
import com.voole.hobbit2.storm.test.kafka.avro.PlayBgn;
import com.voole.hobbit2.storm.test.kafka.avro.PlayEnd;
import com.voole.hobbit2.storm.test.kafka.avro.PlayType;

class KafkaEmitter implements Emitter<Long, KafkaSpoutPartition, Long> {
	private Random r;

	public KafkaEmitter() {
		r = new Random();
	}

	@Override
	public Long emitPartitionBatch(TransactionAttempt tx,
			TridentCollector collector, KafkaSpoutPartition partition,
			Long lastPartitionMeta) {
		if (lastPartitionMeta == null) {
			lastPartitionMeta = 0l;
		}
		try {
			TimeUnit.SECONDS.sleep(r.nextInt(2));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if (partition.getLongId() == 1) {
			PlayBgn bgn = new PlayBgn();
			bgn.setSessionId("sessionId_" + lastPartitionMeta);
			bgn.setStamp(new Date().getTime());
			bgn.setType(PlayType.BGN);

			collector.emit(new Values(PlayType.BGN, bgn));
		} else if (partition.getLongId() == 2) {
			PlayAlive alive = new PlayAlive();
			alive.setSessionId("sessionId_" + lastPartitionMeta);
			alive.setStamp(new Date().getTime());
			alive.setType(PlayType.ALIVE);

			collector.emit(new Values(PlayType.ALIVE, alive));
		} else {
			PlayEnd end = new PlayEnd();
			end.setSessionId("sessionId_" + lastPartitionMeta);
			end.setStamp(new Date().getTime());
			end.setType(PlayType.ALIVE);

			collector.emit(new Values(PlayType.END, end));
		}

		if (lastPartitionMeta == 0l) {
			PartitionInfo partitionInfo = new PartitionInfo();
			partitionInfo.setType(PartitionInfoType.BGN);

			if (partition.getLongId() == 1) {
				partitionInfo.setTopic("bgn");
			} else if (partition.getLongId() == 2) {
				partitionInfo.setTopic("alive");
			} else {
				partitionInfo.setTopic("end");
			}

			collector.emit(new Values(PlayType.PARTITIONINFO, partitionInfo));
		}

		if (lastPartitionMeta == 500 && partition.getLongId() == 1) {
			PartitionInfo partitionInfo = new PartitionInfo();
			partitionInfo.setType(PartitionInfoType.END);
			partitionInfo.setTopic("bgn");
			collector.emit(new Values(PlayType.PARTITIONINFO, partitionInfo));
		}
		if (lastPartitionMeta == 1000 && partition.getLongId() == 2) {
			PartitionInfo partitionInfo = new PartitionInfo();
			partitionInfo.setType(PartitionInfoType.END);
			partitionInfo.setTopic("alive");
			collector.emit(new Values(PlayType.PARTITIONINFO, partitionInfo));
		}

		if (lastPartitionMeta == 1500 && partition.getLongId() == 3) {
			PartitionInfo partitionInfo = new PartitionInfo();
			partitionInfo.setType(PartitionInfoType.END);
			partitionInfo.setTopic("end");
			collector.emit(new Values(PlayType.PARTITIONINFO, partitionInfo));
		}
		lastPartitionMeta++;
		return lastPartitionMeta;
	}

	@Override
	public void refreshPartitions(
			List<KafkaSpoutPartition> partitionResponsibilities) {

	}

	@Override
	public List<KafkaSpoutPartition> getOrderedPartitions(Long allPartitionInfo) {
		List<KafkaSpoutPartition> list = new ArrayList<KafkaSpoutPartition>();
		for (int i = 0; i < allPartitionInfo; i++) {
			list.add(new KafkaSpoutPartition("kafka_spout_", (long) i));
		}
		return list;
	}

	@Override
	public void close() {

	}

}