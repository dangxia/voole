package com.voole.hobbit2.camus.meta;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.voole.hobbit2.camus.meta.map.CamusInputSplit;
import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.KafkaPartition;

public class KafkaSplitPartitionStateTest extends WritableTests {
	@Test
	public void test() throws IOException {
		CamusInputSplit s1 = new CamusInputSplit();
		KafkaPartition p = new KafkaPartition(new Broker("test1", 9092, 1),
				"topic", 2);
		s1.setLatestOffset(10000);
		s1.setOffset(20);
		s1.setPartition(p);

		s1.write(getOutput());
		getOutput().flush();

		CamusInputSplit s2 = new CamusInputSplit();
		s2.readFields(getInput());
		System.out.println(s1);
		System.out.println(s2);

		Assert.assertEquals(s1, s2);
	}
}
