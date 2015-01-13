package com.voole.hobbit2.camus.v3a;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.camus.api.IStampFinder;
import com.voole.hobbit2.camus.api.TopicMeta;
import com.voole.hobbit2.camus.api.TopicMetaRegister;
import com.voole.hobbit2.camus.api.transform.ITransformer;
import com.voole.hobbit2.camus.api.transform.TransformException;
import com.voole.hobbit2.camus.api.transform.V3aAvroKafkaTransformer;

public class V3aAvroTopicMetaRegister implements TopicMetaRegister {

	@Override
	public List<TopicMeta> getTopicMetas() throws TransformException {
		List<TopicMeta> list = new ArrayList<TopicMeta>();
		IStampFinder stampFinder = new V3aAvroStampFinder();
		for (String topic : V3aAvroTopicUtils.topicBiClazz.keySet()) {
			Class<?> clazz = V3aAvroTopicUtils.topicBiClazz.get(topic);
			Schema schema = V3aAvroTopicUtils.topicBiSchema.get(topic);
			ITransformer<byte[], SpecificRecordBase> transformer = new V3aAvroKafkaTransformer(
					schema);
			list.add(new TopicMeta(topic, clazz, stampFinder, transformer,
					schema));
		}

		return list;
	}

}
