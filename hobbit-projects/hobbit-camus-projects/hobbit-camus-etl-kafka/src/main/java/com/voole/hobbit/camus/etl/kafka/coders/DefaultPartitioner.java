package com.voole.hobbit.camus.etl.kafka.coders;

import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import com.voole.hobbit.camus.coders.Partitioner;
import com.voole.hobbit.camus.etl.IEtlKey;
import com.voole.hobbit.camus.etl.kafka.CamusConfigs;
import com.voole.hobbit.camus.etl.kafka.common.DateUtils;

public class DefaultPartitioner implements Partitioner {
	protected static final String OUTPUT_DATE_FORMAT = "YYYY/MM/dd/HH";
	protected DateTimeFormatter outputDateFormatter = null;

	@Override
	public String encodePartition(JobContext context, IEtlKey key) {
		long outfilePartitionMs = CamusConfigs
				.getEtlOutputFileTimePartitionMins(context) * 60000L;
		return "" + DateUtils.getPartition(outfilePartitionMs, key.getTime());
	}

	@Override
	public String generatePartitionedPath(JobContext context, String topic,
			int brokerId, int partitionId, String encodedPartition) {
		// We only need to initialize outputDateFormatter with the default
		// timeZone once.
		if (outputDateFormatter == null) {
			outputDateFormatter = DateUtils.getDateTimeFormatter(
					OUTPUT_DATE_FORMAT, DateTimeZone.forID(CamusConfigs
							.getDefaultTimeZone(context)));
		}

		StringBuilder sb = new StringBuilder();
		sb.append(topic).append("/");
		sb.append(CamusConfigs.getDestPathTopicSubDir(context)).append("/");
		DateTime bucket = new DateTime(Long.valueOf(encodedPartition));
		sb.append(bucket.toString(outputDateFormatter.print(bucket)));
		return sb.toString();
	}
}
