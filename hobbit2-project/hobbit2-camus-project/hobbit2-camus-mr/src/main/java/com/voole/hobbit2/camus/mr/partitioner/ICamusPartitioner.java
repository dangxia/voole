/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.mr.partitioner;

/**
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public interface ICamusPartitioner {

	long getPartitionStamp(long stamp);

	String getPath(long partitionStamp);
}
