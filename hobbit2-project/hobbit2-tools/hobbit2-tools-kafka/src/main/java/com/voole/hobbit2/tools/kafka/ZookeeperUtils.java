/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.kafka;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import com.google.common.base.Optional;

/**
 * @author XuehuiHe
 * @date 2014年8月21日
 */
public class ZookeeperUtils {
	public static Optional<String> readDataMaybeNull(ZkClient zkClient,
			String path) {
		String result = zkClient.readData(path);
		if (result == null) {
			return Optional.absent();
		} else {
			return Optional.of(result);
		}
	}

	public static Optional<List<String>> getChildrenParentMayNotExist(
			ZkClient zkClient, String path) {
		List<String> result = zkClient.getChildren(path);
		if (result == null) {
			return Optional.absent();
		} else {
			return Optional.of(result);
		}
	}

	public static ZkClient createZKClient(String zkServers, int sessionTimeout,
			int connectionTimeout) {
		return new ZkClient(zkServers, sessionTimeout, connectionTimeout,
				new UTF8StringZkSerializer());
	}

	public static class UTF8StringZkSerializer implements ZkSerializer {

		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			if (data != null) {
				return data.toString().getBytes();
			}
			return null;
		}

		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			if (bytes != null) {
				try {
					return new String(bytes, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					throw new ZkMarshallingError(e);
				}
			}
			return null;
		}

	}
}
