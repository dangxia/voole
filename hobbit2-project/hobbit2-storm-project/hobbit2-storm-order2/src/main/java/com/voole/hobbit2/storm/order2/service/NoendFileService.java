package com.voole.hobbit2.storm.order2.service;

import java.util.List;

public interface NoendFileService {
	public void clean();

	public void finish(String fileName);

	public List<String> finished();
}
