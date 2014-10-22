package com.voole.dungbeetle.ad.dao;

import java.util.List;
import java.util.Map;



public interface IPlatFormDao {
	
	public List<Map<String,Object>> findOemids(Map<String,String> params);
	
	public List<Map<String,Object>> findAllChannelProgramInfoAccessPlatform();
	
	public List<Map<String,Object>> findAllMovieTypes();
	
}
  