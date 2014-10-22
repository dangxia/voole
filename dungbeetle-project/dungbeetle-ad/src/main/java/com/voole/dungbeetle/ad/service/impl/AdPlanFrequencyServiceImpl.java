package com.voole.dungbeetle.ad.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import com.voole.dungbeetle.ad.dao.IAdPlanFrequencyDao;
import com.voole.dungbeetle.ad.service.IAdPlanFrequencyService;

/**
 * @author Administrator
 *
 */
@Service
public class AdPlanFrequencyServiceImpl implements IAdPlanFrequencyService {

	@Resource
	public IAdPlanFrequencyDao adFreqao;
	
	/* (non-Javadoc)
	 * @see com.voole.service.IAdPlanFrequencyService#updateAdPuttimes(java.util.Map)
	 */
	@Override
	public int updateAdPuttimes(Map<String, Integer> map) {
		
		if(map != null){
			Map<String, Object> adMap = new HashMap<String, Object>();
			Set<Entry<String,Integer>> entrySet = map.entrySet();
			for (Entry<String, Integer> adplan : entrySet) {
				
				adMap.put("seqno", adplan.getKey());
				adMap.put("puttimes", adplan.getValue());
				adFreqao.updateAdPuttimes(adMap);
			}
		}
		return 0;
	}

}
