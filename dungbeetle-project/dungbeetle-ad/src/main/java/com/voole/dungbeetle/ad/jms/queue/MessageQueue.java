package com.voole.dungbeetle.ad.jms.queue;

import java.util.Hashtable;
import java.util.Map;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Service;

import com.voole.dungbeetle.ad.jms.AdFrequencyProducer;
import com.voole.dungbeetle.ad.jms.AdPlayLogNielsenProducer;
import com.voole.dungbeetle.ad.model.AdPlayStat;
import com.voole.dungbeetle.ad.service.IAdPlanFrequencyService;

@Service
public class MessageQueue extends ResMsgDispatchListener {

	@Resource
	private AdFrequencyProducer adFrequencyProducer;

	@Resource
	private IAdPlanFrequencyService adFreService;

	@Resource
	private AdPlayLogNielsenProducer nielsenProducer;

	public MessageQueue() {
		this.start();
	}

	@Override
	public void handleMsg(Message msg) {

		if (msg != null) {
			switch (msg.getProducer()) {
			case 1: {
				Object msgObj = msg.getMessage();
				if (msgObj != null) {
					Map<String, Integer> planHash = (Hashtable<String, Integer>) msgObj;
					// update db
					adFreService.updateAdPuttimes(planHash);
					adFrequencyProducer.send(planHash);
				}
				;
				break;
			}
			case 2: {
				Object msgObj = msg.getMessage();
				if (msgObj != null) {
					AdPlayStat adplaystat = (AdPlayStat) msgObj;
					nielsenProducer.send(adplaystat);
				}
				break;
			}
			}

		}
	}

	@PreDestroy
	public void shutdown() {
		this.interrupt();
	}

}
