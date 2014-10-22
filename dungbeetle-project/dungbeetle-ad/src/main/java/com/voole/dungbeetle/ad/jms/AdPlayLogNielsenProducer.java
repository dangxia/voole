package com.voole.dungbeetle.ad.jms;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import com.voole.dungbeetle.ad.model.AdPlayStat;


public class AdPlayLogNielsenProducer {
	static Log log = LogFactory.getLog(AdPlayLogNielsenProducer.class);

	public AdPlayLogNielsenProducer() {
	}
   
	private JmsTemplate jmsTemplate;
	public JmsTemplate getJmsTemplate() {
		return jmsTemplate;
	}
	public void setJmsTemplate(JmsTemplate jmsTemplate) {
		this.jmsTemplate = jmsTemplate;
	}
	
	
	public void sendByString(final String content) {
		jmsTemplate.send(new MessageCreator() {
			@Override
			public Message createMessage(Session session) throws JMSException {
				return session.createTextMessage(content);
			}
		});
	}
	
	public void send(AdPlayStat playstat){
		jmsTemplate.convertAndSend(playstat);
	}

}
