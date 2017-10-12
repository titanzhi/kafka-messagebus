package com.s3d.messagebus.kafka.context;

import javax.servlet.ServletContext;

import com.s3d.messagebus.kafka.IKafkaConsumerManager;
import com.s3d.messagebus.kafka.KafkaProducerManager;
import com.s3d.messagebus.kafka.util.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消息总线上下文
 * @author sulta
 *
 */
public class MessagebusContext implements IMessagebusContext {
	
	private final Logger LOG = LoggerFactory.getLogger(getClass());

	private final IKafkaConsumerManager kafkaConsumerManager;
	
	private ServletContext servletContext;
	
	@Override
	public ServletContext getServletContext() {
		return servletContext;
	}

	public IKafkaConsumerManager getKafkaConsumerManager() {
		return kafkaConsumerManager;
	}
	
	public MessagebusContext(boolean startAtonce) {
		this.kafkaConsumerManager = KafkaConsumerManager.getInstance();
		
		if (startAtonce) {
			kafkaConsumerManager.startAllControllers(); 
			LOG.info("All kafka consumer has been started.");
		}
	}
	
	public MessagebusContext(ServletContext servletContext) {
		this.servletContext = servletContext;
		KafkaConsumerManager.getInstance().setMessagebusContext(this);
		this.kafkaConsumerManager = KafkaConsumerManager.getInstance();
		boolean instanton = false;
		String strinstanton = servletContext.getInitParameter("instanton"); //是否立即启动
		if (!KafkaUtils.isBlank(strinstanton)) {
			try {
	            instanton = Boolean.parseBoolean(strinstanton);
            } catch (Exception e) {
            	throw new ExceptionInInitializerError("instanton parameter error,is must be boolean true / false.");
            }
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("instanton param : " + instanton);
		}
		if (instanton) {
			kafkaConsumerManager.startAllControllers();
			LOG.info("All kafka consumer has been started.");
		}
	}
	@Override
	public void close() {
		KafkaProducerManager.close();
		if(kafkaConsumerManager!=null){
			kafkaConsumerManager.shutdownAllControllers();
		}
	}	
}
