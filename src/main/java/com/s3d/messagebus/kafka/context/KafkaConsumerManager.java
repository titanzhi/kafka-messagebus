
package com.s3d.messagebus.kafka.context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.s3d.messagebus.kafka.ControllerException;
import com.s3d.messagebus.kafka.ControllerState;
import com.s3d.messagebus.kafka.IKafkaConsumerManager;
import com.s3d.messagebus.kafka.conf.ConsumerGroupConf;
import com.s3d.messagebus.kafka.conf.KafkaConf;
import com.s3d.messagebus.kafka.consumer.ConsumerController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka消费者管理器
 * @author sulta
 *
 */
public class KafkaConsumerManager implements IKafkaConsumerManager {
	
	private Map<String, ConsumerController> controllerContainer = new LinkedHashMap<String, ConsumerController>();
	
	private final Logger LOG = LoggerFactory.getLogger(getClass());
	
	private IMessagebusContext messagebusContext;
	
	private final static Object lock = new Object();
	
	private static KafkaConsumerManager instance = null;
	
	/**
	 * 禁止从外部 new KafkaConsumerManager()
	 */
	private KafkaConsumerManager() {
		for (ConsumerGroupConf.ConsumerGroup group : KafkaConf.getKafkaConf().getConsumerGroupConf().getConsumerGroups()) {
			ConsumerController controller = new ConsumerController(group);
			this.controllerContainer.put(group.getId(),controller);
			if (LOG.isInfoEnabled()) {
				LOG.info("Add Consumer Controller,{}",new Object[]{
						group.getConsumerclass()
				});
			}
		}
	}
	
	public synchronized static KafkaConsumerManager getInstance() {
		if(instance==null) 
			instance = new KafkaConsumerManager();
		return instance;
	}
	
	public IMessagebusContext getMessagebusContext() {
		return messagebusContext;
	}

	public void setMessagebusContext(IMessagebusContext messagebusContext) {
		this.messagebusContext = messagebusContext;
	}
	
    private Collection<ConsumerController> getControllersByConsumerclass(String consumerClass) {
		List<ConsumerController> results = new ArrayList<>();
		synchronized (lock) {
			Collection<ConsumerController> all = controllerContainer.values();
			for (ConsumerController controller : all) {
				if(controller.getConsumerGroup().getConsumerclass().equalsIgnoreCase(consumerClass)){
					results.add(controller);
				}
			}
		}
	    return results;
    }
    
    private void startControllers0(Collection<ConsumerController> controllers) {
		for (ConsumerController controller : controllers) {
			try {
	            startController(controller.getConsumerGroup().getId());
            } catch (ControllerException e) {
            	LOG.warn("", e);
            }
		}
    }
    
	@Override
    public ConsumerController getController(String consumerid) {
		return controllerContainer.get(consumerid);
    }

	@Override
    public Collection<ConsumerController> getAllControllers() {
	    return this.controllerContainer.values();
    }
		
	@Override
    public void startController(String consumerid) throws ControllerException {
		ConsumerController controller = this.getController(consumerid);
		if (controller == null) {
			throw new ControllerException(ControllerException.ErrorCode.Controller_Not_Found);
		}
		if (controller.getControllerState().equals(ControllerState.Started)) {
			throw new ControllerException(ControllerException.ErrorCode.Controller_Hasbean_Started);
		}
		
		if (controller.getControllerState().equals(ControllerState.Not_Run)) {
			controller.start();
		}else {
			controller.reboot();
		}
    }
	
	@Override
    public void startAllControllers() {
		startControllers0(this.controllerContainer.values());
    }
	

	@Override
	public void startControllersByConsumerclass(String consumerclass) {
		Collection<ConsumerController> controlleres = getControllersByConsumerclass(consumerclass);
		startControllers0(controlleres);
	}

	
	@Override
    public void shutdownController(String consumerid) throws ControllerException{
		ConsumerController controller = this.getController(consumerid);
		if (controller == null) {
			throw new ControllerException(ControllerException.ErrorCode.Controller_Not_Found);
		}
		controller.shutdownNow();
    }

	@Override
    public void shutdownAllControllers() {
		for (ConsumerController controller : this.controllerContainer.values()) {
			try {
				shutdownController(controller.getConsumerGroup().getId());
            } catch (ControllerException e) {
            	LOG.error("", e);
            }
		}
    }

	@Override
    public void deleteController(String consumerid) {
		synchronized (lock) {
	        ConsumerController controller = this.controllerContainer.get(consumerid);
	        if (controller == null) {
	        	return;
	        }
	        controller.shutdownNow();
	        this.controllerContainer.remove(consumerid);
		}
    }	
}
