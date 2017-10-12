package com.s3d.messagebus.kafka;

import java.util.Collection;

import com.s3d.messagebus.kafka.consumer.ConsumerController;
import com.s3d.messagebus.kafka.context.IMessagebusContext;

/**
 * 消费者管理器
 * 根据当前环境下，topic数量以及topic定义线程数量来批量处理消息，并初始化到当前环境中
 * @author sulta
 */
public interface IKafkaConsumerManager {
	
	/**
	 * 获取消费者控制器
	 * @param id
	 * @return
	 */
	ConsumerController getController(String consumerid);
	
	/**
	 * 获取全部消费者控制器
	 * @return
	 */
	Collection<ConsumerController> getAllControllers();
	
	/**
	 * 启动消费者任务控制器
	 * @param consumerid
	 */
	void startController(String consumerid) throws ControllerException ;
	
	/**
	 * 启动全部消费者任务控制器
	 */
	void startAllControllers();
	
	/**
	 * 启动消费者任务控制器
	 */
	void startControllersByConsumerclass(String consumerclass);
	
	/**
	 * 停止消费者任务控制
	 * @param consumerid
	 */
	void shutdownController(String consumerid) throws ControllerException;
	
	/**
	 * 停止全部控制器
	 */
	void shutdownAllControllers();
	
	/**
	 * 删除控制器
	 * @param consumerid
	 */
	void deleteController(String consumerid);
	
	/**
	 * 
	 * @return
	 */
	IMessagebusContext getMessagebusContext();
}
