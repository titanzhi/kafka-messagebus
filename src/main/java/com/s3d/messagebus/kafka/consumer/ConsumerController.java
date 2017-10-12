package com.s3d.messagebus.kafka.consumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.s3d.messagebus.kafka.ControllerState;
import com.s3d.messagebus.kafka.codec.Codec;
import com.s3d.messagebus.kafka.conf.ConsumerGroupConf;
import com.s3d.messagebus.kafka.conf.KafkaConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author sulta
 *
 */
public class ConsumerController {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerController.class);
	/**
	 * 当前控制器线程池
	 */
	private ExecutorService executorService;
	/**
	 * 当前控制器的配置项
	 */
	private ConsumerGroupConf.ConsumerGroup consumerGroup;
	
	public ConsumerController(ConsumerGroupConf.ConsumerGroup consumerGroup) {
		this.consumerGroup = consumerGroup;
	}
	
	/**
	 * 启动当前控制器
	 */
	public void start() { 
		if(consumerGroup.getSpecifiedPartition() == null) { 
			Properties properties = new Properties();
			properties.put("zookeeper.connect", KafkaConf.getKafkaConf().getConsumerGroupConf().getZookeeperConnect());
			properties.put("group.id", consumerGroup.getGroupid());
			properties.put("zookeeper.session.timeout.ms", KafkaConf.getKafkaConf().getConsumerGroupConf().getZookeeperSessionTimeoutMs());
			properties.put("zookeeper.sync.time.ms", KafkaConf.getKafkaConf().getConsumerGroupConf().getZookeeperSyncTimeMs());
			properties.put("auto.commit.interval.ms", KafkaConf.getKafkaConf().getConsumerGroupConf().getAutoCommitIntervalMs());
			
			// 设置平衡相关参数
			if (KafkaConf.getKafkaConf().getConsumerGroupConf().getRebalanceBackoffMs() != null &&
					KafkaConf.getKafkaConf().getConsumerGroupConf().getRebalanceMaxRetries() != null) {
				properties.put("rebalance.backoff.ms", KafkaConf.getKafkaConf().getConsumerGroupConf().getRebalanceBackoffMs());
				properties.put("rebalance.max.retries", KafkaConf.getKafkaConf().getConsumerGroupConf().getRebalanceMaxRetries());

				if (logger.isInfoEnabled()) {
					logger.info("使用zookeeper平衡参数：rebalance.backoff.ms({})和rebalance.max.retries({})",
							KafkaConf.getKafkaConf().getConsumerGroupConf().getRebalanceBackoffMs(),
							KafkaConf.getKafkaConf().getConsumerGroupConf().getRebalanceMaxRetries());
				}
			} else {
				if (logger.isInfoEnabled()) {
					logger.info("未使用zookeeper平衡参数。");
				}
			}
			
			executorService = Executors.newFixedThreadPool(this.consumerGroup.getExecuteserviceCount());
			
			submitConsumer(properties);
		}else{ //只从指定的分区消费
			executorService = Executors.newSingleThreadExecutor();
			submitConsumer(null);
		}
		
	}
	
	private void submitConsumer(Properties properties){
		try {
			AbstractConsumer consumer = (AbstractConsumer)Class.forName(consumerGroup.getConsumerclass()).newInstance();
			consumer.setProps(properties);
			
			consumer.setTopic(consumerGroup.getTopic());
			
			if (consumerGroup.getSpecifiedPartition() != null)
				consumer.setPartition(consumerGroup.getSpecifiedPartition());
			
			if (consumerGroup.getSpecifiedBrokers() != null)
				consumer.setBrokers(consumerGroup.getSpecifiedBrokers());
			
			Codec codec =  (Codec)Class.forName(consumerGroup.getConsumercodec()).newInstance();
			consumer.setCodec(codec);
			executorService.execute(consumer);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new ExceptionInInitializerError(e);
		} catch (InstantiationException e) {
			e.printStackTrace();
			throw new ExceptionInInitializerError(e);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new ExceptionInInitializerError(e);
		}
	}
	/**
	 * 获取当前控制器状态
	 * @return
	 */
	public ControllerState getControllerState() {
		if (this.executorService == null) {
			return ControllerState.Not_Run;
		}else if (this.executorService.isShutdown()) {
			return ControllerState.Shutdown;
		}else if (this.executorService.isTerminated()) {
			return ControllerState.Terminated;
		}else {
			return ControllerState.Started;
		}
	}

	public ConsumerGroupConf.ConsumerGroup getConsumerGroup() {
		return consumerGroup;
	}
	
	/**
	 * 关闭
	 */
	public void shutdown() {
		if (this.executorService != null && !this.executorService.isShutdown() && !this.executorService.isTerminated()) {
			this.executorService.shutdown();
		}
	}
	
	/**
	 * 强制关闭
	 */
	public void shutdownNow() {
		if (this.executorService != null && !this.executorService.isShutdown() && !this.executorService.isTerminated()) {
			this.executorService.shutdownNow();
		}
	}
	
	/**
	 * 强制关闭，等待3秒后，重新启动当前控制器
	 */
	public void reboot() {
		this.shutdownNow();
		try {
	        TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
	        e.printStackTrace();
        }
		this.start();
	}
}
