package com.s3d.messagebus.kafka;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import com.s3d.messagebus.kafka.conf.KafkaConf;

/**
 * Kafka消息总线生产者管理器
 * @author sulta
 */
public abstract class KafkaProducerManager {
	
	private final static Map<String, Producer<KafkaMessageKey, Object>> producers = new ConcurrentHashMap<String, Producer<KafkaMessageKey,Object>>(); 
	
	private static Producer<KafkaMessageKey, Object> defaultProducer = null;
	/**
	 * 获取生产者
	 * @return
	 */
	public static Producer<KafkaMessageKey, Object> getProducer(Class<?> partitionClass) {
		if(partitionClass == null){
			synchronized (KafkaProducerManager.class) {
				if(defaultProducer==null){
					defaultProducer = createProducer(null);
				}
				return defaultProducer;
			}
		}
		Producer<KafkaMessageKey, Object> producer = producers.get(partitionClass.getName());
		if(producer != null){
			return producer;
		}
		producer = createProducer(partitionClass);
		producers.put(partitionClass.getName(), producer);
		return producer;
	}
	
	/**
	 * 获取生产者
	 * @return
	 */
	public static Producer<KafkaMessageKey, Object> getProducer(){
		return getProducer(null);
	}
	
	private static Producer<KafkaMessageKey, Object> createProducer(Class<?> partitionClass) {
		KafkaConf kafkaConf = KafkaConf.getKafkaConf();
		Properties properties = new Properties();
		properties.put("metadata.broker.list", kafkaConf.getProducerConf().getMetaBrokerList());
		properties.put("serializer.class", kafkaConf.getProducerConf().getSerializerClass());
		properties.put("request.required.acks", kafkaConf.getProducerConf().getRequestRequiredAcks());
		if(partitionClass==null){
			properties.put("partitioner.class", "org.kafka.DefaultPartition");
		}else{
			properties.put("partitioner.class", partitionClass.getName());
		}
		ProducerConfig config = new ProducerConfig(properties);
		return new Producer<KafkaMessageKey, Object>(config);
	}
	
	/**
	 * 关闭所有生产者
	 */
	public static void close() {
		if(defaultProducer==null){
			try{
				defaultProducer.close();
			}catch (Exception e) {
			}
		}
		for (Producer producer : producers.values()) {
			try{
				producer.close();
			}catch (Exception e) {
			}
		}
	}
}
