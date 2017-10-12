package com.s3d.messagebus.kafka.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import com.s3d.messagebus.kafka.conf.ConsumerGroupConf.ConsumerGroup;
import com.s3d.messagebus.kafka.util.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author sulta
 *
 */
public class KafkaConf {
	
	private static final Logger log = LoggerFactory.getLogger(KafkaConf.class);
	
	private static final String KAFKA_CONF = "kafka.properties";
		
	private static final String CONSUMER_CONF = "kafka-consumer.xml";
	
	/**
	 * 生产者配置
	 */
	private ProducerConf producerConf;
	/**
	 * 消费者配置
	 */
	private ConsumerGroupConf consumerGroupConf;
	
	private static KafkaConf kafkaConf = new KafkaConf() ;
	
    @SuppressWarnings("unchecked")
    private KafkaConf() {
		Properties kafkaProperties = new Properties();
		InputStream pis = null;
		try {
			File file = new File("config/"+KAFKA_CONF);
			if(log.isTraceEnabled())
				log.trace("try to load kafka configuration from {}" , file.getAbsoluteFile());
			if(file.exists()){
				pis = new FileInputStream(file);
			} else {
				pis = KafkaConf.class.getResourceAsStream("/"+KAFKA_CONF);
				if(log.isTraceEnabled()) log.trace("try to load kafka configuration from classpath");
			}
			if (pis == null) {
				log.warn("could not load kafka configuration !!!");
			}else{
				kafkaProperties.load(pis);
			}
		} catch (IOException e) {
			log.error("",e);
		} finally {
        	try {
				if (pis != null) {
        			pis.close();
    			}
			} catch (Exception e) {
            }
        }
		
		if (kafkaProperties.isEmpty()) {
			throw new ExceptionInInitializerError("please config kafka.properties");
		}
		
		//初始化生产者配置信息
		//metadata.broker.list
		String strMetadataBrokerList = kafkaProperties.getProperty("metadata.broker.list");
		if (KafkaUtils.isBlank(strMetadataBrokerList)) {
			throw new ExceptionInInitializerError("metadata.broker.list is empty.");
		}
		//serializer.class
		String strSerializerClass = kafkaProperties.getProperty("serializer.class");
		if (KafkaUtils.isBlank(strSerializerClass)) {
			throw new ExceptionInInitializerError("serializer.class is empty.");
		}
		//request.required.acks
		String strRequestRequiredAcks = kafkaProperties.getProperty("request.required.acks");
		if (KafkaUtils.isBlank(strRequestRequiredAcks)) {
			throw new ExceptionInInitializerError("request.required.acks is empty.");
		}
		try {
			Integer.parseInt(strRequestRequiredAcks);
		} catch (Exception e) {
			throw new ExceptionInInitializerError("request.required.acks is not number.");
		}
		
		producerConf = new ProducerConf();
		producerConf.setMetaBrokerList(strMetadataBrokerList);
		producerConf.setRequestRequiredAcks(strRequestRequiredAcks);
		producerConf.setSerializerClass(strSerializerClass);
		
		String strZKConnect = kafkaProperties.getProperty("zookeeper.connect");
		String strZKSessionTimeout = kafkaProperties.getProperty("zookeeper.session.timeout.ms");
		String strZKSyncTime = kafkaProperties.getProperty("zookeeper.sync.time.ms");
		String strCommitInterval = kafkaProperties.getProperty("auto.commit.interval.ms");
		String strRebalanceBackoffMs = kafkaProperties.getProperty("rebalance.backoff.ms");
		String strRebalanceMaxRetries = kafkaProperties.getProperty("rebalance.max.retries");
		
		consumerGroupConf = new ConsumerGroupConf();
		consumerGroupConf.setZookeeperConnect(strZKConnect);
		consumerGroupConf.setZookeeperSessionTimeoutMs(strZKSessionTimeout);
		consumerGroupConf.setZookeeperSyncTimeMs(strZKSyncTime);
		consumerGroupConf.setAutoCommitIntervalMs(strCommitInterval);
		consumerGroupConf.setRebalanceBackoffMs(strRebalanceBackoffMs);
		consumerGroupConf.setRebalanceMaxRetries(strRebalanceMaxRetries);
		
		// 两个平衡参数同时配置才进行设置
		if (consumerGroupConf.getRebalanceBackoffMs() == null ^ consumerGroupConf.getRebalanceMaxRetries() == null) {
			throw new IllegalStateException("必须同时配置zookeeper的两个平衡参数：rebalance.backoff.ms, rebalance.max.retries");
		}
		// 使用平衡参数
		if (consumerGroupConf.getRebalanceBackoffMs() != null && consumerGroupConf.getRebalanceMaxRetries() != null) {
			// 平衡总时间长度必须大于会话时间
			if (Integer.parseInt(consumerGroupConf.getRebalanceBackoffMs()) * Integer.parseInt(consumerGroupConf.getRebalanceMaxRetries()) <= Integer
					.parseInt(consumerGroupConf.getZookeeperSessionTimeoutMs())) {
				throw new IllegalStateException("zookeeper平衡总时长" + consumerGroupConf.getRebalanceBackoffMs() + " * " + consumerGroupConf.getRebalanceMaxRetries()
						+ "必须大于会话超时时长" + consumerGroupConf.getZookeeperSessionTimeoutMs());
			}
		}

		try {
			Enumeration<URL> urls = KafkaConf.class.getClassLoader().getResources(CONSUMER_CONF);
			while (urls.hasMoreElements()) {
				URL url = (URL) urls.nextElement();
				
				//初始化消费者配置信息
				SAXReader saxReader = new SAXReader();
				Document document = null;
				
				InputStream cis = null;				
				try {
					cis = url.openStream();
		            document = saxReader.read(cis);
					if (document != null) {
		            	Element rootElement = document.getRootElement();
		    			
		    			//consumer
		    			List<Element> consumerElements = rootElement.elements("consumer");
		    			if (consumerElements == null || consumerElements.size() <=0) {
		    				
		    			}else{
		    				for (Element consumerElement : consumerElements) {
		    					ConsumerGroup group = new ConsumerGroup();
		    					
		    					String id = consumerElement.attributeValue("id");
		    					if (id == null) {
		    						id = UUID.randomUUID().toString();
		    					}
		    					group.setId(id);
		    					//groupid
		    					Element groupidElement = consumerElement.element("groupid");
		    					if (groupidElement == null || KafkaUtils.isBlank(groupidElement.getTextTrim())) {
		    						throw new ExceptionInInitializerError("consumer groupid empty.");
		    					}
		    					group.setGroupid(groupidElement.getTextTrim());
		    					
		    					//topic
		    					Element topicElement = consumerElement.element("topic");
		    					if (topicElement == null || KafkaUtils.isBlank(topicElement.getTextTrim())) {
		    						throw new ExceptionInInitializerError("topic empty.");
		    					}
		    					group.setTopic(topicElement.getTextTrim());
		    					
		    					//topic-desc
		    					Element topicDescElement = consumerElement.element("topic-desc");
		    					if (topicDescElement == null || KafkaUtils.isBlank(topicDescElement.getTextTrim())) {
		    						
		    					}else{
		    						group.setTopicDesc(topicDescElement.getTextTrim());
		    					}
		    								
		    					//specified-partition
		    					Element partitionElement = consumerElement.element("specified-partition");
		    					if (partitionElement == null || KafkaUtils.isBlank(partitionElement.getTextTrim())) {
		    						
		    					}else{
		    						group.setSpecifiedPartition(Integer.parseInt(partitionElement.getTextTrim()));
		    					}
		    					
		    					//specified-brokerlist
		    					Element brokerlistElement = consumerElement.element("specified-brokerlist");
		    					if (brokerlistElement == null || KafkaUtils.isBlank(brokerlistElement.getTextTrim())) {
		    						if(group.getSpecifiedPartition()!= null){
		    							throw new ExceptionInInitializerError("specified-brokerlist must not be empty if specified-partition is not empty .");
		    						}
		    					}else{
		    						String[] list = brokerlistElement.getTextTrim().split(",");
		    						group.setSpecifiedBrokers(Arrays.asList(list));
		    					}
		    					
		    					//executeservice-count
		    					Element executeservicecountElement = consumerElement.element("executeservice-count");
		    					if (executeservicecountElement == null || KafkaUtils.isBlank(executeservicecountElement.getTextTrim())) {
		    						throw new ExceptionInInitializerError("executeservice-count empty.");
		    					}
		    					try {
		    						group.setExecuteserviceCount(Integer.parseInt(executeservicecountElement.getTextTrim()));
		    					} catch (Exception e) {
		    						throw new ExceptionInInitializerError("executeservice-count not integer.");
		    					}
		    					//consumerclass
		    					Element consumerclassElement = consumerElement.element("consumerclass");
		    					if (consumerclassElement == null || KafkaUtils.isBlank(consumerclassElement.getTextTrim())) {
		    						throw new ExceptionInInitializerError("consumerclass empty.");
		    					}
		    					group.setConsumerclass(consumerclassElement.getTextTrim());
		    					
		    					//consumercodec
		    					Element consumercodecElement = consumerElement.element("consumercodec");
		    					if (consumercodecElement == null || KafkaUtils.isBlank(consumercodecElement.getTextTrim())) {
		    						throw new ExceptionInInitializerError("consumercodec empty.");
		    					}
		    					group.setConsumercodec(consumercodecElement.getTextTrim());
		    					consumerGroupConf.addConsumerGroup(group);
		    				}
		    			}
		            }
		        } catch (DocumentException e) {
		            throw new ExceptionInInitializerError(e);
		        }finally{
		        	try {
						if (cis != null) {
		        			cis.close();
		    			}
		            }catch (Exception e) {
		            }
		        }
			}
		} catch (IOException e1) {
			log.error("",e1);
		}
	}
	
    public static KafkaConf getKafkaConf() {
		return kafkaConf;
	}
	
	/**
	 * 获取ProducerConf
	 * @return
	 */
	public ProducerConf getProducerConf() {
		return producerConf;
	}
	
	/**
	 * 获取ConsumerGroupConf
	 * @return
	 */
	public ConsumerGroupConf getConsumerGroupConf() {
		return consumerGroupConf;
	}
}
