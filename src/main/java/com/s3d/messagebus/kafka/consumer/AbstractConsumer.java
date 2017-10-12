package com.s3d.messagebus.kafka.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import com.s3d.messagebus.kafka.BrokerFailureException;
import com.s3d.messagebus.kafka.KafkaMessageKey;
import com.s3d.messagebus.kafka.KafkaMessageObject;
import com.s3d.messagebus.kafka.codec.Codec;
import com.s3d.messagebus.kafka.schedule.ExponentialSchedulePolicy;
import com.s3d.messagebus.kafka.schedule.SchedulePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

/**
 * 从kafka读取消息
 * @author sulta
 */
public abstract class AbstractConsumer<K extends KafkaMessageKey,V> implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(AbstractConsumer.class);
	/**
	 * 指定消费分区
	 */
	protected Integer partition;
	
	/**
	 * 指定broker list
	 */
	private List<String> brokers;
	/**
	 * 所在kafka topic 
	 */
	protected String topic;
	
	/**
	 * 
	 */
	private Properties props;
	/**
	 * 编码器
	 */
	protected Codec<K, V> codec;
	
	protected final AtomicBoolean stopping = new AtomicBoolean(false);
	
	//InterestedConsumption
	private final static String LEADER_LKUP_CLIENTID = "leaderLookup";
	private final static int BUFFER_SIZE = 64 * 1024;
	private final static int DEF_BROKER_PORT = 9092;
	private PartitionMetadata metadata;
	
	private final List<String> m_replicaBrokers = new ArrayList<String>();
	private String leadBroker;
	private String clientName;
	private SimpleConsumer consumer;
	private long readOffset = 0;
	
	/**
	 * 通过zk来自动分配,还是指定分区读取
	 */
	private boolean readViaZK = true;	
	
	@Override
	public final void run() {
		if(partition != null){
			readViaZK = false;
			metadata = findLeader(brokers, DEF_BROKER_PORT, topic, partition);
			if (metadata == null) {
				log.error("Can't find metadata for Topic:{},and Partition :{}. ",new Object[]{
						topic, partition
				});
				throw new ExceptionInInitializerError("Can't find metadata for Topic and Partition. ");
			}
			if (metadata.leader() == null) {
				log.error("Can't find Leader for  Topic:{},and Partition :{}. ",new Object[]{
						topic, partition
				});
				throw new ExceptionInInitializerError("Can't find Leader for Topic and Partition. ");
			}

			leadBroker = metadata.leader().host();
			clientName = "Client_" + topic + "_" + partition;

			consumer = new SimpleConsumer(leadBroker, DEF_BROKER_PORT,
					100000, BUFFER_SIZE, clientName);
			readOffset = getLastOffset(consumer, topic, partition,
					//kafka.api.OffsetRequest.EarliestTime()
					kafka.api.OffsetRequest.LatestTime() //
					, clientName);
		}
		
		onInitialize();
		
		if( readViaZK ){
			readViaZK();
		}else{
			readViaSpecifiedPartition();
			if (consumer != null) {
				consumer.close();
				consumer = null;
				log.info("closing Simple Consumer ....");
			}
		}
	}
	
	/**
	 * 
	 * @throws ExceptionInInitializerError
	 */
	public abstract void onInitialize() throws ExceptionInInitializerError;
	
	/**
	 * 
	 * @param message
	 * @throws InterruptedException
	 */
	public abstract void onNewMessageArrived(KafkaMessageObject<K, V> message) throws InterruptedException;
	
	/**
	 * 
	 * @param messages
	 * @throws InterruptedException
	 */
	public abstract void onNewMessagesArrived(List<KafkaMessageObject<K, V>> messages);
	
	/**
	 *  由zookeeper来自动分配
	 * @return
	 */
	private void readViaZK() {
		KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
		
		try {
			kafkaConsumer = new KafkaConsumer<>(props);
			kafkaConsumer.subscribe(Arrays.asList(topic));
			
			SchedulePolicy retryPolicy = new ExponentialSchedulePolicy(500, 5000);
			
			while (!stopping.get()) {
				ConsumerRecords<byte[],byte[]> records = ConsumerRecords.empty();
				try {
					records = kafkaConsumer.poll(0);
					retryPolicy.succeess();
				} catch (Exception e) {
					log.warn("Pull messages failed!", e);
					retryPolicy.fail(true);
					continue;
				}
				
				List<KafkaMessageObject<K, V>> msgs = new ArrayList<KafkaMessageObject<K, V>>();
				
				for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
					if (!stopping.get()) {
						long offset = -1;
						try {
							offset = consumerRecord.offset();
							
							K k = consumerRecord.key() != null ? codec.decodeKey(consumerRecord.key()) : (K)null;
							if(k != null){
								k.setReceiveTimeStamp(System.currentTimeMillis());
								if(k.getTopic() == null){
									k.setTopic(topic);
								}
							}
							
							V message = null;
							
							if(consumerRecord.value() != null){
								try {
									message = codec.decodeValue(consumerRecord.value());
								} catch (Exception e) {
									log.error("error on decode",e);
								}
							}
							
							if (null != message) {
								KafkaMessageObject<K, V> object = new KafkaMessageObject<K, V>(k, message , consumerRecord.partition(),
									      consumerRecord.offset());
								
								msgs.add(object);
							}
							
						} catch (Exception e) {
							log.warn("Kafka consumer failed Topic:{} Partition:{} Offset:{} Exception:{}", new Object[] {
									consumerRecord.topic(), consumerRecord.partition(), offset, e.getMessage() });
						}
					}
				}
				if (msgs.size() > 0 && !stopping.get()){
					onNewMessagesArrived(msgs);
				}
			}
		} catch (WakeupException e) {
			if (!stopping.get())
				throw e;
		} catch (Exception e) {
			if (!stopping.get()) {
				log.error("Consumer exited abnormally.", e);
				throw e;
			}
		} finally {
			log.info("Closing kafka consumer...");
			if (kafkaConsumer != null) {
				Set<TopicPartition> assignment = kafkaConsumer.assignment();
				kafkaConsumer.close();
				log.info("Closed assignment: " + assignment);
			}
		}
	}
	
	/**
	 * 从批量分区读取消息
	 */
	private void readViaSpecifiedPartition() {
		int numErrors = 0;
		while (!stopping.get()) {
			if (consumer == null) {
				consumer = new SimpleConsumer(leadBroker, DEF_BROKER_PORT, 100000,
						BUFFER_SIZE, clientName);
			}
			FetchRequest req = new FetchRequestBuilder().clientId(clientName)
					.addFetch(topic, partition, readOffset, 100000).build(); //TODO 10000 要调整合适的
			FetchResponse fetchResponse = consumer.fetch(req);

			if (fetchResponse.hasError()) {
				numErrors++;
				// Something went wrong!
				short code = fetchResponse.errorCode(topic, partition);
				log.error("Error fetching data from the Broker:"
						+ leadBroker + " Reason: " + code);
				if (numErrors > 5)
					break;
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					// We asked for an invalid offset. For simple case ask for the last element to reset
					readOffset = getLastOffset(consumer, topic, partition,
							kafka.api.OffsetRequest.LatestTime(), clientName);
					continue;
				}
				consumer.close();
				consumer = null;
				leadBroker = findNewLeader(leadBroker, topic, partition,
						DEF_BROKER_PORT);
				continue;
			}
			
			numErrors = 0;
			int numRead = 0;
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(
					topic, partition)) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < readOffset) {
					if(log.isTraceEnabled()){
						log.trace("Found an old offset: " + currentOffset
								+ " Expecting: " + readOffset);
					}
					continue;
				}
				readOffset = messageAndOffset.nextOffset();
				
				Message message = messageAndOffset.message();
				if(message != null){
					byte[] msgKey = null;
					if(message.hasKey()){
						ByteBuffer key = message.key();
						msgKey = new byte[key.limit()];
						key.get(msgKey);
					}
					ByteBuffer payload = message.payload();			
					byte[] msgValue = new byte[payload.limit()];
					payload.get(msgValue);
					
					K k = msgKey != null ? codec.decodeKey(msgKey) : (K)null;
					
					if(k != null){
						k.setReceiveTimeStamp(System.currentTimeMillis());
						if(k.getTopic() == null){
							k.setTopic(topic);
						}
					}
					numRead++;
					
					if(msgValue != null){
						V value = codec.decodeValue(msgValue);
						if(null != value){
							KafkaMessageObject<K, V> object = new KafkaMessageObject<K, V>(
								 	k,
								 	value
								);
							try {
								onNewMessageArrived(object);
							} catch (InterruptedException e) {
								stopping.set(true);
								Thread.currentThread().interrupt();
								break;
							}
						}
					}
				}
			}
			//TODO 如果 sleep 消息会有delay。如果不sleep,对broker的压力会增大。暂时先不sleep，先观察一阵时间
//			if (numRead == 0) {
//				try {
//					Thread.sleep(100);
//				} catch (InterruptedException ie) {
//				}
//			}
		}
	}
	
	/**
	 * 从broker list中找出对应topic及分区所在的 server
	 * @param a_seedBrokers
	 * @param a_port
	 * @param a_topic
	 * @param a_partition
	 * @return
	 */
	private PartitionMetadata findLeader(List<String> a_seedBrokers,
			int a_port, String a_topic, int a_partition) {
		PartitionMetadata returnMetaData = null;
		loop: for (String seed : a_seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, a_port, 100000, BUFFER_SIZE, LEADER_LKUP_CLIENTID);
				List<String> topics = Collections.singletonList(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				throw new ExceptionInInitializerError("Error communicating with Broker [" + seed
						+ "] to find Leader for [" + a_topic + ", "
						+ a_partition + "] Reason: " + e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		if (returnMetaData != null) {
			m_replicaBrokers.clear();
			for (kafka.cluster.BrokerEndPoint replica : returnMetaData.replicas()) {
				m_replicaBrokers.add(replica.host());
			}
		}
		return returnMetaData;
	}
	
	private String findNewLeader(String a_oldLeader, String a_topic,
			int a_partition, int a_port){
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port,
					a_topic, a_partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host())
					&& i == 0) {
				// first time through if the leader hasn't changed give
				// ZooKeeper a second to recover
				// second time, assume the broker did recover before failover,
				// or it was a non-Broker issue
				//
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		throw new BrokerFailureException(
				"Unable to find new leader after Broker failure. Exiting");
	}
	
	private static long getLastOffset(SimpleConsumer consumer, String topic,
			int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
				requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
				clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			log.warn("Error fetching data Offset Data the Broker. Reason: "
					+ response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}
	
	//	---------------------------------------------------

	public void setPartition(Integer partition) {
		this.partition = partition;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
		
	public void setBrokers(List<String> brokers) {
		this.brokers = brokers;
	}
	
	public Properties getProps() {
		return props;
	}

	public void setProps(Properties props) {
		this.props = props;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void setCodec(Codec codec) {
		this.codec = codec;
	}
}
