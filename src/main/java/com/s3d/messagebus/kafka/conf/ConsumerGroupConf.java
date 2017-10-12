package com.s3d.messagebus.kafka.conf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 消费者配置
 * @author sulta
 *
 */
public class ConsumerGroupConf {
	
	/**
	 * zookeeper配置
	 */
	private String zookeeperConnect;
	private String zookeeperSessionTimeoutMs;
	private String zookeeperSyncTimeMs;
	private String autoCommitIntervalMs;
	/**
	 * 平衡zookeeper节点超时时间，单位为毫秒
	 * <br/>必须保证rebalanceBackoffMs * rebalanceMaxRetries > zookeeperSessionTimeoutMs
	 */
	private String rebalanceBackoffMs;
	/**
	 * 平衡zookeeper节点的重试次数
	 * <br/>必须保证rebalanceBackoffMs * rebalanceMaxRetries > zookeeperSessionTimeoutMs
	 */
	private String rebalanceMaxRetries;
	
	private Collection<ConsumerGroupConf.ConsumerGroup> consumerGroups = new ArrayList<ConsumerGroupConf.ConsumerGroup>();
	
	/**
	 * 获取Zookeeper
	 * @return
	 */
	public String getZookeeperConnect() {
		return zookeeperConnect;
	}
	
	public String getZookeeperSessionTimeoutMs() {
		return zookeeperSessionTimeoutMs;
	}

	public String getZookeeperSyncTimeMs() {
		return zookeeperSyncTimeMs;
	}

	public String getAutoCommitIntervalMs() {
		return autoCommitIntervalMs;
	}

	public void setZookeeperConnect(String zookeeperConnect) {
		this.zookeeperConnect = zookeeperConnect;
	}

	public void setZookeeperSessionTimeoutMs(String zookeeperSessionTimeoutMs) {
		this.zookeeperSessionTimeoutMs = zookeeperSessionTimeoutMs;
	}

	public void setZookeeperSyncTimeMs(String zookeeperSyncTimeMs) {
		this.zookeeperSyncTimeMs = zookeeperSyncTimeMs;
	}

	public void setAutoCommitIntervalMs(String autoCommitIntervalMs) {
		this.autoCommitIntervalMs = autoCommitIntervalMs;
	}
	
	public String getRebalanceBackoffMs() {
		return rebalanceBackoffMs;
	}

	public void setRebalanceBackoffMs(String rebalanceBackoffMs) {
		this.rebalanceBackoffMs = rebalanceBackoffMs;
	}

	public String getRebalanceMaxRetries() {
		return rebalanceMaxRetries;
	}

	public void setRebalanceMaxRetries(String rebalanceMaxRetries) {
		this.rebalanceMaxRetries = rebalanceMaxRetries;
	}

	/**
	 * 获取消费者组配置项
	 * @return
	 */
	public Collection<ConsumerGroup> getConsumerGroups() {
		return consumerGroups;
	}
	
	public void addConsumerGroup(ConsumerGroup consumerGroup) {
		this.consumerGroups.add(consumerGroup);
	}
	
	public static class ConsumerGroup {
		/**
		 * 消费者id
		 */
		private String id;
		/**
		 * 消费者组
		 */
		private String groupid;
		/**
		 * 消费主题
		 */
		private String topic;
		/**
		 * 主题描述
		 */
		private String topicDesc;
		/**
		 * 只从指定的分区消费
		 */
		private Integer specifiedPartition;
		/**
		 * 指定broker list
		 */
		private List<String> specifiedBrokers;		
		/**
		 * 线程池容量
		 */
		private Integer executeserviceCount;
		/**
		 * 消费者处理类
		 */
		private String consumerclass;
		/**
		 * 消费者采用的编码器
		 */
		private String consumercodec;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getGroupid() {
			return groupid;
		}

		public void setGroupid(String groupid) {
			this.groupid = groupid;
		}

		public String getTopic() {
			return topic;
		}

		public void setTopic(String topic) {
			this.topic = topic;
		}

		public String getTopicDesc() {
			return topicDesc;
		}

		public void setTopicDesc(String topicDesc) {
			this.topicDesc = topicDesc;
		}
		
		public Integer getSpecifiedPartition() {
			return specifiedPartition;
		}

		public void setSpecifiedPartition(Integer specifiedPartition) {
			this.specifiedPartition = specifiedPartition;
		}
		
		public List<String> getSpecifiedBrokers() {
			return specifiedBrokers;
		}

		public void setSpecifiedBrokers(List<String> specifiedBrokers) {
			this.specifiedBrokers = specifiedBrokers;
		}

		public Integer getExecuteserviceCount() {
			return executeserviceCount;
		}

		public void setExecuteserviceCount(Integer executeserviceCount) {
			this.executeserviceCount = executeserviceCount;
		}

		public String getConsumerclass() {
			return consumerclass;
		}

		public void setConsumerclass(String consumerclass) {
			this.consumerclass = consumerclass;
		}

		public String getConsumercodec() {
			return consumercodec;
		}

		public void setConsumercodec(String consumercodec) {
			this.consumercodec = consumercodec;
		}
		
	}
}
