package com.s3d.messagebus.kafka;

import java.security.SecureRandom;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 默认的kafka分区选择器(随机)
 * @author sulta
 *
 */
public class DefaultPartition implements Partitioner{
	
	public static final SecureRandom random = new SecureRandom();
	private static Log LOG = LogFactory.getLog(DefaultPartition.class);
	
    public DefaultPartition (VerifiableProperties props) {
    }
    
	@Override
	public int partition(Object key, int numPartitions) {
		int n = random.nextInt(numPartitions);
		if (LOG.isTraceEnabled()) {
			LOG.trace("共有 " + numPartitions + "个分区,找不到固定的responsePartiton,发往：" + n);
		}
		return n;
	}
}
