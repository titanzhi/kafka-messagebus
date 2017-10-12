package com.s3d.messagebus.kafka.partition;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.s3d.messagebus.kafka.DefaultPartition;
import com.s3d.messagebus.kafka.message.PartitionedMessageKey;

import kafka.utils.VerifiableProperties;

/**
 * 
 * @author sulta
 *
 */
public class SpecifiedPartitioner extends DefaultPartition{
	
	private static Log LOG = LogFactory.getLog(SpecifiedPartitioner.class);
	
    public SpecifiedPartitioner (VerifiableProperties props) {
    	super(props);
    }
    
	@Override
	public int partition(Object key, int numPartitions) {
		int n = 0;
		if (key instanceof PartitionedMessageKey) {
			PartitionedMessageKey messageKey = (PartitionedMessageKey) key;
			if(messageKey.getResponsePartiton()!=null){
				n =  messageKey.getResponsePartiton();
				if (LOG.isTraceEnabled()) {
					LOG.trace("共有 " + numPartitions + "个分区,发往：" + n);
				}
				return n;
			}
		}
		n = super.partition(key, numPartitions);
		return n;
	}
}
