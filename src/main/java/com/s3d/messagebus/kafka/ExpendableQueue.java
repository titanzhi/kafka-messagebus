package com.s3d.messagebus.kafka;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;

public interface ExpendableQueue<E> {
	
	/**
	 * @see BlockingQueue#poll()
	 * @return
	 */
	E poll();
	
	/**
	 * @see BlockingQueue#drainTo(Collection<? super E> c)
	 * @return
	 */
	int drainTo(Collection<? super E> c);
}
