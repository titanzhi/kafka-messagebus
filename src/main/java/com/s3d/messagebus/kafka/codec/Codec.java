package com.s3d.messagebus.kafka.codec;

import java.util.Collection;


public interface Codec<K, V> {

	K decodeKey(byte[] bytes);

	V decodeValue(byte[]  bytes);
	
	Collection<V> decodeValueList(Collection<byte[]> byteslist);
	
	Collection<K> decodeKeyList(Collection<byte[]> byteslist);

	byte[] encodeKey(K key);
	
	byte[] encodeName(String name);

	byte[] encodeValue(V value);
	
	Collection<byte[]> encodeValueList(Collection<V> valueList);
	
	Collection<byte[]> encodeKeyList(Collection<K> keyList);
}
