package com.s3d.messagebus.kafka.codec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTypeResolverBuilder;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;

public class JsonJacksonCodec implements Codec<Object, Object> , Encoder<Object> {
	private static final Logger logger = LoggerFactory.getLogger(JsonJacksonCodec.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private ObjectMapper mapObjectMapper = new ObjectMapper();

    public JsonJacksonCodec() {
        createObjectMapper(objectMapper);
        TypeResolverBuilder<?> typer = new DefaultTypeResolverBuilder(DefaultTyping.NON_FINAL);
        typer.init(JsonTypeInfo.Id.CLASS, null);
        typer.inclusion(JsonTypeInfo.As.PROPERTY);
        objectMapper.setDefaultTyping(typer);

        createObjectMapper(mapObjectMapper);
        // type info inclusion
        TypeResolverBuilder<?> mapTyper = new DefaultTypeResolverBuilder(DefaultTyping.NON_FINAL) {
            public boolean useForType(JavaType t)
            {
                switch (_appliesFor) {
                case NON_CONCRETE_AND_ARRAYS:
                    while (t.isArrayType()) {
                        t = t.getContentType();
                    }
                    // fall through
                case OBJECT_AND_NON_CONCRETE:
                    return (t.getRawClass() == Object.class) || !t.isConcrete();
                case NON_FINAL:
                    while (t.isArrayType()) {
                        t = t.getContentType();
                    }
                    // to fix problem with wrong long to int conversion
                    if (t.getRawClass() == Long.class) {
                        return true;
                    }
                    return !t.isFinal(); // includes Object.class
                default:
                //case JAVA_LANG_OBJECT:
                    return (t.getRawClass() == Object.class);
                }
            }
        };
        mapTyper.init(JsonTypeInfo.Id.CLASS, null);
        mapTyper.inclusion(JsonTypeInfo.As.PROPERTY);
        mapObjectMapper.setDefaultTyping(mapTyper);
    }

    private void createObjectMapper(ObjectMapper objectMapper) {
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        objectMapper.setVisibilityChecker(objectMapper.getSerializationConfig().getDefaultVisibilityChecker()
                                            .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                                            .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                                            .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                                            .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    }


    private Object decodeObject(byte[] bytes) {
    	if(bytes==null) return null;
    	
        try {
        	return objectMapper.readValue(bytes, Object.class);
        } catch (Exception e) {
        	try {
        		logger.error("decodeObject with json failed:" + new String(bytes),e);
        		return null;
			} catch (Exception e2) {
			}
        	logger.error("decodeObject error", e);
        }
        return null;
    }

    private byte[] encode(Object value) {
    	if(value==null) return null;
    	
        try {
            return objectMapper.writeValueAsBytes(value);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    
    @Override
	public Object decodeKey(byte[] bytes) {
		return decodeObject(bytes);
	}

	@Override
	public Object decodeValue(byte[] bytes) {
		return decodeObject(bytes);
	}
	
	@Override
	public Collection<Object> decodeKeyList(Collection<byte[]> byteslist) {
		return decodeValueList(byteslist);
	}	
	
	@Override
	public Collection<Object> decodeValueList(Collection<byte[]> byteslist) {
		List<Object> results = new ArrayList<Object>(byteslist.size());
		for (byte[] bytes : byteslist) {
			results.add(decodeValue(bytes));
		}
		return results;
	}
	
	@Override
	public byte[] encodeName(String name) {
		return encode(name);
	}
	
	@Override
	public byte[] encodeKey(Object key) {
		return encode(key);
	}

	@Override
	public byte[] encodeValue(Object value) {
		return encode(value);
	}
	
	@Override
	public Collection<byte[]> encodeKeyList(Collection<Object> keyList) {
		return encodeValueList(keyList);
	}
	
	@Override
	public Collection<byte[]> encodeValueList(Collection<Object> valueList) {
		List<byte[]> results = new ArrayList<byte[]>(valueList.size());
		for (Object object : valueList) {
			results.add(encodeValue(object));
		}
		return results;
	}

	@Override
	public byte[] toBytes(Object arg0) {
		return encode(arg0);
	}
	
	public JsonJacksonCodec(VerifiableProperties verifiableProperties) {
		this();
    }
}
