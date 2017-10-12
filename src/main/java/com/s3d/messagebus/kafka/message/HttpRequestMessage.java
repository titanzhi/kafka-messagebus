package com.s3d.messagebus.kafka.message;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 简单的http request message
 * @author sulta
 *
 */
public class HttpRequestMessage {
	private final static String METHOD_GET = "GET";
	public HttpRequestMessage() {
	}
	private String serverHost;
	private String httpMethod;
	private String baseUri;
	private String pathInfo;
	private String queryString;	
	private String remoteUser;
	private String remoteAddr;
	private String sessionId;
	private long contentLength;
	private String contentType;
	private List<HttpCookie> cookies;
	private byte[] content;

	private Map<String, List<String>> parameters = null;
	
	public String getServerHost() {
		return serverHost;
	}

	public void setServerHost(String serverHost) {
		this.serverHost = serverHost;
	}

	public String getHttpMethod() {
		if(httpMethod==null) {
			return METHOD_GET;
		}
		return httpMethod;
	}

	public void setHttpMethod(String httpMethod) {
		this.httpMethod = httpMethod;
	}

	public String getBaseUri() {
		return baseUri;
	}

	public void setBaseUri(String baseUri) {
		this.baseUri = baseUri;
	}

	public String getPathInfo() {
		return pathInfo;
	}

	public void setPathInfo(String pathInfo) {
		this.pathInfo = pathInfo;
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public String getRemoteUser() {
		return remoteUser;
	}

	public void setRemoteUser(String remoteUser) {
		this.remoteUser = remoteUser;
	}
	
	public String getRemoteAddr() {
		return remoteAddr;
	}

	public void setRemoteAddr(String remoteAddr) {
		this.remoteAddr = remoteAddr;
	}
	
	public long getContentLength() {
		return contentLength;
	}

	public void setContentLength(long contentLength) {
		this.contentLength = contentLength;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

	public void setParameters(Map<String, List<String>> parameters) {
		this.parameters = parameters;
	}

	public Map<String, List<String>> getParameters() {
		if(parameters == null){
			parameters = new HashMap<>();
		}
		return Collections.unmodifiableMap(parameters);
	}
	
	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	
	public List<HttpCookie> getCookies() {
		return cookies;
	}

	public void setCookies(List<HttpCookie> cookies) {
		this.cookies = cookies;
	}

	public String getParameter(final String name) {

		final Map<String, List<String>> params = getParameters();

		if (params != null) {

			final List<String> values = params.get(name);

			if (values != null && values.size() > 0) {
				return values.get(0);
			}

		}

		return null;

	}

	public List<String> getParameterList(final String name) {

		final Map<String, List<String>> params = getParameters();

		if (params != null) {
			return params.get(name);
		}

		return null;
	}
	
	@Override
	public String toString() {
		return "HttpRequestMessage [baseUri=" + baseUri
				+ ", pathInfo=" + pathInfo
				+ ", httpMethod=" + httpMethod
				+ ", queryString=" + queryString
				+ ", remoteUser=" + remoteUser
				+ ", remoteAddr=" + remoteAddr
				+ ", sessionId=" + sessionId
				+ "]";
	}
}
