package com.s3d.messagebus.kafka.message;

import java.util.List;



/**
 * 简单的http response message
 * @author sulta
 *
 */
public class HttpResponseMessage {
	private int status = 200;
	
	private String contentType;
	
	private byte[] content;
	
	private String errorMsg;
	
	private String redirectedUrl;
	
	private List<HttpCookie> cookies;
	
	public HttpResponseMessage() {
	}
	
	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
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

	public String getErrorMsg() {
		return errorMsg;
	}

	public void setErrorMsg(String errorMsg) {
		this.errorMsg = errorMsg;
	}
	
	public String getRedirectedUrl() {
		return redirectedUrl;
	}

	public void setRedirectedUrl(String redirectedUrl) {
		this.redirectedUrl = redirectedUrl;
	}
	
	public List<HttpCookie> getCookies() {
		return cookies;
	}

	public void setCookies(List<HttpCookie> cookies) {
		this.cookies = cookies;
	}

	@Override
	public String toString() {
		return "HttpResponseMessage [errorMsg=" + errorMsg
				+ ", content-Length =" + (content==null?0:content.length)
				+ "]";
	}
}
