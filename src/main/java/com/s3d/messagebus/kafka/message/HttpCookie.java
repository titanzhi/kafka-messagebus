package com.s3d.messagebus.kafka.message;


public class HttpCookie {
    private String name;
    private String value;
    private String domain;
    private String path;
    private Long maxAge;
    private Boolean isHttpOnly = false;
	private Integer version = 0;
	private Boolean secure;
	
    public HttpCookie() {
	}
    
    public HttpCookie(String name,String value,String domain,String path,Long maxAge) {
    	this.name = name;
    	this.value = value;
    	this.domain = domain;
    	this.path = path;
    	this.maxAge = maxAge;
	}
    
    public HttpCookie(String name,String value,String domain,String path,Integer maxAge) {
    	this.name = name;
    	this.value = value;
    	this.domain = domain;
    	this.path = path;
    	if(maxAge!=null)this.maxAge = maxAge.longValue();
	}
    
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public Long getMaxAge() {
		return maxAge;
	}
	public void setMaxAge(Long maxAge) {
		this.maxAge = maxAge;
	}
	
	public Boolean getIsHttpOnly() {
		return isHttpOnly;
	}

	public void setIsHttpOnly(Boolean isHttpOnly) {
		this.isHttpOnly = isHttpOnly;
	}
	
	public Integer getVersion() {
		return version;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	public Boolean getSecure() {
		return secure;
	}

	public void setSecure(Boolean secure) {
		this.secure = secure;
	}

	@Override
	public String toString() {
		return "HttpCookie [name=" + name
				+ ", value=" + value
				+ ", domain=" + domain
				+ ", path=" + path
				+ ", maxAge=" + maxAge
				+ ", isHttpOnly=" + isHttpOnly
				+ ", version=" + version
				+ ", secure=" + secure
				+ "]";
	}
}
