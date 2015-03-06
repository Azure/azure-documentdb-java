package com.microsoft.azure.documentdb;

/**
 * The user agent object, which is used to track the version of the SDK. 
 */
class UserAgentContainer {

    private static final String BASE_USER_AGENT = HttpConstants.Versions.USER_AGENT;
    private static final int MAX_SUFFIX_LENGTH = 64;
    private String suffix;
    private String userAgent;
    
    UserAgentContainer() {
        this.suffix = "";
        this.userAgent = BASE_USER_AGENT;
    }

    public void setSuffix(String suffix) {
        if(suffix.length() > MAX_SUFFIX_LENGTH) {
            suffix = suffix.substring(0, MAX_SUFFIX_LENGTH);
        }
       
        this.suffix = suffix;
        this.userAgent = BASE_USER_AGENT.concat(this.suffix);
    }

    public String getSuffix() {
        return this.suffix;
    }
    
    String getUserAgent() {
        return this.userAgent;
    }
}
