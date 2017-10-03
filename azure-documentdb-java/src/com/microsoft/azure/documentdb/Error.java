package com.microsoft.azure.documentdb;

/**
 * Encapsulates error related details.
 */
public class Error extends Resource {
    /**
     * Initialize a Error object from json string.
     * 
     * @param jsonString the jsonString that represents the error.
     */
    public Error(String jsonString) {
        super(jsonString);
    }

    /**
     * Gets the error code.
     * 
     * @return the error code.
     */
    public String getCode() {
        return super.getString(Constants.Properties.CODE);
    }

    /**
     * Gets the error message.
     * 
     * @return the error message.
     */
    public String getMessage() {
        return super.getString(Constants.Properties.MESSAGE);
    }

    /**
     * Gets the error details.
     * 
     * @return the error details.
     */
    public String getErrorDetails() {
        return super.getString(Constants.Properties.ERROR_DETAILS);
    }
}
