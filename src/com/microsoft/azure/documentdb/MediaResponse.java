package com.microsoft.azure.documentdb;

import java.io.InputStream;
import java.util.Map;

/**
 * Response associated with retrieving attachment content.
 */
public final class MediaResponse {
    private InputStream media = null;
    private Map<String, String> responseHeaders = null;

    MediaResponse(DocumentServiceResponse response) {
        this.media = response.getContentStream();
        this.responseHeaders = response.getResponseHeaders();
    }

    /**
     * Gets the attachment content stream.
     * 
     * @return the attachment content stream.
     */
    public InputStream getMedia() {
        return this.media;
    }

    /**
     * Gets the headers associated with the response.
     * 
     * @return the response headers.
     */
    public Map<String, String> getResponseHeaders() {
        return this.responseHeaders;
    }
}
