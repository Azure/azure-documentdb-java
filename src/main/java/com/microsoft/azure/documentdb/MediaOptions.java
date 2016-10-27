package com.microsoft.azure.documentdb;

/**
 * Options used with attachment content (aka media) creation.
 */
public final class MediaOptions {
    private String slug;
    private String contentType;

    /**
     * Gets the HTTP Slug header value.
     *
     * @return the slug.
     */
    public String getSlug() {
        return this.slug;
    }

    /**
     * Sets the HTTP Slug header value.
     *
     * @param slug the slug.
     */
    public void setSlug(String slug) {
        this.slug = slug;
    }

    /**
     * Gets the HTTP ContentType header value.
     *
     * @return the content type.
     */
    public String getContentType() {
        return this.contentType;
    }

    /**
     * Sets the HTTP ContentType header value.
     *
     * @param contentType the content type.
     */
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }
}
