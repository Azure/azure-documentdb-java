package com.microsoft.azure.documentdb;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

/**
 * Represents a document attachment.
 * <p>
 * Each document may contain zero or more attachemnts. Attachments can be of any MIME type - text, image, binary data.
 * These are stored externally in Azure Blob storage. Attachments are automatically deleted when the parent document
 * is deleted.
 */
public class Attachment extends Resource {
    /**
     * Initialize an attachment object.
     */
    public Attachment() {
        super();
    }

    /**
     * Initialize an attachment object from json string.
     *
     * @param source the json string representation of the Attachment.
     */
    public Attachment(String source) {
        super(source);
    }

    /**
     * Initialize an attachment object from json object.
     *
     * @param jsonObject the json object representation of the Attachment.
     */
    public Attachment(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Gets the MIME content type of the attachment.
     *
     * @return the content type.
     */
    public String getContentType() {
        return super.getString(Constants.Properties.CONTENT_TYPE);
    }

    /**
     * Sets the MIME content type of the attachment.
     *
     * @param contentType the content type to use.
     */
    public void setContentType(String contentType) {
        super.set(Constants.Properties.CONTENT_TYPE, contentType);
    }

    /**
     * Gets the media link associated with the attachment content.
     *
     * @return the media link.
     */
    public String getMediaLink() {
        return super.getString(Constants.Properties.MEDIA_LINK);
    }

    /**
     * Sets the media link associated with the attachment content.
     *
     * @param mediaLink the media link to use.
     */
    public void setMediaLink(String mediaLink) {
        super.set(Constants.Properties.MEDIA_LINK, mediaLink);
    }
}
