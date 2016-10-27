package com.microsoft.azure.documentdb;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

/**
 * Represents a document.
 * <p>
 * A document is a structured JSON document. There is no set schema for the JSON documents, and a document may contain
 * any number of custom properties as well as an optional list of attachments. Document is an application resource and
 * can be authorized using the master key or resource keys.
 */
public class Document extends Resource {
    /**
     * Initialize a document object.
     */
    public Document() {
        super();
    }

    /**
     * Initialize a document object from json string.
     *
     * @param jsonString the json string that represents the document object.
     */
    public Document(String jsonString) {
        super(jsonString);
    }

    /**
     * Initialize a document object from json object.
     *
     * @param jsonObject the json object that represents the document object.
     */
    public Document(JSONObject jsonObject) {
        super(jsonObject);
    }

    static Document FromObject(Object document) {
        Document typedDocument;
        if (document instanceof Document) {
            typedDocument = (Document) document;
        } else {
            ObjectMapper mapper = new ObjectMapper();
            try {
                return new Document(mapper.writeValueAsString(document));
            } catch (IOException e) {
                throw new IllegalArgumentException("Can't serialize the object into the json string", e);
            }
        }
        return typedDocument;
    }

    /**
     * Gets the document's time-to-live value.
     *
     * @return the document's time-to-live value in seconds.
     */
    public Integer getTimeToLive() {
        if (super.has(Constants.Properties.TTL)) {
            return super.getInt(Constants.Properties.TTL);
        }

        return null;
    }

    /**
     * Sets the document's time-to-live value.
     * <p>
     * A document's time-to-live value is an optional property. If set, the document expires after the specified number
     * of seconds since its last write time. The value of this property should be one of the following:
     * <p>
     * null - indicates the time-to-live value for this document inherits from the parent collection's default time-to-live value.
     * <p>
     * nonzero positive integer - indicates the number of seconds before the document expires. It overrides the default time-to-live
     * value specified on the parent collection, unless the parent collection's default time-to-live is null.
     * <p>
     * -1 - indicates the document never expires. It overrides the default time-to-live
     * value specified on the parent collection, unless the parent collection's default time-to-live is null.
     *
     * @param timeToLive the document's time-to-live value in seconds.
     */
    public void setTimeToLive(Integer timeToLive) {
        // a "null" value is represented as a missing element on the wire.
        // setting timeToLive to null should remove the property from the property bag.
        if (timeToLive != null) {
            super.set(Constants.Properties.TTL, timeToLive);
        } else if (super.has(Constants.Properties.TTL)) {
            super.remove(Constants.Properties.TTL);
        }
    }
}
