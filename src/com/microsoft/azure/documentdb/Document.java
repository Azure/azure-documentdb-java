package com.microsoft.azure.documentdb;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

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
                return new Document (mapper.writeValueAsString(document));
            } catch (IOException e) {
                throw new IllegalArgumentException("Can't serialize the object into the json string", e);
            }
        }
        return typedDocument;
    }
}
