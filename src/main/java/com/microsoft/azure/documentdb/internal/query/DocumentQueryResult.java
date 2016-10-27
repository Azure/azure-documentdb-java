package com.microsoft.azure.documentdb.internal.query;

import java.util.*;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.*;

public final class DocumentQueryResult extends Document {
    private List<OrderByItem> orderByItems;
    private Document payload;

    public DocumentQueryResult(String jsonString) {
        super(jsonString);
    }

    public DocumentQueryResult(JSONObject jsonObject) {
        super(jsonObject);
    }

    public List<OrderByItem> getOrderByItems() {
        return this.orderByItems != null ? this.orderByItems
                : (this.orderByItems = (List<OrderByItem>) super.getCollection("orderByItems", OrderByItem.class));
    }

    public Document getPayload() {
        return this.payload != null ? this.payload : (this.payload = super.getObject("payload", Document.class));
    }
}