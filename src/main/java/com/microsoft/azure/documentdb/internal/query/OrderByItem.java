package com.microsoft.azure.documentdb.internal.query;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.*;

public final class OrderByItem extends JsonSerializable {
    private Object item;

    public OrderByItem(String jsonString) {
        super(jsonString);
    }

    public OrderByItem(JSONObject jsonObject) {
        super(jsonObject);
    }

    public Object getItem() {
        if (this.item == null) {
            Object rawItem = super.get("item");
            this.item = rawItem == null ? Undefined.Value() : rawItem;
        }

        return this.item;
    }
}
