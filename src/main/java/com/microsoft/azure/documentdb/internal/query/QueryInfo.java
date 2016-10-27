package com.microsoft.azure.documentdb.internal.query;

import java.util.Collection;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import com.microsoft.azure.documentdb.JsonSerializable;

public final class QueryInfo extends JsonSerializable {
    private Integer top;
    private Collection<SortOrder> orderBy;
    private String rewrittenQuery;

    public QueryInfo() { }

    public QueryInfo(String jsonString) {
        super(jsonString);
    }

    public QueryInfo(JSONObject jsonObject) {
        super(jsonObject);
    }

    public Integer getTop() {
        return this.top != null ? this.top : (this.top = super.getInt("top"));
    }

    public Collection<SortOrder> getOrderBy() {
        return this.orderBy != null ? this.orderBy : (this.orderBy = super.getCollection("orderBy", SortOrder.class));
    }

    public String getRewrittenQuery() {
        return this.rewrittenQuery != null ? this.rewrittenQuery
                : (this.rewrittenQuery = super.getString("rewrittenQuery"));
    }

    public boolean hasTop() {
        return this.getTop() != null;
    }

    public boolean hasOrderBy() {
        Collection<SortOrder> orderBy = this.getOrderBy();
        return orderBy != null && orderBy.size() > 0;
    }

    public boolean hasRewrittenQuery() {
        return !StringUtils.isEmpty(this.getRewrittenQuery());
    }
}