package com.microsoft.azure.documentdb;

import java.util.*;

public final class SqlQuerySpec extends JsonSerializable {

    private SqlParameterCollection parameters;

    /**
     * Initializes a new instance of the SqlQuerySpec class.
     */
    public SqlQuerySpec() {
        super();
    }

    /**
     * Initializes a new instance of the SqlQuerySpec class with the text of the
     * query.
     * 
     * @param queryText
     *            the query text.
     */
    public SqlQuerySpec(String queryText) {
        super();
        this.setQueryText(queryText);
    }

    /**
     * Initializes a new instance of the SqlQuerySpec class with the text of the
     * query and parameters.
     * 
     * @param queryText  the query text.
     * @param parameters the query parameters.
     */
    public SqlQuerySpec(String queryText, SqlParameterCollection parameters) {
        super();
        this.setQueryText(queryText);
        this.parameters = parameters;
    }

    /**
     * Gets the text of the query.
     * 
     * @return the query text.
     */
    public String getQueryText() {
        return super.getString("query");
    }

    /**
     * Sets the text of the query.
     * 
     * @param queryText
     *            the query text.
     */
    public void setQueryText(String queryText) {
        super.set("query", queryText);
    }

    /**
     * Gets the collection of query parameters.
     * 
     * @return the query parameters.
     */
    public SqlParameterCollection getParameters() {
        if (this.parameters == null) {
            Collection<SqlParameter> sqlParameters = super.getCollection("parameters", SqlParameter.class);
            if (sqlParameters == null) {
                sqlParameters = new ArrayList<SqlParameter>();
            }

            this.parameters = new SqlParameterCollection(sqlParameters);
        }

        return this.parameters;
    }

    /**
     * Sets the collection of query parameters.
     * 
     * @param parameters
     *            the query parameters.
     */
    public void setParameters(SqlParameterCollection parameters) {
        this.parameters = parameters;
    }

    @Override
    void onSave() {
        boolean defaultParameters = (this.parameters != null && this.parameters.size() != 0);

        if (defaultParameters) {
            super.set("parameters", this.parameters);
        } else {
            super.remove("parameters");
        }
    }
}
