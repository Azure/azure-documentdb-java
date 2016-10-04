package com.microsoft.azure.documentdb;

public final class SqlParameter extends JsonSerializable {


    /**
     * Initializes a new instance of the SqlParameter class.
     */
    public SqlParameter() {
        super();
    }

    /**
     * Initializes a new instance of the SqlParameter class with the name and value of the parameter.
     *
     * @param name  the name of the parameter.
     * @param value the value of the parameter.
     */
    public SqlParameter(String name, Object value) {
        super();
        this.setName(name);
        this.setValue(value);
    }

    /**
     * Gets the name of the parameter.
     *
     * @return the name of the parameter.
     */
    public String getName() {
        return super.getString("name");
    }

    /**
     * Sets the name of the parameter.
     *
     * @param name the name of the parameter.
     */
    public void setName(String name) {
        super.set("name", name);
    }

    /**
     * Gets the value of the parameter.
     *
     * @param c    the class of the parameter value.
     * @param <T>  the type of the parameter
     * @return     the value of the parameter.
     */
    public <T extends Object> Object getValue(Class<T> c) {
        return super.getObject("value", c);
    }

    /**
     * Sets the value of the parameter.
     *
     * @param value the value of the parameter.
     */
    public void setValue(Object value) {
        super.set("value", value);
    }
}
