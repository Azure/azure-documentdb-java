package com.microsoft.azure.documentdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public final class SqlParameterCollection implements Collection<SqlParameter> {

    private List<SqlParameter> parameters;

    /**
     * Initializes a new instance of the SqlParameterCollection class.
     */
    public SqlParameterCollection() {
        this.parameters = new ArrayList<SqlParameter>();
    }

    /**
     * Initializes a new instance of the SqlParameterCollection class from an array of parameters.
     *
     * @param parameters the array of parameters.
     */
    public SqlParameterCollection(SqlParameter... parameters) {
        if (parameters == null) {
            throw new IllegalArgumentException("parameters");
        }

        this.parameters = Arrays.asList(parameters);
    }

    /**
     * Initializes a new instance of the SqlParameterCollection class from a collection of parameters.
     *
     * @param parameters the collection of parameters.
     */
    public SqlParameterCollection(Collection<SqlParameter> parameters) {
        if (parameters == null) {
            throw new IllegalArgumentException("parameters");
        }

        this.parameters = new ArrayList<SqlParameter>(parameters);
    }

    @Override
    public boolean add(SqlParameter parameter) {
        return this.parameters.add(parameter);
    }

    @Override
    public boolean addAll(Collection<? extends SqlParameter> parameters) {
        return this.parameters.addAll(parameters);
    }

    @Override
    public void clear() {
        this.parameters.clear();
    }

    @Override
    public boolean contains(Object parameter) {
        return this.parameters.contains(parameter);
    }

    @Override
    public boolean containsAll(Collection<?> parameters) {
        return this.parameters.containsAll(parameters);
    }

    @Override
    public boolean isEmpty() {
        return this.parameters.isEmpty();
    }

    @Override
    public Iterator<SqlParameter> iterator() {
        return this.parameters.iterator();
    }

    @Override
    public boolean remove(Object parameter) {
        return this.parameters.remove(parameter);
    }

    @Override
    public boolean removeAll(Collection<?> parameters) {
        return this.parameters.removeAll(parameters);
    }

    @Override
    public boolean retainAll(Collection<?> parameters) {
        return this.parameters.retainAll(parameters);
    }

    @Override
    public int size() {
        return this.parameters.size();
    }

    @Override
    public Object[] toArray() {
        return this.parameters.toArray();
    }

    @Override
    public <T> T[] toArray(T[] parameters) {
        return this.parameters.toArray(parameters);
    }
}
