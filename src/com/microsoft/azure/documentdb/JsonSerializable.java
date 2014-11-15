package com.microsoft.azure.documentdb;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;

class JsonSerializable {
    JSONObject propertyBag = null;

    JsonSerializable() {
        this.propertyBag = new JSONObject();
    }

    /**
     * Constructor.
     * 
     * @param jsonString the json string that represents the JsonSerializable.
     */
    JsonSerializable(String jsonString) {
        this.propertyBag = new JSONObject(jsonString);
    }

    /**
     * Constructor.
     * 
     * @param jsonObject the json object that represents the JsonSerializable.
     */
    JsonSerializable(JSONObject jsonObject) {
        this.propertyBag = new JSONObject(jsonObject);
    }

    /**
     * Returns the propertybag(JSONObject) in a hashMap
     * 
     * @return the HashMap. 
     */
    public HashMap<String, Object> getHashMap() {
        return JsonSerializable.toMap(this.propertyBag);
    }

    private static HashMap<String, Object> toMap(JSONObject object) throws JSONException {
        HashMap<String, Object> map = new HashMap<String, Object>();

        @SuppressWarnings("unchecked")  // Using legacy API
        Iterator<String> keysItr = object.keys();
        while(keysItr.hasNext()) {
            String key = keysItr.next();
            Object value = object.get(key);

            if(value instanceof JSONArray) {
                value = toList((JSONArray) value);
            }

            else if(value instanceof JSONObject) {
                value = toMap((JSONObject) value);
            }
            map.put(key, value);
        }
        return map;
    }

    private static List<Object> toList(JSONArray array) throws JSONException {
        List<Object> list = new ArrayList<Object>();
        for(int i = 0; i < array.length(); i++)
        {
            Object value = array.get(i);
            if(value instanceof JSONArray) {
                value = toList((JSONArray) value);
            }

            else if(value instanceof JSONObject) {
                value = toMap((JSONObject) value);
            }
            list.add(value);
        }
        return list;
    }

    /**
     * Checks whether a property exists.
     * 
     * @param propertyName the property to look up.
     * @return true if the property exists.
     */
    public boolean has(String propertyName) {
        return this.propertyBag.has(propertyName);
    }

    /**
     * Removes a value by propertyName.
     * 
     * @param propertyName the property to remove.
     */
    public void remove(String propertyName) {
        this.propertyBag.remove(propertyName);
    }

    /**
     * Sets the value of a property.
     * 
     * @param <T> the type of the object.
     * @param propertyName the property to set.
     * @param value, the value of the property.
     */
    public <T extends Object> void set(String propertyName, T value) {
        if (value instanceof Number || value instanceof Boolean || value instanceof String ||
                value instanceof JSONObject) {
            // JSONObject, number (includes int, float, double etc), boolean, and string.
            this.propertyBag.put(propertyName, value);
        } else if (value instanceof JsonSerializable) {
            // JsonSerializable
            JsonSerializable castedValue = (JsonSerializable)value;
            if (castedValue != null) {
                castedValue.onSave();
            }
            this.propertyBag.put(propertyName, castedValue != null ? castedValue.propertyBag : null);
        } else {
            // POJO
            ObjectMapper mapper = new ObjectMapper();
            try {
                this.propertyBag.put(propertyName, new JSONObject(mapper.writeValueAsString(value)));
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalArgumentException("Can't serialize the object into the json string", e);
            }
        }

    }

    /**
     * Sets the value of a property with a collection.
     * 
     * @param <T> the type of the objects in the collection.
     * @param propertyName the property to set.
     * @param collection the value of the property.
     */
    public <T> void set(String propertyName, Collection<T> collection) {
        if (this.propertyBag == null) {
            this.propertyBag = new JSONObject();
        }

        if (collection != null) {
            JSONArray jsonArray = new JSONArray();
            ObjectMapper mapper = null;

            for (T childValue : collection) {
                if (childValue instanceof Number || childValue instanceof Boolean || childValue instanceof String ||
                        childValue instanceof JSONObject) {
                    // JSONObject, Number (includes Int, Float, Double etc), Boolean, and String.
                    jsonArray.put(childValue);
                } else if (childValue instanceof JsonSerializable) {
                    // JsonSerializable
                    JsonSerializable castedValue = (JsonSerializable)childValue;
                    castedValue.onSave();
                    jsonArray.put(castedValue.propertyBag != null ? castedValue.propertyBag : new JSONObject());
                } else {
                    // POJO
                    if (mapper == null) mapper = new ObjectMapper();
                    try {
                        jsonArray.put(new JSONObject(mapper.writeValueAsString(childValue)));
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new IllegalArgumentException("Can't serialize the object into the json string", e);
                    }
                }
            }

            this.propertyBag.put(propertyName, jsonArray);
        }
    }

    /**
     * Gets a string value.
     * 
     * @param propertyName the property to get.
     * @return the string value.
     */
    public String getString(String propertyName) {
        if (this.has(propertyName)) {
            return this.propertyBag.getString(propertyName);
        } else {
            return null;
        }
    }

    /**
     * Gets a boolean value.
     * 
     * @param propertyName the property to get.
     * @return the boolean value.
     */
    public Boolean getBoolean(String propertyName) {
        if (this.has(propertyName)) {
            return new Boolean(this.propertyBag.getBoolean(propertyName));
        } else {
            return null;
        }
    }

    /**
     * Gets an integer value.
     * 
     * @param propertyName the property to get.
     * @return the boolean value
     */
    public Integer getInt(String propertyName) {
        if (this.has(propertyName)) {
            return new Integer(this.propertyBag.getInt(propertyName));
        } else {
            return null;
        }
    }

    /**
     * Gets a long value.
     * 
     * @param propertyName the property to get.
     * @return the long value
     */
    public Long getLong(String propertyName) {
        if (this.has(propertyName)) {
            return new Long(this.propertyBag.getLong(propertyName));
        } else {
            return null;
        }
    }

    /**
     * Gets a double value.
     * 
     * @param propertyName the property to get.
     * @return the double value.
     */
    public Double getDouble(String propertyName) {
        if (this.has(propertyName)) {
            return new Double(this.propertyBag.getDouble(propertyName));
        } else {
            return null;
        }
    }

    /**
     * Gets an object value.
     * 
     * @param <T> the type of the object.
     * @param propertyName the property to get.
     * @param c, the class of the object. If c is a POJO class, it must be a member (and not an anonymous or local)
     *     and a static one.
     * @return the object value.
     */
    public <T extends Object> T getObject(String propertyName, Class<T> c) {
        if (this.propertyBag.has(propertyName)) {
            JSONObject jsonObj = this.propertyBag.getJSONObject(propertyName);
            if (Number.class.isAssignableFrom(c) || String.class.isAssignableFrom(c) ||
                    Boolean.class.isAssignableFrom(c)) {
                // Number, String, Boolean
                return c.cast(jsonObj);
            } else if (JsonSerializable.class.isAssignableFrom(c)) {
                try {
                    return c.getConstructor(String.class).newInstance(
                        jsonObj.toString());
                } catch (InstantiationException | IllegalAccessException
                        | IllegalArgumentException | InvocationTargetException
                        | NoSuchMethodException | SecurityException e) {
                    throw new IllegalStateException(
                            "Failed to instantiate class object.", e);
                }
            } else {
                // POJO.
                if (!c.isMemberClass() || !Modifier.isStatic(c.getModifiers())) {
                    throw new IllegalArgumentException(
                            "c must be a member (not an anonymous or local) and static class.");
                }
                try {
                    return new ObjectMapper().readValue(jsonObj.toString(), c);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new IllegalStateException("Failed to get POJO.", e);
                }
            }
        }

        return null;
    }

    /**
     * Gets an object collection.
     * 
     * @param <T> the type of the objects in the collection.
     * @param propertyName the property to get
     * @param c the class of the object. If c is a POJO class, it must be a member (and not an anonymous or local)
     *     and a static one.
     * @return the object collection.
     */
    public <T extends Object> Collection<T> getCollection(String propertyName, Class<T> c) {
        if (this.propertyBag != null && this.propertyBag.has(propertyName)) {
            JSONArray jsonArray = this.propertyBag.getJSONArray(propertyName);
            Collection<T> result = new ArrayList<T>();
            ObjectMapper mapper = null;

            for (int i = 0; i < jsonArray.length(); i++) {
                if (Number.class.isAssignableFrom(c) || String.class.isAssignableFrom(c) ||
                        Boolean.class.isAssignableFrom(c)) {
                    // Number, String, Boolean 
                    result.add(c.cast(jsonArray.get(i)));
                } else if (JsonSerializable.class.isAssignableFrom(c)) {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    // JsonSerializable
                    try {
                        result.add(c.getConstructor(String.class).newInstance(jsonObject.toString()));
                    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException |
                            InvocationTargetException | NoSuchMethodException | SecurityException e) {
                        throw new IllegalStateException(
                                "Failed to instantiate class object.", e);
                    }
                } else {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    // POJO
                    if (mapper == null) {
                        mapper = new ObjectMapper();
                        // Checks once.
                        if (!c.isMemberClass() || !Modifier.isStatic(c.getModifiers())) {
                            throw new IllegalArgumentException(
                                    "c must be a member (not an anonymous or local) and static class.");
                        }
                    }
                    try {
                        result.add(mapper.readValue(jsonObject.toString(), c));
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new IllegalStateException("Failed to get POJO.", e);
                    }
                }
            }

            return result;
        }

        return null;
    }

    /**
     * Gets a JSONObject.
     * 
     * @param propertyName the property to get.
     * @return the JSONObject.
     */
    public JSONObject getObject(String propertyName) {
        if (this.propertyBag.has(propertyName)) {
            JSONObject jsonObj = this.propertyBag.getJSONObject(propertyName);
            return jsonObj;
        }
        return null;
    }

    /**
     * Gets a JSONObject collection.
     * 
     * @param propertyName the property to get.
     * @return the JSONObject collection.
     */
    public Collection<JSONObject> getCollection(String propertyName) {
        Collection<JSONObject> result = null;
        if (this.propertyBag != null && this.propertyBag.has(propertyName)) {
            result = new ArrayList<JSONObject>();
            JSONArray jsonArray = this.propertyBag.getJSONArray(propertyName);

            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                result.add(jsonObject);
            }
        }

        return result;    
    }

    void onSave() {
    }

    /**
     * Converts to an Object (only POJOs and JSONObject are supported).
     * 
     * @param <T> the type of the object.
     * @param c the class of the object, either a POJO class or JSONObject. If c is a POJO class, it must be a member
     *     (and not an anonymous or local) and a static one.
     * @return the POJO.
     */
    public <T extends Object> T toObject(Class<T> c) {
    	if (JsonSerializable.class.isAssignableFrom(c) || String.class.isAssignableFrom(c) ||
    			Number.class.isAssignableFrom(c) || Boolean.class.isAssignableFrom(c)) {
    		throw new IllegalArgumentException("c can only be a POJO class or JSONObject");
    	}
    	if (JSONObject.class.isAssignableFrom(c)) {
    		// JSONObject
    		if (JSONObject.class != c) {
    			throw new IllegalArgumentException("We support JSONObject but not its sub-classes.");
    		}
    		return c.cast(this.propertyBag);
    	} else {
    		// POJO
            if (!c.isMemberClass() || !Modifier.isStatic(c.getModifiers())) {
                throw new IllegalArgumentException(
                        "c must be a member (not an anonymous or local) and static class.");
            }
            try {
                return new ObjectMapper().readValue(this.toString(), c);
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalStateException("Failed to get POJO.", e);
            }
    	}
    }

    /**
     * Converts to a JSON string.
     * 
     * @return the JSON string.
     */
    public String toString() {
        this.onSave();
        return this.propertyBag.toString();
    }

    /**
     * Converts to a formatted JSON string.
     * 
     * @param indentFactor the indent factor.
     * @return the formatted JSON string.
     * @throws JSONException the json exception.
     */
    public String toString(int indentFactor) throws JSONException {
        this.onSave();
        return this.propertyBag.toString(indentFactor);
    }
}
