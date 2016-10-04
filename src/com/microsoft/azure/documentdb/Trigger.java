package com.microsoft.azure.documentdb;

import org.apache.commons.lang3.text.WordUtils;
import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

/**
 * Represents a trigger.
 * <p>
 * DocumentDB supports pre and post triggers defined in JavaScript to be executed on creates, updates and deletes. For
 * additional details, refer to the server-side JavaScript API documentation.
 */
public class Trigger extends Resource {

    /**
     * Constructor.
     */
    public Trigger() {
        super();
    }

    /**
     * Constructor.
     *
     * @param jsonString the json string that represents the trigger.
     */
    public Trigger(String jsonString) {
        super(jsonString);
    }

    /**
     * Constructor.
     *
     * @param jsonObject the json object that represents the trigger.
     */
    public Trigger(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Get the body of the trigger.
     *
     * @return the body of the trigger.
     */
    public String getBody() {
        return super.getString(Constants.Properties.BODY);
    }

    /**
     * Set the body of the trigger.
     *
     * @param body the body of the trigger.
     */
    public void setBody(String body) {
        super.set(Constants.Properties.BODY, body);
    }

    /**
     * Get the type of the trigger.
     *
     * @return the trigger type.
     */
    public TriggerType getTriggerType() {
        TriggerType result = TriggerType.Pre;
        try {
            result = TriggerType.valueOf(
                    WordUtils.capitalize(super.getString(Constants.Properties.TRIGGER_TYPE)));
        } catch (IllegalArgumentException e) {
            // ignore the exception and return the default
            this.getLogger().warning(
                    String.format("Invalid triggerType value %s.", super.getString(Constants.Properties.TRIGGER_TYPE)));
        }
        return result;
    }

    /**
     * Set the type of the resource.
     *
     * @param triggerType the trigger type.
     */
    public void setTriggerType(TriggerType triggerType) {
        super.set(Constants.Properties.TRIGGER_TYPE, triggerType.name());
    }

    /**
     * Get the operation type of the trigger.
     *
     * @return the trigger operation.
     */
    public TriggerOperation getTriggerOperation() {
        TriggerOperation result = TriggerOperation.Create;
        try {
            result = TriggerOperation.valueOf(
                    WordUtils.capitalize(super.getString(Constants.Properties.TRIGGER_OPERATION)));
        } catch (IllegalArgumentException e) {
            // ignore the exception and return the default
            this.getLogger().warning(
                    String.format("Invalid triggerOperation value %s.", super.getString(Constants.Properties.TRIGGER_OPERATION)));
        }
        return result;
    }

    /**
     * Set the operation type of the trigger.
     *
     * @param triggerOperation the trigger operation.
     */
    public void setTriggerOperation(TriggerOperation triggerOperation) {
        super.set(Constants.Properties.TRIGGER_OPERATION, triggerOperation.name());
    }
}
