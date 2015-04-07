package com.microsoft.azure.documentdb;

import org.json.JSONObject;

/**
 * Represents the offer for a resource (collection).
 * 
 */
public final class Offer extends Resource {
    /**
     * Initialize an offer object.
     */
    public Offer() {
        super();
    }

    /**
     * Initialize an offer object from json string.
     * 
     * @param jsonString the json string that represents the offer.
     */
    public Offer(String jsonString) {
        super(jsonString);
    }

    /**
     * Initialize an offer object from json object.
     * 
     * @param jsonObject the json object that represents the offer.
     */
    public Offer(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Gets the self-link of a resource to which the resource offer applies.
     * 
     * @return the resource link.
     */
    public String getResourceLink() {
        return super.getString(Constants.Properties.RESOURCE_LINK);
    }

    /**
     * Sets the self-link of a resource to which the resource offer applies.
     * 
     * @param resourceLink the resource link.
     */
    void setResourceLink(String resourceLink) {
        super.set(Constants.Properties.RESOURCE_LINK, resourceLink);
    }

    /**
     * Gets the OfferType for the resource offer.
     * 
     * @return the offer type.
     */
    public String getOfferType() {
        return super.getString(Constants.Properties.OFFER_TYPE);
    }

    /**
     * Sets the OfferType for the resource offer.
     * 
     * @param offerType the offer type.
     */
    public void setOfferType(String offerType) {
        super.set(Constants.Properties.OFFER_TYPE, offerType);
    }
}