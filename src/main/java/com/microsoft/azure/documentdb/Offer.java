package com.microsoft.azure.documentdb;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

/**
 * Represents the offer for a resource (collection).
 */
public class Offer extends Resource {
    /**
     * Initialize an offer object.
     */
    public Offer() {
        super();
        this.setOfferVersion(Constants.Properties.OFFER_VERSION_V1);
    }

    /**
     * Initialize an offer object and copy all properties from the other offer.
     *
     * @param otherOffer the Offer object whose properties to copy over.
     */
    public Offer(Offer otherOffer) {
        super();
        String serializedString = otherOffer.toString();
        this.propertyBag = new Offer(serializedString).propertyBag;
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
     * Sets the target resource id of a resource to which this offer applies.
     *
     * @return the resource id.
     */
    public String getOfferResourceId() {
        return super.getString(Constants.Properties.OFFER_RESOURCE_ID);
    }

    /**
     * Sets the target resource id of a resource to which this offer applies.
     *
     * @param resourceId the resource id.
     */
    void setOfferResourceId(String resourceId) {
        super.set(Constants.Properties.OFFER_RESOURCE_ID, resourceId);
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
        if (StringUtils.isNotEmpty(offerType)) {
            // OfferType is only supported for V2 offers.
            this.setOfferVersion(Constants.Properties.OFFER_VERSION_V1);
        }
    }

    /**
     * Gets the version of the current offer.
     *
     * @return the offer version.
     */
    public String getOfferVersion() {
        return super.getString(Constants.Properties.OFFER_VERSION);
    }

    /**
     * Sets the offer version.
     *
     * @param offerVersion the version of the offer.
     */
    public void setOfferVersion(String offerVersion) {
        super.set(Constants.Properties.OFFER_VERSION, offerVersion);
    }

    /**
     * Gets the content object that contains the details of the offer.
     *
     * @return the offer content.
     */
    public JSONObject getContent() {
        return super.getObject(Constants.Properties.OFFER_CONTENT);
    }

    /**
     * Sets the offer content that contains the details of the offer.
     *
     * @param offerContent the content object.
     */
    public void setContent(JSONObject offerContent) {
        super.set(Constants.Properties.OFFER_CONTENT, offerContent);
    }
}