package com.microsoft.azure.documentdb;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

public class OfferV2 extends Offer {

    /**
     * Initialize an new instance of the OfferV2 object.
     *
     * @param offerThroughput the throughput value for this offer.
     */
    public OfferV2(int offerThroughput) {
        this.setOfferVersion(Constants.Properties.OFFER_VERSION_V2);
        this.setOfferType("");
        JSONObject content = new JSONObject();
        content.put(Constants.Properties.OFFER_THROUGHPUT, offerThroughput);
        this.setContent(content);
    }

    /**
     * Initialize an new instance of the OfferV2 object, copy the base
     * properties from another Offer object and set the throughput value.
     *
     * @param otherOffer the Offer object whose base properties are to be copied.
     */
    public OfferV2(Offer otherOffer) {
        super(otherOffer);

        this.setOfferVersion(Constants.Properties.OFFER_VERSION_V2);
        this.setOfferType("");

        JSONObject content = this.getContent();
        if (content == null) {
            content = new JSONObject();
            this.setContent(content);
        }
    }

    /**
     * Gets the offer throughput for this offer.
     *
     * @return the offer throughput.
     */
    public int getOfferThroughput() {
        return this.getContent().getInt(Constants.Properties.OFFER_THROUGHPUT);
    }

    /**
     * Sets the offer throughput for this offer.
     *
     * @param throughput the throughput of this offer.
     */
    public void setOfferThroughput(int throughput) {
        this.getContent().put(Constants.Properties.OFFER_THROUGHPUT, throughput);
    }
}
