package com.microsoft.azure.documentdb;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.OfferV2;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import com.microsoft.azure.documentdb.SqlQuerySpec;

public class JavaOfferTests extends GatewayTestBase {

	@Test
	public void testOfferReadAndQuery() throws DocumentClientException {
		List<Offer> offerList = this.client.readOffers(null).getQueryIterable().toList();
		int originalOffersCount = offerList.size();
		Offer expectedOffer = null;

		String trimmedCollectionLink = StringUtils
				.removeEnd(StringUtils.removeStart(this.collectionForTest.getSelfLink(), "/"), "/");

		for (Offer offer : offerList) {
			String trimmedOfferResourceLink = StringUtils
					.removeEnd(StringUtils.removeStart(offer.getResourceLink(), "/"), "/");
			if (trimmedOfferResourceLink.equals(trimmedCollectionLink)) {
				expectedOffer = offer;
				break;
			}
		}
		// There is an offer for the test collection we have created
		Assert.assertNotNull(expectedOffer);

		this.validateOfferResponseBody(expectedOffer, null);

		// Read the offer
		Offer readOffer = client.readOffer(expectedOffer.getSelfLink()).getResource();
		this.validateOfferResponseBody(readOffer, expectedOffer.getOfferType());
		// Check if the read resource is what we expect
		Assert.assertEquals(expectedOffer.getId(), readOffer.getId());
		Assert.assertEquals(expectedOffer.getResourceId(), readOffer.getResourceId());
		Assert.assertEquals(expectedOffer.getSelfLink(), readOffer.getSelfLink());
		Assert.assertEquals(expectedOffer.getResourceLink(), readOffer.getResourceLink());

		// Query for the offer.
		List<Offer> queryResultList = client
				.queryOffers(new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
						new SqlParameterCollection(new SqlParameter("@id", expectedOffer.getId()))), null)
				.getQueryIterable().toList();

		// We should find only one offer with the given id
		Assert.assertEquals(queryResultList.size(), 1);
		Offer oneQueryResult = queryResultList.get(0);

		String trimmedOfferResourceLink = StringUtils
				.removeEnd(StringUtils.removeStart(oneQueryResult.getResourceLink(), "/"), "/");
		Assert.assertTrue(trimmedCollectionLink.equals(trimmedOfferResourceLink));

		this.validateOfferResponseBody(oneQueryResult, expectedOffer.getOfferType());
		// Check if the query result is what we expect
		Assert.assertEquals(expectedOffer.getId(), oneQueryResult.getId());
		Assert.assertEquals(expectedOffer.getResourceId(), oneQueryResult.getResourceId());
		Assert.assertEquals(expectedOffer.getSelfLink(), oneQueryResult.getSelfLink());
		Assert.assertEquals(expectedOffer.getResourceLink(), oneQueryResult.getResourceLink());

		// Modify the SelfLink
		String offerLink = expectedOffer.getSelfLink().substring(0, expectedOffer.getSelfLink().length() - 1) + "x";

		// Read the offer
		try {
			readOffer = client.readOffer(offerLink).getResource();
			Assert.fail("Expected an exception when reading offer with bad offer link");
		} catch (DocumentClientException ex) {
			Assert.assertEquals(400, ex.getStatusCode());
		}

		client.deleteCollection(this.collectionForTest.getSelfLink(), null);

		// Now try to get the read the offer after the collection is deleted
		try {
			client.readOffer(expectedOffer.getSelfLink()).getResource();
			Assert.fail("Expected an exception when reading deleted offer");
		} catch (DocumentClientException ex) {
			Assert.assertEquals(404, ex.getStatusCode());
		}

		// Make sure read offers returns one offer less that the original list
		// of offers
		offerList = client.readOffers(null).getQueryIterable().toList();
		Assert.assertEquals(originalOffersCount - 1, offerList.size());
	}

	@Test
	public void testOfferReplace() throws DocumentClientException {
		List<Offer> offerList = this.client.readOffers(null).getQueryIterable().toList();
		Offer expectedOffer = null;

		String trimmedCollectionLink = StringUtils
				.removeEnd(StringUtils.removeStart(this.collectionForTest.getSelfLink(), "/"), "/");

		for (Offer offer : offerList) {
			String trimmedOfferResourceLink = StringUtils
					.removeEnd(StringUtils.removeStart(offer.getResourceLink(), "/"), "/");
			if (trimmedOfferResourceLink.equals(trimmedCollectionLink)) {
				expectedOffer = offer;
				break;
			}
		}
		Assert.assertNotNull(expectedOffer);

		this.validateOfferResponseBody(expectedOffer, null);
		Offer offerToReplace = new Offer(expectedOffer.toString());

		// Modify the offer
		offerToReplace.setOfferType("S2");

		// Replace the offer
		Offer replacedOffer = client.replaceOffer(offerToReplace).getResource();
		this.validateOfferResponseBody(replacedOffer, "S2");

		// Check if the replaced offer is what we expect
		Assert.assertEquals(offerToReplace.getId(), replacedOffer.getId());
		Assert.assertEquals(offerToReplace.getResourceId(), replacedOffer.getResourceId());
		Assert.assertEquals(offerToReplace.getSelfLink(), replacedOffer.getSelfLink());
		Assert.assertEquals(offerToReplace.getResourceLink(), replacedOffer.getResourceLink());

		offerToReplace.setResourceId("NotAllowed");
		try {
			client.replaceOffer(offerToReplace).getResource();
			Assert.fail("Expected an exception when replaceing an offer with bad id");
		} catch (DocumentClientException ex) {
			Assert.assertEquals(400, ex.getStatusCode());
		}

		offerToReplace.setResourceId("InvalidRid");
		try {
			client.replaceOffer(offerToReplace).getResource();
			Assert.fail("Expected an exception when replaceing an offer with bad Rid");
		} catch (DocumentClientException ex) {
			Assert.assertEquals(400, ex.getStatusCode());
		}

		offerToReplace.setId(null);
		offerToReplace.setResourceId(null);
		try {
			client.replaceOffer(offerToReplace).getResource();
			Assert.fail("Expected an exception when replaceing an offer with null id and rid");
		} catch (DocumentClientException ex) {
			Assert.assertEquals(400, ex.getStatusCode());
		}
	}

	@Test
	public void testCreateCollectionWithOfferType() throws DocumentClientException {
		// Create a new collection of offer type S2.
		DocumentCollection collectionDefinition = new DocumentCollection();
		collectionDefinition.setId(GatewayTests.getUID());
		RequestOptions requestOptions = new RequestOptions();
		requestOptions.setOfferType("S2");
		client.createCollection(this.databaseForTest.getSelfLink(), collectionDefinition, requestOptions);

		// We should have an offer of type S2.
		List<Offer> offerList = this.client.readOffers(null).getQueryIterable().toList();
		boolean hasS2 = false;
		for (Offer offer : offerList) {
			if (offer.getOfferType().equals("S2")) {
				hasS2 = true;
				break;
			}
		}
		Assert.assertTrue("There should be an offer of type S2.", hasS2);
	}

	@Test
	public void testCreateCollectionWithOfferThroughput() throws DocumentClientException {
		DocumentCollection collectionDefinition = new DocumentCollection();
		collectionDefinition.setId(GatewayTests.getUID());

		Integer throughput = 10000;
		RequestOptions options = new RequestOptions();
		options.setOfferThroughput(throughput);

		DocumentCollection createdCollection = client
				.createCollection(getDatabaseLink(this.databaseForTest, true), collectionDefinition, options)
				.getResource();

		Assert.assertNotNull(createdCollection);

		// We should have an offer of version V2 with the correct throughput.
		List<Offer> offerList = this.client.readOffers(null).getQueryIterable().toList();
		Offer v2Offer = null;
		for (Offer offer : offerList) {
			if (offer.getOfferVersion().equalsIgnoreCase("V2")) {
				if (offer.getOfferResourceId().equalsIgnoreCase(createdCollection.getResourceId())) {
					v2Offer = offer;
				}
			}
		}

		Assert.assertNotNull(v2Offer);
		JSONObject content = v2Offer.getContent();
		Assert.assertNotNull(content);
		Assert.assertEquals(throughput, (Integer) content.getInt("offerThroughput"));
		Assert.assertEquals(v2Offer.getResourceLink(), createdCollection.getSelfLink());
	}

	@Test
	public void testCreateMultiPartitionCollectionWithOfferThroughput() throws DocumentClientException {
		String collectionId = GatewayTests.getUID();
		PartitionKeyDefinition partitionKeyDef = new PartitionKeyDefinition();
		ArrayList<String> paths = new ArrayList<String>();
		paths.add("/id");
		partitionKeyDef.setPaths(paths);

		Integer throughput = 50000;
		RequestOptions options = new RequestOptions();
		options.setOfferThroughput(throughput);

		DocumentCollection collectionDefinition = new DocumentCollection();
		collectionDefinition.setId(collectionId);
		collectionDefinition.setPartitionKey(partitionKeyDef);
		DocumentCollection createdCollection = this.client
				.createCollection(getDatabaseLink(this.databaseForTest, true), collectionDefinition, options)
				.getResource();

		Assert.assertNotNull(createdCollection);

		// We should have an offer of version V2 with the correct throughput.
		List<Offer> offerList = this.client.readOffers(null).getQueryIterable().toList();
		Offer v2Offer = null;
		for (Offer offer : offerList) {
			if (offer.getOfferVersion().equalsIgnoreCase("V2")
					&& offer.getOfferResourceId().equalsIgnoreCase(createdCollection.getResourceId())) {
				v2Offer = offer;
			}
		}

		Assert.assertNotNull(v2Offer);
		JSONObject content = v2Offer.getContent();
		Assert.assertNotNull(content);
		Assert.assertEquals(throughput, (Integer) content.getInt("offerThroughput"));
		Assert.assertEquals(v2Offer.getResourceLink(), createdCollection.getSelfLink());
	}

	@Test
	public void testChangeOfferVersion() throws DocumentClientException {
		String collectionId = GatewayTests.getUID();

		RequestOptions options = new RequestOptions();
		options.setOfferType("S1");

		DocumentCollection collectionDefinition = new DocumentCollection();
		collectionDefinition.setId(collectionId);
		DocumentCollection createdCollection = this.client
				.createCollection(getDatabaseLink(this.databaseForTest, true), collectionDefinition, options)
				.getResource();

		List<Offer> offerList = this.client.readOffers(null).getQueryIterable().toList();
		Offer v1Offer = null;
		for (Offer offer : offerList) {
			if (offer.getOfferVersion().equalsIgnoreCase("V1") && offer.getOfferType().equalsIgnoreCase("S1")
					&& offer.getOfferResourceId().equalsIgnoreCase(createdCollection.getResourceId())) {
				v1Offer = offer;
			}
		}

		Assert.assertNotNull(v1Offer);

		int throughput = 10000;
		OfferV2 v2Offer = new OfferV2(v1Offer);
		v2Offer.setOfferThroughput(throughput);
		Offer replacedOffer = client.replaceOffer(v2Offer).getResource();
		v2Offer = new OfferV2(replacedOffer);
		JSONObject content = replacedOffer.getContent();
		Assert.assertNotNull(content);
		Assert.assertEquals(throughput, content.getInt("offerThroughput"));
		Assert.assertEquals(throughput, v2Offer.getOfferThroughput());

		Offer v1NewOffer = new Offer(v2Offer);
		v1NewOffer.setOfferType("S3");
		replacedOffer = client.replaceOffer(v1NewOffer).getResource();
		Assert.assertEquals("V1", replacedOffer.getOfferVersion());
		Assert.assertEquals("S3", replacedOffer.getOfferType());
	}

	private void validateOfferResponseBody(Offer offer, String expectedOfferType) {
		Assert.assertNotNull("Id cannot be null", offer.getId());
		Assert.assertNotNull("Resource Id (Rid) cannot be null", offer.getResourceId());
		Assert.assertNotNull("Self link cannot be null", offer.getSelfLink());
		Assert.assertNotNull("Resource Link cannot be null", offer.getResourceLink());
		Assert.assertTrue("Offer id not contained in offer self link", offer.getSelfLink().contains(offer.getId()));

		if (expectedOfferType != null) {
			Assert.assertEquals(expectedOfferType, offer.getOfferType());
		}
	}
}
