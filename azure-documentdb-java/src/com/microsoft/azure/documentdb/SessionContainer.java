/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

final class SessionContainer {
	private final ConcurrentHashMap<Long, ConcurrentHashMap<String, Long>> sessionTokens;
	private final ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> sessionTokensNameBased;
	private final String hostName;

	public SessionContainer(final String hostName) {
		this.hostName = hostName;
		this.sessionTokens = new ConcurrentHashMap<Long, ConcurrentHashMap<String, Long>>();
		this.sessionTokensNameBased = new ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>();
	}

	public String getHostName() {
		return this.hostName;
	}

	public String resolveSessionToken(final DocumentServiceRequest request) {
		String result = null;
		if (!request.getIsNameBased()) {
			if (!StringUtils.isEmpty(request.getResourceId())) {
				ResourceId resourceId = ResourceId.parse(request.getResourceId());
				if (resourceId.getDocumentCollection() != 0) {
					result = this.getCombinedSessionToken(
							this.sessionTokens.get(resourceId.getUniqueDocumentCollectionId()));
				}
			}
		} else {
			String collectionName = Utils.getCollectionName(request.getPath());
			if (!StringUtils.isEmpty(collectionName)) {
				result = this.getCombinedSessionToken(this.sessionTokensNameBased.get(collectionName));
			}
		}

		return result;
	}

	public void clearToken(final DocumentServiceRequest request, final DocumentServiceResponse response) {
		String ownerFullName = response.getResponseHeaders().get(HttpConstants.HttpHeaders.OWNER_FULL_NAME);
		String ownerId = response.getResponseHeaders().get(HttpConstants.HttpHeaders.OWNER_ID);

		String collectionName = Utils.getCollectionName(ownerFullName);

		if (!request.getIsNameBased()) {
			ownerId = request.getResourceId();
		}

		if (!StringUtils.isEmpty(ownerId)) {
			ResourceId resourceId = ResourceId.parse(ownerId);
			if (resourceId.getDocumentCollection() != 0 && !StringUtils.isEmpty(collectionName)) {
				this.sessionTokens.remove(resourceId.getUniqueDocumentCollectionId());
				this.sessionTokensNameBased.remove(collectionName);
			}
		}
	}

	public void setSessionToken(DocumentServiceRequest request, DocumentServiceResponse response) {
		String sessionToken = response.getResponseHeaders().get(HttpConstants.HttpHeaders.SESSION_TOKEN);

		if (!StringUtils.isEmpty(sessionToken)) {
			String ownerFullName = response.getResponseHeaders().get(HttpConstants.HttpHeaders.OWNER_FULL_NAME);
			String ownerId = response.getResponseHeaders().get(HttpConstants.HttpHeaders.OWNER_ID);

			String collectionName = Utils.getCollectionName(ownerFullName);

			if (!request.getIsNameBased()) {
				ownerId = request.getResourceId();
			}

			if (!StringUtils.isEmpty(ownerId)) {
				ResourceId resourceId = ResourceId.parse(ownerId);

				if (resourceId.getDocumentCollection() != 0 && !StringUtils.isEmpty(collectionName)) {
					Long uniqueDocumentCollectionId = resourceId.getUniqueDocumentCollectionId();
					this.sessionTokens.putIfAbsent(uniqueDocumentCollectionId, new ConcurrentHashMap<String, Long>());
					this.compareAndSetToken(sessionToken, this.sessionTokens.get(uniqueDocumentCollectionId));

					this.sessionTokensNameBased.putIfAbsent(collectionName, new ConcurrentHashMap<String, Long>());
					this.compareAndSetToken(sessionToken, this.sessionTokensNameBased.get(collectionName));
				}
			}
		}
	}

	private String getCombinedSessionToken(ConcurrentHashMap<String, Long> tokens) {
		StringBuilder result = new StringBuilder();
		if (tokens != null) {
			for (Iterator<Entry<String, Long>> iterator = tokens.entrySet().iterator(); iterator.hasNext();) {
				Entry<String, Long> entry = iterator.next();
				result = result.append(entry.getKey()).append(":").append(entry.getValue());
				if (iterator.hasNext()) {
					result = result.append(",");
				}
			}
		}

		return result.toString();
	}

	private void compareAndSetToken(String newToken, ConcurrentHashMap<String, Long> oldTokens) {
		if (StringUtils.isNotEmpty(newToken)) {
			String[] newTokenParts = newToken.split(":");
			if (newTokenParts.length == 2) {
				String range = newTokenParts[0];
				Long newLSN = Long.parseLong(newTokenParts[1]);
				Boolean success;
				do {
				    Long oldLSN = oldTokens.putIfAbsent(range, newLSN);
				    // If there exists no previous value or if the previous value is greater than 
				    // the current value, then we're done.
				    success = (oldLSN == null || newLSN < oldLSN);
				    if (!success)
				    {
				    	// replace the previous value with the current value.
			            success = oldTokens.replace(range, oldLSN, newLSN);
				    }
				} while (!success);
			}
		}
	}
}
