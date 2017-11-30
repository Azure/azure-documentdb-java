/**
 * The MIT License (MIT)
 * Copyright (c) 2017 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.documentdb.bulkimport.bulkread;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.StoredProcedureResponse;

public class BulkReadStoredProcedureExecutor {
	
	private static final ObjectMapper objectMapper = new ObjectMapper();
	
	private DocumentClient client;
	
	private String bulkReadSprocLink;
	
	private RequestOptions requestOptions;
	
	public BulkReadStoredProcedureExecutor(DocumentClient client,
			String bulkReadSprocLink,
			String partitionKeyRangeId) {
		this.client = client;
		this.bulkReadSprocLink = bulkReadSprocLink;
		
		class RequestOptionsInternal extends RequestOptions {
			RequestOptionsInternal(String partitionKeyRangeId) {
				setPartitionKeyRengeId(partitionKeyRangeId);
			}
		}

		this.requestOptions = new RequestOptionsInternal(partitionKeyRangeId);
		this.requestOptions.setScriptLoggingEnabled(true);
	}
	
	public BulkReadStoredProcedureResponse execute(String pkDefintition,
			String continuationPk,
			String continuationToken,
			int maxBatchCount) throws DocumentClientException, JsonParseException,
	JsonMappingException, IOException {
		
		StoredProcedureResponse response = client.executeStoredProcedure(bulkReadSprocLink,
				requestOptions,
				new Object[] { pkDefintition, continuationPk, continuationToken, maxBatchCount, null });
		
		// System.out.println(response.getResponseHeaders().get("x-ms-documentdb-script-log-results"));
		
		BulkReadStoredProcedureResponse bulkReadResponse = parseFrom(response);
		
		bulkReadResponse.requestUnitsConsumed = response.getRequestCharge();
		
		return bulkReadResponse;
	}
	
	private BulkReadStoredProcedureResponse parseFrom(StoredProcedureResponse storedProcResponse) throws JsonParseException, JsonMappingException, IOException {
		String res = storedProcResponse.getResponseAsString();
		
		if (StringUtils.isEmpty(res))
			return null;

		return objectMapper.readValue(res, BulkReadStoredProcedureResponse.class);
	}
}
