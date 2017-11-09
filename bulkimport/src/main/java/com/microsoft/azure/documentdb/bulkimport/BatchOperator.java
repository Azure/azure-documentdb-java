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
package com.microsoft.azure.documentdb.bulkimport;

import java.util.Iterator;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.RequestOptions;

public abstract class BatchOperator {

	protected static final ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * The index of the physical partition this batch operator is responsible for.
	 */
	protected String partitionKeyRangeId;

	/**
	 * The document client to use.
	 */
	protected DocumentClient client;

	/**
	 * Provides a mean to cancel Batch Operator from doing any more work.
	 */
	protected volatile boolean cancel = false;

	/**
	 * Request options specifying the underlying partition key range id.
	 */
	protected RequestOptions requestOptions;

	/**
	 * Gets a stream of tasks that return InsertMetrics that when awaited on operate on the next mini-batch to the document collection.
	 * @return stream of operator tasks
	 */
	abstract protected Iterator<Callable<InsertMetrics>> miniBatchExecutionCallableIterator();
}
