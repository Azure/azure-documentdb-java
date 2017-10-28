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

import java.time.Duration;

import com.google.common.base.Preconditions;

class InsertMetrics {

    final long numberOfDocumentsInserted;
    final Duration timeTaken;
    final double requestUnitsConsumed;
    final long numberOfThrottles;

    /**
     * Initializes a new instance of the InsertMetrics class (default constructor).
     */
    public InsertMetrics() {
        this(0, Duration.ZERO, 0, 0);
    }

    /**
     * Initializes a new instance of the InsertMetrics class (instance constructor).
     * @param numberOfDocumentsInserted Number of documents inserted.
     * @param timeTaken Amount of time taken to insert the documents.
     * @param requestUnitsConsumed The request units consumed to insert the documents.
     * @param numberOfThrottles The number of throttles encountered to insert the documents.
     */
    public InsertMetrics(long numberOfDocumentsInserted, Duration timeTaken, double requestUnitsConsumed, long numberOfThrottles) {
        Preconditions.checkArgument(numberOfDocumentsInserted >= 0, "numberOfDocumentsInserted must be non negative");
        Preconditions.checkArgument(requestUnitsConsumed >= 0, "requestUnitsConsumed must be non negative");
        Preconditions.checkArgument(numberOfThrottles >= 0, "numberOfThrottles must be non negative");

        this.numberOfDocumentsInserted = numberOfDocumentsInserted;
        this.timeTaken = timeTaken;
        this.requestUnitsConsumed = requestUnitsConsumed;
        this.numberOfThrottles = numberOfThrottles;
    }

    /**
     * Sums two {@link InsertMetrics} instances
     * @param m1
     * @param m2
     * @return the sum aggregate result
     */
    public static InsertMetrics sum(InsertMetrics m1, InsertMetrics m2) {
        long totalDocsInserted = m1.numberOfDocumentsInserted + m2.numberOfDocumentsInserted;
        Duration totalTimeTaken = m1.timeTaken.plus(m2.timeTaken);
        double totalRequestUnitsConsumed = m1.requestUnitsConsumed + m2.requestUnitsConsumed;
        long totalNumberOfThrottles = m1.numberOfThrottles + m2.numberOfThrottles;

        return new InsertMetrics(totalDocsInserted, totalTimeTaken, totalRequestUnitsConsumed, totalNumberOfThrottles);
    }
}
