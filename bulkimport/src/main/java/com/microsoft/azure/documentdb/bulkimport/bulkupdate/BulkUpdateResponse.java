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
package com.microsoft.azure.documentdb.bulkimport.bulkupdate;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class BulkUpdateResponse {
    /**
     * Total number of documents updated.
     */
    final private int numberOfDocumentsUpdated;

    /**
     * Total request units consumed.
     */
    final private double totalRequestUnitsConsumed;

    /**
     * Total bulk update time.
     */
    final private Duration totalTimeTaken;

    /**
     * Keeps failures which surfaced out during execution.
     */
    final private List<Exception> failures;

    public BulkUpdateResponse(int numberOfDocumentsUpdated, double totalRequestUnitsConsumed, Duration totalTimeTaken, List<Exception> failures) {
        this.numberOfDocumentsUpdated = numberOfDocumentsUpdated;
        this.totalRequestUnitsConsumed = totalRequestUnitsConsumed;
        this.totalTimeTaken = totalTimeTaken;
        this.failures = failures;
    }

    /**
     * Gets failure list if some documents failed to get updated.
     *
     * @return list of errors or empty list if no error.
     */
    public List<Exception> getErrors() {
        return Collections.unmodifiableList(failures);
    }

    /**
     * Gets number of documents successfully updated.
     *
     * <p> If this number is less than actual batch size (meaning some documents failed to get updated),
     * use {@link #getErrors()} to get the failure cause.
     * @return the numberOfDocumentsUpdated
     */
    public int getNumberOfDocumentsUpdated() {
        return numberOfDocumentsUpdated;
    }

    /**
     * @return the totalRequestUnitsConsumed
     */
    public double getTotalRequestUnitsConsumed() {
        return totalRequestUnitsConsumed;
    }

    /**
     * @return the totalTimeTaken
     */
    public Duration getTotalTimeTaken() {
        return totalTimeTaken;
    }
}
