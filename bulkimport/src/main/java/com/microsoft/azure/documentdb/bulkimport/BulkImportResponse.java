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
import java.util.Collections;
import java.util.List;

public class BulkImportResponse {
    /**
     * Total number of documents imported.
     */
    final private int numberOfDocumentsImported;

    /**
     * Total request units consumed.
     */
    final private double totalRequestUnitsConsumed;

    /**
     * Total bulk import time.
     */
    final private Duration totalTimeTaken;

    /**
     * keeps failures which surfaced out
     */
    final private List<Exception> failures;

    BulkImportResponse(int numberOfDocumentsImported, double totalRequestUnitsConsumed, Duration totalTimeTaken, List<Exception> failures) {
        this.numberOfDocumentsImported = numberOfDocumentsImported;
        this.totalRequestUnitsConsumed = totalRequestUnitsConsumed;
        this.totalTimeTaken = totalTimeTaken;
        this.failures = failures;
    }

    /**
     * Gets failure list if some documents failed to get inserted.
     *
     * @return
     */
    public List<Exception> getErrors() {
        return Collections.unmodifiableList(failures);
    }

    /**
     * Gets number of documents successfully inserted.
     *
     * <p> If this number is less than actual batch size (meaning some documents failed to get inserted),
     * use {@link #getErrors()} to get the failure cause.
     * @return the numberOfDocumentsImported
     */
    public int getNumberOfDocumentsImported() {
        return numberOfDocumentsImported;
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
