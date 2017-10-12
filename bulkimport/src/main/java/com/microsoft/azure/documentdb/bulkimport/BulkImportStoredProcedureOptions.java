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

import com.fasterxml.jackson.annotation.JsonProperty;

class BulkImportStoredProcedureOptions {

    @JsonProperty("disableAutomaticIdGeneration")
    public boolean disableAutomaticIdGeneration;

    @JsonProperty("softStopOnConflict")
    public boolean softStopOnConflict;

    @JsonProperty("systemCollectionId")
    public String systemCollectionId;

    @JsonProperty("enableBsonSchema")
    public boolean enableBsonSchema;

    @JsonProperty("enableUpsert")
    public boolean enableUpsert;

    public BulkImportStoredProcedureOptions(
            boolean disableAutomaticIdGeneration,
            boolean softStopOnConflict,
            String systemCollectionId,
            boolean enableBsonSchema) {
        this(disableAutomaticIdGeneration, softStopOnConflict, systemCollectionId, enableBsonSchema, false);
    }

    public BulkImportStoredProcedureOptions(
            boolean disableAutomaticIdGeneration,
            boolean softStopOnConflict,
            String systemCollectionId,
            boolean enableBsonSchema,
            boolean enableUpsert) {
        this.disableAutomaticIdGeneration = disableAutomaticIdGeneration;
        this.softStopOnConflict = softStopOnConflict;
        this.systemCollectionId = systemCollectionId;
        this.enableBsonSchema = enableBsonSchema;
        this.enableUpsert = enableUpsert;
    }
}
