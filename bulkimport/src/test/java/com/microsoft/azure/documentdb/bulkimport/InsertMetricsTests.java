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


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;

import org.junit.Test;

public class InsertMetricsTests {

    @Test
    public void sum() {
        InsertMetrics metrics1 = new InsertMetrics();
        InsertMetrics metrics2 = new InsertMetrics(2, Duration.ofSeconds(3), 1.5, 4);
        InsertMetrics metrics3 = new InsertMetrics(20, Duration.ofSeconds(30), 15.0, 40);

        InsertMetrics sum = InsertMetrics.sum(metrics1, metrics2);
        sum = InsertMetrics.sum(sum, metrics3);

        assertThat(sum.numberOfDocumentsInserted, equalTo(22l));
        assertThat(sum.numberOfThrottles, equalTo(44L));
        assertThat(sum.requestUnitsConsumed, equalTo(16.5));
        assertThat(sum.timeTaken, equalTo(Duration.ofSeconds(33)));
    }
}
