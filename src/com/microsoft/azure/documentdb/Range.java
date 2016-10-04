/*
The MIT License (MIT)
Copyright (c) 2014 Microsoft Corporation

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package com.microsoft.azure.documentdb;

import org.apache.commons.lang3.StringUtils;

@Deprecated
public class Range<T extends Comparable<T>> implements Comparable<Range<T>> {
    private T low;
    private T high;

    public Range(T low, T high) {
        if (low == null || high == null) {
            throw new IllegalArgumentException("Range value cannot be null.");
        }
        if (low.compareTo(high) > 0) {
            throw new IllegalArgumentException("Range low value must be less than or equal the high value.");
        }

        this.low = low;
        this.high = high;
    }

    public Range(T point) {
        this(point, point);
    }

    public boolean contains(Range<T> other) {
        if (other == null) {
            throw new IllegalArgumentException("Range cannot be null.");
        }

        if (other.low.compareTo(this.low) >= 0 && other.high.compareTo(this.high) <= 0) {
            return true;
        }

        return false;
    }

    public boolean contains(T value) {
        return this.contains(new Range<T>(value));
    }

    public boolean intersect(Range<T> other) {
        T maxLow = this.low.compareTo(other.low) >= 0 ? this.low : other.low;
        T minHigh = this.high.compareTo(other.high) <= 0 ? this.high : other.high;

        if (maxLow.compareTo(minHigh) <= 0) {
            return true;
        }

        return false;
    }

    @Override
    public int compareTo(Range<T> other) {
        if (this.low.compareTo(other.low) == 0 && this.high.compareTo(other.high) == 0) {
            return 0;
        }
        if (this.low.compareTo(other.low) < 0 || this.high.compareTo(other.high) < 0) {
            return -1;
        }

        return 1;
    }

    @Override
    public String toString() {
        return StringUtils.join(new String[] {this.low.toString(), this.high.toString()}, ",");
    }
}
