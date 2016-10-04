package com.microsoft.azure.documentdb.internal.routing;

import java.util.Comparator;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.*;

public final class Range<T extends Comparable<T>> extends JsonSerializable {
    public Range() {
        super();
    }

    public Range(String jsonString) {
        super(jsonString);
    }

    public Range(JSONObject jsonObject) {
        super(jsonObject);
    }

    public Range(T min, T max, boolean isMinInclusive, boolean isMaxInclusive) {
        this.setMin(min);
        this.setMax(max);
        this.setMinInclusive(isMinInclusive);
        this.setMaxInclusive(isMaxInclusive);
    }

    public static <T extends Comparable<T>> Range<T> getPointRange(T value) {
        return new Range<T>(value, value, true, true);
    }

    public static <T extends Comparable<T>> Range<T> getEmptyRange(T value) {
        return new Range<T>(value, value, true, false);
    }

    public static <T extends Comparable<T>> boolean checkOverlapping(Range<T> range1, Range<T> range2) {
        if (range1 == null || range2 == null || range1.isEmpty() || range2.isEmpty()) {
            return false;
        }

        int cmp1 = range1.getMin().compareTo(range2.getMax());
        int cmp2 = range2.getMin().compareTo(range1.getMax());

        if (cmp1 <= 0 && cmp2 <= 0) {
            return !((cmp1 == 0 && !(range1.isMinInclusive() && range2.isMaxInclusive()))
                    || (cmp2 == 0 && !(range2.isMinInclusive() && range1.isMaxInclusive())));
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    public T getMin() {
        return (T) super.get("min");
    }

    public void setMin(T min) {
        super.set("min", min);
    }

    @SuppressWarnings("unchecked")
    public T getMax() {
        return (T) super.get("max");
    }

    public void setMax(T max) {
        super.set("max", max);
    }

    public boolean isMinInclusive() {
        return super.getBoolean("isMinInclusive");
    }

    public void setMinInclusive(boolean isMinInclusive) {
        super.set("isMinInclusive", isMinInclusive);
    }

    public boolean isMaxInclusive() {
        return super.getBoolean("isMaxInclusive");
    }

    public void setMaxInclusive(boolean isMaxInclusive) {
        super.set("isMaxInclusive", isMaxInclusive);
    }

    public boolean isSingleValue() {
        return this.isMinInclusive() && this.isMaxInclusive() && this.getMin().compareTo(this.getMax()) == 0;
    }

    public boolean isEmpty() {
        return this.getMin().compareTo(this.getMax()) == 0 && !(this.isMinInclusive() && this.isMaxInclusive());
    }

    public boolean contains(T value) {
        int minToValueRelation = this.getMin().compareTo(value);
        int maxToValueRelation = this.getMax().compareTo(value);

        return ((this.isMinInclusive() && minToValueRelation <= 0)
                || (!this.isMinInclusive() && minToValueRelation < 0))
                && ((this.isMaxInclusive() && maxToValueRelation >= 0)
                        || (!this.isMaxInclusive() && maxToValueRelation > 0));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Range<?>))
            return false;
        if (obj == this)
            return true;
        @SuppressWarnings("unchecked")
        Range<T> otherRange = (Range<T>) obj;

        return this.getMin().compareTo(otherRange.getMin()) == 0 && this.getMax().compareTo(otherRange.getMax()) == 0
                && this.isMinInclusive() == otherRange.isMinInclusive()
                && this.isMaxInclusive() == otherRange.isMaxInclusive();
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash = (hash * 397) ^ this.getMin().hashCode();
        hash = (hash * 397) ^ this.getMax().hashCode();
        hash = (hash * 397) ^ Boolean.compare(this.isMinInclusive(), false);
        hash = (hash * 397) ^ Boolean.compare(this.isMaxInclusive(), false);
        return hash;
    }

    @Override
    public String toString() {
        return new StringBuilder().append(this.isMinInclusive() ? '[' : ')').append(this.getMin()).append(',')
                .append(this.getMax()).append(this.isMaxInclusive() ? ']' : ')').toString();
    }

    public static class MinComparator<T extends Comparable<T>> implements Comparator<Range<T>> {
        @Override
        public int compare(Range<T> range1, Range<T> range2) {
            int result = range1.getMin().compareTo(range2.getMin());
            if (result != 0 || range1.isMinInclusive() == range2.isMinInclusive()) {
                return result;
            }

            return range1.isMinInclusive() ? -1 : 1;
        }
    }

    public static class MaxComparator<T extends Comparable<T>> implements Comparator<Range<T>> {
        @Override
        public int compare(Range<T> range1, Range<T> range2) {
            int result = range1.getMax().compareTo(range2.getMax());
            if (result != 0 || range1.isMaxInclusive() == range2.isMaxInclusive()) {
                return result;
            }

            return range1.isMaxInclusive() ? 1 : -1;
        }
    }
}
