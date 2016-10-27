package com.microsoft.azure.documentdb.internal.query;

import java.util.Comparator;

final class OrderByItemComparator implements Comparator<Object> {
    private OrderByItemComparator() {
    }

    private static class SingletonHelper {
        private static final OrderByItemComparator INSTANCE = new OrderByItemComparator();
    }

    public static OrderByItemComparator getInstance() {
        return SingletonHelper.INSTANCE;
    }

    @Override
    public int compare(Object obj1, Object obj2) {
        OrderByItemType type1 = OrderByItemTypeHelper.getOrderByItemType(obj1);
        OrderByItemType type2 = OrderByItemTypeHelper.getOrderByItemType(obj2);

        int cmp = Integer.compare(type1.getVal(), type2.getVal());

        if (cmp != 0) {
            return cmp;
        }

        switch (type1) {
        case NoValue:
        case Null:
            return 0;
        case Boolean:
            return Boolean.compare((Boolean) obj1, (Boolean) obj2);
        case Number:
            return Double.compare(((Number) obj1).doubleValue(), ((Number) obj2).doubleValue());
        case String:
            return ((String) obj1).compareTo((String) obj2);
        default:
            throw new ClassCastException(String.format("Unexpected type: %s", type1.toString()));
        }
    }
}
