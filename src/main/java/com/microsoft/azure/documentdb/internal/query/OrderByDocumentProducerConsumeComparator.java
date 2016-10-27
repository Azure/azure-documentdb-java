package com.microsoft.azure.documentdb.internal.query;

import java.util.*;

final class OrderByDocumentProducerConsumeComparator implements Comparator<DocumentProducer> {
    private static final boolean CHECK_ITEM_TYPE = true;
    private final List<SortOrder> sortOrders;
    private List<OrderByItemType> itemTypes;

    public OrderByDocumentProducerConsumeComparator(Collection<SortOrder> sortOrders) {
        this.sortOrders = new ArrayList<SortOrder>(sortOrders);
    }

    @Override
    public int compare(DocumentProducer producer1, DocumentProducer producer2) {
        List<OrderByItem> result1 = ((DocumentQueryResult) producer1.peek()).getOrderByItems();
        List<OrderByItem> result2 = ((DocumentQueryResult) producer2.peek()).getOrderByItems();

        if (result1.size() != result2.size()) {
            throw new IllegalStateException("OrderByItems cannot have different sizes.");
        }

        if (result1.size() != this.sortOrders.size()) {
            throw new IllegalStateException(
                    String.format("OrderByItems cannot have a different size than sort orders."));
        }

        if (CHECK_ITEM_TYPE) {
            if (this.itemTypes == null) {
                synchronized (this) {
                    if (this.itemTypes == null) {
                        this.itemTypes = new ArrayList<OrderByItemType>(result1.size());
                        for (OrderByItem item : result1) {
                            this.itemTypes.add(OrderByItemTypeHelper.getOrderByItemType(item.getItem()));
                        }
                    }
                }
            }

            this.checkOrderByItemType(result1);
            this.checkOrderByItemType(result2);
        }

        for (int i = 0; i < result1.size(); ++i) {
            int cmp = OrderByItemComparator.getInstance().compare(result1.get(i).getItem(), result2.get(i).getItem());
            if (cmp != 0) {
                switch (this.sortOrders.get(i)) {
                case Ascending:
                    return cmp;
                case Descending:
                    return -cmp;
                }
            }
        }

        return producer1.getTargetRange().getMinInclusive().compareTo(producer2.getTargetRange().getMinInclusive());
    }

    private void checkOrderByItemType(List<OrderByItem> orderByItems) {
        for (int i = 0; i < this.itemTypes.size(); ++i) {
            OrderByItemType type = OrderByItemTypeHelper.getOrderByItemType(orderByItems.get(i).getItem());
            if (type != this.itemTypes.get(i)) {
                throw new UnsupportedOperationException(
                        String.format("Expected %s, but got %s.", this.itemTypes.get(i).toString(), type.toString()));
            }
        }
    }
}
