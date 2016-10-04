package com.microsoft.azure.documentdb.internal.query;

import java.util.Comparator;

final class DefaultDocumentProducerComparator implements Comparator<DocumentProducer> {
    private DefaultDocumentProducerComparator() {
    }

    private static class SingletonHelper {
        private static final DefaultDocumentProducerComparator INSTANCE = new DefaultDocumentProducerComparator();
    }

    public static DefaultDocumentProducerComparator getInstance() {
        return SingletonHelper.INSTANCE;
    }

    @Override
    public int compare(DocumentProducer producer1, DocumentProducer producer2) {
        return producer1.getTargetRange().getMinInclusive().compareTo(producer2.getTargetRange().getMinInclusive());
    }
}
