package com.microsoft.azure.documentdb.internal.query;

import java.util.Comparator;

final class OrderByDocumentProducerProduceComparator implements Comparator<DocumentProducer> {
    @Override
    public int compare(DocumentProducer producer1, DocumentProducer producer2) {
        return Integer.compare(producer1.size(), producer2.size());
    }
}
