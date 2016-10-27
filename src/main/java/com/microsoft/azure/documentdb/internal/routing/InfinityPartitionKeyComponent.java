package com.microsoft.azure.documentdb.internal.routing;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonGenerator;

class InfinityPartitionKeyComponent implements IPartitionKeyComponent {
    @Override
    public int CompareTo(IPartitionKeyComponent other) {
        if (other.getClass() != InfinityPartitionKeyComponent.class) {
            throw new IllegalArgumentException("other");
        }

        return 0;
    }

    @Override
    public int GetTypeOrdinal() {
        return PartitionKeyComponentType.INFINITY.getValue();
    }

    @Override
    public void JsonEncode(JsonGenerator writer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void WriteForHashing(OutputStream outputStream) {
        throw new IllegalStateException();
    }

    @Override
    public void WriteForBinaryEncoding(OutputStream outputStream) {
        try {
            outputStream.write(PartitionKeyComponentType.INFINITY.getValue());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
