package com.microsoft.azure.documentdb.internal.routing;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonGenerator;

class MaxNumberPartitionKeyComponent implements IPartitionKeyComponent {
    public static final MaxNumberPartitionKeyComponent VALUE = new MaxNumberPartitionKeyComponent();

    @Override
    public int CompareTo(IPartitionKeyComponent other) {
        if (!(other instanceof MaxNumberPartitionKeyComponent)) {
            throw new IllegalArgumentException("other");
        }

        return 0;
    }

    @Override
    public int GetTypeOrdinal() {
        return PartitionKeyComponentType.MAXNUMBER.ordinal();
    }

    @Override
    public void JsonEncode(JsonGenerator writer) {
        PartitionKeyInternal.PartitionKeyInternalJsonSerializer.jsonEncode(this, writer);
    }

    @Override
    public void WriteForHashing(OutputStream outputStream) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void WriteForBinaryEncoding(OutputStream outputStream) {
        try {
            outputStream.write(PartitionKeyComponentType.MAXNUMBER.getValue());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
