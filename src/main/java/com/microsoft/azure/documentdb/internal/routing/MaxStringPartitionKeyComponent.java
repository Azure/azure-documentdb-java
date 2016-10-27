package com.microsoft.azure.documentdb.internal.routing;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonGenerator;

class MaxStringPartitionKeyComponent implements IPartitionKeyComponent {
    public static final MaxStringPartitionKeyComponent VALUE = new MaxStringPartitionKeyComponent();

    @Override
    public int CompareTo(IPartitionKeyComponent other) {
        if (!(other instanceof MaxStringPartitionKeyComponent)) {
            throw new IllegalArgumentException("other");
        }

        return 0;
    }

    @Override
    public int GetTypeOrdinal() {
        return PartitionKeyComponentType.MAXSTRING.ordinal();
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
            outputStream.write(PartitionKeyComponentType.MAXSTRING.getValue());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
