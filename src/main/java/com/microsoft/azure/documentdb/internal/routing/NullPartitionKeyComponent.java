package com.microsoft.azure.documentdb.internal.routing;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonGenerator;

class NullPartitionKeyComponent implements IPartitionKeyComponent {

    public static final NullPartitionKeyComponent VALUE = new NullPartitionKeyComponent();

    @Override
    public int CompareTo(IPartitionKeyComponent other) {
        if (!(other instanceof NullPartitionKeyComponent)) {
            throw new IllegalArgumentException("other");
        }

        return 0;
    }

    @Override
    public int GetTypeOrdinal() {
        return PartitionKeyComponentType.NULL.getValue();
    }

    @Override
    public void JsonEncode(JsonGenerator writer) {
        try {
            writer.writeObject((Object) null);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void WriteForHashing(OutputStream outputStream) {
        try {
            outputStream.write((byte) PartitionKeyComponentType.NULL.getValue());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void WriteForBinaryEncoding(OutputStream outputStream) {
        try {
            outputStream.write((byte) PartitionKeyComponentType.NULL.getValue());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
