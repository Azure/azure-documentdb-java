package com.microsoft.azure.documentdb.internal.routing;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonGenerator;

class UndefinedPartitionKeyComponent implements IPartitionKeyComponent {

    public static final UndefinedPartitionKeyComponent VALUE = new UndefinedPartitionKeyComponent();

    @Override
    public int CompareTo(IPartitionKeyComponent other) {
        if (!(other instanceof UndefinedPartitionKeyComponent)) {
            throw new IllegalArgumentException("other");
        }

        return 0;
    }

    @Override
    public int GetTypeOrdinal() {
        return PartitionKeyComponentType.UNDEFINED.getValue();
    }

    @Override
    public void JsonEncode(JsonGenerator writer) {
        try {
            writer.writeStartObject();
            writer.writeEndObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void WriteForHashing(OutputStream outputStream) {
        try {
            outputStream.write((byte) PartitionKeyComponentType.UNDEFINED.getValue());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void WriteForBinaryEncoding(OutputStream outputStream) {
        try {
            outputStream.write((byte) PartitionKeyComponentType.UNDEFINED.getValue());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
