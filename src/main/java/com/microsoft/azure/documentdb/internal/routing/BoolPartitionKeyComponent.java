package com.microsoft.azure.documentdb.internal.routing;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonGenerator;

class BoolPartitionKeyComponent implements IPartitionKeyComponent {

    private final boolean value;

    public BoolPartitionKeyComponent(boolean value) {
        this.value = value;
    }

    @Override
    public int CompareTo(IPartitionKeyComponent other) {
        if (!(other instanceof BoolPartitionKeyComponent)) {
            throw new IllegalArgumentException("other");
        }

        return (int) Math.signum((this.value ? 1 : 0) - (((BoolPartitionKeyComponent) other).value ? 1 : 0));
    }

    @Override
    public int GetTypeOrdinal() {
        return this.value ? PartitionKeyComponentType.TRUE.getValue() : PartitionKeyComponentType.FALSE.getValue();
    }

    @Override
    public void JsonEncode(JsonGenerator writer) {
        try {
            writer.writeBoolean(this.value);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void WriteForHashing(OutputStream outputStream) {
        try {
            outputStream.write((byte) (this.value ? PartitionKeyComponentType.TRUE.getValue()
                    : PartitionKeyComponentType.FALSE.getValue()));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void WriteForBinaryEncoding(OutputStream outputStream) {
        try {
            outputStream.write((byte) (this.value ? PartitionKeyComponentType.TRUE.getValue()
                    : PartitionKeyComponentType.FALSE.getValue()));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
