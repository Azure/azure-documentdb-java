package com.microsoft.azure.documentdb.internal.routing;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import com.fasterxml.jackson.core.JsonGenerator;

class StringPartitionKeyComponent implements IPartitionKeyComponent {

    public static final int MaxStringComponentLength = 100;
    private final String value;

    public StringPartitionKeyComponent(String value) {
        if (value == null) {
            throw new IllegalArgumentException("value");
        }

        this.value = value.length() < MaxStringComponentLength ? value : value.substring(0, MaxStringComponentLength);
    }

    @Override
    public int CompareTo(IPartitionKeyComponent other) {
        if (!(other instanceof StringPartitionKeyComponent)) {
            throw new IllegalArgumentException("other");
        }

        StringPartitionKeyComponent otherComponent = (StringPartitionKeyComponent) other;

        byte[] left;
        byte[] right;
        try {
            left = this.value.getBytes("UTF-8");
            right = otherComponent.value.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }

        for (int i = 0; i < Math.min(left.length, right.length); i++) {
            if (left[i] != right[i]) {
                return (int) Math.signum(left[i] - right[i]);
            }
        }

        return (int) Math.signum(left.length - right.length);
    }

    @Override
    public int GetTypeOrdinal() {
        return PartitionKeyComponentType.STRING.getValue();
    }

    @Override
    public void JsonEncode(JsonGenerator writer) {
        try {
            writer.writeString(this.value);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void WriteForHashing(OutputStream outputStream) {
        try {
            outputStream.write(PartitionKeyComponentType.STRING.getValue());
            outputStream.write(this.value.getBytes("UTF-8"));
            outputStream.write((byte) 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void WriteForBinaryEncoding(OutputStream outputStream) {
        try {
            outputStream.write(PartitionKeyComponentType.STRING.getValue());

            boolean shortString = true;

            for (int index = 0; index < this.value.length(); index++) {
                byte charByte = (byte) this.value.charAt(index);
                if (index <= MaxStringComponentLength) {
                    if (charByte < 0xFF) charByte++;
                    outputStream.write(charByte);
                } else {
                    shortString = false;
                    break;
                }
            }

            if (shortString) {
                outputStream.write((byte) 0x00);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
