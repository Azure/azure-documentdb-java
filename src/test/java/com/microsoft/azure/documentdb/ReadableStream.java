package com.microsoft.azure.documentdb;

import java.io.IOException;
import java.io.InputStream;

public class ReadableStream extends InputStream {

    byte[] bytes;
    int index;

    ReadableStream(String content) {
        this.bytes = content.getBytes();
        this.index = 0;
    }

    @Override
    public int read() throws IOException {
        if (this.index < this.bytes.length) {
            return this.bytes[this.index++];
        }
        return -1;
    }
}

