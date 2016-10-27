package com.microsoft.azure.documentdb.internal.routing;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonGenerator;

interface IPartitionKeyComponent {
    int CompareTo(IPartitionKeyComponent other);

    int GetTypeOrdinal();

    void JsonEncode(JsonGenerator writer);

    void WriteForHashing(OutputStream outputStream);

    void WriteForBinaryEncoding(OutputStream outputStream);
}
