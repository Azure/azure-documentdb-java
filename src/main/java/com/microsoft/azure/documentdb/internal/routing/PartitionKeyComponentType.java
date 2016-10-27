package com.microsoft.azure.documentdb.internal.routing;

enum PartitionKeyComponentType {
    UNDEFINED((byte) 0x0),
    NULL((byte) 0x1),
    FALSE((byte) 0x2),
    TRUE((byte) 0x3),
    MINNUMBER((byte) 0x4),
    NUMBER((byte) 0x5),
    MAXNUMBER((byte) 0x6),
    MINSTRING((byte) 0x7),
    STRING((byte) 0x8),
    MAXSTRING((byte) 0x9),
    INFINITY((byte) 0xFF);

    private byte value;

    private PartitionKeyComponentType(byte value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
