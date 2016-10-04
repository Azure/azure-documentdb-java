package com.microsoft.azure.documentdb.internal.query;

enum OrderByItemType {
    NoValue(0x0), Null(0x1), Boolean(0x2), Number(0x4), String(0x5);

    private final int val;

    private OrderByItemType(int val) {
        this.val = val;
    }

    public int getVal() {
        return this.val;
    }
}
