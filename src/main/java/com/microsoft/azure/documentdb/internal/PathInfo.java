package com.microsoft.azure.documentdb.internal;

public final class PathInfo {
    public final boolean isFeed;
    public final String resourcePath;
    public final String resourceIdOrFullName;
    public final boolean isNameBased;

    public PathInfo(boolean isFeed, String resourcePath, String resourceIdOrFullName, boolean isNameBased) {
        this.isFeed = isFeed;
        this.resourcePath = resourcePath;
        this.resourceIdOrFullName = resourceIdOrFullName;
        this.isNameBased = isNameBased;
    }
}