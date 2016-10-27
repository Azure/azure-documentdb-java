package com.microsoft.azure.documentdb.internal.query;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.Resource;

public interface QueryExecutionContext<T extends Resource> extends Iterator<T> {
    public Map<String, String> getResponseHeaders();

    public List<T> fetchNextBlock() throws DocumentClientException;
    
    public void onNotifyStop();
}
