package com.microsoft.azure.documentdb.internal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class DocumentDBExecutorService {
    private DocumentDBExecutorService() {
    }

    private static class SingletonHelper {
        private static final ExecutorService POOL = Executors.newCachedThreadPool();
    }

    public static ExecutorService getExecutorService() {
        return SingletonHelper.POOL;
    }
}