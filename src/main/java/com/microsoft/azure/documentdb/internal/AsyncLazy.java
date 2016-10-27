package com.microsoft.azure.documentdb.internal;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public final class AsyncLazy<T> {
    private final Callable<T> callable;
    private Future<T> future;

    public AsyncLazy(final Callable<T> callable) {
        this.callable = callable;
    }

    public synchronized Future<T> getValue() {
        if (this.future == null) {
            this.future = DocumentDBExecutorService.getExecutorService().submit(this.callable);
        }

        return this.future;
    }

    public boolean isDone() {
        return this.getValue().isDone();
    }

    public boolean isCancelled() {
        return this.getValue().isCancelled();
    }
}
