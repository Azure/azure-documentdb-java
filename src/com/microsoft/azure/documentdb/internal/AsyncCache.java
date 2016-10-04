package com.microsoft.azure.documentdb.internal;

import java.util.*;
import java.util.concurrent.*;

// Cache which supports asynchronous value initialization.
// It ensures that for given key only single initialization function is running at any point in time.
public final class AsyncCache<TKey, TValue> {
    private final ConcurrentHashMap<TKey, AsyncLazy<TValue>> values;

    public AsyncCache() {
        this.values = new ConcurrentHashMap<TKey, AsyncLazy<TValue>>();
    }

    public Set<TKey> getKeys() {
        return this.values.keySet();
    }

    public AsyncLazy<TValue> put(TKey key, final TValue value) {
        return this.values.put(key, new AsyncLazy<TValue>(new Callable<TValue>() {
            @Override
            public TValue call() throws Exception {
                return value;
            }
        }));
    }

    public Future<TValue> get(TKey key, TValue obsoleteValue, Callable<TValue> callable) {
        AsyncLazy<TValue> initialAsyncLazy = this.values.get(key);
        if (initialAsyncLazy != null && !initialAsyncLazy.isCancelled()) {
            try {
                if (!initialAsyncLazy.isDone() || !this.areEqual(initialAsyncLazy.getValue().get(), obsoleteValue)) {
                    return initialAsyncLazy.getValue();
                }
            } catch (CancellationException | ExecutionException | InterruptedException e) {
                // Nothing to do, proceed with replacing new AsyncLazy
            }
        }

        AsyncLazy<TValue> newAsyncLazy = new AsyncLazy<TValue>(callable);

        // $ISSUE-felixfan-2016-08-03: We should use ConcurrentHashMap.merge when moving to Java 8
        AsyncLazy<TValue> actualAsyncLazy;
        if (initialAsyncLazy == null) {
            actualAsyncLazy = this.values.putIfAbsent(key, newAsyncLazy);
            if (actualAsyncLazy == null) {
                actualAsyncLazy = newAsyncLazy;
            }
        } else if (this.values.replace(key, initialAsyncLazy, newAsyncLazy)) {
            actualAsyncLazy = newAsyncLazy;
        } else {
            actualAsyncLazy = this.values.get(key);
        }

        return actualAsyncLazy.getValue();
    }

    public AsyncLazy<TValue> remove(TKey key) {
        return this.values.remove(key);
    }

    public void refresh(TKey key, Callable<TValue> callable) {
        AsyncLazy<TValue> initialAsyncLazy = this.values.get(key);
        if (initialAsyncLazy != null && initialAsyncLazy.isDone()) {
            AsyncLazy<TValue> newAsyncLazy = new AsyncLazy<TValue>(callable);
            // $ISSUE-felixfan-2016-08-03: We should use ConcurrentHashMap.merge when moving to Java 8
            if (this.values.replace(key, initialAsyncLazy, newAsyncLazy)) {
                newAsyncLazy.getValue();
            }
        }
    }

    private boolean areEqual(TValue value1, TValue value2) {
        if (value1 == value2)
            return true;
        if (value1 == null || value2 == null)
            return false;
        return value1.equals(value2);
    }
}