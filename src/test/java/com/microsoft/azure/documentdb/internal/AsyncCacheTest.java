package com.microsoft.azure.documentdb.internal;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

public class AsyncCacheTest {
    @Test
    public void testGet() throws Exception {
        final AtomicInteger numberOfCacheRefreshes = new AtomicInteger();
        Function<Integer, Integer> refreshFunc = new Function<Integer, Integer>() {
            @Override
            public Integer apply(Integer t) {
                numberOfCacheRefreshes.incrementAndGet();
                return t * 2;
            }
        };

        AsyncCache<Integer, Integer> cache = new AsyncCache<Integer, Integer>();

        List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
        for (int i = 0; i < 10; ++i) {
            for (int j = 0; j < 10; ++j) {
                int key = j;
                futures.add(cache.get(key, -1, new TestCallable(key, refreshFunc)));
            }
        }

        for (Future<Integer> future : futures) {
            future.get();
        }

        Assert.assertEquals(10, numberOfCacheRefreshes.get());
        Assert.assertEquals(4, cache.get(2, -1, new TestCallable(2, refreshFunc)).get().intValue());

        Function<Integer, Integer> refreshFunc1 = new Function<Integer, Integer>() {
            @Override
            public Integer apply(Integer t) {
                numberOfCacheRefreshes.incrementAndGet();
                return t * 2 + 1;
            }
        };

        List<Future<Integer>> futures1 = new ArrayList<Future<Integer>>();
        for (int i = 0; i < 10; ++i) {
            for (int j = 0; j < 10; ++j) {
                int key = j;
                futures1.add(cache.get(key, key * 2, new TestCallable(key, refreshFunc1)));
            }

            for (int j = 0; j < 10; ++j) {
                int key = j;
                futures1.add(cache.get(key, key * 2, new TestCallable(key, refreshFunc1)));
            }
        }

        for (Future<Integer> future : futures1) {
            future.get();
        }

        Assert.assertEquals(20, numberOfCacheRefreshes.get());
        Assert.assertEquals(5, cache.get(2, -1, new TestCallable(2, refreshFunc1)).get().intValue());
    }

    private static class TestCallable implements Callable<Integer> {
        private final int key;
        private final Function<Integer, Integer> func;

        public TestCallable(int key, Function<Integer, Integer> func) {
            this.key = key;
            this.func = func;
        }

        @Override
        public Integer call() throws Exception {
            return this.func.apply(this.key);
        }
    }
}
