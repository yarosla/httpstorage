package com.github.yarosla.httpstorage;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class CacheStatistics {
    private long currentCount;
    private long currentWeight;
    private long evictionCount;
    private long evictionWeight;
    private long hitCount;
    private long missCount;
    private long requestCount;
    private List<String> hottest;

    public CacheStatistics() {
    }

    CacheStatistics(Cache<String, ?> cache) {
        CacheStats stats = cache.stats();
        currentCount = cache.estimatedSize();
        evictionCount = stats.evictionCount();
        evictionWeight = stats.evictionWeight();
        hitCount = stats.hitCount();
        missCount = stats.missCount();
        requestCount = stats.requestCount();
        cache.policy().eviction().ifPresent(eviction -> {
            currentWeight = eviction.weightedSize().orElse(0);
            hottest = eviction.hottest(10).entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        });
    }

    public long getCurrentCount() {
        return currentCount;
    }

    public long getCurrentWeight() {
        return currentWeight;
    }

    public long getEvictionCount() {
        return evictionCount;
    }

    public long getEvictionWeight() {
        return evictionWeight;
    }

    public long getHitCount() {
        return hitCount;
    }

    public long getMissCount() {
        return missCount;
    }

    public long getRequestCount() {
        return requestCount;
    }

    public List<String> getHottest() {
        return hottest;
    }
}
