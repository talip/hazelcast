package com.hazelcast.web;

class LocalCacheEntry {
    private volatile boolean dirty;
    public volatile boolean reload;
    public boolean removed;
    public Object value;
    private final boolean transientEntry;

    public LocalCacheEntry(boolean transientEntry) {
        this.transientEntry = transientEntry;
    }

    public boolean isTransient() {
        return transientEntry;
    }

    public boolean isDirty() {
        return !transientEntry && dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }
}
