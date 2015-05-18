package com.hazelcast.web;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionContext;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HazelcastHttpSession implements HttpSession {

    private WebFilter webFilter;
    volatile boolean valid = true;
    final String id;
    final HttpSession originalSession;
    private final Map<String, LocalCacheEntry> localCache = new ConcurrentHashMap<String, LocalCacheEntry>();
    private final boolean deferredWrite;
    // only true if session is created first time in the cluster
    private volatile boolean clusterWideNew;
    private Set<String> transientAttributes;

    public HazelcastHttpSession(WebFilter webFilter, final String sessionId, final HttpSession originalSession,
                                final boolean deferredWrite) {
        this.webFilter = webFilter;
        this.id = sessionId;
        this.originalSession = originalSession;
        this.deferredWrite = deferredWrite;
        if (this.deferredWrite) {
            buildLocalCache();
        }
        String transientAttributes = webFilter.getParam("transient-attributes");
        if (transientAttributes == null) {
            this.transientAttributes = Collections.emptySet();
        } else {
            this.transientAttributes = tokenizeToSet(transientAttributes);
        }
    }

    private Set<String> tokenizeToSet(String in) {
        StringTokenizer st = new StringTokenizer(in, ",");
        Set<String> set = new HashSet<String>();
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            set.add(token.trim());
        }
        return set;
    }

    public String getOriginalSessionId() {
        return originalSession != null ? originalSession.getId() : null;
    }

    public void setAttribute(final String name, final Object value) {
        if (name == null) {
            throw new NullPointerException("name must not be null");
        }
        if (value == null) {
            removeAttribute(name);
            return;
        }
        boolean transientEntry = false;
        if (transientAttributes.contains(name)) {
            transientEntry = true;
        }
        LocalCacheEntry entry = localCache.get(name);
        if (entry == null || entry == WebFilter.NULL_ENTRY) {
            entry = new LocalCacheEntry(transientEntry);
            localCache.put(name, entry);
        }
        entry.value = value;
        entry.setDirty(true);
        if (!deferredWrite && !transientEntry) {
            try {
                webFilter.clusteredSessionService.setAttribute(id, name, value);
                entry.setDirty(false);
            } catch (Exception e) {
            }
        }
    }

    public Object getAttribute(final String name) {
        boolean transientAttribute = transientAttributes.contains(name);
        if (deferredWrite) {
            LocalCacheEntry cacheEntry = localCache.get(name);
            if (cacheEntry == null || (cacheEntry.reload && !cacheEntry.isDirty())) {
                Object value = null;
                try {
                    value = webFilter.clusteredSessionService.getAttribute(id, name);
                } catch (Exception ignored) {
                }
                if (value == null) {
                    cacheEntry = WebFilter.NULL_ENTRY;
                } else {
                    cacheEntry = new LocalCacheEntry(transientAttribute);
                    cacheEntry.value = value;
                    cacheEntry.reload = false;
                }
                localCache.put(name, cacheEntry);
            }
            return cacheEntry != WebFilter.NULL_ENTRY ? cacheEntry.value : null;
        }
        try {
            Object value = null;
            if (!transientAttribute) {
                value = webFilter.clusteredSessionService.getAttribute(id, name);
            }
            LocalCacheEntry cacheEntry = localCache.get(name);
            if (cacheEntry == null) return value;
            if (cacheEntry.isDirty()) {
                return (cacheEntry.removed) ? null : value;
            }
            return value;
        } catch (Exception e) {
            return getLocalAttribute(name);
        }
    }

    private Object getLocalAttribute(String name) {
        LocalCacheEntry cacheEntry = localCache.get(name);
        if (cacheEntry == null || cacheEntry.removed) return null;
        return cacheEntry.value;
    }

    public Enumeration<String> getAttributeNames() {
        final Set<String> keys = selectKeys();
        return new Enumeration<String>() {
            private final String[] elements = keys.toArray(new String[keys.size()]);
            private int index;

            @Override
            public boolean hasMoreElements() {
                return index < elements.length;
            }

            @Override
            public String nextElement() {
                return elements[index++];
            }
        };
    }

    public String getId() {
        return id;
    }

    public ServletContext getServletContext() {
        return webFilter.servletContext;
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public HttpSessionContext getSessionContext() {
        return originalSession.getSessionContext();
    }

    public Object getValue(final String name) {
        return getAttribute(name);
    }

    public String[] getValueNames() {
        final Set<String> keys = selectKeys();
        return keys.toArray(new String[keys.size()]);
    }

    public void invalidate() {
        originalSession.invalidate();
        webFilter.destroySession(this, true);
    }

    public boolean isNew() {
        return originalSession.isNew() && clusterWideNew;
    }

    public void putValue(final String name, final Object value) {
        setAttribute(name, value);
    }

    public void removeAttribute(final String name) {
        LocalCacheEntry entry = localCache.get(name);
        if (entry != null && entry != WebFilter.NULL_ENTRY) {
            entry.value = null;
            entry.removed = true;
            // dirty needs to be set as last value for memory visibility reasons!
            entry.setDirty(true);
        }
        if (!deferredWrite) {
            try {
                webFilter.clusteredSessionService.deleteAttribute(id, name);
            } catch (Exception e) {
            }
        }
    }

    public void removeValue(final String name) {
        removeAttribute(name);
    }

    /**
     * @return {@code true} if {@link #deferredWrite} is enabled <i>and</i> at least one entry in the local
     * cache is dirty; otherwise, {@code false}
     */
    public boolean sessionChanged() {
        if (!deferredWrite) {
            return false;
        }
        for (Map.Entry<String, LocalCacheEntry> entry : localCache.entrySet()) {
            if (entry.getValue().isDirty()) {
                return true;
            }
        }
        return false;
    }

    public long getCreationTime() {
        return originalSession.getCreationTime();
    }

    public long getLastAccessedTime() {
        return originalSession.getLastAccessedTime();
    }

    public int getMaxInactiveInterval() {
        return originalSession.getMaxInactiveInterval();
    }

    public void setMaxInactiveInterval(int maxInactiveSeconds) {
        originalSession.setMaxInactiveInterval(maxInactiveSeconds);
    }

    void destroy() {
        valid = false;
        webFilter.clusteredSessionService.deleteSession(id);
    }

    public boolean isValid() {
        return valid;
    }

    private void buildLocalCache() {
        Set<Map.Entry<String, Object>> entrySet = null;
        try {
            entrySet = webFilter.clusteredSessionService.getAttributes(id);
        } catch (Exception e) {
            return;
        }
        if (entrySet != null) {
            for (Map.Entry<String, Object> entry : entrySet) {
                String attributeKey = entry.getKey();
                LocalCacheEntry cacheEntry = localCache.get(attributeKey);
                if (cacheEntry == null) {
                    cacheEntry = new LocalCacheEntry(transientAttributes.contains(attributeKey));
                    localCache.put(attributeKey, cacheEntry);
                }
                if (WebFilter.LOGGER.isFinestEnabled()) {
                    WebFilter.LOGGER.finest("Storing " + attributeKey + " on session " + id);
                }
                cacheEntry.value = entry.getValue();
                cacheEntry.setDirty(false);
            }
        }
    }

    void sessionDeferredWrite() {
        if (sessionChanged() || isNew()) {
            if (localCache != null) {
                Map<String, Object> updates = new HashMap<String, Object>(1);
                for (Map.Entry<String, LocalCacheEntry> entry : localCache.entrySet()) {
                    String name = entry.getKey();
                    LocalCacheEntry cacheEntry = entry.getValue();
                    if (!cacheEntry.isTransient() && entry.getValue().isDirty()) {
                        if (cacheEntry.removed) {
                            updates.put(name, null);
                        } else {
                            updates.put(name, cacheEntry.value);
                        }
                    }
                }
                try {
                    webFilter.clusteredSessionService.updateAttributes(id, updates);
                    Iterator<Map.Entry<String, LocalCacheEntry>> iterator = localCache.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, LocalCacheEntry> entry = iterator.next();
                        LocalCacheEntry cacheEntry = entry.getValue();
                        if (cacheEntry.isDirty()) {
                            if (cacheEntry.removed) {
                                iterator.remove();
                            } else {
                                cacheEntry.setDirty(false);
                            }
                        }
                    }
                } catch (Exception e) {
                }
            }
        }
    }

    private Set<String> selectKeys() {
        Set<String> keys = new HashSet<String>();
        if (!deferredWrite) {
            Set<String> attributeNames = null;
            try {
                attributeNames = webFilter.clusteredSessionService.getAttributeNames(id);
            } catch (Exception ignored) {
                for (Map.Entry<String, LocalCacheEntry> entry : localCache.entrySet()) {
                    if (!entry.getValue().removed && entry.getValue() != WebFilter.NULL_ENTRY) {
                        keys.add(entry.getKey());
                    }
                }
            }
            if (attributeNames != null) {
                keys.addAll(attributeNames);
            }
        } else {
            for (Map.Entry<String, LocalCacheEntry> entry : localCache.entrySet()) {
                if (!entry.getValue().removed && entry.getValue() != WebFilter.NULL_ENTRY) {
                    keys.add(entry.getKey());
                }
            }
        }
        return keys;
    }

    public void setClusterWideNew(boolean clusterWideNew) {
        this.clusterWideNew = clusterWideNew;
    }
} // END of HazelSession
