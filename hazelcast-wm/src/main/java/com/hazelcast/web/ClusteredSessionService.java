package com.hazelcast.web;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.SerializationServiceSupport;

import javax.servlet.FilterConfig;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ClusteredSessionService {

    private volatile IMap clusterMap;
    private volatile SerializationServiceSupport sss;
    private volatile HazelcastInstance hazelcastInstance;

    private static final String jvmId = UUID.randomUUID().toString();
    private final FilterConfig filterConfig;
    private final Properties properties;
    private final String clusterMapName;
    private final String sessionTTL;
    protected static final ILogger LOGGER = Logger.getLogger(ClusteredSessionService.class);
    private final Queue<String> orphanSessions = new LinkedBlockingQueue<String>();
    private volatile boolean failedConnection = true;
    private volatile long lastConnectionTry = 0;
    private final ExecutorService es = Executors.newSingleThreadExecutor();

    public ClusteredSessionService(FilterConfig filterConfig, Properties properties, String clusterMapName, String sessionTTL) {
        this.filterConfig = filterConfig;
        this.properties = properties;
        this.clusterMapName = clusterMapName;
        this.sessionTTL = sessionTTL;
        es.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(1000);
                        ensureInstance();
                    } catch (Exception e) {
                    }
                }
            }
        });
        try {
            ensureInstance();
        } catch (Exception ignored) {
        }
    }

    private void ensureInstance() throws Exception {
        final long RETRY_MILLIS = 7000;
        if (failedConnection && System.currentTimeMillis() > lastConnectionTry + RETRY_MILLIS) {
            synchronized (this) {
                try {
                    if (failedConnection && System.currentTimeMillis() > lastConnectionTry + RETRY_MILLIS) {
                        LOGGER.info("Retrying the connection!!");
                        lastConnectionTry = System.currentTimeMillis();
                        hazelcastInstance = HazelcastInstanceLoader.createInstance(filterConfig, properties);
                        clusterMap = hazelcastInstance.getMap(clusterMapName);
                        sss = (SerializationServiceSupport) hazelcastInstance;
                        try {
                            if (sessionTTL != null) {
                                Config hzConfig = hazelcastInstance.getConfig();
                                MapConfig mapConfig = hzConfig.getMapConfig(clusterMapName);
                                mapConfig.setTimeToLiveSeconds(Integer.parseInt(sessionTTL));
                                hzConfig.addMapConfig(mapConfig);
                            }
                        } catch (UnsupportedOperationException ignored) {
                            LOGGER.info("client cannot access Config.");
                        }
                        failedConnection = false;
                        LOGGER.info("Successfully Connected!");
                        String sessionId = orphanSessions.poll();
                        while (sessionId != null) {
                            if (!deleteSession(sessionId, false)) {
                                sessionId = null;  // do not continue
                            } else {
                                sessionId = orphanSessions.poll();
                            }
                        }
                    }
                } catch (Exception e) {
                    failedConnection = true;
                    throw e;
                }
            }
        }
    }

    Object executeOnKey(String sessionId, EntryProcessor processor) throws Exception {
        try {
            ensureInstance();
            if (processor instanceof JvmIdAware) {
                ((JvmIdAware) processor).setJvmId(jvmId);
            }
            return clusterMap.executeOnKey(sessionId, processor);
        } catch (Exception e) {
            failedConnection = true;
            throw e;
        }
    }

    Set<Map.Entry<String, Object>> getAttributes(String sessionId) throws Exception {
        GetSessionState entryProcessor = new GetSessionState();
        entryProcessor.setJvmId(jvmId);
        SessionState sessionState = (SessionState) executeOnKey(sessionId, entryProcessor);
        if (sessionState == null) {
            return null;
        }
        Map<String, Data> dataAttributes = sessionState.attributes;
        Set<Map.Entry<String, Object>> attributes = new HashSet<Map.Entry<String, Object>>(dataAttributes.size());
        for (Map.Entry<String, Data> entry : dataAttributes.entrySet()) {
            String key = entry.getKey();
            Object value = sss.getSerializationService().toObject(entry.getValue());
            attributes.add(new MapEntrySimple<String, Object>(key, value));
        }
        return attributes;
    }

    Object getAttribute(String sessionId, String attributeName) throws Exception {
        GetAttribute entryProcessor = new GetAttribute(attributeName);
        entryProcessor.setJvmId(jvmId);
        return executeOnKey(sessionId, entryProcessor);
    }

    void deleteAttribute(String sessionId, String attributeName) throws Exception {
        setAttribute(sessionId, attributeName, null);
    }

    void setAttribute(String sessionId, String attributeName, Object value) throws Exception {
        Data dataValue = (value == null) ? null : sss.getSerializationService().toData(value);
        SessionUpdateProcessor sessionUpdateProcessor = new SessionUpdateProcessor(attributeName, dataValue);
        sessionUpdateProcessor.setJvmId(jvmId);
        executeOnKey(sessionId, sessionUpdateProcessor);
    }

    /**
     * @param sessionId  sessionId
     * @param invalidate if true remove the distributed session, otherwise just
     *                   remove the jvm reference
     */
    public boolean deleteSession(String sessionId, boolean invalidate) {
        try {
            doDeleteSession(sessionId, invalidate);
            return true;
        } catch (Exception e) {
            orphanSessions.add(sessionId);
            return false;
        }
    }

    private void doDeleteSession(String sessionId, boolean invalidate) throws Exception {
        DeleteSession entryProcessor = new DeleteSession(sessionId, invalidate);
        entryProcessor.setJvmId(jvmId);
        executeOnKey(sessionId, entryProcessor);
    }

    public Set<String> getAttributeNames(String id) throws Exception {
        return (Set<String>) executeOnKey(id, new GetAttributeNames());
    }

    public void updateAttributes(String id, Map<String, Object> updates) throws Exception {
        SerializationService ss = sss.getSerializationService();
        SessionUpdateProcessor sessionUpdate = new SessionUpdateProcessor(updates.size());
        sessionUpdate.setJvmId(jvmId);
        for (Map.Entry<String, Object> entry : updates.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            sessionUpdate.attributes.put(name, ss.toData(value));
        }
        executeOnKey(id, sessionUpdate);
    }

    public void destroy() {
        if (hazelcastInstance != null) {
            try {
                hazelcastInstance.getLifecycleService().shutdown();
                es.awaitTermination(1, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
    }

    public static class GetAttribute implements EntryProcessor<String, SessionState>,
            IdentifiedDataSerializable, JvmIdAware {

        String attributeName;

        public GetAttribute(String attributeName) {
            this.attributeName = attributeName;
        }

        public GetAttribute() {
            this(null);
        }

        private String jvmId = null;

        public String getJvmId() {
            return jvmId;
        }

        public void setJvmId(String jvmId) {
            this.jvmId = jvmId;
        }

        @Override
        public int getFactoryId() {
            return WebDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return WebDataSerializerHook.GET_ATTRIBUTE;
        }

        @Override
        public Data process(Map.Entry<String, SessionState> entry) {
            SessionState sessionState = entry.getValue();
            if (sessionState == null) {
                return null;
            }
            sessionState.addJvmId(jvmId);
            return sessionState.attributes.get(attributeName);
        }

        @Override
        public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
            return null;
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            attributeName = in.readUTF();
            jvmId = in.readUTF();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(attributeName);
            out.writeUTF(jvmId);
        }
    }

    public static class GetAttributeNames implements EntryProcessor<String, SessionState>,
            IdentifiedDataSerializable, JvmIdAware {

        public GetAttributeNames() {
        }

        private String jvmId = null;

        public String getJvmId() {
            return jvmId;
        }

        public void setJvmId(String jvmId) {
            this.jvmId = jvmId;
        }

        @Override
        public int getFactoryId() {
            return WebDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return WebDataSerializerHook.GET_ATTRIBUTE_NAMES;
        }

        @Override
        public Object process(Map.Entry<String, SessionState> entry) {
            SessionState sessionState = entry.getValue();
            if (sessionState == null) {
                return null;
            }
            sessionState.addJvmId(jvmId);
            return new HashSet<String>(sessionState.attributes.keySet());
        }

        @Override
        public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(jvmId);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            jvmId = in.readUTF();
        }
    }

    public static class SessionUpdateProcessor
            implements EntryProcessor<String, SessionState>,
            EntryBackupProcessor<String, SessionState>, IdentifiedDataSerializable, JvmIdAware {

        private Map<String, Data> attributes = null;

        public SessionUpdateProcessor(int size) {
            this.attributes = new HashMap<String, Data>(size);
        }

        public SessionUpdateProcessor(String key, Data value) {
            attributes = new HashMap<String, Data>(1);
            attributes.put(key, value);
        }

        private String jvmId = null;

        public String getJvmId() {
            return jvmId;
        }

        public void setJvmId(String jvmId) {
            this.jvmId = jvmId;
        }

        public SessionUpdateProcessor() {
            attributes = Collections.emptyMap();
        }

        @Override
        public int getFactoryId() {
            return WebDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return WebDataSerializerHook.SESSION_UPDATE;
        }

        @Override
        public Object process(Map.Entry<String, SessionState> entry) {
            SessionState sessionState = entry.getValue();
            if (sessionState == null) {
                sessionState = new SessionState();
            }
            sessionState.addJvmId(jvmId);
            for (Map.Entry<String, Data> attribute : attributes.entrySet()) {
                String name = attribute.getKey();
                Data value = attribute.getValue();
                if (value == null) {
                    sessionState.attributes.remove(name);
                } else {
                    sessionState.attributes.put(name, value);
                }
            }
            entry.setValue(sessionState);
            return Boolean.TRUE;
        }

        @Override
        public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
            return this;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(jvmId);
            out.writeInt(attributes.size());
            for (Map.Entry<String, Data> entry : attributes.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeData(entry.getValue());
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            jvmId = in.readUTF();
            int attCount = in.readInt();
            attributes = new HashMap<String, Data>(attCount);
            for (int i = 0; i < attCount; i++) {
                attributes.put(in.readUTF(), in.readData());
            }
        }

        @Override
        public void processBackup(Map.Entry<String, SessionState> entry) {
            process(entry);
        }
    }

    public static class DeleteSession
            implements EntryProcessor<String, SessionState>,
            EntryBackupProcessor<String, SessionState>, IdentifiedDataSerializable, JvmIdAware {

        private String jvmId = null;
        private boolean invalidate = false;
        private boolean removed = false;

        public DeleteSession(String jvmId, boolean invalidate) {
            this.jvmId = jvmId;
            this.invalidate = invalidate;
        }

        public DeleteSession() {
        }

        public String getJvmId() {
            return jvmId;
        }

        public void setJvmId(String jvmId) {
            this.jvmId = jvmId;
        }

        @Override
        public int getFactoryId() {
            return WebDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return WebDataSerializerHook.SESSION_DELETE;
        }

        @Override
        public Object process(Map.Entry<String, SessionState> entry) {
            SessionState sessionState = entry.getValue();
            if (sessionState == null) {
                return Boolean.FALSE;
            }
            System.out.println(sessionState.getJvmIds());
            System.out.println(invalidate + " !!!  DELETING !!! " + sessionState + " jvmId > " + jvmId);
            sessionState.removeJvmId(jvmId);
            System.out.println(sessionState.getJvmIds());
            if (invalidate || sessionState.getJvmIds().size() == 0) {
                entry.setValue(null);
                removed = true;
            }
            return Boolean.TRUE;
        }

        @Override
        public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
            return (removed) ? this : null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(jvmId);
            out.writeBoolean(invalidate);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            jvmId = in.readUTF();
            invalidate = in.readBoolean();
        }

        @Override
        public void processBackup(Map.Entry<String, SessionState> entry) {
            SessionState sessionState = entry.getValue();
            if (sessionState != null) {
                entry.setValue(null);
            }
        }
    }

    public static class GetSessionState implements EntryProcessor<String, SessionState>,
            IdentifiedDataSerializable, JvmIdAware {

        public GetSessionState() {
        }

        private String jvmId = null;

        public String getJvmId() {
            return jvmId;
        }

        public void setJvmId(String jvmId) {
            this.jvmId = jvmId;
        }

        @Override
        public int getFactoryId() {
            return WebDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return WebDataSerializerHook.GET_SESSION_STATE;
        }

        @Override
        public Object process(Map.Entry<String, SessionState> entry) {
            SessionState sessionState = entry.getValue();
            if (sessionState == null) {
                return null;
            }
            sessionState.addJvmId(jvmId);
            entry.setValue(sessionState);
            return sessionState;
        }

        @Override
        public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(jvmId);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            jvmId = in.readUTF();
        }
    }
}