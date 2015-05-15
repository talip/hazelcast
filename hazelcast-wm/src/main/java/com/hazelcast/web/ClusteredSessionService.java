package com.hazelcast.web;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.SerializationServiceSupport;

import java.io.IOException;
import java.util.*;

public class ClusteredSessionService {

    final AbstractIMapWrapper clusterMap;
    final SerializationServiceSupport sss;

    public ClusteredSessionService(HazelcastInstance hazelcastInstance, String clusterMapName) {
        clusterMap = new AbstractIMapWrapper(hazelcastInstance.getMap(clusterMapName));
        sss = (SerializationServiceSupport) hazelcastInstance;
    }

    Set<Map.Entry<String, Object>> getAttributes(String sessionId) {
        SessionState sessionState = (SessionState) clusterMap.executeOnKey(sessionId, new GetSessionState());
        if (sessionState == null) {
            System.out.println(sessionId + " session is " + clusterMap.get(sessionId));
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

    Object getAttribute(String sessionId, String attributeName) {
        return clusterMap.executeOnKey(sessionId, new GetAttribute(attributeName));
    }

    void deleteAttribute(String sessionId, String attributeName) {
        setAttribute(sessionId, attributeName, null);
    }

    void setAttribute(String sessionId, String attributeName, Object value) {
        Data dataValue = (value == null) ? null : sss.getSerializationService().toData(value);
        SessionUpdateProcessor sessionUpdateProcessor = new SessionUpdateProcessor(attributeName, dataValue);
        clusterMap.executeOnKey(sessionId, sessionUpdateProcessor);
    }

    public void incrementSessionReference(String id) {
        clusterMap.executeOnKey(id, new UpdateSessionReference(true));
    }

    public int decrementAndGetSessionReference(String id) {
        return (Integer) clusterMap.executeOnKey(id, new UpdateSessionReference(false));
    }

    public void deleteSession(String id) {
        System.out.println("CSS deleting session " + id);
        clusterMap.delete(id);
    }

    public Set<String> getAttributeNames(String id) {
        return (Set<String>) clusterMap.executeOnKey(id, new GetAttributeNames());
    }

    public void updateAttributes(String id, Map<String, Object> updates) {
        System.out.println("CSS updating session " + id);
        SerializationService ss = sss.getSerializationService();
        SessionUpdateProcessor sessionUpdate = new SessionUpdateProcessor(updates.size());
        for (Map.Entry<String, Object> entry : updates.entrySet()) {
            sessionUpdate.attributes.put(entry.getKey(), ss.toData(entry.getValue()));
        }
        clusterMap.executeOnKey(id, sessionUpdate);
        System.out.println("Map size " + clusterMap.size());
        System.out.println("after update " + clusterMap.get(id));
    }

    public static class UpdateSessionReference implements EntryProcessor<String, SessionState>,
            EntryBackupProcessor<String, SessionState>, IdentifiedDataSerializable {

        boolean increment = true;

        public UpdateSessionReference(boolean increment) {
            this.increment = increment;
        }

        public UpdateSessionReference() {
        }

        @Override
        public int getId() {
            return WebDataSerializerHook.INCREMENT_REFERENCE;
        }

        @Override
        public Object process(Map.Entry<String, SessionState> entry) {
            SessionState sessionState = entry.getValue();
            if (sessionState == null) {
                System.out.println(" reference update no STATE!! found " + entry.getKey());
                return 0;
            }
            if (increment) {
                sessionState.referenceCount++;
            } else {
                sessionState.referenceCount--;
            }
            return sessionState.referenceCount;
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            increment = in.readBoolean();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeBoolean(increment);
        }

        @Override
        public void processBackup(Map.Entry<String, SessionState> entry) {
            process(entry);
        }

        @Override
        public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
            return this;
        }

        @Override
        public int getFactoryId() {
            return WebDataSerializerHook.F_ID;
        }
    }

    public static class GetAttribute implements EntryProcessor<String, SessionState>,
            IdentifiedDataSerializable {

        String attributeName;

        public GetAttribute(String attributeName) {
            this.attributeName = attributeName;
        }

        public GetAttribute() {
            this(null);
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
            return sessionState.attributes.get(attributeName);
        }

        @Override
        public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
            return null;
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            attributeName = in.readUTF();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(attributeName);
        }
    }

    public static class GetAttributeNames implements EntryProcessor<String, SessionState>,
            IdentifiedDataSerializable {

        public GetAttributeNames() {
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
            return new HashSet<String>(sessionState.attributes.keySet());
        }

        @Override
        public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    public static class SessionUpdateProcessor
            implements EntryProcessor<String, SessionState>,
            EntryBackupProcessor<String, SessionState>, IdentifiedDataSerializable {

        private Map<String, Data> attributes = null;

        public SessionUpdateProcessor(int size) {
            this.attributes = new HashMap<String, Data>(size);
        }

        public SessionUpdateProcessor(String key, Data value) {
            attributes = new HashMap<String, Data>(1);
            attributes.put(key, value);
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
            out.writeInt(attributes.size());
            for (Map.Entry<String, Data> entry : attributes.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeData(entry.getValue());
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
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

    public static class GetSessionState implements EntryProcessor<String, SessionState>,
            IdentifiedDataSerializable {

        public GetSessionState() {
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
            sessionState.referenceCount++;
            System.out.println("GetSessionState incremented sessionState to " + sessionState.referenceCount + " for " + entry.getKey());
            entry.setValue(sessionState);
            return sessionState;
        }

        @Override
        public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }
}
