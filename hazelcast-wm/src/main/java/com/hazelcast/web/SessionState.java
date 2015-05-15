package com.hazelcast.web;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SessionState implements IdentifiedDataSerializable {
    public int referenceCount = 1;
    public Map<String, Data> attributes = new HashMap<String, Data>(1);

    @Override
    public int getFactoryId() {
        return WebDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return WebDataSerializerHook.SESSION_STATE;
    }

    public void setAttribute(String key, Data value) {
        attributes.put(key, value);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(referenceCount);
        out.writeInt(attributes.size());
        for (Map.Entry<String, Data> entry : attributes.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeData(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        referenceCount = in.readInt();
        int attCount = in.readInt();
        for (int i = 0; i < attCount; i++) {
            attributes.put(in.readUTF(), in.readData());
        }
    }

    public void set(Map<String, Data> attributes) {
        this.attributes.putAll(attributes);
    }

    @Override
    public String toString() {
        return "SessionState{" +
                "referenceCount=" + referenceCount +
                ", attributes=" + ((attributes == null) ? 0 : attributes.size()) +
                ", keys=" + attributes.keySet() +
                '}';
    }
}
