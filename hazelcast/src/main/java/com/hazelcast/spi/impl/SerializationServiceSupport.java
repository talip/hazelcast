package com.hazelcast.spi.impl;

import com.hazelcast.nio.serialization.SerializationService;

public interface SerializationServiceSupport {
    SerializationService getSerializationService();
}
