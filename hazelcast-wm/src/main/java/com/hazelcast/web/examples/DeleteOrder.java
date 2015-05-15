package com.hazelcast.web.examples;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;

import java.io.Serializable;
import java.util.Map;

public class DeleteOrder implements EntryProcessor, EntryBackupProcessor, Serializable {
    private long orderId;

    public DeleteOrder(long orderId) {
        this.orderId = orderId;
    }

    @Override
    public Integer process(Map.Entry entry) {
        Customer customer = (Customer) entry.getValue();
        customer.removeOrder(orderId);
        return customer.getOrderCount();
    }

    @Override
    public EntryBackupProcessor getBackupProcessor() {
        return this;
    }

    @Override
    public void processBackup(Map.Entry entry) {
        process(entry);
    }
}


