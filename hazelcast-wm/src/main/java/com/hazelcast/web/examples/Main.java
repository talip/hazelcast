package com.hazelcast.web.examples;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class Main {
    final HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance();

    public static void main(String[] args) {
        Main main = new Main();
        main.removeOrder(10, 10);
    }

    public int removeOrder(long customerId, long orderId) {
        IMap<Long, Customer> mapCustomers = hazelcast.getMap("customers");
        DeleteOrder deleteOrder = new DeleteOrder(orderId);
        return (Integer) mapCustomers.executeOnKey(customerId, deleteOrder);
    }
}


