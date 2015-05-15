package com.hazelcast.web.examples;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class ClientServer {

    void client() {


        HazelcastInstance hazelcastServer1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hazelcastServer2 = Hazelcast.newHazelcastInstance();

        HazelcastInstance hazelcastClient = HazelcastClient.newHazelcastClient();




    }
}
