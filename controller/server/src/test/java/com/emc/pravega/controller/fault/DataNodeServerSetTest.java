/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.fault;

import com.google.common.collect.ImmutableSet;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.thrift.ServiceInstance;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertTrue;

public class DataNodeServerSetTest {

    private static final String ZK_URL = "zk://localhost:2182";
    private LinkedBlockingQueue<ImmutableSet<ServiceInstance>> serverSetBuffer = new LinkedBlockingQueue<>();
    private TestingServer zkTestServer;

    private DynamicHostSet.HostChangeMonitor<ServiceInstance> serverSetMonitor = (list) -> {
        System.out.println("===> " + Thread.currentThread().getName() + "Host List got modified:" + list);
        serverSetBuffer.offer(list);
    };

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer(2182);
    }

    @After
    public void stopZookeeper() throws IOException {
        zkTestServer.stop();
    }

    @Test
    public void testMemberShipChanges() throws DynamicHostSet.MonitorException,
            InterruptedException, ServerSet.UpdateException, Group.JoinException {

        DataNodeServerSet nodeSet = createDataNodeSet(ZK_URL, serverSetMonitor);

        ImmutableSet<ServiceInstance> result = serverSetBuffer.take();
        Assert.assertEquals(0, result.size());

        joinServerSet(nodeSet, "HostA", 1234, 1);
        assertTrue(checkhostName(Arrays.asList("HostA")));

        joinServerSet(nodeSet, "HostB", 1235, 2);
        assertTrue(checkhostName(Arrays.asList("HostA", "HostB")));

        //Create a separate nodeSet and register HostC
        DynamicHostSet.HostChangeMonitor<ServiceInstance> serverSetMonitor2 = (list) -> {
            System.out.println("===> " + Thread.currentThread().getName() + "Host list got modified:" + list);
        };
        DataNodeServerSet nodeSetInstance2 = createDataNodeSet(ZK_URL, serverSetMonitor2);

        ServerSet.EndpointStatus statusC = joinServerSet(nodeSetInstance2, "HostC", 1234, 3);
        checkhostName(Arrays.asList("HostA", "HostB", "HostC"));

        statusC.leave(); //remove endpoint from serverset
        assertTrue(checkhostName(Arrays.asList("HostA", "HostB")));

        ServerSet.EndpointStatus statusD = joinServerSet(nodeSetInstance2, "HostD", 1234, 3);
        checkhostName(Arrays.asList("HostA", "HostB", "HostD"));

        close(nodeSetInstance2); //simulate a host going down
        assertTrue(checkhostName(Arrays.asList("HostA", "HostB")));
    }

    private boolean checkhostName(final List<String> hostList) throws InterruptedException {
        long count = serverSetBuffer.take().stream().distinct()
                .filter(list -> hostList.contains(list.getServiceEndpoint().getHost()))
                .count();
        if (count == hostList.size()) {
            return true;
        }
        //Try again since the list might have not been updated completely.
        return serverSetBuffer.take().stream().distinct()
                .filter(list -> hostList.contains(list.getServiceEndpoint().getHost()))
                .count() == hostList.size();
    }

    private void close(DataNodeServerSet nodeSet) {
        nodeSet.getZkClient().close();
    }

    private ServerSet.EndpointStatus joinServerSet(DataNodeServerSet nodeSet, String hostName,
                                                   int port, int id) throws Group.JoinException, InterruptedException {
        ServerSet.EndpointStatus status = null;
        status = nodeSet.getServerSet()
                .join(InetSocketAddress.createUnresolved(hostName, port), Collections.EMPTY_MAP, id);
        return status;
    }

    private DataNodeServerSet createDataNodeSet(String zkURI, DynamicHostSet.HostChangeMonitor<ServiceInstance> monitor)
            throws DynamicHostSet.MonitorException {
        DataNodeServerSet nodeSet = DataNodeServerSet.of(URI.create(zkURI), 10000);
        System.out.println("node Set created");
        nodeSet.getServerSet().watch(serverSetMonitor);
        return nodeSet;
    }
}