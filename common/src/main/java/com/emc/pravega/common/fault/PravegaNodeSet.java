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
package com.emc.pravega.common.fault;

import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ServerSets;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.thrift.ServiceInstance;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.ZooDefs;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Represent a ServerSet using com.twitter.common.zookeeper.ServerSet. The nodes that can take part in this
 * ServerSet can either be a Data Node / a controller Node.
 */
@Slf4j
@Data
public class PravegaNodeSet {
    private static final String CLUSTER_ZK_PATH = "/Node";
    private static final int ZK_SESSION_TIMEOUT = 10000; //TODO configuration item.

    private final NodeType type;
    private ServerSet serverSet;
    private ZooKeeperClient zkClient;

    //private constructor
    private PravegaNodeSet(final ZooKeeperClient zkCLient, final ServerSet serverSet, final NodeType type) {
        this.zkClient = zkCLient;
        this.serverSet = serverSet;
        this.type = type;
    }

    /**
     * Construct a PravegaNodeSet
     *
     * @param zkURI    - ZK URI zk://hostname:port
     * @param nodeType - Indicate the nodeType i.e If it is a Controller Node/ Data Node
     * @param monitor  - Behaviour of the HostChange monitor call back. This will be invoked when a node/Endpoint leaves /joins the serverSet
     * @return - Return a PravegaNodeSet instance.
     */
    public static PravegaNodeSet of(final URI zkURI, NodeType nodeType,
                                    final Consumer<Set<ServiceInstance>> monitor) {
        String zkPath = new StringBuilder(zkURI.getPath()).append("/").append(CLUSTER_ZK_PATH).toString();
        ZooKeeperClient client = new ZooKeeperClient(Amount.of(ZK_SESSION_TIMEOUT, Time.MILLISECONDS),
                InetSocketAddress.createUnresolved(zkURI.getHost(), zkURI.getPort()));
        ServerSet serverSet = ServerSets.create(client, ZooDefs.Ids.OPEN_ACL_UNSAFE, zkPath);
        registerCallback(serverSet, monitor);
        return new PravegaNodeSet(client, serverSet, nodeType);
    }

    public static PravegaNodeSet of(final URI zkURI, final NodeType nodeType) {
        return PravegaNodeSet.of(zkURI, nodeType, null);
    }

    /**
     * Joint a service/endpoint to the server set
     *
     * @param hostName -
     * @param port
     * @param id       - Unique Id for the member of the service.
     * @return - return EndpointStatus
     * @throws NodeSetException
     */
    public ServerSet.EndpointStatus joinServerSet(final String hostName, final int port, final int id)
            throws NodeSetException {
        try {
            return getServerSet()
                    .join(InetSocketAddress.createUnresolved(hostName, port), Collections.EMPTY_MAP, id);
        } catch (Group.JoinException | InterruptedException e) {
            log.error("Error while joining the Server Set", e);
            throw new NodeSetException(e);
        }
    }

    /**
     * Close the zkClient
     *
     * @throws Exception
     */
    public void close() throws Exception {
        zkClient.close();
    }

    private static void registerCallback(ServerSet serverSet, DynamicHostSet.HostChangeMonitor<ServiceInstance> monitor) {
        if (monitor != null) {
            try {
                serverSet.watch(monitor); //set the hostchangeMonitor callback
            } catch (DynamicHostSet.MonitorException e) {
                log.error("Error while registering a HostChangeMonitor", e);
            }
        }
    }

    private static void registerCallback(final ServerSet serverSet, final Consumer<Set<ServiceInstance>> callback) {
        if (callback != null) {
            try {
                DynamicHostSet.HostChangeMonitor<ServiceInstance> monitor = hostSet -> callback.accept(Collections.unmodifiableSet(hostSet));
                serverSet.watch(monitor); //set the hostchange Monitor callback
            } catch (DynamicHostSet.MonitorException e) {
                log.error("Error while registering a HostChangeMonitor", e);
            }
        }
    }


}
