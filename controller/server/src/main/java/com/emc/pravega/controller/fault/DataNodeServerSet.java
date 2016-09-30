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

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ServerSets;
import com.twitter.common.zookeeper.ZooKeeperClient;
import lombok.Data;
import org.apache.zookeeper.ZooDefs;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * Represent a data node ServerSet using com.twitter.common.zookeeper.ServerSet.
 * TODO: Move it to an appropriate package/project
 */
@Data
public class DataNodeServerSet implements AutoCloseable {

    private static final String CLUSTER_ZK_PATH = "/dataNode";
    private ZooKeeperClient zkClient;
    private ServerSet serverSet;

    private DataNodeServerSet(final ZooKeeperClient zkCLient, final ServerSet serverSet) {
        this.zkClient = zkCLient;
        this.serverSet = serverSet;
    }

    public static DataNodeServerSet of(final URI zkURI, final int zkSessionTimeoutMs) {
        String zkPath = new StringBuilder(zkURI.getPath()).append("/").append(CLUSTER_ZK_PATH).toString();
        ZooKeeperClient client = new ZooKeeperClient(Amount.of(zkSessionTimeoutMs, Time.MILLISECONDS),
                InetSocketAddress.createUnresolved(zkURI.getHost(), zkURI.getPort()));
        ServerSet serverSet = ServerSets.create(client, ZooDefs.Ids.OPEN_ACL_UNSAFE, zkPath);
        return new DataNodeServerSet(client, serverSet);
    }

    @Override
    public void close() throws Exception {
        zkClient.close();
    }
}
