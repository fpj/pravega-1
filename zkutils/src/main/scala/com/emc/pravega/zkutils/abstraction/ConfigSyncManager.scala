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


package com.emc.pravega.zkutils.abstraction

import com.emc.pravega.zkutils.dummy.DummyZK
import com.emc.pravega.zkutils.vnest.VnestClient
import com.emc.pravega.zkutils.zkimplementation.ZookeeperClient
import org.apache.zookeeper.{WatchedEvent, Watcher}

/**
  * Created by kandha on 8/8/16.
  */
trait ConfigSyncManager {



  /**
    * Sample configuration/synchronization methods. Will add more as implementation progresses
    * */
  def createEntry(path:String, value: Array[Byte]):String;
  def deleteEntry(path:String): Unit;
  def refreshCluster(): Unit;
  def registerPravegaNode(host: String, port: Int, jsonMetadata: String): Unit;
  def registerPravegaController(host: String, port: Int, jsonMetadata: String): Unit;
}

object ConfigSyncManager extends Watcher {
  /**
    * Creates a ConfigSyncManager object depdning on type.
    * This is used to interact with the ZK or VNext
    * @param zkType
    * @param connectionString
    * @param sessionTimeout
    * @return
    */
  def createManager(zkType: String, connectionString: String, sessionTimeout: Int): ConfigSyncManager = {
    zkType match {
      case "dummy" => new DummyZK(connectionString, sessionTimeout, this)
      case "vnest" => new VnestClient(connectionString, sessionTimeout, this)
      case _       => new ZookeeperClient(connectionString, sessionTimeout, this)
    }
  }

  /**
    *
    * @param event
    */
  override def process(event: WatchedEvent): Unit = {

  }
}
