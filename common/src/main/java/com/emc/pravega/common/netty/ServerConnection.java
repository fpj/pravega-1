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
package com.emc.pravega.common.netty;

/**
 * A connection object. Represents the TCP connection in the server process that is coming from the client.
 */
public interface ServerConnection extends AutoCloseable {

    /**
     * Sends the provided command asynchronously. This operation is non-blocking.
     */
    void send(WireCommand cmd);

    /**
     * @param cp Sets the command processor to receive incoming commands from the client. This
     *            method may only be called once.
     */
    void setRequestProcessor(RequestProcessor cp);

    void pauseReading();

    void resumeReading();

    /**
     * Drop the connection. No further operations may be performed.
     */
    @Override
    void close();

}
