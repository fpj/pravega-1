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

package com.emc.pravega.service.server.host.selftest;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.MissingPropertyException;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration for Self-Tester.
 */
class TestConfig extends ComponentConfig {
    //region Members

    static final String COMPONENT_CODE = "selftest";
    static final String PROPERTY_OPERATION_COUNT = "operationCount";
    static final String PROPERTY_SEGMENT_COUNT = "segmentCount";
    static final String PROPERTY_TRANSACTION_FREQUENCY = "transactionFrequency";
    static final String PROPERTY_MAX_TRANSACTION_SIZE = "maxTransactionSize";
    static final String PROPERTY_PRODUCER_COUNT = "producerCount";
    static final String PROPERTY_THREAD_POOL_SIZE = "threadPoolSize";
    static final String PROPERTY_TIMEOUT_MILLIS = "timeoutMillis";

    private static final int DEFAULT_OPERATION_COUNT = 1000 * 1000;
    private static final int DEFAULT_SEGMENT_COUNT = 100;
    private static final int DEFAULT_TRANSACTION_FREQUENCY = 100;
    private static final int DEFAULT_MAX_TRANSACTION_APPEND_COUNT = 10;
    private static final int DEFAULT_PRODUCER_COUNT = 1;
    private static final int DEFAULT_THREAD_POOL_SIZE = 100;
    private static final int DEFAULT_TIMEOUT_MILLIS = 10*1000;

    private int operationCount;
    private int segmentCount;
    private int transactionFrequency;
    private int maxTransactionAppendCount;
    private int producerCount;
    private int threadPoolSize;
    private Duration timeout;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TestConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws MissingPropertyException Whenever a required Property is missing from the given properties collection.
     * @throws NumberFormatException    Whenever a Property has a value that is invalid for it.
     * @throws NullPointerException     If any of the arguments are null.
     */
    TestConfig(Properties properties) throws ConfigurationException {
        super(properties, COMPONENT_CODE);
    }

    //endregion

    //region ComponentConfig Implementation

    @Override
    protected void refresh() throws ConfigurationException {
        this.operationCount = getInt32Property(PROPERTY_OPERATION_COUNT, DEFAULT_OPERATION_COUNT);
        this.segmentCount = getInt32Property(PROPERTY_SEGMENT_COUNT, DEFAULT_SEGMENT_COUNT);
        this.transactionFrequency = getInt32Property(PROPERTY_TRANSACTION_FREQUENCY, DEFAULT_TRANSACTION_FREQUENCY);
        this.maxTransactionAppendCount = getInt32Property(PROPERTY_MAX_TRANSACTION_SIZE, DEFAULT_MAX_TRANSACTION_APPEND_COUNT);
        this.producerCount = getInt32Property(PROPERTY_PRODUCER_COUNT, DEFAULT_PRODUCER_COUNT);
        this.threadPoolSize = getInt32Property(PROPERTY_THREAD_POOL_SIZE, DEFAULT_THREAD_POOL_SIZE);
        int timeoutMillis = getInt32Property(PROPERTY_TIMEOUT_MILLIS, DEFAULT_TIMEOUT_MILLIS);
        this.timeout = Duration.ofMillis(timeoutMillis);
    }

    //endregion

    //region Properties

    public int getOperationCount() {
        return this.operationCount;
    }

    public int getSegmentCount() {
        return this.segmentCount;
    }

    public int getTransactionFrequency() {
        return this.transactionFrequency;
    }

    public int getMaxTransactionAppendCount() {
        return this.maxTransactionAppendCount;
    }

    public int getProducerCount() {
        return this.producerCount;
    }

    public int getThreadPoolSize(){
        return this.threadPoolSize;
    }

    public Duration getTimeout(){
        return this.timeout;
    }

    //endregion

    static Properties convert(String componentCode, Properties rawProperties) {
        Properties p = new Properties();
        for (Map.Entry<Object, Object> e : rawProperties.entrySet()) {
            ServiceBuilderConfig.set(p, componentCode, e.getKey().toString(), e.getValue().toString());
        }

        return p;
    }
}