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

package com.emc.pravega.service.server.logs.operations;

import java.util.Random;

import org.junit.Assert;

import com.emc.pravega.common.MathHelpers;

/**
 * Unit tests for StreamSegmentSealOperation class.
 */
public class StreamSegmentSealOperationTests extends OperationTestsBase<StreamSegmentSealOperation> {
    @Override
    protected StreamSegmentSealOperation createOperation(Random random) {
        return new StreamSegmentSealOperation(random.nextLong());
    }

    @Override
    protected boolean isPreSerializationConfigRequired(StreamSegmentSealOperation operation) {
        return operation.getStreamSegmentLength() < 0;
    }

    @Override
    protected void configurePreSerialization(StreamSegmentSealOperation operation, Random random) {
        if (operation.getStreamSegmentLength() < 0) {
            operation.setStreamSegmentLength(MathHelpers.abs(random.nextLong()));
        } else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }
}
