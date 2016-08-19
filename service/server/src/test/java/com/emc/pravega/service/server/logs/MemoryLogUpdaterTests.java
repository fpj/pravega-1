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

package com.emc.pravega.service.server.logs;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ReadIndex;
import com.emc.pravega.service.server.StreamSegmentInformation;
import com.emc.pravega.service.server.containers.StreamSegmentContainerMetadata;
import com.emc.pravega.service.server.logs.operations.MergeBatchOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentMapOperation;
import com.emc.pravega.testcommon.AssertExtensions;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Unit tests for MemoryLogUpdater class.
 */
public class MemoryLogUpdaterTests {
    /**
     * Tests the functionality of the add() method.
     *
     * @throws Exception
     */
    @Test
    public void testAdd() throws Exception {
        int segmentCount = 10;
        int operationCountPerType = 5;

        // Add to MTL + Add to ReadIndex (append; beginMerge).
        MemoryOperationLog opLog = new MemoryOperationLog();
        ArrayList<TestReadIndex.MethodInvocation> methodInvocations = new ArrayList<>();
        TestReadIndex readIndex = new TestReadIndex(methodInvocations::add);
        MemoryLogUpdater updater = new MemoryLogUpdater(opLog, readIndex);
        ArrayList<Operation> operations = populate(updater, segmentCount, operationCountPerType);

        // Verify they were properly processed.
        Assert.assertEquals("Unexpected size for MemoryOperationLog.", operations.size(), opLog.getSize());
        Assert.assertEquals("Unexpected number of items added to ReadIndex.", operations.size() - segmentCount * operationCountPerType, methodInvocations.size());

        Iterator<Operation> logIterator = opLog.read(op -> true, opLog.getSize());
        int currentIndex = -1;
        int currentReadIndex = -1;
        while (logIterator.hasNext()) {
            currentIndex++;
            Operation expected = operations.get(currentIndex);
            Assert.assertEquals("Unexpected operation queued to MemoryOperationLog at sequence " + currentIndex, expected, logIterator.next());
            if (expected instanceof StorageOperation) {
                currentReadIndex++;
                TestReadIndex.MethodInvocation invokedMethod = methodInvocations.get(currentReadIndex);
                if (expected instanceof StreamSegmentAppendOperation) {
                    StreamSegmentAppendOperation appendOp = (StreamSegmentAppendOperation) expected;
                    Assert.assertEquals("Append with SeqNo " + expected.getSequenceNumber() + " was not added to the ReadIndex.", TestReadIndex.APPEND, invokedMethod.methodName);
                    Assert.assertEquals("Append with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.", appendOp.getStreamSegmentId(), invokedMethod.args.get("streamSegmentId"));
                    Assert.assertEquals("Append with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.", appendOp.getStreamSegmentOffset(), invokedMethod.args.get("offset"));
                    Assert.assertEquals("Append with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.", appendOp.getData(), invokedMethod.args.get("data"));
                } else if (expected instanceof MergeBatchOperation) {
                    MergeBatchOperation mergeOp = (MergeBatchOperation) expected;
                    Assert.assertEquals("Merge with SeqNo " + expected.getSequenceNumber() + " was not added to the ReadIndex.", TestReadIndex.BEGIN_MERGE, invokedMethod.methodName);
                    Assert.assertEquals("Merge with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.", mergeOp.getStreamSegmentId(), invokedMethod.args.get("targetStreamSegmentId"));
                    Assert.assertEquals("Merge with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.", mergeOp.getTargetStreamSegmentOffset(), invokedMethod.args.get("offset"));
                    Assert.assertEquals("Merge with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.", mergeOp.getBatchStreamSegmentId(), invokedMethod.args.get("sourceStreamSegmentId"));
                }
            }
        }

        // Test DataCorruptionException.
        AssertExtensions.assertThrows(
                "MemoryLogUpdater accepted an operation that was out of order.",
                () -> updater.add(new MergeBatchOperation(1, 2)), // This does not have a SequenceNumber set, so it should trigger a DCE.
                ex -> ex instanceof DataCorruptionException);
    }

    /**
     * Tests the ability of the MemoryLogUpdater to delegate Enter/Exit recovery mode to the read index.
     */
    @Test
    public void testRecoveryMode() throws Exception {
        // Check it's properly delegated to Read index.
        MemoryOperationLog opLog = new MemoryOperationLog();
        ArrayList<TestReadIndex.MethodInvocation> methodInvocations = new ArrayList<>();
        TestReadIndex readIndex = new TestReadIndex(methodInvocations::add);
        MemoryLogUpdater updater = new MemoryLogUpdater(opLog, readIndex);

        StreamSegmentContainerMetadata metadata1 = new StreamSegmentContainerMetadata("1");
        StreamSegmentContainerMetadata metadata2 = new StreamSegmentContainerMetadata("1");
        updater.enterRecoveryMode(metadata1);
        updater.exitRecoveryMode(true);

        Assert.assertEquals("Unexpected number of method invocations.", 2, methodInvocations.size());
        TestReadIndex.MethodInvocation enterRecovery = methodInvocations.get(0);
        Assert.assertEquals("ReadIndex.enterRecoveryMode was not called when expected.", TestReadIndex.ENTER_RECOVERY_MODE, enterRecovery.methodName);
        Assert.assertEquals("ReadIndex.enterRecoveryMode was called with the wrong arguments.", metadata1, enterRecovery.args.get("recoveryMetadataSource"));

        TestReadIndex.MethodInvocation exitRecovery = methodInvocations.get(1);
        Assert.assertEquals("ReadIndex.exitRecoveryMode was not called when expected.", TestReadIndex.EXIT_RECOVERY_MODE, exitRecovery.methodName);
        Assert.assertEquals("ReadIndex.exitRecoveryMode was called with the wrong arguments.", true, exitRecovery.args.get("successfulRecovery"));
    }

    /**
     * Tests the functionality of the flush() method, and that it can trigger future reads on the ReadIndex.
     *
     * @throws Exception
     */
    @Test
    public void testFlush() throws Exception {
        int segmentCount = 10;
        int operationCountPerType = 5;

        // Add to MTL + Add to ReadIndex (append; beginMerge).
        MemoryOperationLog opLog = new MemoryOperationLog();
        ArrayList<TestReadIndex.MethodInvocation> methodInvocations = new ArrayList<>();
        TestReadIndex readIndex = new TestReadIndex(methodInvocations::add);
        MemoryLogUpdater updater = new MemoryLogUpdater(opLog, readIndex);
        ArrayList<Operation> operations = populate(updater, segmentCount, operationCountPerType);

        methodInvocations.clear(); // We've already tested up to here.
        updater.flush();
        Assert.assertEquals("Unexpected number of calls to the ReadIndex.", 1, methodInvocations.size());
        TestReadIndex.MethodInvocation mi = methodInvocations.get(0);
        Assert.assertEquals("No call to ReadIndex.triggerFutureReads() after call to flush().", TestReadIndex.TRIGGER_FUTURE_READS, mi.methodName);
        Collection<Long> triggerSegmentIds = (Collection<Long>) mi.args.get("streamSegmentIds");
        HashSet<Long> expectedSegmentIds = new HashSet<>();
        for (Operation op : operations) {
            if (op instanceof StorageOperation) {
                expectedSegmentIds.add(((StorageOperation) op).getStreamSegmentId());
            }
        }

        AssertExtensions.assertContainsSameElements("ReadIndex.triggerFutureReads() was called with the wrong set of StreamSegmentIds.", expectedSegmentIds, triggerSegmentIds);
    }

    /**
     * Tests the clear() method on the MemoryLogUpdater (clear ReadIndex+MemoryLog; immediate calls to flush() will not
     * trigger any future reads on ReadIndex).
     *
     * @throws Exception
     */
    @Test
    public void testClear() throws Exception {
        int segmentCount = 10;
        int operationCountPerType = 5;

        // Add to MTL + Add to ReadIndex (append; beginMerge).
        MemoryOperationLog opLog = new MemoryOperationLog();
        ArrayList<TestReadIndex.MethodInvocation> methodInvocations = new ArrayList<>();
        TestReadIndex readIndex = new TestReadIndex(methodInvocations::add);
        MemoryLogUpdater updater = new MemoryLogUpdater(opLog, readIndex);
        populate(updater, segmentCount, operationCountPerType);

        methodInvocations.clear(); // We've already tested up to here.
        updater.clear();
        updater.flush();
        Assert.assertEquals("Unexpected size for MemoryOperationLog after calling clear.", 0, opLog.getSize());

        Assert.assertEquals("Unexpected number of calls to the ReadIndex.", 2, methodInvocations.size());
        TestReadIndex.MethodInvocation mi = methodInvocations.get(0);
        Assert.assertEquals("No call to ReadIndex.clear() after call to clear().", TestReadIndex.CLEAR, mi.methodName);

        mi = methodInvocations.get(1);
        Assert.assertEquals("No call to ReadIndex.triggerFutureReads() after call to flush().", TestReadIndex.TRIGGER_FUTURE_READS, mi.methodName);
        Collection<Long> triggerSegmentIds = (Collection<Long>) mi.args.get("streamSegmentIds");
        Assert.assertEquals("Call to ReadIndex.triggerFutureReads() with non-empty collection after call to clear() and flush().", 0, triggerSegmentIds.size());
    }

    private ArrayList<Operation> populate(MemoryLogUpdater updater, int segmentCount, int operationCountPerType) throws DataCorruptionException {
        ArrayList<Operation> operations = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            for (int j = 0; j < operationCountPerType; j++) {
                StreamSegmentMapOperation mapOp = new StreamSegmentMapOperation(new StreamSegmentInformation("a", i * j, false, false, new Date()));
                mapOp.setStreamSegmentId(i);
                operations.add(mapOp);
                operations.add(new StreamSegmentAppendOperation(i, Integer.toString(i).getBytes(), new AppendContext(UUID.randomUUID(), i * j)));
                operations.add(new MergeBatchOperation(i, j));
            }
        }

        for (int i = 0; i < operations.size(); i++) {
            operations.get(i).setSequenceNumber(i);
            updater.add(operations.get(i));
        }

        return operations;
    }

    private static class TestReadIndex implements ReadIndex {
        public static final String APPEND = "append";
        public static final String BEGIN_MERGE = "beginMerge";
        public static final String COMPLETE_MERGE = "completeMerge";
        public static final String READ = "read";
        public static final String TRIGGER_FUTURE_READS = "triggerFutureReads";
        public static final String CLEAR = "clear";
        public static final String PERFORM_GARBAGE_COLLECTION = "performGarbageCollection";
        public static final String ENTER_RECOVERY_MODE = "enterRecoveryMode";
        public static final String EXIT_RECOVERY_MODE = "exitRecoveryMode";

        private final Consumer<MethodInvocation> methodInvokeCallback;
        private boolean closed;

        public TestReadIndex(Consumer<MethodInvocation> methodInvokeCallback) {
            this.methodInvokeCallback = methodInvokeCallback;
        }

        @Override
        public void append(long streamSegmentId, long offset, byte[] data) {
            invoke(new MethodInvocation(APPEND)
                    .withArg("streamSegmentId", streamSegmentId)
                    .withArg("offset", offset)
                    .withArg("data", data));
        }

        @Override
        public void beginMerge(long targetStreamSegmentId, long offset, long sourceStreamSegmentId) {
            invoke(new MethodInvocation(BEGIN_MERGE)
                    .withArg("targetStreamSegmentId", targetStreamSegmentId)
                    .withArg("offset", offset)
                    .withArg("sourceStreamSegmentId", sourceStreamSegmentId));
        }

        @Override
        public void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId) {
            invoke(new MethodInvocation(COMPLETE_MERGE)
                    .withArg("targetStreamSegmentId", targetStreamSegmentId)
                    .withArg("sourceStreamSegmentId", sourceStreamSegmentId));
        }

        @Override
        public ReadResult read(long streamSegmentId, long offset, int maxLength, Duration timeout) {
            invoke(new MethodInvocation(READ)
                    .withArg("offset", offset)
                    .withArg("maxLength", maxLength));
            return null;
        }

        @Override
        public void triggerFutureReads(Collection<Long> streamSegmentIds) {
            invoke(new MethodInvocation(TRIGGER_FUTURE_READS)
                    .withArg("streamSegmentIds", streamSegmentIds));
        }

        @Override
        public void clear() {
            invoke(new MethodInvocation(CLEAR));
        }

        @Override
        public void performGarbageCollection() {
            invoke(new MethodInvocation(PERFORM_GARBAGE_COLLECTION));
        }

        @Override
        public void enterRecoveryMode(ContainerMetadata recoveryMetadataSource) {
            invoke(new MethodInvocation(ENTER_RECOVERY_MODE)
                    .withArg("recoveryMetadataSource", recoveryMetadataSource));
        }

        @Override
        public void exitRecoveryMode(boolean successfulRecovery) {
            invoke(new MethodInvocation(EXIT_RECOVERY_MODE)
                    .withArg("successfulRecovery", successfulRecovery));
        }

        @Override
        public void close() {
            this.closed = true;
        }

        private void invoke(MethodInvocation methodInvocation) {
            Exceptions.checkNotClosed(this.closed, this);
            if (this.methodInvokeCallback != null) {
                this.methodInvokeCallback.accept(methodInvocation);
            }
        }

        public static class MethodInvocation {
            public final String methodName;
            public final AbstractMap<String, Object> args;

            public MethodInvocation(String name) {
                this.methodName = name;
                this.args = new HashMap<>();
            }

            public MethodInvocation withArg(String name, Object value) {
                this.args.put(name, value);
                return this;
            }
        }
    }
}