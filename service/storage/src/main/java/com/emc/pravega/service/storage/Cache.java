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

package com.emc.pravega.service.storage;

import com.emc.pravega.common.util.ByteArraySegment;

/**
 * Defines a Cache that can be used by the ReadIndex.
 */
public interface Cache extends AutoCloseable {
    /**
     * Gets a value indicating the Id of this cache.
     *
     * @return
     */
    String getId();

    /**
     * Inserts a new entry into the cache.
     *
     * @param key  The the key of the entry.
     * @param data The payload associated with the given key.
     */
    void insert(Key key, byte[] data);

    /**
     * Inserts a new entry into the cache.
     *
     * @param key  The the key of the entry.
     * @param data A ByteArraySegment representing the payload associated with the given key.
     */
    void insert(Key key, ByteArraySegment data);

    /**
     * Retrieves a cache entry with given key.
     *
     * @param key The key to search by.
     * @return The payload associated with the key, or null if no such entry exists.
     */
    byte[] get(Key key);

    /**
     * Removes any cache entry that is associated with the given key.
     *
     * @param key The key of the entry to remove.
     * @return True if removed, false if no such entry exists.
     */
    boolean remove(Key key);

    /**
     * Clears all the contents of this cache and brings it back to the initial (empty) state.
     */
    void reset();

    /**
     * Closes this cache and releases all resources owned by it.
     */
    @Override
    void close();

    //region Key

    /**
     * Defines a generic Key for an entry in the Cache.
     */
    abstract class Key {
        private boolean inCache;

        /**
         * Gets a pointer to a byte array representing the serialization of the Cache Key.
         *
         * @return
         */
        public abstract byte[] getSerialization();

        /**
         * Indicates that this Key has been added to the cache (or removed).
         *
         * @param isInCache True if it has been added to the cache, false otherwise.
         */
        public void markInCache(boolean isInCache) {
            this.inCache = true;
        }

        /**
         * Gets a value indicating whether this Key has been added to the cache.
         *
         * @return
         */
        public boolean isInCache() {
            return this.inCache;
        }

        /**
         * For in-memory representations of the Cache, hashCode() is required.
         *
         * @return
         */
        @Override
        public abstract int hashCode();

        /**
         * For in-memory representation of the Cache, equals() is required.
         *
         * @param obj
         * @return
         */
        @Override
        public abstract boolean equals(Object obj);
    }

    //endregion
}
