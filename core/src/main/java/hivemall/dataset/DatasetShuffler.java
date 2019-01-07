/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.dataset;

import hivemall.utils.lang.ArrayUtils;

import java.io.IOException;
import java.util.Random;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class DatasetShuffler<T> {

    private final int numBuffers;

    private final AgedObject<T>[] slots;
    private int position;

    private final Random random;

    private DropoutListener<T> listener = null;

    @SuppressWarnings("unchecked")
    public DatasetShuffler(int numBuffers) {
        if (numBuffers < 1) {
            throw new IllegalArgumentException("numBuffers must be greater than 0: " + numBuffers);
        }
        this.numBuffers = numBuffers;
        this.slots = new AgedObject[numBuffers];
        this.position = 0;
        this.random = new Random();
    }

    @SuppressWarnings("unchecked")
    public DatasetShuffler(int numBuffers, long seed) {
        if (numBuffers < 1) {
            throw new IllegalArgumentException("numBuffers must be greater than 0: " + numBuffers);
        }
        this.numBuffers = numBuffers;
        this.slots = new AgedObject[numBuffers];
        this.position = 0;
        this.random = new Random(seed);
    }

    public void setDropoutListener(@Nullable DropoutListener<T> listener) {
        this.listener = listener;
    }

    public void add(@Nonnull T storedObj) throws IOException {
        if (position < numBuffers) {
            slots[position] = new AgedObject<T>(storedObj);
            position++;
            if (position == numBuffers) {
                ArrayUtils.shuffle(slots, random);
            }
        } else {
            int rindex1 = random.nextInt(numBuffers);
            int rindex2 = random.nextInt(numBuffers);
            AgedObject<T> replaced1 = slots[rindex1];
            AgedObject<T> replaced2 = slots[rindex2];
            assert (replaced1 != null);
            assert (replaced2 != null);
            if (replaced1.timestamp >= replaced2.timestamp) {// bias to hold old entry
                dropout(replaced1.object);
                replaced1.set(storedObj);
            } else {
                dropout(replaced2.object);
                replaced2.set(storedObj);
            }
        }
    }

    public void sweepAll() throws IOException {
        if (position < numBuffers && position > 1) {// shuffle an unfilled buffer
            ArrayUtils.shuffle(slots, position, random);
        }
        for (int i = 0; i < numBuffers; i++) {
            AgedObject<T> sweepedObj = slots[i];
            if (sweepedObj != null) {
                dropout(sweepedObj.object);
                slots[i] = null;
            }
        }
    }

    protected void dropout(T dropped) throws IOException {
        if (dropped == null) {
            throw new IllegalStateException("Illegal condition that dropped object is null");
        }
        if (listener != null) {
            listener.onDrop(dropped);
        }
    }

    private static final class AgedObject<T> {

        private T object;
        private long timestamp;

        AgedObject(T obj) {
            this.object = obj;
            this.timestamp = System.nanoTime();
        }

        void set(T object) {
            this.object = object;
            this.timestamp = System.nanoTime();
        }
    }

    public interface DropoutListener<T> {
        void onDrop(T dropped) throws IOException;
    }

}
