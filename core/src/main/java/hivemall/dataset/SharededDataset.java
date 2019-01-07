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

import java.io.IOException;
import java.util.Random;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

public final class SharededDataset implements Dataset {

    @Nonnull
    private final Random rand;
    @Nonnegative
    private final int numFiles;
    @Nonnull
    private final FioDataset[] shards;

    public SharededDataset(@Nonnegative int numFiles, @Nonnegative int bufferSize, long seed)
            throws IOException {
        this.rand = new Random(seed);
        final FioDataset[] shards = new FioDataset[numFiles];
        for (int i = 0; i < numFiles; i++) {
            shards[i] = new FioDataset(bufferSize, rand.nextLong());
        }
        this.numFiles = numFiles;
        this.shards = shards;
    }

    @Override
    public void resetPosition() {
        for (FioDataset shard : shards) {
            shard.resetPosition();
        }
    }

    @Override
    public boolean next(@Nonnull final Record r) {
        final int pos = rand.nextInt(numFiles);
        for (int i = pos; i < numFiles; i++) {
            if (shards[i].next(r)) {
                return true;
            }
        }
        for (int i = 0; i < pos; i++) {
            if (shards[i].next(r)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void add(@Nonnull Record r) throws IOException {
        final int pos = rand.nextInt(numFiles);
        shards[pos].add(r);
    }

    @Override
    public void close() throws IOException {
        for (FioDataset shard : shards) {
            shard.close();
        }
    }

}
