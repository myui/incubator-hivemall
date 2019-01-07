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

import hivemall.dataset.DatasetShuffler.DropoutListener;
import hivemall.model.FeatureValue;
import hivemall.utils.io.NIOUtils;
import hivemall.utils.io.NioStatefulSegment;
import hivemall.utils.lang.SizeOf;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class FioDataset implements Dataset, DropoutListener<Record> {
    private static final Log logger = LogFactory.getLog(FioDataset.class);

    @Nullable
    private final DatasetShuffler<Record> shuffler;
    @Nullable
    private final NioStatefulSegment fio1;
    @Nullable
    private final ByteBuffer inputBuf;

    public FioDataset(@Nonnegative int bufferSize, long seed) throws IOException {
        this.shuffler = new DatasetShuffler<>(bufferSize, seed);
        this.inputBuf = ByteBuffer.allocateDirect(1024 * 1024); // 1 MiB
        File file = prepareFile();
        this.fio1 = new NioStatefulSegment(file, false);
    }

    @Nonnegative
    private static File prepareFile() throws IOException {
        File file = File.createTempFile("fio_dataset", ".sgmt");
        file.deleteOnExit();
        if (!file.canWrite()) {
            throw new IOException("Cannot write a temporary file: " + file.getAbsolutePath());
        }
        logger.info("Record training samples to a file: " + file.getAbsolutePath());
        return file;
    }

    @Override
    public void resetPosition() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean next(@Nonnull Record r) {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public void add(@Nonnull Record r) throws IOException {
        shuffler.add(r);
    }


    @Override
    public void onDrop(@Nonnull Record r) throws IOException {
        storeRecord(r.getFeatures(), r.getLabel());
    }

    private void storeRecord(@Nonnull final FeatureValue[] featureVector, final float target)
            throws IOException {
        int featureVectorBytes = 0;
        for (FeatureValue f : featureVector) {
            if (f == null) {
                continue;
            }
            int featureLength = f.getFeatureAsString().length();

            // feature as String (even if it is Text or Integer)
            featureVectorBytes += SizeOf.CHAR * featureLength;

            // NIOUtils.putString() first puts the length of string before string itself
            featureVectorBytes += SizeOf.INT;

            // value
            featureVectorBytes += SizeOf.DOUBLE;
        }

        // feature length, feature 1, feature 2, ..., feature n, target
        int recordBytes = SizeOf.INT + featureVectorBytes + SizeOf.FLOAT;
        int requiredBytes = SizeOf.INT + recordBytes; // need to allocate space for "recordBytes" itself

        final ByteBuffer buf = this.inputBuf;
        int remain = buf.remaining();
        if (remain < requiredBytes) {
            writeBuffer(buf, fio1);
        }

        buf.putInt(recordBytes);
        buf.putInt(featureVector.length);
        for (FeatureValue f : featureVector) {
            writeFeatureValue(buf, f);
        }
        buf.putFloat(target);
    }

    private static void writeBuffer(@Nonnull ByteBuffer srcBuf, @Nonnull NioStatefulSegment dst)
            throws IOException {
        srcBuf.flip();
        dst.write(srcBuf);
        srcBuf.clear();
    }

    private static void writeFeatureValue(@Nonnull final ByteBuffer buf,
            @Nonnull final FeatureValue f) {
        NIOUtils.putString(f.getFeatureAsString(), buf);
        buf.putDouble(f.getValue());
    }

    @Override
    public void close() throws IOException {
        fio1.close(true);
    }

}
