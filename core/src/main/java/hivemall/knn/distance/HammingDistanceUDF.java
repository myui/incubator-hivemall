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
package hivemall.knn.distance;

import static hivemall.utils.lang.NumberUtils.diff;

import java.math.BigInteger;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

@Description(name = "hamming_distance",
        value = "_FUNC_(A, B [,int k]) - Returns Hamming distance between A and B", extended = "")
@UDFType(deterministic = true, stateful = false)
public final class HammingDistanceUDF extends UDF {

    public IntWritable evaluate(int a, int b) {
        return new IntWritable(hammingDistance(a, b));
    }

    public IntWritable evaluate(long a, long b) {
        return new IntWritable(hammingDistance(a, b));
    }

    @Nullable
    public IntWritable evaluate(@Nullable String a, @Nullable String b) {
        if (a == null || b == null) {
            return null;
        }

        int x = a.length();
        int y = b.length();
        final int min = Math.min(x, y);
        int distance = diff(x, y); // padding
        for (int i = 0; i < min; i++) {
            if (a.charAt(i) != b.charAt(i)) {
                distance++;
            }
        }
        return new IntWritable(distance);
    }

    @Nullable
    public IntWritable evaluate(@Nullable List<LongWritable> a, @Nullable List<LongWritable> b) {
        if (a == null || b == null) {
            return null;
        }

        int alen = a.size();
        int blen = b.size();

        final int min, max;
        final List<LongWritable> r;
        if (alen < blen) {
            min = alen;
            max = blen;
            r = b;
        } else {
            min = blen;
            max = alen;
            r = a;
        }

        int result = 0;
        for (int i = 0; i < min; i++) {
            result += hammingDistance(a.get(i).get(), b.get(i).get());
        }
        for (int j = min; j < max; j++) {
            result += hammingDistance(0L, r.get(j).get());
        }
        return new IntWritable(result);
    }

    public static int hammingDistance(final int a, final int b) {
        return Integer.bitCount(a ^ b);
    }

    public static int hammingDistance(final long a, final long b) {
        return Long.bitCount(a ^ b);
    }

    public static int hammingDistance(@Nonnull final BigInteger a, @Nonnull final BigInteger b) {
        BigInteger xor = a.xor(b);
        return xor.bitCount();
    }

}
