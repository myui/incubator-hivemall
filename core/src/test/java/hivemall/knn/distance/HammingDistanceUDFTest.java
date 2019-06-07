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

import static java.lang.Integer.toBinaryString;

import org.junit.Assert;
import org.junit.Test;

public class HammingDistanceUDFTest {

    @Test
    public void testEvaluateStringString() {
        HammingDistanceUDF udf = new HammingDistanceUDF();

        Assert.assertNull(udf.evaluate("abc", null));
        Assert.assertEquals(0, udf.evaluate("abc", "abc").get());
        Assert.assertEquals(1, udf.evaluate("adc", "abc").get());
        Assert.assertEquals(2, udf.evaluate("abc", "acb").get());
        Assert.assertEquals(3, udf.evaluate("abcd", "acb").get());
    }

    @Test
    public void testEvaluateIntStringIntString() {
        HammingDistanceUDF udf = new HammingDistanceUDF();

        Assert.assertEquals(1, udf.evaluate("10", "101").get());
        Assert.assertEquals(2, udf.evaluate("1011101", "1001001").get());
        Assert.assertEquals(3, udf.evaluate("2173896", "2233796").get());
        Assert.assertEquals(5, udf.evaluate("2173896", "223379600").get());
        Assert.assertEquals(6, udf.evaluate("2173896000", "2233796").get());
    }

    @Test
    public void testEvaluateIntInt() {
        HammingDistanceUDF udf = new HammingDistanceUDF();

        Assert.assertEquals(1, udf.evaluate(1, 3).get()); // 0011
        Assert.assertEquals(2, udf.evaluate(1, 2).get()); // 0001

        Assert.assertEquals(1, udf.evaluate(0x01, 0x11).get()); // 0011
        Assert.assertEquals(2, udf.evaluate(0x01, 0x10).get()); // 0001
        Assert.assertEquals(3, udf.evaluate(0x01, 0x110).get()); // 0001
        Assert.assertEquals(1, udf.evaluate(0x01, 0x101).get()); // 0001

        // binary 
        Assert.assertEquals(0, udf.evaluate(0b1011101, 0b1011101).get());
        Assert.assertEquals(2, udf.evaluate(0b1011101, 0b1001001).get());
        Assert.assertEquals(3, udf.evaluate(0b1011101, 0b1001000).get());
    }

    @Test
    public void testEvaluateBinary() {
        HammingDistanceUDF udf = new HammingDistanceUDF();

        Assert.assertEquals(udf.evaluate(toBinaryString(1011101), toBinaryString(1001001)).get(),
            udf.evaluate(1011101, 1001001).get());
        Assert.assertEquals(udf.evaluate(toBinaryString(2173896), toBinaryString(2233796)).get(),
            udf.evaluate(2173896, 2233796).get());
    }
}
