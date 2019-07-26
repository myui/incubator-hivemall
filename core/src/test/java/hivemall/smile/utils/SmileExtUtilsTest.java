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
package hivemall.smile.utils;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.junit.Assert;
import org.junit.Test;

public class SmileExtUtilsTest {

    @Test
    public void testResolveAttributes() throws UDFArgumentException {
        Assert.assertTrue(SmileExtUtils.resolveAttributes("Q,Q,Q").isEmpty());
        Assert.assertEquals(4, SmileExtUtils.resolveAttributes("Q,C,C,Q,C,Q,C").getCardinality());
        Assert.assertEquals(SmileExtUtils.resolveAttributes("Q,C,C,Q,C"),
            SmileExtUtils.resolveAttributes("1,2,4"));
    }

    @Test(expected = UDFArgumentException.class)
    public void testResolveAttributesInvalidFormat() throws UDFArgumentException {
        Assert.assertTrue(SmileExtUtils.resolveAttributes("Q,Q,3,Q").isEmpty());
    }

}
