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
package hivemall.tools.solver;

import hivemall.UDFWithOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@Description(name = "lp_solve",
value = "_FUNC_() - ")
@UDFType(deterministic = true, stateful = false)
public final class LPsolveUDF extends UDFWithOptions {

    @Override
    protected Options getOptions() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected CommandLine processOptions(String optionValue) throws UDFArgumentException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getDisplayString(String[] args) {
        return null;
    }

}
