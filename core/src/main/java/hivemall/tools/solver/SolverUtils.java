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

import javax.annotation.Nonnull;

import org.apache.commons.math3.optim.linear.LinearConstraint;
import org.apache.commons.math3.optim.linear.Relationship;

public final class SolverUtils {

    private SolverUtils() {}


    /**
     * Converts a test string to a {@link LinearConstraint}. Ex: x0 + x1 + x2 + x3 - x12 = 0
     */
    @Nonnull
    public static LinearConstraint equationFromString(@Nonnull String s) {
        final Relationship relationship;
        if (s.contains(">=")) {
            relationship = Relationship.GEQ;
        } else if (s.contains("<=")) {
            relationship = Relationship.LEQ;
        } else if (s.contains("=")) {
            relationship = Relationship.EQ;
        } else {
            throw new IllegalArgumentException();
        }

        String[] equationParts = s.split("[>|<]?=");
        double rhs = Double.parseDouble(equationParts[1].trim());

        String left = equationParts[0].replaceAll(" ?x", "");
        final String[] coefficients = left.split(" ");
        final double[] lhs = new double[coefficients.length];
        for (String coefficient : coefficients) {
            double value = coefficient.charAt(0) == '-' ? -1 : 1;
            int index = Integer.parseInt(coefficient.replaceFirst("[+|-]", "").trim());
            lhs[index] = value;
        }
        return new LinearConstraint(lhs, relationship, rhs);
    }

}
