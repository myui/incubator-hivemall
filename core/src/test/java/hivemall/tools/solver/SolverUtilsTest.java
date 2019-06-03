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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.LinearConstraint;
import org.apache.commons.math3.optim.linear.LinearConstraintSet;
import org.apache.commons.math3.optim.linear.LinearObjectiveFunction;
import org.apache.commons.math3.optim.linear.NonNegativeConstraint;
import org.apache.commons.math3.optim.linear.PivotSelectionRule;
import org.apache.commons.math3.optim.linear.Relationship;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.linear.SimplexSolver;
import org.junit.Assert;
import org.junit.Test;

public class SolverUtilsTest {
    
    @Test
    public void testMath842Cycle() {
        // from http://www.math.toronto.edu/mpugh/Teaching/APM236_04/bland
        //      maximize 10 x1 - 57 x2 - 9 x3 - 24 x4
        //      subject to
        //          1/2 x1 - 11/2 x2 - 5/2 x3 + 9 x4  <= 0
        //          1/2 x1 -  3/2 x2 - 1/2 x3 +   x4  <= 0
        //              x1                  <= 1
        //      x1,x2,x3,x4 >= 0

        LinearObjectiveFunction f = new LinearObjectiveFunction(new double[] { 10, -57, -9, -24}, 0);

        ArrayList<LinearConstraint> constraints = new ArrayList<LinearConstraint>();

        constraints.add(new LinearConstraint(new double[] {0.5, -5.5, -2.5, 9}, Relationship.LEQ, 0));
        constraints.add(new LinearConstraint(new double[] {0.5, -1.5, -0.5, 1}, Relationship.LEQ, 0));
        constraints.add(new LinearConstraint(new double[] {  1,    0,    0, 0}, Relationship.LEQ, 1));

        double epsilon = 1e-6;
        SimplexSolver solver = new SimplexSolver();
        PointValuePair solution = solver.optimize(f, new LinearConstraintSet(constraints),
                                                  GoalType.MAXIMIZE,
                                                  new NonNegativeConstraint(true),
                                                  PivotSelectionRule.BLAND);
        Assert.assertEquals(1.0d, solution.getValue(), epsilon);
        Assert.assertTrue(validSolution(solution, constraints, epsilon));
    }


}
