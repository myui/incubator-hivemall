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
package hivemall.factorization.cofactor;

import hivemall.factorization.cofactor.CofactorModel.RankInitScheme;
import hivemall.utils.lang.Primitives;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

final class CofactorHyperParameters {

    /** The number of latent factors */
    int factor = 10;
    /** The scaling hyperparameter for zero entries in the rank matrix */
    float c0 = 0.1f;
    /** The scaling hyperparameter for non-zero entries in the rank matrix */
    float c1 = 1.0f;
    /** The initial mean rating */
    float globalBias = 0.f;
    /** Whether update (and return) the mean rating or not */
    boolean updateGlobalBias = false;
    /** The number of iterations */
    int maxIters = 1;
    /** Whether to use bias clause */
    boolean useBiasClause = true;
    /** Whether to use normalization */
    boolean useL2Norm = true;

    // regularization hyperparameters
    float lambdaTheta = 1e-5f;
    float lambdaBeta = 1e-5f;
    float lambdaGamma = 1.0f;

    RankInitScheme rankInit;

    // validations
    String validationMetricOpt = "auc";
    int numValPerRecord = 10;
    double validationRatio = 0.125d;

    // convergence check
    boolean convergenceCheck = true;
    double convergenceRate = 0.005d;

    CofactorHyperParameters() {}

    @Nonnull
    static Options getOptions() {
        Options opts = new Options();
        opts.addOption("k", "factor", true, "The number of latent factor [default: 10] "
                + " Note this is alias for `factors` option.");
        opts.addOption("f", "factors", true, "The number of latent factor [default: 10]");
        opts.addOption("lt", "lambda_theta", true,
            "The theta regularization factor [default: 1e-5]");
        opts.addOption("lb", "lambda_beta", true, "The beta regularization factor [default: 1e-5]");
        opts.addOption("lg", "lambda_gamma", true,
            "The gamma regularization factor [default: 1.0]");
        opts.addOption("c0", "c0", true,
            "The scaling hyperparameter for zero entries in the rank matrix [default: 0.1]");
        opts.addOption("c1", "c1", true,
            "The scaling hyperparameter for non-zero entries in the rank matrix [default: 1.0]");
        opts.addOption("gb", "global_bias", true, "The global bias [default: 0.0]");
        opts.addOption("update_gb", "update_global_bias", true,
            "Whether update (and return) the global bias or not [default: false]");
        opts.addOption("rankinit", true,
            "Initialization strategy of rank matrix [random, gaussian] (default: gaussian)");
        opts.addOption("maxval", "max_init_value", true,
            "The maximum initial value in the rank matrix [default: 1.0]");
        opts.addOption("min_init_stddev", true,
            "The minimum standard deviation of initial rank matrix [default: 0.01]");
        opts.addOption("iters", "iterations", true, "The number of iterations [default: 1]");
        opts.addOption("iter", true,
            "The number of iterations [default: 1] Alias for `-iterations`");
        opts.addOption("max_iters", "max_iters", true, "The number of iterations [default: 1]");
        opts.addOption("disable_bias", "no_bias", false, "Turn off bias clause");
        // normalization
        opts.addOption("disable_norm", "disable_l2norm", false,
            "Disable instance-wise L2 normalization");
        // validation
        opts.addOption("disable_cv", "disable_cvtest", false,
            "Whether to disable convergence check [default: enabled]");
        opts.addOption("cv_rate", "convergence_rate", true,
            "Threshold to determine convergence [default: 0.005]");
        opts.addOption("val_metric", "validation_metric", true,
            "Metric to use for validation ['auc', 'objective']");
        opts.addOption("val_ratio", "validation_ratio", true,
            "Proportion of examples to use as validation data [default: 0.125]");
        opts.addOption("num_val", "num_validation_examples_per_record", true,
            "Number of validation examples to use per record [default: 10]");
        return opts;
    }

    void processOptions(@Nonnull CommandLine cl) throws UDFArgumentException {
        String rankInitOpt = "gaussian";
        float maxInitValue = 1.f;
        double initStdDev = 0.01d;

        if (cl.hasOption("factors")) {
            this.factor = Primitives.parseInt(cl.getOptionValue("factors"), factor);
        } else {
            this.factor = Primitives.parseInt(cl.getOptionValue("factor"), factor);
        }
        this.lambdaTheta = Primitives.parseFloat(cl.getOptionValue("lambda_theta"), lambdaTheta);
        this.lambdaBeta = Primitives.parseFloat(cl.getOptionValue("lambda_beta"), lambdaBeta);
        this.lambdaGamma = Primitives.parseFloat(cl.getOptionValue("lambda_gamma"), lambdaGamma);

        this.c0 = Primitives.parseFloat(cl.getOptionValue("c0"), c0);
        this.c1 = Primitives.parseFloat(cl.getOptionValue("c1"), c1);

        this.globalBias = Primitives.parseFloat(cl.getOptionValue("global_bias"), globalBias);
        this.updateGlobalBias = cl.hasOption("update_global_bias");

        rankInitOpt = cl.getOptionValue("rankinit", rankInitOpt);
        maxInitValue = Primitives.parseFloat(cl.getOptionValue("max_init_value"), maxInitValue);
        initStdDev = Primitives.parseDouble(cl.getOptionValue("min_init_stddev"), initStdDev);

        if (cl.hasOption("iter")) {
            this.maxIters = Primitives.parseInt(cl.getOptionValue("iter"), maxIters);
        } else {
            this.maxIters = Primitives.parseInt(cl.getOptionValue("max_iters"), maxIters);
        }
        if (maxIters < 1) {
            throw new UDFArgumentException(
                "'-max_iters' must be greater than or equal to 1: " + maxIters);
        }

        convergenceCheck = !cl.hasOption("disable_cvtest");
        convergenceRate = Primitives.parseDouble(cl.getOptionValue("cv_rate"), convergenceRate);
        validationMetricOpt = cl.getOptionValue("validation_metric", validationMetricOpt);
        this.numValPerRecord = Primitives.parseInt(
            cl.getOptionValue("num_validation_examples_per_record"), numValPerRecord);
        this.validationRatio =
                Primitives.parseDouble(cl.getOptionValue("validation_ratio"), this.validationRatio);
        if (this.validationRatio > 1 || this.validationRatio < 0) {
            throw new UDFArgumentException("'-validation_ratio' must be between 0.0 and 1.0");
        }
        boolean noBias = cl.hasOption("no_bias");
        this.useBiasClause = !noBias;
        if (noBias && updateGlobalBias) {
            throw new UDFArgumentException("Cannot set both `update_gb` and `no_bias` option");
        }
        this.useL2Norm = !cl.hasOption("disable_l2norm");

        this.rankInit = RankInitScheme.resolve(rankInitOpt);
        rankInit.setMaxInitValue(maxInitValue);
        rankInit.setInitStdDev(initStdDev);
    }



}
