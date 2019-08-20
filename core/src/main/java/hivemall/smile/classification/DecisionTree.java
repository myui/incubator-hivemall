/*
 * Copyright (c) 2010 Haifeng Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// This file includes a modified version of Smile:
// https://github.com/haifengl/smile/blob/master/core/src/main/java/smile/classification/DecisionTree.java
package hivemall.smile.classification;

import static hivemall.smile.utils.SmileExtUtils.NOMINAL;
import static hivemall.smile.utils.SmileExtUtils.NUMERIC;
import static hivemall.smile.utils.SmileExtUtils.resolveFeatureName;
import static hivemall.smile.utils.SmileExtUtils.resolveName;

import hivemall.annotations.VisibleForTesting;
import hivemall.math.matrix.Matrix;
import hivemall.math.matrix.ints.ColumnMajorIntMatrix;
import hivemall.math.random.PRNG;
import hivemall.math.random.RandomNumberGeneratorFactory;
import hivemall.math.vector.DenseVector;
import hivemall.math.vector.SparseVector;
import hivemall.math.vector.Vector;
import hivemall.math.vector.VectorProcedure;
import hivemall.smile.utils.SmileExtUtils;
import hivemall.utils.collections.lists.IntArrayList;
import hivemall.utils.function.IntPredicate;
import hivemall.utils.lang.ObjectUtils;
import hivemall.utils.lang.StringUtils;
import hivemall.utils.lang.mutable.MutableInt;
import hivemall.utils.sampling.IntReservoirSampler;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import smile.classification.Classifier;
import smile.math.Math;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

/**
 * Decision tree for classification. A decision tree can be learned by splitting the training set
 * into subsets based on an attribute value test. This process is repeated on each derived subset in
 * a recursive manner called recursive partitioning. The recursion is completed when the subset at a
 * node all has the same value of the target variable, or when splitting no longer adds value to the
 * predictions.
 * <p>
 * The algorithms that are used for constructing decision trees usually work top-down by choosing a
 * variable at each step that is the next best variable to use in splitting the set of items. "Best"
 * is defined by how well the variable splits the set into homogeneous subsets that have the same
 * value of the target variable. Different algorithms use different formulae for measuring "best".
 * Used by the CART algorithm, Gini impurity is a measure of how often a randomly chosen element
 * from the set would be incorrectly labeled if it were randomly labeled according to the
 * distribution of labels in the subset. Gini impurity can be computed by summing the probability of
 * each item being chosen times the probability of a mistake in categorizing that item. It reaches
 * its minimum (zero) when all cases in the node fall into a single target category. Information
 * gain is another popular measure, used by the ID3, C4.5 and C5.0 algorithms. Information gain is
 * based on the concept of entropy used in information theory. For categorical variables with
 * different number of levels, however, information gain are biased in favor of those attributes
 * with more levels. Instead, one may employ the information gain ratio, which solves the drawback
 * of information gain.
 * <p>
 * Classification and Regression Tree techniques have a number of advantages over many of those
 * alternative techniques.
 * <dl>
 * <dt>Simple to understand and interpret.</dt>
 * <dd>In most cases, the interpretation of results summarized in a tree is very simple. This
 * simplicity is useful not only for purposes of rapid classification of new observations, but can
 * also often yield a much simpler "model" for explaining why observations are classified or
 * predicted in a particular manner.</dd>
 * <dt>Able to handle both numerical and categorical data.</dt>
 * <dd>Other techniques are usually specialized in analyzing datasets that have only one type of
 * variable.</dd>
 * <dt>Tree methods are nonparametric and nonlinear.</dt>
 * <dd>The final results of using tree methods for classification or regression can be summarized in
 * a series of (usually few) logical if-then conditions (tree nodes). Therefore, there is no
 * implicit assumption that the underlying relationships between the predictor variables and the
 * dependent variable are linear, follow some specific non-linear link function, or that they are
 * even monotonic in nature. Thus, tree methods are particularly well suited for data mining tasks,
 * where there is often little a priori knowledge nor any coherent set of theories or predictions
 * regarding which variables are related and how. In those types of data analytics, tree methods can
 * often reveal simple relationships between just a few variables that could have easily gone
 * unnoticed using other analytic techniques.</dd>
 * </dl>
 * One major problem with classification and regression trees is their high variance. Often a small
 * change in the data can result in a very different series of splits, making interpretation
 * somewhat precarious. Besides, decision-tree learners can create over-complex trees that cause
 * over-fitting. Mechanisms such as pruning are necessary to avoid this problem. Another limitation
 * of trees is the lack of smoothness of the prediction surface.
 * <p>
 * Some techniques such as bagging, boosting, and random forest use more than one decision tree for
 * their analysis.
 */
public class DecisionTree implements Classifier<Vector> {

    /**
     * Training dataset.
     */
    @Nonnull
    private final Matrix _X;
    /**
     * class labels.
     */
    @Nonnull
    private final int[] _y;
    /**
     * The samples for training this node. Note that samples[i] is the number of sampling of
     * dataset[i]. 0 means that the datum is not included and values of greater than 1 are possible
     * because of sampling with replacement.
     */
    @Nonnull
    private final int[] _samples;
    /**
     * The index of training values in ascending order. Note that only numeric attributes will be
     * sorted.
     */
    @Nonnull
    private final ColumnMajorIntMatrix _order;
    /**
     * An index that maps their current position in the {@link #_order} to their original locations
     * in {@link #_samples}.
     */
    @Nonnull
    private final int[] _sampleIndex;
    /**
     * The attributes of independent variable.
     */
    @Nonnull
    private final RoaringBitmap _nominalAttrs;
    /**
     * Variable importance. Every time a split of a node is made on variable the (GINI, information
     * gain, etc.) impurity criterion for the two descendant nodes is less than the parent node.
     * Adding up the decreases for each individual variable over the tree gives a simple measure of
     * variable importance.
     */
    @Nonnull
    private final Vector _importance;
    /**
     * The root of the regression tree
     */
    @Nonnull
    private final Node _root;
    /**
     * The maximum number of the tree depth
     */
    private final int _maxDepth;
    /**
     * The splitting rule.
     */
    @Nonnull
    private final SplitRule _rule;
    /**
     * The number of classes.
     */
    private final int _k;
    /**
     * The number of input variables to be used to determine the decision at a node of the tree.
     */
    private final int _numVars;
    /**
     * The number of instances in a node below which the tree will not split.
     */
    private final int _minSplit;
    /**
     * The minimum number of samples in a leaf node.
     */
    private final int _minSamplesLeaf;
    /**
     * The random number generator.
     */
    @Nonnull
    private final PRNG _rnd;

    /**
     * The criterion to choose variable to split instances.
     */
    public static enum SplitRule {
        /**
         * Used by the CART algorithm, Gini impurity is a measure of how often a randomly chosen
         * element from the set would be incorrectly labeled if it were randomly labeled according
         * to the distribution of labels in the subset. Gini impurity can be computed by summing the
         * probability of each item being chosen times the probability of a mistake in categorizing
         * that item. It reaches its minimum (zero) when all cases in the node fall into a single
         * target category.
         */
        GINI,
        /**
         * Used by the ID3, C4.5 and C5.0 tree generation algorithms.
         */
        ENTROPY,
        /**
         * Classification error.
         */
        CLASSIFICATION_ERROR
    }

    /**
     * Classification tree node.
     */
    public static final class Node implements Externalizable {

        /**
         * Predicted class label for this node.
         */
        int output = -1;
        /**
         * A posteriori probability based on sample ratios in this node.
         */
        @Nullable
        double[] posteriori = null;
        /**
         * The split feature for this node.
         */
        int splitFeature = -1;
        /**
         * The type of split feature
         */
        boolean quantitativeFeature = true;
        /**
         * The split value.
         */
        double splitValue = Double.NaN;
        /**
         * Reduction in splitting criterion.
         */
        double splitScore = 0.0;
        /**
         * Children node.
         */
        Node trueChild = null;
        /**
         * Children node.
         */
        Node falseChild = null;
        /**
         * Predicted output for children node.
         */
        int trueChildOutput = -1;
        /**
         * Predicted output for children node.
         */
        int falseChildOutput = -1;

        public Node() {}// for Externalizable

        public Node(int output, @Nonnull double[] posteriori) {
            this.output = output;
            this.posteriori = posteriori;
        }

        private boolean isLeaf() {
            return posteriori != null;
        }

        private void markAsLeaf() {
            this.splitFeature = -1;
            this.splitValue = Double.NaN;
            this.splitScore = 0.0;
            this.trueChild = null;
            this.falseChild = null;
        }

        @VisibleForTesting
        public int predict(@Nonnull final double[] x) {
            return predict(new DenseVector(x));
        }

        /**
         * Evaluate the regression tree over an instance.
         */
        public int predict(@Nonnull final Vector x) {
            if (trueChild == null && falseChild == null) {
                return output;
            } else {
                if (quantitativeFeature) {
                    if (x.get(splitFeature, Double.NaN) <= splitValue) {
                        return trueChild.predict(x);
                    } else {
                        return falseChild.predict(x);
                    }
                } else {
                    if (x.get(splitFeature, Double.NaN) == splitValue) {
                        return trueChild.predict(x);
                    } else {
                        return falseChild.predict(x);
                    }
                }
            }
        }

        /**
         * Evaluate the regression tree over an instance.
         */
        public void predict(@Nonnull final Vector x, @Nonnull final PredictionHandler handler) {
            if (trueChild == null && falseChild == null) {
                handler.handle(output, posteriori);
            } else {
                if (quantitativeFeature) {
                    if (x.get(splitFeature, Double.NaN) <= splitValue) {
                        trueChild.predict(x, handler);
                    } else {
                        falseChild.predict(x, handler);
                    }
                } else {
                    if (x.get(splitFeature, Double.NaN) == splitValue) {
                        trueChild.predict(x, handler);
                    } else {
                        falseChild.predict(x, handler);
                    }
                }
            }
        }

        public void exportJavascript(@Nonnull final StringBuilder builder,
                @Nullable final String[] featureNames, @Nullable final String[] classNames,
                final int depth) {
            if (trueChild == null && falseChild == null) {
                indent(builder, depth);
                builder.append("").append(resolveName(output, classNames)).append(";\n");
            } else {
                indent(builder, depth);
                if (quantitativeFeature) {
                    if (featureNames == null) {
                        builder.append("if( x[")
                               .append(splitFeature)
                               .append("] <= ")
                               .append(splitValue)
                               .append(" ) {\n");
                    } else {
                        builder.append("if( ")
                               .append(resolveFeatureName(splitFeature, featureNames))
                               .append(" <= ")
                               .append(splitValue)
                               .append(" ) {\n");
                    }
                } else {
                    if (featureNames == null) {
                        builder.append("if( x[")
                               .append(splitFeature)
                               .append("] == ")
                               .append(splitValue)
                               .append(" ) {\n");
                    } else {
                        builder.append("if( ")
                               .append(resolveFeatureName(splitFeature, featureNames))
                               .append(" == ")
                               .append(splitValue)
                               .append(" ) {\n");
                    }
                }
                trueChild.exportJavascript(builder, featureNames, classNames, depth + 1);
                indent(builder, depth);
                builder.append("} else  {\n");
                falseChild.exportJavascript(builder, featureNames, classNames, depth + 1);
                indent(builder, depth);
                builder.append("}\n");
            }
        }

        public void exportGraphviz(@Nonnull final StringBuilder builder,
                @Nullable final String[] featureNames, @Nullable final String[] classNames,
                @Nonnull final String outputName, @Nullable final double[] colorBrew,
                @Nonnull final MutableInt nodeIdGenerator, final int parentNodeId) {
            final int myNodeId = nodeIdGenerator.getValue();

            if (trueChild == null && falseChild == null) {
                // fillcolor=h,s,v
                // https://en.wikipedia.org/wiki/HSL_and_HSV
                // http://www.graphviz.org/doc/info/attrs.html#k:colorList
                String hsvColor = (colorBrew == null || output >= colorBrew.length) ? "#00000000"
                        : String.format("%.4f,1.000,1.000", colorBrew[output]);
                builder.append(
                    String.format(" %d [label=<%s = %s>, fillcolor=\"%s\", shape=ellipse];\n",
                        myNodeId, outputName, resolveName(output, classNames), hsvColor));

                if (myNodeId != parentNodeId) {
                    builder.append(' ').append(parentNodeId).append(" -> ").append(myNodeId);
                    if (parentNodeId == 0) {
                        if (myNodeId == 1) {
                            builder.append(
                                " [labeldistance=2.5, labelangle=45, headlabel=\"True\"]");
                        } else {
                            builder.append(
                                " [labeldistance=2.5, labelangle=-45, headlabel=\"False\"]");
                        }
                    }
                    builder.append(";\n");
                }
            } else {
                if (quantitativeFeature) {
                    builder.append(
                        String.format(" %d [label=<%s &le; %s>, fillcolor=\"#00000000\"];\n",
                            myNodeId, resolveFeatureName(splitFeature, featureNames),
                            Double.toString(splitValue)));
                } else {
                    builder.append(
                        String.format(" %d [label=<%s = %s>, fillcolor=\"#00000000\"];\n", myNodeId,
                            resolveFeatureName(splitFeature, featureNames),
                            Double.toString(splitValue)));
                }
                if (myNodeId != parentNodeId) {
                    builder.append(' ').append(parentNodeId).append(" -> ").append(myNodeId);
                    if (parentNodeId == 0) {//only draw edge label on top
                        if (myNodeId == 1) {
                            builder.append(
                                " [labeldistance=2.5, labelangle=45, headlabel=\"True\"]");
                        } else {
                            builder.append(
                                " [labeldistance=2.5, labelangle=-45, headlabel=\"False\"]");
                        }
                    }
                    builder.append(";\n");
                }

                nodeIdGenerator.addValue(1);
                trueChild.exportGraphviz(builder, featureNames, classNames, outputName, colorBrew,
                    nodeIdGenerator, myNodeId);
                nodeIdGenerator.addValue(1);
                falseChild.exportGraphviz(builder, featureNames, classNames, outputName, colorBrew,
                    nodeIdGenerator, myNodeId);
            }
        }

        @Deprecated
        public int opCodegen(@Nonnull final List<String> scripts, int depth) {
            int selfDepth = 0;
            final StringBuilder buf = new StringBuilder();
            if (trueChild == null && falseChild == null) {
                buf.append("push ").append(output);
                scripts.add(buf.toString());
                buf.setLength(0);
                buf.append("goto last");
                scripts.add(buf.toString());
                selfDepth += 2;
            } else {
                if (quantitativeFeature) {
                    buf.append("push ").append("x[").append(splitFeature).append("]");
                    scripts.add(buf.toString());
                    buf.setLength(0);
                    buf.append("push ").append(splitValue);
                    scripts.add(buf.toString());
                    buf.setLength(0);
                    buf.append("ifle ");
                    scripts.add(buf.toString());
                    depth += 3;
                    selfDepth += 3;
                    int trueDepth = trueChild.opCodegen(scripts, depth);
                    selfDepth += trueDepth;
                    scripts.set(depth - 1, "ifle " + String.valueOf(depth + trueDepth));
                    int falseDepth = falseChild.opCodegen(scripts, depth + trueDepth);
                    selfDepth += falseDepth;
                } else {
                    buf.append("push ").append("x[").append(splitFeature).append("]");
                    scripts.add(buf.toString());
                    buf.setLength(0);
                    buf.append("push ").append(splitValue);
                    scripts.add(buf.toString());
                    buf.setLength(0);
                    buf.append("ifeq ");
                    scripts.add(buf.toString());
                    depth += 3;
                    selfDepth += 3;
                    int trueDepth = trueChild.opCodegen(scripts, depth);
                    selfDepth += trueDepth;
                    scripts.set(depth - 1, "ifeq " + String.valueOf(depth + trueDepth));
                    int falseDepth = falseChild.opCodegen(scripts, depth + trueDepth);
                    selfDepth += falseDepth;
                }
            }
            return selfDepth;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(splitFeature);
            out.writeByte(quantitativeFeature ? NUMERIC : NOMINAL);
            out.writeDouble(splitValue);

            if (isLeaf()) {
                out.writeBoolean(true);

                out.writeInt(output);
                out.writeInt(posteriori.length);
                for (int i = 0; i < posteriori.length; i++) {
                    out.writeDouble(posteriori[i]);
                }
            } else {
                out.writeBoolean(false);

                if (trueChild == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    trueChild.writeExternal(out);
                }
                if (falseChild == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    falseChild.writeExternal(out);
                }
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.splitFeature = in.readInt();
            final byte typeId = in.readByte();
            this.quantitativeFeature = (typeId == NUMERIC);
            this.splitValue = in.readDouble();

            if (in.readBoolean()) {//isLeaf
                this.output = in.readInt();

                final int size = in.readInt();
                final double[] posteriori = new double[size];
                for (int i = 0; i < size; i++) {
                    posteriori[i] = in.readDouble();
                }
                this.posteriori = posteriori;
            } else {
                if (in.readBoolean()) {
                    this.trueChild = new Node();
                    trueChild.readExternal(in);
                }
                if (in.readBoolean()) {
                    this.falseChild = new Node();
                    falseChild.readExternal(in);
                }
            }
        }

    }

    private static void indent(final StringBuilder builder, final int depth) {
        for (int i = 0; i < depth; i++) {
            builder.append("  ");
        }
    }

    /**
     * Classification tree node for training purpose.
     */
    private final class TrainNode implements Comparable<TrainNode> {
        /**
         * The associated regression tree node.
         */
        @Nonnull
        final Node node;
        /**
         * Depth of the node in the tree
         */
        final int depth;
        /**
         * The lower bound (inclusive) in the order array of the samples belonging to this node.
         */
        final int low;
        /**
         * The upper bound (exclusive) in the order array of the samples belonging to this node.
         */
        final int high;
        /**
         * The number of samples
         */
        final int samples;

        public TrainNode(@Nonnull Node node, int depth, int low, int high, int samples) {
            if (low >= high) {
                throw new IllegalArgumentException(
                    "Unexpected condition was met. low=" + low + ", high=" + high);
            }
            this.node = node;
            this.depth = depth;
            this.low = low;
            this.high = high;
            this.samples = samples;
        }

        @Override
        public int compareTo(TrainNode a) {
            return (int) Math.signum(a.node.splitScore - node.splitScore);
        }

        /**
         * Finds the best attribute to split on at the current node.
         *
         * @return true if a split exists to reduce squared error, false otherwise.
         */
        public boolean findBestSplit() {
            // avoid split if tree depth is larger than threshold
            if (depth >= _maxDepth) {
                return false;
            }
            // avoid split if the number of samples is less than threshold
            if (samples <= _minSplit) {
                return false;
            }

            // Sample count in each class.
            final int[] count = new int[_k];
            final boolean pure = countSamples(count);
            if (pure) {// if all instances have same label, stop splitting.
                return false;
            }

            final double impurity = impurity(count, samples, _rule);

            final int[] falseCount = new int[_k];
            for (int varJ : variableIndex()) {
                final Node split = findBestSplit(samples, count, falseCount, impurity, varJ);
                if (split.splitScore > node.splitScore) {
                    node.splitFeature = split.splitFeature;
                    node.quantitativeFeature = split.quantitativeFeature;
                    node.splitValue = split.splitValue;
                    node.splitScore = split.splitScore;
                    node.trueChildOutput = split.trueChildOutput;
                    node.falseChildOutput = split.falseChildOutput;
                }
            }

            return node.splitFeature != -1;
        }

        @Nonnull
        private int[] variableIndex() {
            final Matrix X = _X;
            final IntReservoirSampler sampler = new IntReservoirSampler(_numVars, _rnd.nextLong());
            if (X.isSparse()) {
                // sample columns from sampled examples
                final RoaringBitmap cols = new RoaringBitmap();
                final VectorProcedure proc = new VectorProcedure() {
                    public void apply(final int col) {
                        cols.add(col);
                    }
                };
                final int[] sampleIndex = _sampleIndex;
                for (int i = low; i < high; i++) {
                    int row = sampleIndex[i];
                    X.eachColumnIndexInRow(row, proc);
                }
                cols.forEach(new IntConsumer() {
                    public void accept(final int k) {
                        sampler.add(k);
                    }
                });
            } else {
                final int ncols = X.numColumns();
                for (int i = 0; i < ncols; i++) {
                    sampler.add(i);
                }
            }
            return sampler.getSample();
        }

        private boolean countSamples(@Nonnull final int[] count) {
            final int[] sampleIndex = _sampleIndex;
            final int[] y = _y;

            boolean pure = true;

            for (int i = low, label = -1; i < high; i++) {
                int index = sampleIndex[i];
                int y_i = y[index];
                count[y_i]++;

                if (label == -1) {
                    label = y_i;
                } else if (y_i != label) {
                    pure = false;
                }
            }

            return pure;
        }

        /**
         * Finds the best split cutoff for attribute j at the current node.
         *
         * @param n the number instances in this node.
         * @param count the sample count in each class.
         * @param falseCount an array to store sample count in each class for false child node.
         * @param impurity the impurity of this node.
         * @param j the attribute index to split on.
         */
        private Node findBestSplit(final int n, final int[] count, final int[] falseCount,
                final double impurity, final int j) {
            final int[] samples = _samples;
            final int[] sampleIndex = _sampleIndex;
            final Matrix X = _X;
            final int[] y = _y;
            final int classes = _k;

            final Node splitNode = new Node();

            if (_nominalAttrs.contains(j)) {
                final Int2ObjectMap<int[]> trueCount = new Int2ObjectOpenHashMap<int[]>();

                for (int i = low; i < high; i++) {
                    final int index = sampleIndex[i];
                    final int numSamples = samples[index];
                    if (numSamples == 0) {
                        continue;
                    }

                    final double v = X.get(index, j, Double.NaN);
                    if (Double.isNaN(v)) {
                        continue;
                    }
                    int x_ij = (int) v;
                    int[] tc_x = trueCount.get(x_ij);
                    if (tc_x == null) {
                        tc_x = new int[classes];
                        trueCount.put(x_ij, tc_x);
                    }
                    tc_x[y[index]] += numSamples;
                }

                for (Int2ObjectMap.Entry<int[]> e : trueCount.int2ObjectEntrySet()) {
                    final int l = e.getIntKey();
                    final int[] trueCount_l = e.getValue();

                    final int tc = Math.sum(trueCount_l);
                    final int fc = n - tc;

                    // skip splitting this feature.
                    if (tc < _minSplit || fc < _minSplit) {
                        continue;
                    }

                    for (int k = 0; k < classes; k++) {
                        falseCount[k] = count[k] - trueCount_l[k];
                    }

                    final double gain =
                            impurity - (double) tc / n * impurity(trueCount_l, tc, _rule)
                                    - (double) fc / n * impurity(falseCount, fc, _rule);

                    if (gain > splitNode.splitScore) {
                        // new best split
                        splitNode.splitFeature = j;
                        splitNode.quantitativeFeature = false;
                        splitNode.splitValue = l;
                        splitNode.splitScore = gain;
                        splitNode.trueChildOutput = Math.whichMax(trueCount_l);
                        splitNode.falseChildOutput = Math.whichMax(falseCount);
                    }
                }
            } else {
                final int[] trueCount = new int[classes];

                _order.eachNonNullInColumn(j, low, high, new VectorProcedure() {
                    double prevx = Double.NaN;
                    int prevy = -1;

                    public void apply(final int row, final int i) {
                        final int numSamples = samples[i];
                        if (numSamples == 0) {
                            return;
                        }

                        final double x_ij = X.get(i, j, Double.NaN);
                        if (Double.isNaN(x_ij)) {
                            return;
                        }
                        final int y_i = y[i];

                        if (Double.isNaN(prevx) || x_ij == prevx || y_i == prevy) {
                            prevx = x_ij;
                            prevy = y_i;
                            trueCount[y_i] += numSamples;
                            return;
                        }

                        final int tc = Math.sum(trueCount);
                        final int fc = n - tc;

                        // skip splitting this feature.
                        if (tc < _minSplit || fc < _minSplit) {
                            prevx = x_ij;
                            prevy = y_i;
                            trueCount[y_i] += numSamples;
                            return;
                        }

                        for (int l = 0; l < classes; l++) {
                            falseCount[l] = count[l] - trueCount[l];
                        }

                        final double gain =
                                impurity - (double) tc / n * impurity(trueCount, tc, _rule)
                                        - (double) fc / n * impurity(falseCount, fc, _rule);

                        if (gain > splitNode.splitScore) {
                            // new best split
                            splitNode.splitFeature = j;
                            splitNode.quantitativeFeature = true;
                            splitNode.splitValue = (x_ij + prevx) / 2.d;
                            splitNode.splitScore = gain;
                            splitNode.trueChildOutput = Math.whichMax(trueCount);
                            splitNode.falseChildOutput = Math.whichMax(falseCount);
                        }

                        prevx = x_ij;
                        prevy = y_i;
                        trueCount[y_i] += numSamples;
                    }//apply()
                });
            }

            return splitNode;
        }

        /**
         * Split the node into two children nodes. Returns true if split success.
         *
         * @return true if split occurred. false if the node is set to leaf.
         */
        public boolean split(@Nullable final PriorityQueue<TrainNode> nextSplits) {
            if (node.splitFeature < 0) {
                throw new IllegalStateException("Split a node with invalid feature.");
            }

            final IntPredicate goesLeft = getPredicate();

            // split samples
            final int tc, fc, pivot;
            final double[] trueChildPosteriori = new double[_k],
                    falseChildPosteriori = new double[_k];
            {
                MutableInt tc_ = new MutableInt(0);
                MutableInt fc_ = new MutableInt(0);
                pivot = splitSamples(tc_, fc_, trueChildPosteriori, falseChildPosteriori, goesLeft);
                tc = tc_.get();
                fc = fc_.get();
            }

            if (tc < _minSamplesLeaf || fc < _minSamplesLeaf) {
                node.markAsLeaf();
                return false;
            }

            for (int i = 0; i < _k; i++) {
                trueChildPosteriori[i] /= tc; // divide by zero never happens
                falseChildPosteriori[i] /= fc;
            }

            partitionOrder(low, pivot, high, goesLeft, new int[high - pivot]);

            int leaves = 0;

            node.trueChild = new Node(node.trueChildOutput, trueChildPosteriori);
            TrainNode trueChild = new TrainNode(node.trueChild, depth + 1, low, pivot, tc);
            if (tc >= _minSplit && trueChild.findBestSplit()) {
                if (nextSplits != null) {
                    nextSplits.add(trueChild);
                } else {
                    if (trueChild.split(null) == false) {
                        leaves++;
                    }
                }
            } else {
                leaves++;
            }

            node.falseChild = new Node(node.falseChildOutput, falseChildPosteriori);
            TrainNode falseChild = new TrainNode(node.falseChild, depth + 1, pivot, high, fc);
            if (fc >= _minSplit && falseChild.findBestSplit()) {
                if (nextSplits != null) {
                    nextSplits.add(falseChild);
                } else {
                    if (falseChild.split(null) == false) {
                        leaves++;
                    }
                }
            } else {
                leaves++;
            }

            // Prune meaningless branches
            if (leaves == 2) {// both left and right child are leaf node
                if (node.trueChild.output == node.falseChild.output) {// found a meaningless branch
                    node.markAsLeaf();
                    return false;
                }
            }

            _importance.incr(node.splitFeature, node.splitScore);
            node.posteriori = null; // a posteriori is not needed for non-leaf nodes

            return true;
        }

        /**
         * @return Pivot to split samples
         */
        private int splitSamples(@Nonnull final MutableInt tc, @Nonnull final MutableInt fc,
                @Nonnull final double[] trueChildPosteriori,
                @Nonnull final double[] falseChildPosteriori,
                @Nonnull final IntPredicate goesLeft) {
            final int[] sampleIndex = _sampleIndex;
            final int[] samples = _samples;
            final int[] y = _y;

            int pivot = low;
            for (int k = low; k < high; k++) {
                final int i = sampleIndex[k];
                final int numSamples = samples[i];
                final int yi = y[i];
                if (goesLeft.test(i)) {
                    tc.addValue(numSamples);
                    trueChildPosteriori[yi] += numSamples;
                    pivot++;
                } else {
                    fc.addValue(numSamples);
                    falseChildPosteriori[yi] += numSamples;
                }
            }
            return pivot;
        }

        /**
         * Modifies {@link #order} and {@link #originalOrder} by partitioning the range from low
         * (inclusive) to high (exclusive) so that all elements o for which goesLeft(o) is true come
         * before all elements for which it is false, but element ordering is otherwise preserved.
         * The number of true elements in left must equal split-low.
         * 
         * @param low the low bound of the segment of the order arrays which will be partitioned.
         * @param pivot where the partition's split point will end up.
         * @param high the high bound of the segment of the order arrays which will be partitioned.
         * @param goesLeft whether an element goes to the left side or the right side of the
         *        partition.
         * @param buffer scratch space large enough to hold all elements for which left is false.
         */
        private void partitionOrder(final int low, final int pivot, final int high,
                @Nonnull final IntPredicate goesLeft, @Nonnull final int[] buffer) {
            _order.eachRow(new VectorProcedure() {
                @Override
                public void apply(int col, @Nonnull final int[] row) {
                    partitionArray(row, low, pivot, high, goesLeft, buffer);
                }
            });
            partitionArray(_sampleIndex, low, pivot, high, goesLeft, buffer);
        }

        @Nonnull
        private IntPredicate getPredicate() {
            if (node.quantitativeFeature) {
                return new IntPredicate() {
                    @Override
                    public boolean test(int i) {
                        return _X.get(i, node.splitFeature, Double.NaN) <= node.splitValue;
                    }
                };
            } else {
                return new IntPredicate() {
                    @Override
                    public boolean test(int i) {
                        return _X.get(i, node.splitFeature, Double.NaN) == node.splitValue;
                    }
                };
            }
        }

    }

    /**
     * Modifies an array in-place by partitioning the range from low (inclusive) to high (exclusive)
     * so that all elements i for which left[i - low] is true come before all elements for which it
     * is false, but element ordering is otherwise preserved. The number of true elements in left
     * must equal split-low. buffer is scratch space large enough to hold all elements for which
     * left is false.
     */
    private static void partitionArray(@Nonnull final int[] a, final int low, final int pivot,
            final int high, @Nonnull final IntPredicate goesLeft, @Nonnull final int[] buffer) {
        int j = low;
        int k = 0;
        for (int i = low, end = Math.min(high, a.length); i < end; i++) {
            if (goesLeft.test(a[i])) {
                a[j++] = a[i];
            } else {
                buffer[k++] = a[i];
            }
        }
        if (k != high - pivot || j != pivot) {
            throw new IllegalStateException("Messed up partition.. low=" + low + ", pivot=" + pivot
                    + ", high=" + high + " ended up splitting at " + j);
        }
        System.arraycopy(buffer, 0, a, pivot, k);
    }

    /**
     * Returns the impurity of a node.
     *
     * @param count the sample count in each class.
     * @param n the number of samples in the node.
     * @param rule the rule for splitting a node.
     * @return the impurity of a node
     */
    private static double impurity(@Nonnull final int[] count, final int n,
            @Nonnull final SplitRule rule) {
        double impurity = 0.0;

        switch (rule) {
            case GINI: {
                impurity = 1.0;
                for (int i = 0; i < count.length; i++) {
                    final int count_i = count[i];
                    if (count_i > 0) {
                        double p = (double) count_i / n;
                        impurity -= p * p;
                    }
                }
                break;
            }
            case ENTROPY: {
                for (int i = 0; i < count.length; i++) {
                    final int count_i = count[i];
                    if (count_i > 0) {
                        double p = (double) count_i / n;
                        impurity -= p * Math.log2(p);
                    }
                }
                break;
            }
            case CLASSIFICATION_ERROR: {
                impurity = 0.d;
                for (int i = 0; i < count.length; i++) {
                    final int count_i = count[i];
                    if (count_i > 0) {
                        impurity = Math.max(impurity, (double) count_i / n);
                    }
                }
                impurity = Math.abs(1.d - impurity);
                break;
            }
        }

        return impurity;
    }

    public DecisionTree(@Nullable RoaringBitmap nominalAttrs, @Nonnull Matrix x, @Nonnull int[] y,
            int numLeafs) {
        this(nominalAttrs, x, y, x.numColumns(), Integer.MAX_VALUE, numLeafs, 2, 1, null, null, SplitRule.GINI, null);
    }

    public DecisionTree(@Nullable RoaringBitmap nominalAttrs, @Nullable Matrix x, @Nullable int[] y,
            int numLeafs, @Nullable PRNG rand) {
        this(nominalAttrs, x, y, x.numColumns(), Integer.MAX_VALUE, numLeafs, 2, 1, null, null, SplitRule.GINI, rand);
    }

    /**
     * Constructor. Learns a classification tree for random forest.
     *
     * @param nominalAttrs the attribute properties.
     * @param x the training instances.
     * @param y the response variable.
     * @param numVars the number of input variables to pick to split on at each node. It seems that
     *        dim/3 give generally good performance, where dim is the number of variables.
     * @param maxLeafs the maximum number of leaf nodes in the tree.
     * @param minSplits the number of minimum elements in a node to split
     * @param minSamplesLeaf The minimum number of samples in a leaf node
     * @param order the index of training values in ascending order. Note that only numeric
     *        attributes need be sorted.
     * @param samples the sample set of instances for stochastic learning. samples[i] is the number
     *        of sampling for instance i.
     * @param rule the splitting rule.
     * @param rand random number generator
     */
    public DecisionTree(@Nullable RoaringBitmap nominalAttrs, @Nonnull Matrix x, @Nonnull int[] y,
            int numVars, int maxDepth, int maxLeafs, int minSplits, int minSamplesLeaf,
            @Nullable int[] samples, @Nullable ColumnMajorIntMatrix order, @Nonnull SplitRule rule,
            @Nullable PRNG rand) {
        checkArgument(x, y, numVars, maxDepth, maxLeafs, minSplits, minSamplesLeaf);

        this._X = x;
        this._y = y;

        this._k = Math.max(y) + 1;
        if (_k < 2) {
            throw new IllegalArgumentException("Only one class or negative class labels.");
        }

        if (nominalAttrs == null) {
            nominalAttrs = new RoaringBitmap();
        }
        this._nominalAttrs = nominalAttrs;

        this._numVars = numVars;
        this._maxDepth = maxDepth;
        this._minSplit = minSplits;
        this._minSamplesLeaf = minSamplesLeaf;
        this._rule = rule;
        this._importance = x.isSparse() ? new SparseVector() : new DenseVector(x.numColumns());
        this._rnd = (rand == null) ? RandomNumberGeneratorFactory.createPRNG() : rand;

        final int n = y.length;
        final int[] count = new int[_k];
        final int[] posIndex;
        int totalNumSamples = 0;
        if (samples == null) {
            samples = new int[n];
            posIndex = new int[n];
            for (int i = 0; i < n; i++) {
                samples[i] = 1;
                count[y[i]]++;
                posIndex[i] = i;
            }
            totalNumSamples = n;
        } else {
            final IntArrayList positions = new IntArrayList(n);
            for (int i = 0; i < n; i++) {
                final int sample = samples[i];
                if (sample != 0) {
                    count[y[i]] += sample;
                    positions.add(i);
                    totalNumSamples += sample;
                }
            }
            posIndex = positions.toArray(true);
        }
        this._samples = samples;
        this._order = (order == null) ? SmileExtUtils.sort(nominalAttrs, x, samples) : order;
        this._sampleIndex = posIndex;

        final double[] posteriori = new double[_k];
        for (int i = 0; i < _k; i++) {
            posteriori[i] = (double) count[i] / n;
        }
        this._root = new Node(Math.whichMax(count), posteriori);

        final TrainNode trainRoot =
                new TrainNode(_root, 1, 0, _sampleIndex.length, totalNumSamples);
        if (maxLeafs == Integer.MAX_VALUE) {
            if (trainRoot.findBestSplit()) {
                trainRoot.split(null);
            }
        } else {
            // Priority queue for best-first tree growing.
            final PriorityQueue<TrainNode> nextSplits = new PriorityQueue<TrainNode>();
            // Now add splits to the tree until max tree size is reached
            if (trainRoot.findBestSplit()) {
                nextSplits.add(trainRoot);
            }
            // Pop best leaf from priority queue, split it, and push
            // children nodes into the queue if possible.
            for (int leaves = 1; leaves < maxLeafs; leaves++) {
                // parent is the leaf to split
                TrainNode parent = nextSplits.poll();
                if (parent == null) {
                    break;
                }
                parent.split(nextSplits); // Split the parent node into two children nodes
            }
        }
    }

    @VisibleForTesting
    Node getRootNode() {
        return _root;
    }

    private static void checkArgument(@Nonnull Matrix x, @Nonnull int[] y, int numVars,
            int maxDepth, int maxLeafs, int minSplits, int minLeafSize) {
        if (x.numRows() != y.length) {
            throw new IllegalArgumentException(
                String.format("The sizes of X and Y don't match: %d != %d", x.numRows(), y.length));
        }
        if (y.length == 0) {
            throw new IllegalArgumentException("No training example given");
        }
        if (numVars <= 0 || numVars > x.numColumns()) {
            throw new IllegalArgumentException(
                "Invalid number of variables to split on at a node of the tree: " + numVars);
        }
        if (maxDepth < 2) {
            throw new IllegalArgumentException("maxDepth should be greater than 1: " + maxDepth);
        }
        if (maxLeafs < 2) {
            throw new IllegalArgumentException("Invalid maximum leaves: " + maxLeafs);
        }
        if (minSplits < 2) {
            throw new IllegalArgumentException(
                "Invalid minimum number of samples required to split an internal node: "
                        + minSplits);
        }
        if (minLeafSize < 1) {
            throw new IllegalArgumentException(
                "Invalid minimum size of leaf nodes: " + minLeafSize);
        }
    }

    /**
     * Returns the variable importance. Every time a split of a node is made on variable the (GINI,
     * information gain, etc.) impurity criterion for the two descendent nodes is less than the
     * parent node. Adding up the decreases for each individual variable over the tree gives a
     * simple measure of variable importance.
     *
     * @return the variable importance
     */
    @Nonnull
    public Vector importance() {
        return _importance;
    }

    @VisibleForTesting
    public int predict(@Nonnull final double[] x) {
        return predict(new DenseVector(x));
    }

    @Override
    public int predict(@Nonnull final Vector x) {
        return _root.predict(x);
    }

    /**
     * Predicts the class label of an instance and also calculate a posteriori probabilities. Not
     * supported.
     */
    public int predict(Vector x, double[] posteriori) {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Nonnull
    public String predictJsCodegen(@Nonnull final String[] featureNames,
            @Nonnull final String[] classNames) {
        StringBuilder buf = new StringBuilder(1024);
        _root.exportJavascript(buf, featureNames, classNames, 0);
        return buf.toString();
    }

    @Deprecated
    @Nonnull
    public String predictOpCodegen(@Nonnull final String sep) {
        List<String> opslist = new ArrayList<String>();
        _root.opCodegen(opslist, 0);
        opslist.add("call end");
        String scripts = StringUtils.concat(opslist, sep);
        return scripts;
    }

    @Nonnull
    public byte[] serialize(boolean compress) throws HiveException {
        try {
            if (compress) {
                return ObjectUtils.toCompressedBytes(_root);
            } else {
                return ObjectUtils.toBytes(_root);
            }
        } catch (IOException ioe) {
            throw new HiveException("IOException cause while serializing DecisionTree object", ioe);
        } catch (Exception e) {
            throw new HiveException("Exception cause while serializing DecisionTree object", e);
        }
    }

    @Nonnull
    public static Node deserialize(@Nonnull final byte[] serializedObj, final int length,
            final boolean compressed) throws HiveException {
        final Node root = new Node();
        try {
            if (compressed) {
                ObjectUtils.readCompressedObject(serializedObj, 0, length, root);
            } else {
                ObjectUtils.readObject(serializedObj, length, root);
            }
        } catch (IOException ioe) {
            throw new HiveException("IOException cause while deserializing DecisionTree object",
                ioe);
        } catch (Exception e) {
            throw new HiveException("Exception cause while deserializing DecisionTree object", e);
        }
        return root;
    }

    @Override
    public String toString() {
        return _root == null ? "" : predictJsCodegen(null, null);
    }

}
