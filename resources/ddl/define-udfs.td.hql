create temporary function hivemall_version as 'hivemall.HivemallVersionUDF';
create temporary function train_perceptron as 'hivemall.classifier.PerceptronUDTF';
create temporary function train_pa as 'hivemall.classifier.PassiveAggressiveUDTF';
create temporary function train_pa1 as 'hivemall.classifier.PassiveAggressiveUDTF$PA1';
create temporary function train_pa2 as 'hivemall.classifier.PassiveAggressiveUDTF$PA2';
create temporary function train_cw as 'hivemall.classifier.ConfidenceWeightedUDTF';
create temporary function train_arow as 'hivemall.classifier.AROWClassifierUDTF';
create temporary function train_arowh as 'hivemall.classifier.AROWClassifierUDTF$AROWh';
create temporary function train_scw as 'hivemall.classifier.SoftConfideceWeightedUDTF$SCW1';
create temporary function train_scw2 as 'hivemall.classifier.SoftConfideceWeightedUDTF$SCW2';
create temporary function train_adagrad_rda as 'hivemall.classifier.AdaGradRDAUDTF';
create temporary function train_multiclass_perceptron as 'hivemall.classifier.multiclass.MulticlassPerceptronUDTF';
create temporary function train_multiclass_pa as 'hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF';
create temporary function train_multiclass_pa1 as 'hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA1';
create temporary function train_multiclass_pa2 as 'hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA2';
create temporary function train_multiclass_cw as 'hivemall.classifier.multiclass.MulticlassConfidenceWeightedUDTF';
create temporary function train_multiclass_arow as 'hivemall.classifier.multiclass.MulticlassAROWClassifierUDTF';
create temporary function train_multiclass_arowh as 'hivemall.classifier.multiclass.MulticlassAROWClassifierUDTF$AROWh';
create temporary function train_multiclass_scw as 'hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW1';
create temporary function train_multiclass_scw2 as 'hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW2';
create temporary function cosine_similarity as 'hivemall.knn.similarity.CosineSimilarityUDF';
create temporary function jaccard_similarity as 'hivemall.knn.similarity.JaccardIndexUDF';
create temporary function angular_similarity as 'hivemall.knn.similarity.AngularSimilarityUDF';
create temporary function euclid_similarity as 'hivemall.knn.similarity.EuclidSimilarity';
create temporary function distance2similarity as 'hivemall.knn.similarity.Distance2SimilarityUDF';
create temporary function popcnt as 'hivemall.knn.distance.PopcountUDF';
create temporary function kld as 'hivemall.knn.distance.KLDivergenceUDF';
create temporary function hamming_distance as 'hivemall.knn.distance.HammingDistanceUDF';
create temporary function euclid_distance as 'hivemall.knn.distance.EuclidDistanceUDF';
create temporary function cosine_distance as 'hivemall.knn.distance.CosineDistanceUDF';
create temporary function angular_distance as 'hivemall.knn.distance.AngularDistanceUDF';
create temporary function jaccard_distance as 'hivemall.knn.distance.JaccardDistanceUDF';
create temporary function manhattan_distance as 'hivemall.knn.distance.ManhattanDistanceUDF';
create temporary function minkowski_distance as 'hivemall.knn.distance.MinkowskiDistanceUDF';
create temporary function minhashes as 'hivemall.knn.lsh.MinHashesUDF';
create temporary function minhash as 'hivemall.knn.lsh.MinHashUDTF';
create temporary function bbit_minhash as 'hivemall.knn.lsh.bBitMinHashUDF';
create temporary function voted_avg as 'hivemall.ensemble.bagging.VotedAvgUDAF';
create temporary function weight_voted_avg as 'hivemall.ensemble.bagging.WeightVotedAvgUDAF';
create temporary function max_label as 'hivemall.ensemble.MaxValueLabelUDAF';
create temporary function maxrow as 'hivemall.ensemble.MaxRowUDAF';
create temporary function argmin_kld as 'hivemall.ensemble.ArgminKLDistanceUDAF';
create temporary function mhash as 'hivemall.ftvec.hashing.MurmurHash3UDF';
create temporary function array_hash_values as 'hivemall.ftvec.hashing.ArrayHashValuesUDF';
create temporary function prefixed_hash_values as 'hivemall.ftvec.hashing.ArrayPrefixedHashValuesUDF';
create temporary function feature_hashing as 'hivemall.ftvec.hashing.FeatureHashingUDF';
create temporary function polynomial_features as 'hivemall.ftvec.pairing.PolynomialFeaturesUDF';
create temporary function powered_features as 'hivemall.ftvec.pairing.PoweredFeaturesUDF';
create temporary function rescale as 'hivemall.ftvec.scaling.RescaleUDF';
create temporary function zscore as 'hivemall.ftvec.scaling.ZScoreUDF';
create temporary function l1_normalize as 'hivemall.ftvec.scaling.L1NormalizationUDF';
create temporary function l2_normalize as 'hivemall.ftvec.scaling.L2NormalizationUDF';
create temporary function chi2 as 'hivemall.ftvec.selection.ChiSquareUDF';
create temporary function snr as 'hivemall.ftvec.selection.SignalNoiseRatioUDAF';
create temporary function amplify as 'hivemall.ftvec.amplify.AmplifierUDTF';
create temporary function rand_amplify as 'hivemall.ftvec.amplify.RandomAmplifierUDTF';
create temporary function add_bias as 'hivemall.ftvec.AddBiasUDF';
create temporary function sort_by_feature as 'hivemall.ftvec.SortByFeatureUDF';
create temporary function extract_feature as 'hivemall.ftvec.ExtractFeatureUDF';
create temporary function extract_weight as 'hivemall.ftvec.ExtractWeightUDF';
create temporary function add_feature_index as 'hivemall.ftvec.AddFeatureIndexUDF';
create temporary function feature as 'hivemall.ftvec.FeatureUDF';
create temporary function feature_index as 'hivemall.ftvec.FeatureIndexUDF';
create temporary function conv2dense as 'hivemall.ftvec.conv.ConvertToDenseModelUDAF';
create temporary function to_dense_features as 'hivemall.ftvec.conv.ToDenseFeaturesUDF';
create temporary function to_dense as 'hivemall.ftvec.conv.ToDenseFeaturesUDF';
create temporary function to_sparse_features as 'hivemall.ftvec.conv.ToSparseFeaturesUDF';
create temporary function to_sparse as 'hivemall.ftvec.conv.ToSparseFeaturesUDF';
create temporary function quantify as 'hivemall.ftvec.conv.QuantifyColumnsUDTF';
create temporary function build_bins as 'hivemall.ftvec.binning.BuildBinsUDAF';
create temporary function feature_binning as 'hivemall.ftvec.binning.FeatureBinningUDF';
create temporary function vectorize_features as 'hivemall.ftvec.trans.VectorizeFeaturesUDF';
create temporary function categorical_features as 'hivemall.ftvec.trans.CategoricalFeaturesUDF';
create temporary function ffm_features as 'hivemall.ftvec.trans.FFMFeaturesUDF';
create temporary function indexed_features as 'hivemall.ftvec.trans.IndexedFeatures';
create temporary function quantified_features as 'hivemall.ftvec.trans.QuantifiedFeaturesUDTF';
create temporary function quantitative_features as 'hivemall.ftvec.trans.QuantitativeFeaturesUDF';
create temporary function binarize_label as 'hivemall.ftvec.trans.BinarizeLabelUDTF';
create temporary function onehot_encoding as 'hivemall.ftvec.trans.OnehotEncodingUDAF';
create temporary function bpr_sampling as 'hivemall.ftvec.ranking.BprSamplingUDTF';
create temporary function item_pairs_sampling as 'hivemall.ftvec.ranking.ItemPairsSamplingUDTF';
create temporary function populate_not_in as 'hivemall.ftvec.ranking.PopulateNotInUDTF';
create temporary function tf as 'hivemall.ftvec.text.TermFrequencyUDAF';
create temporary function logress as 'hivemall.regression.LogressUDTF';
create temporary function train_logistic_regr as 'hivemall.regression.LogressUDTF';
create temporary function train_pa1_regr as 'hivemall.regression.PassiveAggressiveRegressionUDTF';
create temporary function train_pa1a_regr as 'hivemall.regression.PassiveAggressiveRegressionUDTF$PA1a';
create temporary function train_pa2_regr as 'hivemall.regression.PassiveAggressiveRegressionUDTF$PA2';
create temporary function train_pa2a_regr as 'hivemall.regression.PassiveAggressiveRegressionUDTF$PA2a';
create temporary function train_arow_regr as 'hivemall.regression.AROWRegressionUDTF';
create temporary function train_arowe_regr as 'hivemall.regression.AROWRegressionUDTF$AROWe';
create temporary function train_arowe2_regr as 'hivemall.regression.AROWRegressionUDTF$AROWe2';
create temporary function train_adagrad_regr as 'hivemall.regression.AdaGradUDTF';
create temporary function train_adadelta_regr as 'hivemall.regression.AdaDeltaUDTF';
create temporary function float_array as 'hivemall.tools.array.AllocFloatArrayUDF';
create temporary function array_remove as 'hivemall.tools.array.ArrayRemoveUDF';
create temporary function sort_and_uniq_array as 'hivemall.tools.array.SortAndUniqArrayUDF';
create temporary function subarray_endwith as 'hivemall.tools.array.SubarrayEndWithUDF';
create temporary function subarray_startwith as 'hivemall.tools.array.SubarrayStartWithUDF';
create temporary function array_concat as 'hivemall.tools.array.ArrayConcatUDF';
create temporary function array_avg as 'hivemall.tools.array.ArrayAvgGenericUDAF';
create temporary function array_sum as 'hivemall.tools.array.ArraySumUDAF';
create temporary function to_string_array as 'hivemall.tools.array.ToStringArrayUDF';
create temporary function array_intersect as 'hivemall.tools.array.ArrayIntersectUDF';
create temporary function select_k_best as 'hivemall.tools.array.SelectKBestUDF';
create temporary function bits_collect as 'hivemall.tools.bits.BitsCollectUDAF';
create temporary function to_bits as 'hivemall.tools.bits.ToBitsUDF';
create temporary function unbits as 'hivemall.tools.bits.UnBitsUDF';
create temporary function bits_or as 'hivemall.tools.bits.BitsORUDF';
create temporary function inflate as 'hivemall.tools.compress.InflateUDF';
create temporary function deflate as 'hivemall.tools.compress.DeflateUDF';
create temporary function map_get_sum as 'hivemall.tools.map.MapGetSumUDF';
create temporary function map_tail_n as 'hivemall.tools.map.MapTailNUDF';
create temporary function to_map as 'hivemall.tools.map.UDAFToMap';
create temporary function to_ordered_map as 'hivemall.tools.map.UDAFToOrderedMap';
create temporary function sigmoid as 'hivemall.tools.math.SigmoidGenericUDF';
create temporary function transpose_and_dot as 'hivemall.tools.matrix.TransposeAndDotUDAF';
create temporary function taskid as 'hivemall.tools.mapred.TaskIdUDF';
create temporary function jobid as 'hivemall.tools.mapred.JobIdUDF';
create temporary function rowid as 'hivemall.tools.mapred.RowIdUDF';
create temporary function generate_series as 'hivemall.tools.GenerateSeriesUDTF';
create temporary function convert_label as 'hivemall.tools.ConvertLabelUDF';
create temporary function x_rank as 'hivemall.tools.RankSequenceUDF';
create temporary function each_top_k as 'hivemall.tools.EachTopKUDTF';
create temporary function tokenize as 'hivemall.tools.text.TokenizeUDF';
create temporary function is_stopword as 'hivemall.tools.text.StopwordUDF';
create temporary function split_words as 'hivemall.tools.text.SplitWordsUDF';
create temporary function normalize_unicode as 'hivemall.tools.text.NormalizeUnicodeUDF';
create temporary function base91 as 'hivemall.tools.text.Base91UDF';
create temporary function unbase91 as 'hivemall.tools.text.Unbase91UDF';
create temporary function lr_datagen as 'hivemall.dataset.LogisticRegressionDataGeneratorUDTF';
create temporary function f1score as 'hivemall.evaluation.F1ScoreUDAF';
create temporary function fmeasure as 'hivemall.evaluation.FMeasureUDAF';
create temporary function mae as 'hivemall.evaluation.MeanAbsoluteErrorUDAF';
create temporary function mse as 'hivemall.evaluation.MeanSquaredErrorUDAF';
create temporary function rmse as 'hivemall.evaluation.RootMeanSquaredErrorUDAF';
create temporary function r2 as 'hivemall.evaluation.R2UDAF';
create temporary function ndcg as 'hivemall.evaluation.NDCGUDAF';
create temporary function precision_at as 'hivemall.evaluation.PrecisionUDAF';
create temporary function recall_at as 'hivemall.evaluation.RecallUDAF';
create temporary function mrr as 'hivemall.evaluation.MRRUDAF';
create temporary function average_precision as 'hivemall.evaluation.MAPUDAF';
create temporary function auc as 'hivemall.evaluation.AUCUDAF';
create temporary function logloss as 'hivemall.evaluation.LogarithmicLossUDAF';
create temporary function mf_predict as 'hivemall.mf.MFPredictionUDF';
create temporary function train_mf_sgd as 'hivemall.mf.MatrixFactorizationSGDUDTF';
create temporary function train_mf_adagrad as 'hivemall.mf.MatrixFactorizationAdaGradUDTF';
create temporary function train_bprmf as 'hivemall.mf.BPRMatrixFactorizationUDTF';
create temporary function bprmf_predict as 'hivemall.mf.BPRMFPredictionUDF';
create temporary function fm_predict as 'hivemall.fm.FMPredictGenericUDAF';
create temporary function train_fm as 'hivemall.fm.FactorizationMachineUDTF';
create temporary function train_randomforest_classifier as 'hivemall.smile.classification.RandomForestClassifierUDTF';
create temporary function train_randomforest_regressor as 'hivemall.smile.regression.RandomForestRegressionUDTF';
create temporary function train_randomforest_regr as 'hivemall.smile.regression.RandomForestRegressionUDTF';
create temporary function tree_predict as 'hivemall.smile.tools.TreePredictUDF';
create temporary function rf_ensemble as 'hivemall.smile.tools.RandomForestEnsembleUDAF';
create temporary function guess_attribute_types as 'hivemall.smile.tools.GuessAttributesUDF';
-- since Hivemall v0.5
create temporary function changefinder as 'hivemall.anomaly.ChangeFinderUDF';
create temporary function sst as 'hivemall.anomaly.SingularSpectrumTransformUDF';
create temporary function train_lda as 'hivemall.topicmodel.LDAUDTF';
create temporary function lda_predict as 'hivemall.topicmodel.LDAPredictUDAF';
create temporary function train_plsa as 'hivemall.topicmodel.PLSAUDTF';
create temporary function plsa_predict as 'hivemall.topicmodel.PLSAPredictUDAF';
create temporary function tile as 'hivemall.geospatial.TileUDF';
create temporary function map_url as 'hivemall.geospatial.MapURLUDF';
create temporary function lat2tiley as 'hivemall.geospatial.Lat2TileYUDF';
create temporary function lon2tilex as 'hivemall.geospatial.Lon2TileXUDF';
create temporary function tilex2lon as 'hivemall.geospatial.TileX2LonUDF';
create temporary function tiley2lat as 'hivemall.geospatial.TileY2LatUDF';
create temporary function haversine_distance as 'hivemall.geospatial.HaversineDistanceUDF';
create temporary function l2_norm as 'hivemall.tools.math.L2NormUDAF';
create temporary function dimsum_mapper as 'hivemall.knn.similarity.DIMSUMMapperUDTF';
create temporary function train_classifier as 'hivemall.classifier.GeneralClassifierUDTF';
create temporary function train_regressor as 'hivemall.regression.GeneralRegressorUDTF';
create temporary function tree_export as 'hivemall.smile.tools.TreeExportUDF';
create temporary function train_ffm as 'hivemall.fm.FieldAwareFactorizationMachineUDTF';
create temporary function ffm_predict as 'hivemall.fm.FFMPredictGenericUDAF';
create temporary function add_field_indices as 'hivemall.ftvec.trans.AddFieldIndicesUDF';
create temporary function to_ordered_list as 'hivemall.tools.list.UDAFToOrderedList';
create temporary function singularize as 'hivemall.tools.text.SingularizeUDF';
create temporary function train_slim as 'hivemall.recommend.SlimUDTF';
create temporary function hitrate as 'hivemall.evaluation.HitRateUDAF';
create temporary function word_ngrams as 'hivemall.tools.text.WordNgramsUDF';
create temporary function approx_count_distinct as 'hivemall.sketch.hll.ApproxCountDistinctUDAF';
create temporary function array_slice as 'hivemall.tools.array.ArraySliceUDF';

-- NLP features
create temporary function tokenize_ja as 'hivemall.nlp.tokenizer.KuromojiUDF';
create temporary function tokenize_cn as 'hivemall.nlp.tokenizer.SmartcnUDF';

-- Backward compatibilities
create temporary function concat_array as 'hivemall.tools.array.ArrayConcatUDF';
create temporary function pa2a_regress as 'hivemall.regression.PassiveAggressiveRegressionUDTF$PA2a';
create temporary function arow_regress as 'hivemall.regression.AROWRegressionUDTF';
create temporary function addBias as 'hivemall.ftvec.AddBiasUDF';
create temporary function tree_predict_v1 as 'hivemall.smile.tools.TreePredictUDFv1';
create temporary function add_field_indicies as 'hivemall.ftvec.trans.AddFieldIndicesUDF';
create temporary function subarray as 'hivemall.tools.array.ArraySliceUDF';

-- alias for TD
create temporary function approx_distinct as 'hivemall.sketch.hll.ApproxCountDistinctUDAF';

create temporary function try_cast as 'hivemall.tools.TryCastUDF';

create temporary function array_append as 'hivemall.tools.array.ArrayAppendUDF';

create temporary function element_at as 'hivemall.tools.array.ArrayElementAtUDF';

create temporary function array_union as 'hivemall.tools.array.ArrayUnionUDF';

create temporary function first_element as 'hivemall.tools.array.FirstElementUDF';

create temporary function last_element as 'hivemall.tools.array.LastElementUDF';

create temporary function array_flatten as 'hivemall.tools.array.ArrayFlattenUDF';

create temporary function map_include_keys as 'hivemall.tools.map.MapIncludeKeysUDF';

create temporary function map_exclude_keys as 'hivemall.tools.map.MapExcludeKeysUDF';

create temporary function array_to_str as 'hadoop.tools.array.ArrayToStrUDF';

create temporary function map_index as 'hivemall.tools.map.MapIndexUDF';

create temporary function map_key_values as 'hivemall.tools.map.MapKeyValuesUDF';

create temporary function sessionize as 'hivemall.tools.datetime.SessionizeUDF';

create temporary function to_json as 'hivemall.tools.json.ToJsonUDF';

create temporary function from_json as 'hivemall.tools.json.FromJsonUDF';

create temporary function assert as 'hivemall.tools.sanity.AssertUDF';

create temporary function raise_error as 'hivemall.tools.sanity.RaiseErrorUDF';

create temporary function moving_avg as 'hivemall.tools.timeseries.MovingAverageUDTF';

create temporary function vector_add as 'hivemall.tools.vector.VectorAddUDF';

create temporary function vector_dot as 'hivemall.tools.vector.VectorDotUDF';

create temporary function bloom_filter as 'hivemall.sketch.bloom.BloomFilterUDAF';

create temporary function bloom_and as 'hivemall.sketch.bloom.BloomAndUDF';

create temporary function bloom_contains as 'hivemall.sketch.bloom.BloomContainsUDF';

create temporary function bloom_not as 'hivemall.sketch.bloom.BloomNotUDF';

create temporary function bloom_or as 'hivemall.sketch.bloom.BloomOrUDF';
