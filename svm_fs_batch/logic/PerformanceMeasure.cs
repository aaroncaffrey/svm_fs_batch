﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal static class PerformanceMeasure
    {
        public const string ModuleName = nameof(PerformanceMeasure);

        internal static readonly double[] ElevenPoints = {1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0};

        internal static List<ConfusionMatrix> CountPredictionError(Prediction[] predictionList, double? threshold = null, int? thresholdClass = null, bool calculateAuc = true, bool asParallel = false, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(prediction_list),prediction_list.ToString()),
            //    (nameof(threshold),threshold.ToString()),
            //    (nameof(threshold_class),threshold_class.ToString()),
            //    (nameof(calculate_auc),calculate_auc.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(count_prediction_error)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var actualClassIdList = predictionList.Select(a => a.RealClassId).Distinct().OrderBy(a => a).ToList();

            var confusionMatrixList = new List<ConfusionMatrix>();

            for (var i = 0; i < actualClassIdList.Count; i++)
            {
                var actualClassId = actualClassIdList[i];

                var confusionMatrix1 = new ConfusionMatrix
                {
                    XClassId = actualClassId,
                    Metrics = new MetricsBox
                    {
                        CmP = predictionList.Count(b => actualClassId == b.RealClassId),
                        CmN = predictionList.Count(b => actualClassId != b.RealClassId)
                    },
                    XPredictionThreshold = threshold,
                    XPredictionThresholdClass = thresholdClass,
                    Thresholds = predictionList.Select(a => a.ProbabilityEstimates.FirstOrDefault(b => b.ClassId == actualClassId).ProbabilityEstimate) /*.Distinct()*/
                        .OrderByDescending(a => a).ToArray(),
                    Predictions = predictionList.ToArray()
                };
                confusionMatrixList.Add(confusionMatrix1);
            }

            for (var predictionListIndex = 0; predictionListIndex < predictionList.Length; predictionListIndex++)
            {
                var prediction = predictionList[predictionListIndex];

                var actualClassMatrix = confusionMatrixList.First(a => a.XClassId == prediction.RealClassId);
                var predictedClassMatrix = confusionMatrixList.First(a => a.XClassId == prediction.PredictedClassId);

                if (prediction.RealClassId == prediction.PredictedClassId)
                {
                    actualClassMatrix.Metrics.CmPTp++;

                    for (var index = 0; index < confusionMatrixList.Count; index++)
                        if (confusionMatrixList[index].XClassId != prediction.RealClassId)
                            confusionMatrixList[index].Metrics.CmNTn++;
                }

                else if (prediction.RealClassId != prediction.PredictedClassId)
                {
                    actualClassMatrix.Metrics.CmPFn++;

                    predictedClassMatrix.Metrics.CmNFp++;
                }
            }

            if (asParallel) Parallel.ForEach(confusionMatrixList, cm => { cm.CalculateThesholdMetrics(cm.Metrics, calculateAuc, predictionList, ct); });
            else
                foreach (var cm in confusionMatrixList)
                    cm.CalculateThesholdMetrics(cm.Metrics, calculateAuc, predictionList, ct);

            return ct.IsCancellationRequested ? default :confusionMatrixList;
        }


        internal static double AreaUnderCurveTrapz((double x, double y)[] coordinateList) //, bool interpolation = true)
        {
            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(coordinate_list),coordinate_list.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(area_under_curve_trapz)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            //var coords = new List<(double x1, double x2, double y1, double y2)>();
            coordinateList = coordinateList.Distinct().ToArray();
            coordinateList = coordinateList.OrderBy(a => a.x).ThenBy(a => a.y).ToArray();
            var auc = coordinateList.Select((c, i) => i >= coordinateList.Length - 1
                ? 0
                : (coordinateList[i + 1].x - coordinateList[i].x) * ((coordinateList[i].y + coordinateList[i + 1].y) / 2)).Sum();
            return auc;
        }


        internal static async Task<Prediction[]> LoadPredictionFileProbabilityValuesAsync((string test_file, string test_comments_file, string prediction_file, string test_class_sample_id_list_file)[] files, bool asParallel = false, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            // method untested
            const string methodName = nameof(LoadPredictionFileProbabilityValuesAsync);

            //var lines = files.AsParallel().AsOrdered().WithCancellation(ct).Select(async (a, i) =>
            //        (
            //            test_file_lines: await io_proxy.ReadAllLines(true, ct, a.test_file, _CallerModuleName: ModuleName, _CallerMethodName: MethodName).ConfigureAwait(false),
            //            test_comments_file_lines: await io_proxy.ReadAllLines(true, ct, a.test_comments_file, _CallerModuleName: ModuleName, _CallerMethodName: MethodName).ConfigureAwait(false),
            //            prediction_file_lines: await io_proxy.ReadAllLines(true, ct, a.prediction_file, _CallerModuleName: ModuleName, _CallerMethodName: MethodName).ConfigureAwait(false),
            //            test_class_sample_id_list_lines: !string.IsNullOrWhiteSpace(a.test_class_sample_id_list_file) ? await io_proxy.ReadAllLines(true, ct, a.test_class_sample_id_list_file, _CallerModuleName: ModuleName, _CallerMethodName: MethodName).ConfigureAwait(false) : null
            //        )).ToArray();


            var linesTasks = asParallel
                ? files.AsParallel().AsOrdered().WithCancellation(ct).Select(async (a, i) => (test_file_lines: await IoProxy.ReadAllLinesAsync(true, ct, a.test_file, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false), test_comments_file_lines: await IoProxy.ReadAllLinesAsync(true, ct, a.test_comments_file, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false), prediction_file_lines: await IoProxy.ReadAllLinesAsync(true, ct, a.prediction_file, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false), test_class_sample_id_list_lines: !string.IsNullOrWhiteSpace(a.test_class_sample_id_list_file)
                    ? await IoProxy.ReadAllLinesAsync(true, ct, a.test_class_sample_id_list_file, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false)
                    : null)).ToArray()
                : files.Select(async (a, i) => (test_file_lines: await IoProxy.ReadAllLinesAsync(true, ct, a.test_file, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false), test_comments_file_lines: await IoProxy.ReadAllLinesAsync(true, ct, a.test_comments_file, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false), prediction_file_lines: await IoProxy.ReadAllLinesAsync(true, ct, a.prediction_file, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false), test_class_sample_id_list_lines: !string.IsNullOrWhiteSpace(a.test_class_sample_id_list_file)
                    ? await IoProxy.ReadAllLinesAsync(true, ct, a.test_class_sample_id_list_file, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false)
                    : null)).ToArray();

            var lines = await Task.WhenAll(linesTasks).ConfigureAwait(false);

            // prediction file MAY have a header, but only if probability estimates are enabled

            //var test_has_headers = false;
            //var test_comments_has_headers = true;

            // check any prediction file has labels on first line
            var predictionHasHeaders = lines.Any(a => a.prediction_file_lines.FirstOrDefault().StartsWith("labels", StringComparison.Ordinal));

            if (predictionHasHeaders)
                // check all labels in all prediction files match
                if (lines.Select(a => a.prediction_file_lines.FirstOrDefault()).Distinct().Count() != 1)
                    throw new ArgumentOutOfRangeException(nameof(files));

            lines = asParallel
                ? lines.AsParallel().AsOrdered().WithCancellation(ct).Select((a, i) => (a.test_file_lines, a.test_comments_file_lines.Skip(1 /* skip header */).ToArray(), a.prediction_file_lines.Skip(i > 0 && predictionHasHeaders
                    ? 1
                    : 0).ToArray(), a.test_class_sample_id_list_lines)).ToArray()
                : lines.Select((a, i) => (a.test_file_lines, a.test_comments_file_lines.Skip(1 /* skip header */).ToArray(), a.prediction_file_lines.Skip(i > 0 && predictionHasHeaders
                    ? 1
                    : 0).ToArray(), a.test_class_sample_id_list_lines)).ToArray();

            var testFileLines = lines.SelectMany(a => a.test_file_lines).ToArray();
            var testCommentsFileLines = lines.SelectMany(a => a.test_comments_file_lines).ToArray();
            var predictionFileLines = lines.SelectMany(a => a.prediction_file_lines).ToArray();

            var testClassSampleIdList = lines.Where(a => a.test_class_sample_id_list_lines != null).SelectMany(a => a.test_class_sample_id_list_lines).Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();
            if (testClassSampleIdList.Length == 0) testClassSampleIdList = null;

            return ct.IsCancellationRequested ? default :LoadPredictionFileProbabilityValuesFromText(testFileLines, testCommentsFileLines, predictionFileLines, testClassSampleIdList, ct: ct);
        }


        internal static async Task<Prediction[]> LoadPredictionFileProbabilityValuesAsync(string testFile, string testCommentsFile, string predictionFile, string testSampleIdListFile = null, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(LoadPredictionFileProbabilityValuesAsync);
            

            //if (string.IsNullOrWhiteSpace(test_file) || !await io_proxy.Exists(test_file, nameof(performance_measure), nameof(load_prediction_file_regression_values)).ConfigureAwait(false) || new FileInfo(test_file).Length == 0)
            //{
            //    throw new Exception($@"Error: Test data file not found: ""{test_file}"".");
            //}

            //if (!await io_proxy.is_file_available(true, test_file))
            //{
            //    throw new Exception($@"Error: Test data file not available for access: ""{test_file}"".");
            //}

            //if (string.IsNullOrWhiteSpace(prediction_file) || !await io_proxy.Exists(prediction_file, nameof(performance_measure), nameof(load_prediction_file_regression_values)) || new FileInfo(prediction_file).Length == 0)
            //{
            //    throw new Exception($@"Error: Prediction output file not found: ""{prediction_file}"".");
            //}

            //if (!await io_proxy.is_file_available(true, prediction_file))
            //{
            //    throw new Exception($@"Error: Prediction output file not available for access: ""{prediction_file}"".");
            //}

            var testFileLines = await IoProxy.ReadAllLinesAsync(true, ct, testFile, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false);

            var testCommentsFileLines = !string.IsNullOrWhiteSpace(testCommentsFile) && IoProxy.ExistsFile(false, testCommentsFile, ModuleName, methodName)
                ? await IoProxy.ReadAllLinesAsync(true, ct, testCommentsFile, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false)
                : null;

            var predictionFileLines = await IoProxy.ReadAllLinesAsync(true, ct, predictionFile, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false);

            var testSampleIdListLines = !string.IsNullOrWhiteSpace(testSampleIdListFile)
                ? await IoProxy.ReadAllLinesAsync(true, ct, testSampleIdListFile, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false)
                : null;
            var testClassSampleIdList = testSampleIdListLines?.Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();

            return ct.IsCancellationRequested ? default :LoadPredictionFileProbabilityValuesFromText(testFileLines, testCommentsFileLines, predictionFileLines, testClassSampleIdList, ct: ct);
        }

        internal static Prediction[] LoadPredictionFileProbabilityValuesFromText(string[] testFileLines, string[] testCommentsFileLines, string[] predictionFileLines, int[] testClassSampleIdList, bool asParallel = false, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            if (testFileLines == null || testFileLines.Length == 0) throw new ArgumentNullException(nameof(testFileLines));

            if (predictionFileLines == null || predictionFileLines.Length == 0) throw new ArgumentNullException(nameof(predictionFileLines));

            // remove comments from test_file_lines (comments start with #)
            testFileLines = testFileLines.Select(a =>
            {
                var hashIndex = a.IndexOf('#', StringComparison.Ordinal);

                if (hashIndex > -1) return ct.IsCancellationRequested ? default :a.Substring(0, hashIndex).Trim();

                return ct.IsCancellationRequested ? default :a.Trim();
            }).ToArray();


            var testFileCommentsHeader = testCommentsFileLines?.FirstOrDefault()?.Split(',') ?? null;

            if (testCommentsFileLines != null && testCommentsFileLines.Length > 0) testCommentsFileLines = testCommentsFileLines.Skip(1).ToArray();


            var testFileData = testFileLines.Where(a => !string.IsNullOrWhiteSpace(a) && int.TryParse(a.Trim().Split().First(), NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var lineActualClassId)).Select(a => a.Trim().Split(new[] {' '}, StringSplitOptions.RemoveEmptyEntries)).ToArray();


            if (testFileLines.Length == 0) return null;


            var predictionFileData = predictionFileLines.Where(a => !string.IsNullOrWhiteSpace(a) && int.TryParse(a.Trim().Split().First(), NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var linePredictedClassId)).Select(a => a.Trim().Split(new[] {' '}, StringSplitOptions.RemoveEmptyEntries)).ToArray();

            //if (prediction_file_lines.Count == 0) return null;
            if (predictionFileData.Length == 0) return null;

            var probabilityEstimateClassLabels = new List<int>();

            if (predictionFileLines.Where(a => a.Trim().StartsWith(@"labels")).Distinct().Count() > 1) /* should be 0 or 1 for the same model */ throw new Exception(@"Error: more than one set of labels in the same file.");

            if (predictionFileLines.First().Trim().Split().First() == @"labels") probabilityEstimateClassLabels = predictionFileLines.First().Trim().Split(new[] {' '}, StringSplitOptions.RemoveEmptyEntries).Skip(1).Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToList();

            if (testCommentsFileLines != null && testFileData.Length != testCommentsFileLines.Length) throw new Exception($@"Error: test file and test comments file have different instance length: [ {testFileData.Length} : {testCommentsFileLines.Length} ].");

            if (testFileData.Length != predictionFileData.Length) throw new Exception($@"Error: test file and prediction file have different instance length: [ {testFileData.Length} : {predictionFileData.Length} ].");

            if (testClassSampleIdList != null && testClassSampleIdList.Length > 0 && testClassSampleIdList.Length != testFileData.Length) throw new Exception($@"Error: test sample ids and test file data do not match: [ {testFileData.Length} : {predictionFileData.Length} ].");

            var totalPredictions = testFileData.Length;


            var predictionList = asParallel
                ? Enumerable.Range(0, totalPredictions).AsParallel().AsOrdered().WithCancellation(ct).Select(predictionIndex =>
                {
                    var probabilityEstimates = predictionFileData[predictionIndex].Length <= 1
                        ? Array.Empty<(int ClassId, double ProbabilityEstimate)>()
                        : predictionFileData[predictionIndex].Skip(1 /* skip predicted class id */).Select((a, i) => (ClassId: probabilityEstimateClassLabels[i], ProbabilityEstimate: double.TryParse(a, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var peOut)
                            ? peOut
                            : default)).OrderBy(a => a.ClassId).ToArray();

                    //var ProbabilityEstimates_stated = ProbabilityEstimates != null && ProbabilityEstimates.Count > 0 && ProbabilityEstimate_class_labels != null && ProbabilityEstimate_class_labels.Count > 0;

                    var prediction = new Prediction
                    {
                        PredictionIndex = predictionIndex,
                        ClassSampleId = testClassSampleIdList != null && testClassSampleIdList.Length - 1 >= predictionIndex
                            ? testClassSampleIdList[predictionIndex]
                            : -1,
                        Comment = testCommentsFileLines != null && testCommentsFileLines.Length > 0
                            ? testCommentsFileLines[predictionIndex].Split(',').Select((a, i) => (CommentHeader: (testFileCommentsHeader?.Length ?? 0) - 1 >= i
                                ? testFileCommentsHeader[i]
                                : "", CommentValue: a)).ToArray()
                            : Array.Empty<(string CommentHeader, string CommentValue)>(),
                        RealClassId = int.TryParse(testFileData[predictionIndex][0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var outRealClassId)
                            ? outRealClassId
                            : default,
                        PredictedClassId = int.TryParse(predictionFileData[predictionIndex][0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var outPredictedClassId)
                            ? outPredictedClassId
                            : default,
                        ProbabilityEstimates = probabilityEstimates
                        //test_row_vector = test_file_data[prediction_index].Skip(1/* skip class id column */).ToArray(),
                    };

                    return ct.IsCancellationRequested ? default :prediction;
                }).ToArray()
                : Enumerable.Range(0, totalPredictions).Select(predictionIndex =>
                {
                    var probabilityEstimates = predictionFileData[predictionIndex].Length <= 1
                        ? Array.Empty<(int ClassId, double ProbabilityEstimate)>()
                        : predictionFileData[predictionIndex].Skip(1 /* skip predicted class id */).Select((a, i) => (ClassId: probabilityEstimateClassLabels[i], ProbabilityEstimate: double.TryParse(a, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var peOut)
                            ? peOut
                            : default)).OrderBy(a => a.ClassId).ToArray();

                    //var ProbabilityEstimates_stated = ProbabilityEstimates != null && ProbabilityEstimates.Count > 0 && ProbabilityEstimate_class_labels != null && ProbabilityEstimate_class_labels.Count > 0;

                    var prediction = new Prediction
                    {
                        PredictionIndex = predictionIndex,
                        ClassSampleId = testClassSampleIdList != null && testClassSampleIdList.Length - 1 >= predictionIndex
                            ? testClassSampleIdList[predictionIndex]
                            : -1,
                        Comment = testCommentsFileLines != null && testCommentsFileLines.Length > 0
                            ? testCommentsFileLines[predictionIndex].Split(',').Select((a, i) => (CommentHeader: (testFileCommentsHeader?.Length ?? 0) - 1 >= i
                                ? testFileCommentsHeader[i]
                                : "", CommentValue: a)).ToArray()
                            : Array.Empty<(string CommentHeader, string CommentValue)>(),
                        RealClassId = int.TryParse(testFileData[predictionIndex][0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var outRealClassId)
                            ? outRealClassId
                            : default,
                        PredictedClassId = int.TryParse(predictionFileData[predictionIndex][0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var outPredictedClassId)
                            ? outPredictedClassId
                            : default,
                        ProbabilityEstimates = probabilityEstimates
                        //test_row_vector = test_file_data[prediction_index].Skip(1/* skip class id column */).ToArray(),
                    };

                    return ct.IsCancellationRequested ? default :prediction;
                }).ToArray();

            return ct.IsCancellationRequested ? default :predictionList;
        }

        //internal static (List<prediction> prediction_list, List<ConfusionMatrix> CmList) load_prediction_file(List<(string test_file, string test_comments_file, string prediction_file)> files, bool calc_ElevenPoint_thresholds)
        //{


        //    var prediction_list = load_prediction_file_regression_values(test_file, test_comments_file, prediction_file);
        //    var CmList = load_prediction_file(prediction_list, calc_ElevenPoint_thresholds);

        //    return ct.IsCancellationRequested ? default :(prediction_list, CmList);

        //}

        internal static async Task<(Prediction[] prediction_list, ConfusionMatrix[] CmList)> LoadPredictionFileAsync(string testFile, string testCommentsFile, string predictionFile, bool calcElevenPointThresholds, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;
            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(test_file),test_await io_proxy.ToString()),
            //    (nameof(prediction_file),prediction_await io_proxy.ToString()),
            //    (nameof(calc_ElevenPoint_thresholds),calc_ElevenPoint_thresholds.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(load_prediction_file)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var predictionList = await LoadPredictionFileProbabilityValuesAsync(testFile, testCommentsFile, predictionFile, ct: ct).ConfigureAwait(false);
            var cmList = LoadPredictionFile(predictionList, calcElevenPointThresholds, ct);

            return ct.IsCancellationRequested ? default :(predictionList, cmList);
        }

        //internal static (List<prediction> prediction_list, List<ConfusionMatrix> CmList) load_prediction_file(string[] test_file_lines, string[] test_comments_file_lines, string[] prediction_file_lines, bool calc_ElevenPoint_thresholds, int[] test_class_sample_id_list)
        internal static (Prediction[] prediction_list, ConfusionMatrix[] CmList) LoadPredictionFile(string[] testFileLines, string[] testCommentsFileLines, string[] predictionFileLines, bool calcElevenPointThresholds, int[] testClassSampleIdList, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(test_file),test_await io_proxy.ToString()),
            //    (nameof(prediction_file),prediction_await io_proxy.ToString()),
            //    (nameof(calc_ElevenPoint_thresholds),calc_ElevenPoint_thresholds.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(load_prediction_file)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var predictionList = LoadPredictionFileProbabilityValuesFromText(testFileLines, testCommentsFileLines, predictionFileLines, testClassSampleIdList, ct: ct);
            var cmList = LoadPredictionFile(predictionList, calcElevenPointThresholds, ct);

            return ct.IsCancellationRequested ? default :(predictionList, cmList);
        }

        internal static ConfusionMatrix[] LoadPredictionFile(Prediction[] predictionList, bool calcElevenPointThresholds, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            if (predictionList == null || predictionList.Length == 0) throw new ArgumentOutOfRangeException(nameof(predictionList));

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(prediction_list),prediction_list.ToString()),
            //    (nameof(calc_ElevenPoint_thresholds),calc_ElevenPoint_thresholds.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(load_prediction_file)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var classIdList = predictionList.SelectMany(a => new[] {a.RealClassId, a.PredictedClassId}).Distinct().OrderBy(a => a).ToArray();

            if (classIdList == null || classIdList.Length == 0) throw new Exception();

            var confusionMatrixList = new List<ConfusionMatrix>();

            // make confusion matrix performance scores with default decision boundary threshold
            var defaultConfusionMatrixList = CountPredictionError(predictionList, ct: ct);
            confusionMatrixList.AddRange(defaultConfusionMatrixList);


            if (classIdList.Length >= 2 && calcElevenPointThresholds)
                // make confusion matrix performance scores with altered default decision boundary threshold
                for (var classIdX = 0; classIdX < classIdList.Length; classIdX++)
                for (var classIdY = 0; classIdY < classIdList.Length; classIdY++)
                {
                    if (classIdX >= classIdY) continue;

                    var negativeId = classIdList[classIdX];
                    var positiveId = classIdList[classIdY];

                    var thresholdPredictionList = ElevenPoints.Select(th => (positive_threshold: th, prediction_list: predictionList.Select(p => new Prediction(p)
                    {
                        PredictedClassId = p.ProbabilityEstimates.First(e => e.ClassId == positiveId).ProbabilityEstimate >= th
                            ? positiveId
                            : negativeId
                    }).ToArray())).ToArray();

                    var thresholdConfusionMatrixList = thresholdPredictionList.SelectMany(a => CountPredictionError(a.prediction_list, a.positive_threshold, positiveId, false, ct: ct)).ToArray();


                    for (var i = 0; i < thresholdConfusionMatrixList.Length; i++)
                    {
                        var classDefaultCm = defaultConfusionMatrixList.First(a => a.XClassId == thresholdConfusionMatrixList[i].XClassId);

                        // note: AUC_ROC and AUC_PR don't change when altering the default threshold since the predicted class isn't a factor in their calculation.

                        thresholdConfusionMatrixList[i].Metrics.PRocAucApproxAll = classDefaultCm.Metrics.PRocAucApproxAll;
                        thresholdConfusionMatrixList[i].Metrics.PRocAucApproxElevenPoint = classDefaultCm.Metrics.PRocAucApproxElevenPoint;
                        thresholdConfusionMatrixList[i].RocXyStrAll = classDefaultCm.RocXyStrAll;
                        thresholdConfusionMatrixList[i].RocXyStrElevenPoint = classDefaultCm.RocXyStrElevenPoint;

                        thresholdConfusionMatrixList[i].Metrics.PPrAucApproxAll = classDefaultCm.Metrics.PPrAucApproxAll;
                        thresholdConfusionMatrixList[i].Metrics.PPrAucApproxElevenPoint = classDefaultCm.Metrics.PPrAucApproxElevenPoint;
                        thresholdConfusionMatrixList[i].PrXyStrAll = classDefaultCm.PrXyStrAll;
                        thresholdConfusionMatrixList[i].PrXyStrElevenPoint = classDefaultCm.PrXyStrElevenPoint;
                        thresholdConfusionMatrixList[i].PriXyStrAll = classDefaultCm.PriXyStrAll;
                        thresholdConfusionMatrixList[i].PriXyStrElevenPoint = classDefaultCm.PriXyStrElevenPoint;
                    }

                    confusionMatrixList.AddRange(thresholdConfusionMatrixList);
                }

            return ct.IsCancellationRequested ? default :confusionMatrixList.ToArray();
        }

        internal static double Brier(Prediction[] predictionList, int positiveId, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            if (predictionList.Any(a => a.ProbabilityEstimates == null || a.ProbabilityEstimates.Length == 0)) return default;

            predictionList = predictionList.OrderByDescending(a => a.ProbabilityEstimates.First(b => b.ClassId == positiveId).ProbabilityEstimate).ToArray();

            // Calc Brier
            var brierScore = 1 / (double) predictionList.Length * predictionList.Sum(a => Math.Pow(a.ProbabilityEstimates.First(b => b.ClassId == a.PredictedClassId).ProbabilityEstimate - (a.RealClassId == a.PredictedClassId
                    ? 1
                    : 0),
                2));

            return ct.IsCancellationRequested ? default :brierScore;
        }

        internal static (double roc_auc_approx, double roc_auc_actual, double pr_auc_approx, double pri_auc_approx, double ap, double api, (double x, double y)[] roc_xy, (double x, double y)[] pr_xy, (double x, double y)[] pri_xy) Calculate_ROC_PR_AUC(Prediction[] predictionList, int positiveId, ThresholdType thresholdType = ThresholdType.AllThresholds, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            if (predictionList.Any(a => a.ProbabilityEstimates == null || a.ProbabilityEstimates.Length == 0)) return default;

            // Assume binary classifier - get negative class id
            var classIds = predictionList.Select(a => a.RealClassId).Union(predictionList.Select(c => c.PredictedClassId)).Distinct().OrderBy(a => a).ToArray();
            var negativeId = classIds.First(classId => classId != positiveId);

            // Calc P
            var p = (double) predictionList.Count(a => a.RealClassId == positiveId);

            // Calc N
            var n = (double) predictionList.Count(a => a.RealClassId != positiveId);

            // Order predictions descending by positive class probability
            predictionList = predictionList.OrderByDescending(a => a.ProbabilityEstimates.FirstOrDefault(b => b.ClassId == positiveId).ProbabilityEstimate).ToArray();

            var thresholdConfusionMatrixList = Get_theshold_confusion_matrices(predictionList, positiveId, thresholdType, negativeId, ct);


            // Average Precision (Not Approximated)
            var ap = Calc_average_precision(thresholdConfusionMatrixList, ct);

            // Average Precision Interpolated (Not Approximated)
            var api = Calc_average_precision_interpolated(thresholdConfusionMatrixList, ct);


            // PR Curve Coordinates
            var prPlotCoords = Calc_pr_plot(thresholdConfusionMatrixList, false, ct);

            // PR Approx
            var prAucApprox = AreaUnderCurveTrapz(prPlotCoords);


            // PRI Curve Coordinates
            var priPlotCoords = Calc_pr_plot(thresholdConfusionMatrixList, true, ct);

            // PRI Approx
            var priAucApprox = AreaUnderCurveTrapz(priPlotCoords);


            // ROC Curve Coordinates
            var rocPlotCoords = Calc_roc_plot(thresholdConfusionMatrixList, ct);

            // ROC Approx
            var rocAucApprox = AreaUnderCurveTrapz(rocPlotCoords);

            // ROC (Not Approximated, and Not reduced to Eleven Points - Incompatible with 11 points)
            var rocAucActual = Calc_roc_auc(predictionList, positiveId, p, n, ct);

            return ct.IsCancellationRequested ? default :(rocAucApprox, rocAucActual, prAucApprox, priAucApprox, ap, api, roc_xy: rocPlotCoords, pr_xy: prPlotCoords, pri_xy: priPlotCoords);
        }

        private static ConfusionMatrix[] Get_theshold_confusion_matrices(Prediction[] predictionList, int positiveId, ThresholdType thresholdType, int negativeId, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            // Get thresholds list (either all thresholds or 11 points)
            double[] thresholds = null;

            if (thresholdType == ThresholdType.AllThresholds) thresholds = predictionList.Select(a => a.ProbabilityEstimates.FirstOrDefault(b => b.ClassId == positiveId).ProbabilityEstimate).Distinct().OrderByDescending(a => a).ToArray();
            else if (thresholdType == ThresholdType.ElevenPoints) thresholds = ElevenPoints;
            else throw new NotSupportedException();

            // Calc predictions at each threshold
            var thresholdPredictionList = thresholds.Select(t => (positive_threshold: t, prediction_list: predictionList.Select(pl => new Prediction(pl)
            {
                PredictedClassId = pl.ProbabilityEstimates.FirstOrDefault(e => e.ClassId == positiveId).ProbabilityEstimate >= t
                    ? positiveId
                    : negativeId
            }).ToArray())).ToArray();

            // Calc confusion matrices at each threshold
            var thresholdConfusionMatrixList = thresholdPredictionList.SelectMany(a => CountPredictionError(a.prediction_list, a.positive_threshold, positiveId, false, ct: ct)).Where(a => a.XClassId == positiveId).ToArray();

            return ct.IsCancellationRequested ? default :thresholdConfusionMatrixList;
        }

        private static double Calc_roc_auc(Prediction[] predictionList, int positiveId, double p, double n, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            var totalNegForThreshold = predictionList.Select((a, i) => (actual_class: a.RealClassId, total_neg_at_point: predictionList.Where((b, j) => j <= i && b.RealClassId != positiveId).Count())).ToList();

            var rocAucActual = 1 / (p * n) * predictionList.Select((a, i) =>
            {
                if (a.RealClassId != positiveId) return 0;
                var totalNAtCurrentThreshold = totalNegForThreshold[i].total_neg_at_point;

                //var n_more_than_current_n = total_neg_for_threshold.Count(b => b.actual_class == negative_id && b.total_neg_at_point > total_n_at_current_threshold);
                var nMoreThanCurrentN = totalNegForThreshold.Count(b => b.actual_class != positiveId && b.total_neg_at_point > totalNAtCurrentThreshold);

                return ct.IsCancellationRequested ? default :nMoreThanCurrentN;
            }).Sum();
            return ct.IsCancellationRequested ? default :rocAucActual;
        }

        private static (double x, double y)[] Calc_roc_plot(ConfusionMatrix[] thresholdConfusionMatrixList, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;
            if (thresholdConfusionMatrixList == null || thresholdConfusionMatrixList.Length == 0) return null;
            var xy1 = thresholdConfusionMatrixList.Select(a => (x: a.Metrics.PFpr, y: a.Metrics.PTpr)).Distinct().ToArray();

            var needStart = !xy1.Any(a => a.x == 0.0 && a.y == 0.0);
            var needEnd = !xy1.Any(a => a.x == 1.0 && a.y == 1.0);
            if (needStart || needEnd)
            {
                var xy2 = new (double x, double y)[xy1.Length + (needStart
                    ? 1
                    : 0) + (needEnd
                    ? 1
                    : 0)];
                if (needStart) xy2[0] = (0.0, 0.0);
                if (needEnd) xy2[^1] = (1.0, 1.0);
                Array.Copy(xy1,
                    0,
                    xy2,
                    needStart
                        ? 1
                        : 0,
                    xy1.Length);
                xy1 = xy2;
            }

            //todo: check whether 'roc_auc_approx' should be calculated before or after 'OrderBy' y,x statement, or if doesn't matter.
            xy1 = xy1.OrderBy(a => a.y).ThenBy(a => a.x).ToArray();
            return ct.IsCancellationRequested ? default :xy1;
        }

        //private static (double x, double y)[] calc_pri_plot(ConfusionMatrix[] threshold_confusion_matrix_list)
        //{
        //    if (threshold_confusion_matrix_list == null || threshold_confusion_matrix_list.Length == 0) return null;
        //
        //    var size = threshold_confusion_matrix_list.Length;
        //    var need_start = (threshold_confusion_matrix_list.First().metrics.TPR != 0.0);
        //    var need_end = (threshold_confusion_matrix_list.Last().metrics.TPR != 1.0);
        //    var xy = new (double x, double y)[size + (need_start ? 1 : 0) + (need_end ? 1 : 0)];
        //
        //    var pri_plot_coords = threshold_confusion_matrix_list.Select(a =>
        //        {
        //            var max_ppv = threshold_confusion_matrix_list.Where(b => b.metrics.TPR >= a.metrics.TPR).Max(b => b.metrics.PPV);
        //            if (double.IsNaN(max_ppv)) max_ppv = a.metrics.PPV; // 0;
        //
        //            return ct.IsCancellationRequested ? default :(x: a.metrics.TPR, y: max_ppv);
        //        })
        //        .ToArray();
        //
        //    Array.Copy(pri_plot_coords, 0, xy, need_start ? 1 : 0, pri_plot_coords.Length);
        //
        //    if (need_start) { xy[0] = ((double)0.0, threshold_confusion_matrix_list.First().metrics.PPV); }
        //    if (need_end) { var m = threshold_confusion_matrix_list.First(); xy[^1] = ((double)1.0, (double)m.metrics.P / ((double)m.metrics.P + (double)m.metrics.N)); }
        //
        //    return ct.IsCancellationRequested ? default :xy;
        //}

        private static (double x, double y)[] Calc_pr_plot(ConfusionMatrix[] thresholdConfusionMatrixList, bool interpolate, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            if (thresholdConfusionMatrixList == null || thresholdConfusionMatrixList.Length == 0) return null;

            var xy1 = thresholdConfusionMatrixList.Select(a =>
            {
                var maxPpv = thresholdConfusionMatrixList.Where(b => b.Metrics.PTpr >= a.Metrics.PTpr).Max(b => b.Metrics.PPpv);
                if (double.IsNaN(maxPpv)) maxPpv = a.Metrics.PPpv; // 0;

                return ct.IsCancellationRequested ? default :(x: a.Metrics.PTpr, y: interpolate
                    ? maxPpv
                    : a.Metrics.PPpv);
            }).ToArray();

            var needStart = xy1.First().x != 0.0;
            var needEnd = xy1.Last().x != 1.0;
            if (needStart || needEnd)
            {
                var xy2 = new (double x, double y)[xy1.Length + (needStart
                    ? 1
                    : 0) + (needEnd
                    ? 1
                    : 0)];
                Array.Copy(xy1,
                    0,
                    xy2,
                    needStart
                        ? 1
                        : 0,
                    xy1.Length);

                if (needStart) xy2[0] = (0.0, xy1.First().y);

                if (needEnd)
                {
                    var m = thresholdConfusionMatrixList.First();
                    xy2[^1] = (1.0, m.Metrics.CmP / (m.Metrics.CmP + m.Metrics.CmN));
                }

                xy1 = xy2;
            }

            return ct.IsCancellationRequested ? default :xy1;
        }

        private static double Calc_average_precision_interpolated(ConfusionMatrix[] thresholdConfusionMatrixList, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;


            var apiResult = thresholdConfusionMatrixList.Select((a, i) =>
            {
                var maxPpv = thresholdConfusionMatrixList.Where(b => b.Metrics.PTpr >= a.Metrics.PTpr).Max(b => b.Metrics.PPpv);

                if (double.IsNaN(maxPpv) /* || max_ppv == 0*/
                ) maxPpv = a.Metrics.PPpv; // = 0? =1? unknown: should it be a.PPV, 0, or 1 when there are no true results?

                var deltaTpr = Math.Abs(a.Metrics.PTpr - (i == 0
                    ? 0
                    : thresholdConfusionMatrixList[i - 1].Metrics.PTpr));
                //var _ap = a.PPV * delta_tpr;
                var api = maxPpv * deltaTpr;

                if (double.IsNaN(api)) api = 0;

                return ct.IsCancellationRequested ? default :api;
            }).Sum();
            return ct.IsCancellationRequested ? default :apiResult;
        }

        private static double Calc_average_precision(ConfusionMatrix[] thresholdConfusionMatrixList, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;


            var apResult = thresholdConfusionMatrixList.Select((a, i) =>
            {
                //var max_p = threshold_confusion_matrix_list.Where(b => b.TPR >= a.TPR).Max(b => b.PPV);
                var deltaTpr = Math.Abs(a.Metrics.PTpr - (i == 0
                    ? 0
                    : thresholdConfusionMatrixList[i - 1].Metrics.PTpr));
                var ap = a.Metrics.PPpv * deltaTpr;

                if (double.IsNaN(ap)) ap = 0;
                //var _api = max_p * delta_tpr;
                return ct.IsCancellationRequested ? default :ap;
            }).Sum();
            return ct.IsCancellationRequested ? default :apResult;
        }

        internal enum ThresholdType
        {
            AllThresholds, ElevenPoints
        }
    }
}