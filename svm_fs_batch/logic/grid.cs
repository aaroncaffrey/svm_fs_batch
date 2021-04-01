using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    public static class Grid
    {
        public const string ModuleName = nameof(Grid);

        public static GridPoint GetBestRate(List<GridPoint> gridSearchResults, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }

            //const string MethodName = nameof(Getbest_rate);

            // libsvm grid.py: if ((rate > best_rate) || (rate == best_rate && g == best_g && c < best_c))

            var gridPointBest = new GridPoint { GpCvRate = -1 };

            foreach (var result in gridSearchResults)
            {
                var isRateBetter = gridPointBest.GpCvRate <= 0 || result.GpCvRate > -1 && result.GpCvRate > gridPointBest.GpCvRate;

                var isRateSame = result.GpCvRate > -1 && gridPointBest.GpCvRate > -1 && result.GpCvRate == gridPointBest.GpCvRate;

                if (!isRateBetter && isRateSame)
                {
                    var isNewCostLower = result.GpCost != null && gridPointBest.GpCost != null && Math.Abs(result.GpCost.Value) < Math.Abs(gridPointBest.GpCost.Value);
                    var isNewGammaLower = result.GpGamma != null && gridPointBest.GpGamma != null && Math.Abs(result.GpGamma.Value) < Math.Abs(gridPointBest.GpGamma.Value);
                    var isNewEpsilonLower = result.GpEpsilon != null && gridPointBest.GpEpsilon != null && Math.Abs(result.GpEpsilon.Value) < Math.Abs(gridPointBest.GpEpsilon.Value);
                    var isNewCoef0Lower = result.GpCoef0 != null && gridPointBest.GpCoef0 != null && Math.Abs(result.GpCoef0.Value) < Math.Abs(gridPointBest.GpCoef0.Value);
                    var isNewDegreeLower = result.GpDegree != null && gridPointBest.GpDegree != null && Math.Abs(result.GpDegree.Value) < Math.Abs(gridPointBest.GpDegree.Value);
                    var newScore = (isNewCostLower
                        ? 1
                        : 0) + (isNewGammaLower
                        ? 1
                        : 0) + (isNewEpsilonLower
                        ? 1
                        : 0) + (isNewCoef0Lower
                        ? 1
                        : 0) + (isNewDegreeLower
                        ? 1
                        : 0);

                    var isOldCostLower = result.GpCost != null && gridPointBest.GpCost != null && Math.Abs(result.GpCost.Value) > Math.Abs(gridPointBest.GpCost.Value);
                    var isOldGammaLower = result.GpGamma != null && gridPointBest.GpGamma != null && Math.Abs(result.GpGamma.Value) > Math.Abs(gridPointBest.GpGamma.Value);
                    var isOldEpsilonLower = result.GpEpsilon != null && gridPointBest.GpEpsilon != null && Math.Abs(result.GpEpsilon.Value) > Math.Abs(gridPointBest.GpEpsilon.Value);
                    var isOldCoef0Lower = result.GpCoef0 != null && gridPointBest.GpCoef0 != null && Math.Abs(result.GpCoef0.Value) > Math.Abs(gridPointBest.GpCoef0.Value);
                    var isOldDegreeLower = result.GpDegree != null && gridPointBest.GpDegree != null && Math.Abs(result.GpDegree.Value) > Math.Abs(gridPointBest.GpDegree.Value);
                    var oldScore = (isOldCostLower
                        ? 1
                        : 0) + (isOldGammaLower
                        ? 1
                        : 0) + (isOldEpsilonLower
                        ? 1
                        : 0) + (isOldCoef0Lower
                        ? 1
                        : 0) + (isOldDegreeLower
                        ? 1
                        : 0);

                    isRateSame = newScore >= oldScore;
                }

                if (isRateBetter || isRateSame) gridPointBest = new GridPoint(result);
            }

            Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default : gridPointBest;
        }


        public static async Task<GridPoint> GridParameterSearchAsync(bool asParallel, string libsvmTrainExe, string cacheTrainGridCsv, string trainFile, string trainStdoutFile, string trainStderrFile, (int ClassId, double weight)[] classWeights = null, Routines.LibsvmSvmType svmType = Routines.LibsvmSvmType.CSvc, Routines.LibsvmKernelType svmKernel = Routines.LibsvmKernelType.Rbf, int repetitions = -1, int repetitionsIndex = -1, int outerCvFolds = -1, int outerCvIndex = -1, int innerCvFolds = 5, bool probabilityEstimates = false, bool shrinkingHeuristics = true, bool quietMode = true, int memoryLimitMb = 1024, TimeSpan? pointMaxTime = null, double? costExpBegin = -5, double? costExpEnd = 15, double? costExpStep = 2, double? gammaExpBegin = 3, double? gammaExpEnd = -15, double? gammaExpStep = -2, double? epsilonExpBegin = null, //8,
            double? epsilonExpEnd = null, //-1,
            double? epsilonExpStep = null, //1,
            double? coef0ExpBegin = null, double? coef0ExpEnd = null, double? coef0ExpStep = null, double? degreeExpBegin = null, double? degreeExpEnd = null, double? degreeExpStep = null, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            //const string MethodName = nameof(grid_parameter_search);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }


            var cacheList = await GridCacheData.ReadCacheFileAsync(cacheTrainGridCsv, ct).ConfigureAwait(false);

            if (innerCvFolds <= 1) throw new Exception();

            if (svmKernel == Routines.LibsvmKernelType.Precomputed) throw new Exception();


            if (costExpStep == 0)
            {
                if (costExpEnd - costExpBegin != 0) costExpStep = (costExpEnd - costExpBegin) / 10;
                else costExpStep = null;
            }

            if (gammaExpStep == 0)
            {
                if (gammaExpEnd - gammaExpBegin != 0) gammaExpStep = (gammaExpEnd - gammaExpBegin) / 10;
                else gammaExpStep = null;
            }

            if (epsilonExpStep == 0)
            {
                if (epsilonExpEnd - epsilonExpBegin != 0) epsilonExpStep = (epsilonExpEnd - epsilonExpBegin) / 10;
                else epsilonExpStep = null;
            }


            if (coef0ExpStep == 0)
            {
                if (coef0ExpEnd - coef0ExpBegin != 0) coef0ExpStep = (coef0ExpEnd - coef0ExpBegin) / 10;
                else coef0ExpStep = null;
            }

            if (degreeExpStep == 0)
            {
                if (degreeExpEnd - degreeExpBegin != 0) degreeExpStep = (degreeExpEnd - degreeExpBegin) / 10;
                else degreeExpStep = null;
            }

            var costExpList = new List<double?>();
            var gammaExpList = new List<double?>();
            var epsilonExpList = new List<double?>();
            var coef0ExpList = new List<double?>();
            var degreeExpList = new List<double?>();


            // always search for cost, unless not specified
            if (costExpBegin != null && costExpEnd != null && costExpStep != null)
                for (var cExp = costExpBegin; cExp <= costExpEnd && cExp >= costExpBegin || cExp >= costExpEnd && cExp <= costExpBegin; cExp += costExpStep)
                {
                    var cost = Math.Pow(2.0, cExp.Value);
                    costExpList.Add(cost); //(c_exp, c));
                }

            // search gamma only if svm_kernel isn't linear
            if (svmKernel != Routines.LibsvmKernelType.Linear && gammaExpBegin != null && gammaExpEnd != null && gammaExpStep != null)
                for (var gExp = gammaExpBegin; gExp <= gammaExpEnd && gExp >= gammaExpBegin || gExp >= gammaExpEnd && gExp <= gammaExpBegin; gExp += gammaExpStep)
                {
                    var gamma = Math.Pow(2.0, gExp.Value);
                    gammaExpList.Add(gamma);
                }


            // search epsilon only if svm type is svr
            if ((svmType == Routines.LibsvmSvmType.EpsilonSvr || svmType == Routines.LibsvmSvmType.NuSvr) && epsilonExpBegin != null && epsilonExpEnd != null && epsilonExpStep != null)
                for (var pExp = epsilonExpBegin; pExp <= epsilonExpEnd && pExp >= epsilonExpBegin || pExp >= epsilonExpEnd && pExp <= epsilonExpBegin; pExp += epsilonExpStep)
                {
                    var epsilon = Math.Pow(2.0, pExp.Value);
                    epsilonExpList.Add(epsilon);
                }

            // search for coef0 only for sigmoid and polynomial
            if ((svmKernel == Routines.LibsvmKernelType.Sigmoid || svmKernel == Routines.LibsvmKernelType.Polynomial) && coef0ExpBegin != null && coef0ExpEnd != null && coef0ExpStep != null)
                for (var rExp = coef0ExpBegin; rExp <= coef0ExpEnd && rExp >= coef0ExpBegin || rExp >= coef0ExpEnd && rExp <= coef0ExpBegin; rExp += coef0ExpStep)
                {
                    var coef0 = Math.Pow(2.0, rExp.Value);
                    coef0ExpList.Add(coef0);
                }

            // search for degree only for polynomial
            if (svmKernel == Routines.LibsvmKernelType.Polynomial && degreeExpBegin != null && degreeExpEnd != null && degreeExpStep != null)
                for (var dExp = degreeExpBegin; dExp <= degreeExpEnd && dExp >= degreeExpBegin || dExp >= degreeExpEnd && dExp <= degreeExpBegin; dExp += degreeExpStep)
                {
                    var degree = Math.Pow(2.0, dExp.Value);
                    degreeExpList.Add(degree);
                }


            if (costExpList == null || costExpList.Count == 0) costExpList = new List<double?> { null };
            if (gammaExpList == null || gammaExpList.Count == 0) gammaExpList = new List<double?> { null };
            if (epsilonExpList == null || epsilonExpList.Count == 0) epsilonExpList = new List<double?> { null };
            if (coef0ExpList == null || coef0ExpList.Count == 0) coef0ExpList = new List<double?> { null };
            if (degreeExpList == null || degreeExpList.Count == 0) degreeExpList = new List<double?> { null };

            var costExpListLen = costExpList.Count;
            var gammaExpListLen = gammaExpList.Count;
            var epsilonExpListLen = epsilonExpList.Count;
            var coef0ExpListLen = coef0ExpList.Count;
            var degreeExpListLen = degreeExpList.Count;


            var searchGridPoints = new (double? cost, double? gamma, double? epsilon, double? coef0, double? degree)[costExpListLen * gammaExpListLen * epsilonExpListLen * coef0ExpListLen * degreeExpListLen];
            var k = -1;

            for (var costIndex = 0; costIndex < costExpListLen; costIndex++)
                for (var gammaIndex = 0; gammaIndex < gammaExpListLen; gammaIndex++)
                    for (var epsilonIndex = 0; epsilonIndex < epsilonExpListLen; epsilonIndex++)
                        for (var coef0Index = 0; coef0Index < coef0ExpListLen; coef0Index++)
                            for (var degreeIndex = 0; degreeIndex < degreeExpListLen; degreeIndex++)
                                //search_GridPoints.Add((cost_exp_list[cost_index], gamma_exp_list[gamma_index], epsilon_exp_list[epsilon_index], coef0_exp_list[coef0_index], degree_exp_list[degree_index]));
                                searchGridPoints[++k] = (costExpList[costIndex], gammaExpList[gammaIndex], epsilonExpList[epsilonIndex], coef0ExpList[coef0Index], degreeExpList[degreeIndex]);

            searchGridPoints = searchGridPoints.Distinct().OrderByDescending(a => a.cost).ThenByDescending(a => a.gamma).ThenByDescending(a => a.epsilon).ThenByDescending(a => a.coef0).ThenByDescending(a => a.degree).ToArray();

            var cachedSearchGridPoints = searchGridPoints.Where(a => cacheList.Any(b => svmType == b.SvmType && svmKernel == b.SvmKernel && innerCvFolds == b.InnerCvFolds && probabilityEstimates == b.ProbabilityEstimates && shrinkingHeuristics == b.ShrinkingHeuristics && a.cost == b.GridPoint.GpCost && a.gamma == b.GridPoint.GpGamma && a.epsilon == b.GridPoint.GpEpsilon && a.coef0 == b.GridPoint.GpCoef0 && a.degree == b.GridPoint.GpDegree)).ToArray();
            /*
            var cached_search = cache.Where(b => cached_search_GridPoints.Any(a => svm_type == b.svm_type && svm_kernel == b.svm_kernel && inner_cv_folds == b.inner_cv_folds && ProbabilityEstimates == b.ProbabilityEstimates && shrinking_heuristics == b.shrinking_heuristics &&
                                                                             a.cost == b.point.cost &&
                                                                             a.gamma == b.point.gamma &&
                                                                             a.epsilon == b.point.epsilon &&
                                                                             a.coef0 == b.point.coef0 &&
                                                                             a.degree == b.point.degree
            )).ToList();
            */

            searchGridPoints = searchGridPoints.Except(cachedSearchGridPoints).ToArray();

            //var tasks = new List<Task<((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double cv_rate)>>();


            var resultsTasks = asParallel
                ? searchGridPoints.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select(async (point, modelIndex) =>
                {
                    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }

                    var cvRate = (double?)null;

                    while (!ct.IsCancellationRequested)
                    {
                        var modelFilename = $@"{trainFile}_{modelIndex + 1}.model";

                        var trainResult = await Libsvm.TrainAsync(libsvmTrainExe, trainFile, modelFilename, trainStdoutFile, trainStderrFile, point.cost, point.gamma, point.epsilon, point.coef0, point.degree, classWeights, svmType, svmKernel, innerCvFolds, probabilityEstimates, shrinkingHeuristics, pointMaxTime, quietMode, memoryLimitMb, ct: ct).ConfigureAwait(false);

                        if (trainResult == default)
                        {
                            return default;
                        }

                        var trainResultLines = trainResult.stdout?.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToArray();

                        cvRate = LibsvmCvPerf(trainResultLines, ct);

                        if (cvRate != null)
                        {
                            break;
                        }
                    }
                    var gridPoint = new GridPoint(point.cost, point.gamma, point.epsilon, point.coef0, point.degree, cvRate);

                    Logging.LogExit(ModuleName);
                    return ct.IsCancellationRequested ? default : gridPoint;
                }).ToList()
                : searchGridPoints.Select(async (point, modelIndex) =>
                {
                    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }

                    var cvRate = (double?)null;

                    while (!ct.IsCancellationRequested)
                    {
                        var modelFilename = $@"{trainFile}_{modelIndex + 1}.model";

                        var trainResult = await Libsvm.TrainAsync(libsvmTrainExe, trainFile, modelFilename, trainStdoutFile, trainStderrFile, point.cost, point.gamma, point.epsilon, point.coef0, point.degree, classWeights, svmType, svmKernel, innerCvFolds, probabilityEstimates, shrinkingHeuristics, pointMaxTime, quietMode, memoryLimitMb, ct: ct).ConfigureAwait(false);

                        if (trainResult == default)
                        {
                            return default;
                        }

                        var trainResultLines = trainResult.stdout?.Split(new[] {'\r', '\n'}, StringSplitOptions.RemoveEmptyEntries).ToArray();

                        cvRate = LibsvmCvPerf(trainResultLines, ct);

                        if (cvRate != null)
                        {
                            break;
                        }
                    }

                    var gridPoint = new GridPoint(point.cost, point.gamma, point.epsilon, point.coef0, point.degree, cvRate);
                    
                    Logging.LogExit(ModuleName);
                    return ct.IsCancellationRequested ? default : gridPoint;
                }).ToList();

            var results = (await Task.WhenAll(resultsTasks).ConfigureAwait(false)).ToList();

            results = results.Where(a => a != null).ToList();

            if (results == null || results.Count == 0) { Logging.LogExit(ModuleName); return default; }

            results.AddRange(cacheList.Select(cacheItem => cacheItem.GridPoint).ToArray());

            results = results.Distinct().OrderByDescending(a => a.GpCost).ThenByDescending(a => a.GpGamma).ThenByDescending(a => a.GpEpsilon).ThenByDescending(a => a.GpCoef0).ThenByDescending(a => a.GpDegree).ToList();


            if (searchGridPoints.Length > 0)
            {
                var resultsCacheFormat = results.Select(a => new GridCacheData
                {
                    SvmType = svmType,
                    SvmKernel = svmKernel,
                    Repetitions = repetitions,
                    RepetitionsIndex = repetitionsIndex,
                    OuterCvFolds = outerCvFolds,
                    OuterCvIndex = outerCvIndex,
                    InnerCvFolds = innerCvFolds,
                    ProbabilityEstimates = probabilityEstimates,
                    ShrinkingHeuristics = shrinkingHeuristics,
                    GridPoint = new GridPoint(a)
                }).ToArray();

                await GridCacheData.WriteCacheFileAsync(cacheTrainGridCsv, resultsCacheFormat, ct).ConfigureAwait(false);

                //svm_type, svm_kernel, repetitions, RepetitionsIndex, outer_cv_folds, OuterCvIndex, inner_cv_folds, ProbabilityEstimates, shrinking_heuristics, results);
            }

            var bestGridPoint = GetBestRate(results, ct);

            //Logging.WriteLine("Grid search complete.", nameof(grid), nameof(grid_parameter_search));
            Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default : bestGridPoint;
        }


        public static double? LibsvmCvPerf(string[] libsvmResultLines, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);
            //const string MethodName = nameof(libsvm_cv_perf);

            //var v_libsvm_default_cross_validation_index = libsvm_result_lines.FindIndex(a => a.StartsWith("Cross Validation Accuracy = ", StringComparison.Ordinal));
            //var v_libsvm_default_cross_validation_str = v_libsvm_default_cross_validation_index < 0 ? "" : libsvm_result_lines[v_libsvm_default_cross_validation_index].Split()[4];

            if (libsvmResultLines == null || libsvmResultLines.Length == 0)
            {
                Logging.LogExit(ModuleName);
                return null;
            }

            var cvAccuracyLine = libsvmResultLines.FirstOrDefault(a => a.StartsWith("Cross Validation Accuracy = ", StringComparison.OrdinalIgnoreCase));

            if (string.IsNullOrWhiteSpace(cvAccuracyLine))
            {
                Logging.LogExit(ModuleName);
                return null;
            }

            var cvAccuracyLineSplit = cvAccuracyLine.Split();

            var cvAccuracyStr = cvAccuracyLineSplit[4];

            var ret= cvAccuracyStr.Last() == '%'
                ? double.Parse(cvAccuracyStr[..^1], NumberStyles.Float, NumberFormatInfo.InvariantInfo) / 100
                : double.Parse(cvAccuracyStr, NumberStyles.Float, NumberFormatInfo.InvariantInfo);

            Logging.LogExit(ModuleName);

            return ct.IsCancellationRequested
                ? null
                : ret;
        }
    }
}