﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SvmFsBatch.logic;

namespace SvmFsBatch
{
    internal class FsServer
    {
        public const string ModuleName = nameof(FsServer);

        //internal static readonly string _server_id = program.program_args.server_id;//Guid.NewGuid().ToString();
        internal static readonly string ServerFolder = Path.Combine(Program.ProgramArgs.ResultsRootFolder, "_server", Program.ProgramArgs.ServerGuid.ToString());

        internal static async Task FeatureSelectionInitializationAsync(DataSet DataSet, int scoringClassId, string[] scoringMetrics, string ExperimentName, int instanceId, int totalInstances, int repetitions, int outerCvFolds, int outerCvFoldsToRun, int innerFolds, Routines.LibsvmSvmType[] svmTypes, Routines.LibsvmKernelType[] kernels, Scaling.ScaleFunction[] scales,
            //(int ClassId, string ClassName)[] ClassNames,
            (int ClassId, double ClassWeight)[][] classWeightSets, bool calcElevenPointThresholds, int limitIterationNotHigherThanAll = 14, int limitIterationNotHigherThanLast = 7, bool makeOuterCvConfusionMatrices = false, bool testFinalBestBias = false, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return;

            const string methodName = nameof(FeatureSelectionInitializationAsync);
            
            var findBestGroupFeaturesFirst = false;
            var checkIndividualLast = true;


            var serverGuid = Program.ProgramArgs.ServerGuid; // Guid.NewGuid().ToByteArray();
            //var server_guid_bytes = Program.program_args.server_guid.ToByteArray();// Guid.NewGuid().ToByteArray();
            var cp = new ConnectionPool();
            cp.Start("Server", serverGuid, true, false, ct);


            // Get the feature groups within the DataSet
            var groups1 = DataSetGroupMethods.GetMainGroups(DataSet, true, true, true, true, true, true, true, false, false, ct:ct);

            // Limit for testing
            // todo: remove this
            //groups1 = groups1.Take(100).ToArray();

            // Feature select within each gkGroup first, to reduce number of columns
            if (findBestGroupFeaturesFirst)
            {
                Logging.WriteLine($@"Finding best of {groups1.Sum(a => a.columns.Length)} individual columns within the {groups1.Length} groups", ModuleName, methodName);

                // get best features in each gkGroup
                var groups1ReduceInput = DataSetGroupMethods.GetSubGroups(groups1, true, true, true, true, true, true, true, true, true, ct:ct);

                // There is 1 performance test per instance (i.e. each nested cross validation performance test [1-repetition, 5-fold outer, 5-fold inner])
                // This means that if number of groups is less than number of instances, some instances could be idle... problem, but rare enough to ignore.

                var groups1ReduceOutputTasks = groups1ReduceInput
                    //.AsParallel()
                    //.AsOrdered()
                    //.WithCancellation(ct)
                    .Select(async (gkGroup, groupIndex) => await FeatureSelectionWorker(scoringClassId,
                        scoringMetrics,
                        cp,
                        DataSet,
                        gkGroup,
                        true,
                        //save_status: true,
                        null,
                        $"{ExperimentName}_S1_{groupIndex}",
                        //InstanceId: InstanceId,
                        //TotalInstances: TotalInstances,
                        //array_index_start: array_index_start,
                        //array_step: array_step,
                        //array_index_last: array_index_last,
                        repetitions,
                        outerCvFolds,
                        outerCvFoldsToRun,
                        innerFolds,
                        svmTypes,
                        kernels,
                        scales,
                        classWeightSets,
                        calcElevenPointThresholds,
                        0.005,
                        100,
                        limitIterationNotHigherThanAll,
                        limitIterationNotHigherThanLast,
                        ct: ct
                    //make_outer_cv_confusion_matrices: false
                    ).ConfigureAwait(false)).ToArray();

                var groups1ReduceOutput = await Task.WhenAll(groups1ReduceOutputTasks).ConfigureAwait(false);

                // ungroup (gkMember & gkPerspective)
                var groups1ReduceOutputUngrouped = DataSetGroupMethods.Ungroup(groups1ReduceOutput.Select(a => a.BestWinnerGroups).ToArray(), ct:ct);

                // regroup (without gkMember & gkPerspective)
                var groups1ReduceOutputRegrouped = DataSetGroupMethods.GetMainGroups(groups1ReduceOutputUngrouped, true, true, true, true, true, true, true, false, false, ct:ct);

                groups1 = groups1ReduceOutputRegrouped;
            }


            // Feature select between the DataSet groups
            Logging.WriteLine($@"Finding best of {groups1.Length} groups (made of {groups1.Sum(a => a.columns.Length)} columns)", ModuleName, methodName);

            var winner = await FeatureSelectionWorker(scoringClassId,
                scoringMetrics,
                cp,
                DataSet,
                groups1,
                false,
                //save_status: true,
                null,
                $"{ExperimentName}_S2",
                //InstanceId: InstanceId,
                //TotalInstances: TotalInstances,
                //array_index_start: array_index_start,
                //array_step: array_step,
                //array_index_last: array_index_last,
                repetitions,
                outerCvFolds,
                outerCvFoldsToRun,
                innerFolds,
                svmTypes,
                kernels,
                scales,
                classWeightSets,

                //order_by_ppf: order_by_ppf,
                calcElevenPointThresholds,
                0.005,
                100,
                limitIterationNotHigherThanAll,
                limitIterationNotHigherThanLast,
                ct: ct
            //make_outer_cv_confusion_matrices: make_outer_cv_confusion_matrices
            ).ConfigureAwait(false);

            // Column based feature select from the winners
            if (checkIndividualLast)
            {
                // preselect all winner gkGroup columns, then test if feature selection goes backwards.

                var bestWinnerColumns = DataSetGroupMethods.Ungroup(winner.BestWinnerGroups, ct:ct);
                var bestWinnerColumnsInput = DataSetGroupMethods.GetMainGroups(bestWinnerColumns, true, true, true, true, true, true, true, true, true, ct:ct);

                var bestWinnerColumnsOutputStartBackwards = await FeatureSelectionWorker(scoringClassId,
                    scoringMetrics,
                    cp,
                    DataSet,
                    bestWinnerColumnsInput,
                    true,
                    //save_status: true,
                    null,
                    $"{ExperimentName}_S3",
                    //InstanceId: InstanceId,
                    //TotalInstances: TotalInstances,
                    //array_index_start: array_index_start,
                    //array_step: array_step,
                    //array_index_last: array_index_last,
                    repetitions,
                    outerCvFolds,
                    outerCvFoldsToRun,
                    innerFolds,
                    svmTypes,
                    kernels,
                    scales,
                    classWeightSets,
                    calcElevenPointThresholds,
                    0.005,
                    100,
                    limitIterationNotHigherThanAll,
                    limitIterationNotHigherThanLast,
                    ct: ct
                //make_outer_cv_confusion_matrices: false
                ).ConfigureAwait(false);

                var bestWinnerColumnsOutputStartForwards = await FeatureSelectionWorker(scoringClassId,
                    scoringMetrics,
                    cp,
                    DataSet,
                    bestWinnerColumnsInput,
                    false,
                    //save_status: true,
                    null,
                    $"{ExperimentName}_S4",
                    //InstanceId: InstanceId,
                    //TotalInstances: TotalInstances,
                    //array_index_start: array_index_start,
                    //array_step: array_step,
                    //array_index_last: array_index_last,
                    repetitions,
                    outerCvFolds,
                    outerCvFoldsToRun,
                    innerFolds,
                    svmTypes,
                    kernels,
                    scales,
                    classWeightSets,
                    calcElevenPointThresholds,
                    0.005,
                    100,
                    limitIterationNotHigherThanAll,
                    limitIterationNotHigherThanLast,
                    ct: ct
                //make_outer_cv_confusion_matrices: false
                ).ConfigureAwait(false);
            }

            // Check if result is approximately the same with other parameters values (i.e. variance number of repetitions, outer folds, inner folds, etc.)
            if (testFinalBestBias)
            {
                // stage5 ...

                // 1. test variance of kernel & scale
                //feature_selection_worker(DataSet, winner.groups);

                // 2. test variance of repetitions, outer-cv, inner-cv

                // 3. test variance of class weight
            }

            await cp.StopAsync().ConfigureAwait(false);
        }

        internal static async Task<((DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] BestWinnerGroups, (IndexData id, ConfusionMatrix cm, RankScore rs) BestWinnerData, List<(IndexData id, ConfusionMatrix cm, RankScore rs)> winners)> FeatureSelectionWorker(int scoringClassId, string[] scoringMetrics, ConnectionPool cp, DataSet DataSet, (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] groups, bool preselectAllGroups, // preselect all groups
            int[] baseGroupIndexes, //always include these groups
            string ExperimentName,
            //int InstanceId,
            //int TotalInstances,
            int repetitions, int outerCvFolds, int outerCvFoldsToRun, int innerFolds, Routines.LibsvmSvmType[] svmTypes, Routines.LibsvmKernelType[] kernels, Scaling.ScaleFunction[] scales,
            //(int ClassId, string ClassName)[] ClassNames,
            (int ClassId, double ClassWeight)[][] classWeightSets, bool calcElevenPointThresholds, double minScoreIncrease = 0.005, int maxIterations = 100, int limitIterationNotHigherThanAll = 14, int limitIterationNotHigherThanLast = 7,
            //bool make_outer_cv_confusion_matrices = false,
            bool asParallel = true, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(FeatureSelectionWorker);
            const bool overwriteCache = false;

            //while (io_proxy.ExistsFile(true, Path.Combine(_server_folder, $@"exit.csv"), _CallerModuleName: ModuleName, _CallerMethodName: MethodName))
            //{
            //    io_proxy.DeleteFile(true, ct, Path.Combine(_server_folder, $@"exit.csv"), _CallerModuleName: ModuleName, _CallerMethodName: MethodName);
            //}

            
            baseGroupIndexes = baseGroupIndexes?.OrderBy(a => a).Distinct().ToArray();

            (IndexData id, ConfusionMatrix cm, RankScore rs) bestWinnerIdCmRs = default;
            (IndexData id, ConfusionMatrix cm, RankScore rs) lastWinnerIdCmRs = default;
            var allWinnersIdCmRs = new List<(IndexData id, ConfusionMatrix cm, RankScore rs)>();
            var allIterationIdCmRs = new List<(IndexData id, ConfusionMatrix cm, RankScore rs)[]>();
            //var cache_FilesLoaded = new List<string>();
            var featureSelectionFinished = false;
            var iterationIndex = 0;
            var iterationsNotHigherThanBest = 0;
            var iterationsNotHigherThanLast = 0;
            var previousGroupTests = new List<int[]>();
            var selectionExcludedGroups = new List<int>();
            //var has_calibrated = false;
            var calibrate = false;
            var allIndexData = new List<IndexData>();

            void Log(string msg)
            {
                Logging.WriteLine($@"{ExperimentName}, iteration: {iterationIndex}, {msg}.", ModuleName, methodName);
            }

            // todo: add rank positions (for each iteration) to the winning features summary output... 

            Log($@"Total groups: {groups?.Length ?? 0}.");

            while (!featureSelectionFinished)
            {
                if (ct.IsCancellationRequested) return default;
                //var last_iteration_id_cm_rs = all_iteration_id_cm_rs?.LastOrDefault();
                var selectedGroups = lastWinnerIdCmRs.id?.IdGroupArrayIndexes?.ToArray() ?? Array.Empty<int>();
                var selectionExcludedGroups2 = lastWinnerIdCmRs.id != null
                    ? selectionExcludedGroups.Concat(new[] { lastWinnerIdCmRs.id.IdGroupArrayIndex }).ToArray()
                    : selectionExcludedGroups.ToArray();


                if (preselectAllGroups)
                {
                    selectedGroups = selectedGroups.Union(Enumerable.Range(0, groups.Length).ToArray()).ToArray();
                    calibrate = true;
                }

                if (baseGroupIndexes != null && baseGroupIndexes.Length > 0) selectedGroups = selectedGroups.Union(baseGroupIndexes).OrderBy(a => a).ToArray();

                if (calibrate && (selectedGroups == null || selectedGroups.Length == 0))
                {
                    Log($@"{nameof(selectedGroups)}.{nameof(selectedGroups.Length)} = {selectedGroups?.Length ?? 0}.");
                    throw new Exception();
                }


                var groupIndexesToTest = calibrate
                    ? new[] { -1 }
                    : Enumerable.Range(0, groups.Length).Except(selectionExcludedGroups2).ToArray();
                Log($@"{nameof(groupIndexesToTest)}.{nameof(groupIndexesToTest.Length)} = {groupIndexesToTest?.Length ?? 0}.");
                if (groupIndexesToTest == null || groupIndexesToTest.Length == 0) break;

                var previousWinnerGroupIndex = lastWinnerIdCmRs.id?.IdGroupArrayIndex;
                var jobGroupSeries = CacheLoad.JobGroupSeries(DataSet, groups, ExperimentName, iterationIndex, baseGroupIndexes, groupIndexesToTest, selectedGroups, previousWinnerGroupIndex, selectionExcludedGroups2, previousGroupTests, asParallel, ct);
                Log($@"{nameof(jobGroupSeries)}.{nameof(jobGroupSeries.Length)} = {jobGroupSeries?.Length ?? 0}.");
                if (jobGroupSeries == null || jobGroupSeries.Length == 0) break;

                var indexDataContainer = CacheLoad.GetFeatureSelectionInstructions(DataSet, groups, jobGroupSeries, ExperimentName, iterationIndex, groups?.Length ?? 0, /*InstanceId, TotalInstances,*/ repetitions, outerCvFolds, outerCvFoldsToRun, innerFolds, svmTypes, kernels, scales, classWeightSets, calcElevenPointThresholds, baseGroupIndexes, groupIndexesToTest, selectedGroups, previousWinnerGroupIndex, selectionExcludedGroups2, previousGroupTests, ct: ct);

                allIndexData.AddRange(indexDataContainer.IndexesWhole);

                Log($@"{nameof(indexDataContainer)}.{nameof(indexDataContainer.IndexesWhole)}.{nameof(indexDataContainer.IndexesWhole.Length)} = {indexDataContainer?.IndexesWhole?.Length ?? 0}"); //, IndexDataContainer.indexes_partition.Length = {(IndexDataContainer?.indexes_partition?.Length ?? 0)}.");
                if (indexDataContainer.IndexesWhole == null || indexDataContainer.IndexesWhole.Length == 0) break;


                // iteration_all_cm is a list of all merged results (i.e. the individual outer-cross-validation partitions merged)
                var iterationWholeResults = new List<(IndexData id, ConfusionMatrix cm)>();

                // get folder and file names for this iteration (iteration_folder & iteration_whole_cm_filename are the same for all partitions; iteration_partition_cm_filename is specific to the partition)
                var iterationFolder = Program.GetIterationFolder(Program.ProgramArgs.ResultsRootFolder, ExperimentName, iterationIndex, ct: ct);

                //var iteration_whole_cm_filename_full = Path.Combine(iteration_folder, $@"z_{program.GetIterationFilename(IndexDataContainer.IndexesWhole)}_full.cm.csv");
                //var iteration_whole_cm_filename_summary = Path.Combine(iteration_folder, $@"z_{program.GetIterationFilename(IndexDataContainer.IndexesWhole)}_summary.cm.csv");
                //var iteration_partition_cm_filename_full = Path.Combine(iteration_folder, $@"x_{program.GetIterationFilename(IndexDataContainer.indexes_partition)}_full.cm.csv");
                //var iteration_partition_cm_filename_summary = Path.Combine(iteration_folder, $@"x_{program.GetIterationFilename(IndexDataContainer.indexes_partition)}_summary.cm.csv");

                // load cache (first try whole iteration, then try partition, then try individual work items)
                /*
                cache_load.load_cache(ct: ct,
                    //groups: groups,
                    //InstanceId: InstanceId,
                    IterationIndex: IterationIndex,
                    ExperimentName: ExperimentName,
                    wait_for_cache: false,
                    cache_files_already_loaded: cache_FilesLoaded,
                    iteration_cm_sd_list: iteration_whole_results,
                    IndexDataContainer: IndexDataContainer,
                    last_iteration_id_cm_rs: last_iteration_id_cm_rs,
                    last_winner_id_cm_rs: last_winner_id_cm_rs,
                    best_winner_id_cm_rs: best_winner_id_cm_rs);
                */

                // check if this partition is loaded....
                //while (IndexDataContainer.indexes_missing_partition.Any())
                await ServeIpcJobsAsync(cp, indexDataContainer, iterationWholeResults, iterationIndex, ct).ConfigureAwait(false);

                // save partition cache
                //{
                //    // 4. save CM for all groups of this hpc instance (from index start to index end) merged outer-cv results
                //    //var InstanceId2 = InstanceId;
                //    var iteration_partition_results = iteration_whole_results.Where((cm_sd, i) => cm_sd.id.unrolled_InstanceId == InstanceId).ToArray();
                //
                //    ConfusionMatrix.save(ct, iteration_partition_cm_filename_full, iteration_partition_cm_filename_summary, overwrite_cache, iteration_partition_results);
                //    Logging.WriteLine($"[{InstanceId}/{TotalInstances}] {ExperimentName}: Partition cache: Saved for iteration {(IterationIndex)} gkGroup. Files: {iteration_partition_cm_filename_full}, {iteration_partition_cm_filename_summary}.");
                //}


                // check if all partitions are loaded....
                /*
                while (IndexDataContainer.indexes_missing_whole.Any())
                {
                    // 5. load results from other instances (into iteration_whole_results)

                    cache_load.load_cache(ct, IterationIndex, ExperimentName, true, cache_FilesLoaded, iteration_whole_results, IndexDataContainer, last_iteration_id_cm_rs, last_winner_id_cm_rs, best_winner_id_cm_rs);

                    log($@"Partition {(IndexDataContainer.indexes_missing_whole.Length > 0 ? $@"{IndexDataContainer.indexes_missing_whole.Length} incomplete" : $@"complete")} for iteration {(IterationIndex)}.");
                }
                */

                //ConfusionMatrix.save(ct, iteration_whole_cm_filename_full, iteration_whole_cm_filename_summary, overwrite_cache, iteration_whole_results.ToArray());
                //log($@"Full cache: Saved for iteration {(IterationIndex)}. Files: {iteration_whole_cm_filename_full}, {iteration_whole_cm_filename_summary}.");


                // 6. find winner (highest performance of any gkGroup of any class [within scoring_metrics' and 'scoring_ClassIds'])
                //      ensure ordering will be consistent between instances
                //      ensure score_data instances are created, may not have been if from cache.

                var iterationWholeResultsFixedWithRanks = CalculateRanks(scoringClassId, scoringMetrics, iterationWholeResults, iterationIndex, allIterationIdCmRs, bestWinnerIdCmRs, lastWinnerIdCmRs, ct);

                var thisIterationWinnerIdCmRs = iterationWholeResultsFixedWithRanks[0];
                allWinnersIdCmRs.Add(thisIterationWinnerIdCmRs);

                //var iteration_winner_group = this_iteration_winner_id_cm_rs.id.id_group_array_index > -1 ? groups[this_iteration_winner_id_cm_rs.id.id_group_array_index] : default;
                //var iteration_winner_GroupKey = iteration_winner_group != default ? iteration_winner_group.GroupKey : default;


                var numAvailableGroups = NumGroupsAvailable(groups, baseGroupIndexes, calibrate, thisIterationWinnerIdCmRs, selectionExcludedGroups, ct);
                BanPoorPerformanceGroups(ExperimentName, iterationWholeResultsFixedWithRanks, selectionExcludedGroups, numAvailableGroups, iterationIndex, allIterationIdCmRs, ct);
                numAvailableGroups = NumGroupsAvailable(groups, baseGroupIndexes, calibrate, thisIterationWinnerIdCmRs, selectionExcludedGroups, ct);


                featureSelectionFinished = CheckWhetherFinished(ExperimentName, minScoreIncrease, maxIterations, limitIterationNotHigherThanAll, limitIterationNotHigherThanLast, featureSelectionFinished, thisIterationWinnerIdCmRs, lastWinnerIdCmRs, numAvailableGroups, iterationIndex, ref bestWinnerIdCmRs, ref iterationsNotHigherThanLast, ref iterationsNotHigherThanBest,ct);
                await SaveIterationSummaryAsync(ExperimentName, indexDataContainer, /*InstanceId, TotalInstances,*/ iterationFolder, iterationIndex, methodName, overwriteCache, iterationWholeResultsFixedWithRanks, allWinnersIdCmRs, ct).ConfigureAwait(false);

                lastWinnerIdCmRs = thisIterationWinnerIdCmRs;
                allIterationIdCmRs.Add(iterationWholeResultsFixedWithRanks);
                foreach (var iterationWholeResultsFixedWithRank in iterationWholeResultsFixedWithRanks /*.Skip(1)*/)
                {
                    iterationWholeResultsFixedWithRank.cm.ClearSupplemental();
                    iterationWholeResultsFixedWithRank.id.ClearSupplemental();
                }

                iterationIndex++;
                calibrate = false;
                preselectAllGroups = false;
                previousGroupTests.AddRange(jobGroupSeries.Select(a => a.GroupIndexes).ToArray());
            }

            Log($@"Finished: all iterations of feature selection for {groups?.Length ?? 0} groups.");
            Log($@"Finished: winning score = {bestWinnerIdCmRs.rs?.RsFsScore ?? 0}, total columns = {bestWinnerIdCmRs.id?.IdNumColumns ?? 0}.");

            var bestWinnerGroups = bestWinnerIdCmRs.id.IdGroupArrayIndexes.Select(groupIndex => groups[groupIndex]).ToArray();
            await SaveResultsSummaryAsync(allIndexData.ToArray(), groups, ExperimentName, allWinnersIdCmRs, bestWinnerIdCmRs, bestWinnerGroups, methodName, allIterationIdCmRs, ct).ConfigureAwait(false);

            //await io_proxy.WriteAllLines(true, ct, Path.Combine(_server_folder, @"exit.csv"), new[] { "exit" }, _CallerModuleName: ModuleName, _CallerMethodName: MethodName).ConfigureAwait(false);
            return ct.IsCancellationRequested ? default :(bestWinnerGroups, bestWinnerIdCmRs, allWinnersIdCmRs);
        }

        private static (IndexData id, ConfusionMatrix cm, RankScore rs)[] CalculateRanks(int scoringClassId, string[] scoringMetrics, List<(IndexData id, ConfusionMatrix cm)> iterationWholeResults, int iterationIndex, List<(IndexData id, ConfusionMatrix cm, RankScore rs)[]> allIterationIdCmRs, (IndexData id, ConfusionMatrix cm, RankScore rs) bestWinnerIdCmRs, (IndexData id, ConfusionMatrix cm, RankScore rs) lastWinnerIdCmRs, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            var iterationWholeResultsFixed = iterationWholeResults.Where(cmSd => cmSd.id != null && cmSd.cm != null && cmSd.cm.XClassId != null && // class id exists
                                                                                 cmSd.cm.XClassId.Value == scoringClassId && // ...and is the scoring class id
                                                                                 cmSd.cm.XPredictionThreshold == null && // not a threshold altered metric
                                                                                 cmSd.cm.XRepetitionsIndex == -1 && // merged
                                                                                 cmSd.cm.XOuterCvIndex == -1 && // merged
                                                                                 cmSd.id.IdIterationIndex == iterationIndex // this iteration
            ).Select(a =>
            {
                var fsScore = a.cm.Metrics.GetValuesByNames(scoringMetrics).Average();

                return ct.IsCancellationRequested ? default :(a.id, a.cm, fs_score: fsScore);
            }).ToList();

            var iterationWholeResultsFixedWithRanks = SetRanks(iterationWholeResultsFixed, allIterationIdCmRs, bestWinnerIdCmRs, lastWinnerIdCmRs, ct: ct);

            iterationWholeResults.Clear();
            iterationWholeResultsFixed.Clear();

            return !ct.IsCancellationRequested
                ? iterationWholeResultsFixedWithRanks
                : default;
        }

        private static int NumGroupsAvailable((DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] groups, int[] baseGroupIndexes, bool calibrate, (IndexData id, ConfusionMatrix cm, RankScore rs) thisIterationWinnerIdCmRs, List<int> selectionExcludedGroups, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            var availableGroups = Enumerable.Range(0, groups.Length).ToArray();
            if (!calibrate)
            {
                availableGroups = availableGroups.Except(thisIterationWinnerIdCmRs.id.IdGroupArrayIndexes).ToArray();
                if (baseGroupIndexes != null && baseGroupIndexes.Length > 0) availableGroups = availableGroups.Except(baseGroupIndexes).ToArray();
                if (selectionExcludedGroups != null && selectionExcludedGroups.Count > 0) availableGroups = availableGroups.Except(selectionExcludedGroups).ToArray();
            }

            var numAvailableGroups = availableGroups.Length;
            return ct.IsCancellationRequested ? default :numAvailableGroups;
        }

        private static void BanPoorPerformanceGroups(string ExperimentName, (IndexData id, ConfusionMatrix cm, RankScore rs)[] iterationWholeResultsFixedWithRanks, List<int> selectionExcludedGroups, int numAvailableGroups, int iterationIndex, List<(IndexData id, ConfusionMatrix cm, RankScore rs)[]> allIterationIdCmRs, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return;

            void Log(string msg)
            {
                Logging.WriteLine($"{ExperimentName}, iteration: {iterationIndex}, {msg}.");
            }

            {
                var zeroScore = iterationWholeResultsFixedWithRanks.Where(a => a.rs.RsFsScore == 0).Select(a => a.id.IdGroupArrayIndex).ToArray();
                if (zeroScore.Length > 0)
                {
                    selectionExcludedGroups.AddRange(zeroScore);
                    Log($@"Excluding groups with zero scores: {string.Join(", ", zeroScore)}.");
                }
            }

            {
                const int poorTrendIterations = 5;

                if (numAvailableGroups > poorTrendIterations && iterationIndex >= poorTrendIterations - 1)
                {
                    // take bottom 10% for last 5 (poor_trend_iterations) iterations
                    var bottomIndexes = allIterationIdCmRs.TakeLast(poorTrendIterations).SelectMany(a => a.Where(b => b.rs.RsFsScorePercentile <= 0.1).ToArray()).Select(a => a.id.IdGroupArrayIndex).ToArray();
                    var bottomIndexesCount = bottomIndexes.Distinct().Select(a => (IdGroupArrayIndex: a, count: bottomIndexes.Count(b => a == b))).ToArray();

                    // if group_array_index was in bottom 10% the last 5 (poor_trend_iterations) times, then blacklist
                    var alwaysPoor = bottomIndexesCount.Where(a => a.count >= poorTrendIterations).Select(a => a.IdGroupArrayIndex).ToArray();

                    if (alwaysPoor.Length > 0)
                    {
                        selectionExcludedGroups.AddRange(alwaysPoor);
                        Log($@"Excluding groups with always poor scores: {string.Join(", ", alwaysPoor)}.");
                    }
                }
            }
        }

        private static bool CheckWhetherFinished(string ExperimentName, double minScoreIncrease, int maxIterations, int limitIterationNotHigherThanAll, int limitIterationNotHigherThanLast, bool featureSelectionFinished, (IndexData id, ConfusionMatrix cm, RankScore rs) thisIterationWinnerIdCmRs, (IndexData id, ConfusionMatrix cm, RankScore rs) lastWinnerIdCmRs, int numAvailableGroups, int iterationIndex, ref (IndexData id, ConfusionMatrix cm, RankScore rs) bestWinnerIdCmRs, ref int iterationsNotHigherThanLast, ref int iterationsNotHigherThanBest, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            void Log(string msg)
            {
                Logging.WriteLine($"{ExperimentName}, iteration: {iterationIndex}, {msg}.");
            }

            if (!featureSelectionFinished)
            {
                var scoreIncreaseFromLast = thisIterationWinnerIdCmRs.rs.RsFsScore - (lastWinnerIdCmRs.rs?.RsFsScore ?? 0d);
                var scoreIncreaseFromBest = thisIterationWinnerIdCmRs.rs.RsFsScore - (bestWinnerIdCmRs.rs?.RsFsScore ?? 0d);

                iterationsNotHigherThanLast = scoreIncreaseFromLast > 0
                    ? 0
                    : iterationsNotHigherThanLast + 1;
                iterationsNotHigherThanBest = scoreIncreaseFromBest > 0
                    ? 0
                    : iterationsNotHigherThanBest + 1;

                if (scoreIncreaseFromBest > 0) bestWinnerIdCmRs = thisIterationWinnerIdCmRs;


                var notHigherThanLastLimitReached = iterationsNotHigherThanLast >= limitIterationNotHigherThanLast;
                var notHigherThanBestLimitReached = iterationsNotHigherThanBest >= limitIterationNotHigherThanAll;
                var groupsNotAvailable = numAvailableGroups == 0;
                var maxIterationsReached = maxIterations > 0 && iterationIndex + 1 >= maxIterations;
                var scoreIncreaseNotReached = minScoreIncrease > 0 && scoreIncreaseFromLast < minScoreIncrease;


                if (notHigherThanLastLimitReached) Log($@"{nameof(featureSelectionFinished)}: {nameof(notHigherThanLastLimitReached)} = {notHigherThanLastLimitReached}");
                if (notHigherThanBestLimitReached) Log($@"{nameof(featureSelectionFinished)}: {nameof(notHigherThanBestLimitReached)} = {notHigherThanBestLimitReached}");
                if (groupsNotAvailable) Log($@"{nameof(featureSelectionFinished)}: {nameof(groupsNotAvailable)} = {groupsNotAvailable}");
                if (maxIterationsReached) Log($@"{nameof(featureSelectionFinished)}: {nameof(maxIterationsReached)} = {maxIterationsReached}");
                if (scoreIncreaseNotReached) Log($@"{nameof(featureSelectionFinished)}: {nameof(scoreIncreaseNotReached)} = {scoreIncreaseNotReached}");

                featureSelectionFinished = notHigherThanLastLimitReached || notHigherThanBestLimitReached || groupsNotAvailable || maxIterationsReached || scoreIncreaseNotReached;
            }

            Log($@"Finished iteration. {nameof(featureSelectionFinished)} = {featureSelectionFinished}.");

            return featureSelectionFinished;
        }

        private static async Task SaveIterationSummaryAsync(string ExperimentName, IndexDataContainer indexDataContainer, /*int InstanceId, int TotalInstances,*/
            string iterationFolder, int iterationIndex, string methodName, bool overwriteCache, (IndexData id, ConfusionMatrix cm, RankScore rs)[] iterationWholeResultsFixedWithRanks, List<(IndexData id, ConfusionMatrix cm, RankScore rs)> allWinnersIdCmRs, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return;

            void Log(string msg)
            {
                Logging.WriteLine($"{ExperimentName}, iteration: {iterationIndex}, {msg}.");
            }

            var fn = Program.GetIterationFilename(indexDataContainer.IndexesWhole, ct);


            {
                // Save the CM ranked for the current iteration (winner rank #0)
                var iterationCmRanksFn1 = Path.Combine(iterationFolder, $@"iteration_ranks_cm_{fn}_full.csv");
                //var iteration_cm_ranks_fn2 = Path.Combine(iteration_folder, $@"iteration_ranks_cm_{fn}_summary.csv");
                if (await IoProxy.IsFileAvailableAsync(true, ct, iterationCmRanksFn1, false, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false)) // && await io_proxy.IsFileAvailable(true, ct, iteration_cm_ranks_fn2, false, _CallerModuleName: ModuleName, _CallerMethodName: MethodName))
                {
                    Log($@"Already saved for iteration {iterationIndex}. Files: {iterationCmRanksFn1}."); //, {iteration_cm_ranks_fn2}.");
                }
                else
                {
                    Log($@"Unavailable for iteration {iterationIndex}. Files: {iterationCmRanksFn1}."); //, {iteration_cm_ranks_fn2}.");
                    await ConfusionMatrix.SaveAsync(iterationCmRanksFn1, /*iteration_cm_ranks_fn2,*/ overwriteCache, iterationWholeResultsFixedWithRanks, ct: ct).ConfigureAwait(false);
                    Log($@"Saved for iteration {iterationIndex}. Files: {iterationCmRanksFn1}."); //, {iteration_cm_ranks_fn2}.");
                }
            }

            {
                // Save the CM of winners from all iterations
                var winnersCmFn1 = Path.Combine(iterationFolder, $@"winners_cm_{fn}_full.csv");
                //var winners_cm_fn2 = Path.Combine(iteration_folder, $@"winners_cm_{fn}_summary.csv");
                if (await IoProxy.IsFileAvailableAsync(true, ct, winnersCmFn1, false, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false))
                // && await io_proxy.IsFileAvailable(true, ct, winners_cm_fn2, false, _CallerModuleName: ModuleName, _CallerMethodName: MethodName))
                {
                    Log($@"Already saved for iteration {iterationIndex}. Files: {winnersCmFn1}"); //, {winners_cm_fn2}.");
                }
                else
                {
                    Log($@"Unavailable for iteration {iterationIndex}. Files: {winnersCmFn1}."); //, {winners_cm_fn2}.");
                    await ConfusionMatrix.SaveAsync(winnersCmFn1, /*winners_cm_fn2,*/ overwriteCache, allWinnersIdCmRs.ToArray(), ct: ct).ConfigureAwait(false);
                    Log($@"Saved for iteration {iterationIndex}. Files: {winnersCmFn1}."); //, {winners_cm_fn2}.");
                }
            }

            {
                // Save the prediction list for misclassification analysis
                var predictionListFilename = Path.Combine(iterationFolder, $@"iteration_prediction_list_{fn}.csv");
                if (await IoProxy.IsFileAvailableAsync(true, ct, predictionListFilename, false, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false)) { Log($@"Already saved for iteration {iterationIndex}. File: {predictionListFilename}."); }
                else
                {
                    Log($@"Unavailable for iteration {iterationIndex}. File: {predictionListFilename}.");
                    await Prediction.SaveAsync(ct, predictionListFilename, iterationWholeResultsFixedWithRanks).ConfigureAwait(false);
                    Log($@"Saved for iteration {iterationIndex}. File: {predictionListFilename}.");
                }
            }
        }


        private static async Task SaveResultsSummaryAsync(IndexData[] allIndexData, (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] groups, string ExperimentName, List<(IndexData id, ConfusionMatrix cm, RankScore rs)> allWinnersIdCmRs, (IndexData id, ConfusionMatrix cm, RankScore rs) bestWinnerIdCmRs, (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] bestWinnerGroups, string methodName, List<(IndexData id, ConfusionMatrix cm, RankScore rs)[]> allIterationIdCmRs, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return;

            var experimentFolder = Program.GetIterationFolder(Program.ProgramArgs.ResultsRootFolder, ExperimentName, ct:ct);

            var fn = Program.GetIterationFilename(allIndexData,ct);


            {
                var bestWinnerFn = Path.Combine(experimentFolder, $@"best_winner_{fn}.csv");
                var bestWinnerText = new List<string>();

                bestWinnerText.Add("Feature selection iterative winner history:");
                bestWinnerText.Add("");
                bestWinnerText.Add($@"index,{string.Join(",", RankScore.CsvHeaderValuesArray)},{string.Join(",", IndexData.CsvHeaderValuesArray)},{string.Join(",", ConfusionMatrix.CsvHeaderValuesArray)}");
                bestWinnerText.AddRange(allWinnersIdCmRs.Select((a, k1) => $@"{k1},{string.Join(",", a.rs?.CsvValuesArray() ?? RankScore.Empty.CsvValuesArray())},{string.Join(",", a.id?.CsvValuesArray() ?? IndexData.Empty.CsvValuesArray())},{string.Join(",", a.cm?.CsvValuesArray() ?? ConfusionMatrix.Empty.CsvValuesArray())}").ToArray());
                bestWinnerText.Add("");
                bestWinnerText.Add("");

                bestWinnerText.Add("Last best winner score data:");
                bestWinnerText.Add("");
                bestWinnerText.Add($@"{string.Join(",", RankScore.CsvHeaderValuesArray)},{string.Join(",", IndexData.CsvHeaderValuesArray)},{string.Join(",", ConfusionMatrix.CsvHeaderValuesArray)}");
                bestWinnerText.Add($@"{string.Join(",", bestWinnerIdCmRs.rs?.CsvValuesArray() ?? RankScore.Empty.CsvValuesArray())},{string.Join(",", bestWinnerIdCmRs.id?.CsvValuesArray() ?? IndexData.Empty.CsvValuesArray())},{string.Join(",", bestWinnerIdCmRs.cm?.CsvValuesArray() ?? ConfusionMatrix.Empty.CsvValuesArray())}");
                bestWinnerText.Add("");
                bestWinnerText.Add("");

                bestWinnerText.Add("Last best winner gkGroup keys:");
                bestWinnerText.Add("");
                bestWinnerText.Add($"index1,{string.Join(",", DataSetGroupKey.CsvHeaderValuesArray)},columns...");
                bestWinnerText.AddRange(bestWinnerGroups.Select((a, k1) => $"{k1},{string.Join(",", a.GroupKey?.CsvValuesArray() ?? DataSetGroupKey.Empty.CsvValuesArray())},{string.Join(";", a.columns ?? Array.Empty<int>())}").ToList());
                bestWinnerText.Add("");
                bestWinnerText.Add("");

                bestWinnerText.Add("Last best winner gkGroup column keys:");
                bestWinnerText.Add("");
                bestWinnerText.Add($"index1,index2,{string.Join(",", DataSetGroupKey.CsvHeaderValuesArray)},columns...");
                bestWinnerText.AddRange(bestWinnerGroups.SelectMany((a, k1) => a.GroupColumnHeaders.Select((b, k2) => $"{k1},{k2}," + string.Join(",", b.CsvValuesArray() ?? DataSetGroupKey.Empty.CsvValuesArray())).ToList()).ToList());
                bestWinnerText.Add("");
                bestWinnerText.Add("");

                await IoProxy.WriteAllLinesAsync(true, ct, bestWinnerFn, bestWinnerText, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false);

                bestWinnerText.Clear();
                bestWinnerText = null;
            }

            {
                var allDataFn = Path.Combine(experimentFolder, $@"all_data_{fn}.csv");
                var allData = allIterationIdCmRs.SelectMany(a => a).ToArray();
                var allDataText = new List<string>();
                allDataText.Add($@"index,{string.Join(",", RankScore.CsvHeaderValuesArray)},{string.Join(",", IndexData.CsvHeaderValuesArray)},{string.Join(",", ConfusionMatrix.CsvHeaderValuesArray)}");
                allDataText.AddRange(allData.Select((a, k1) => $@"{k1},{string.Join(",", a.rs?.CsvValuesArray() ?? RankScore.Empty.CsvValuesArray())},{string.Join(",", a.id?.CsvValuesArray() ?? IndexData.Empty.CsvValuesArray())},{string.Join(",", a.cm?.CsvValuesArray() ?? ConfusionMatrix.Empty.CsvValuesArray())}").ToArray());
                await IoProxy.WriteAllLinesAsync(true, ct, allDataFn, allDataText, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false);
                allDataText.Clear();
                allDataText = null;

                var allDataGrouped = allData.AsParallel().AsOrdered().WithCancellation(ct).GroupBy(a => (a.id.IdGroupArrayIndex, a.id.IdRepetitions, a.id.IdOuterCvFolds, a.id.IdOuterCvFoldsToRun, a.id.IdInnerCvFolds, a.id.IdExperimentName, a.id.IdTotalGroups, a.id.IdSvmType, a.id.IdSvmKernel, a.id.IdScaleFunction, a.id.IdCalcElevenPointThresholds, a.id.IdSelectionDirection, IdClassWeights: string.Join(";", a.id?.IdClassWeights?.Select(a => $"{a.ClassId}:{a.ClassWeight}").ToArray() ?? Array.Empty<string>()))).Select(a => (key: a.Key, list: a.ToList())).ToArray();
                var rankStatsFn = Path.Combine(experimentFolder, $@"rank_stats_{fn}.csv");
                var rankStatsText = new List<string>();


                
                rankStatsText.Add($@"{string.Join(",", "ListIndex", "ListCount", string.Join(",", DataSetGroupKey.CsvHeaderValuesArray), nameof(IndexData.IdGroupArrayIndex), nameof(IndexData.IdRepetitions), nameof(IndexData.IdOuterCvFolds), nameof(IndexData.IdOuterCvFoldsToRun), nameof(IndexData.IdInnerCvFolds), nameof(IndexData.IdExperimentName), nameof(IndexData.IdTotalGroups), nameof(IndexData.IdSvmType), nameof(IndexData.IdSvmKernel), nameof(IndexData.IdScaleFunction), nameof(IndexData.IdCalcElevenPointThresholds), nameof(IndexData.IdSelectionDirection), nameof(IndexData.IdClassWeights))},{string.Join(",", gkStats.CsvHeaderValuesArray.Select(a => $"FsScore{a}").ToArray())},{string.Join(",", gkStats.CsvHeaderValuesArray.Select(a => $"FsScorePercentile{a}").ToArray())}");
                rankStatsText.AddRange(allDataGrouped.Select((a, k1) =>
                {
                    var fsScore = new gkStats(a.list.Select(b => b.rs.RsFsScore).ToArray());
                    var fsScorePercentile = new gkStats(a.list.Select(b => b.rs.RsFsScorePercentile).ToArray());
                    //var fs_rank_index_percentile = new gkStats(a.list.Select(b => b.rs.fs_rank_index_percentile).ToArray());
                    //var fs_rank_index = new gkStats(a.list.Select(b => (double) b.rs.fs_rank_index).ToArray());
                    var gk = a.key.IdGroupArrayIndex > -1 && groups != null && groups.Length > 0
                        ? groups[a.key.IdGroupArrayIndex].GroupKey
                        : DataSetGroupKey.Empty;

                    return ct.IsCancellationRequested ? default :$"{k1},{a.list.Count},{string.Join(",", gk.CsvValuesArray())},{a.key.IdGroupArrayIndex},{a.key.IdRepetitions},{a.key.IdOuterCvFolds},{a.key.IdOuterCvFoldsToRun},{a.key.IdInnerCvFolds},{a.key.IdExperimentName},{a.key.IdTotalGroups},{a.key.IdSvmType},{a.key.IdSvmKernel},{a.key.IdScaleFunction},{(a.key.IdCalcElevenPointThresholds ? 1 : 0)},{a.key.IdSelectionDirection},{a.key.IdClassWeights},{string.Join(",", fsScore.CsvValuesArray())},{string.Join(",", fsScorePercentile.CsvValuesArray())}";
                }).ToArray());
                await IoProxy.WriteAllLinesAsync(true, ct, rankStatsFn, rankStatsText, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false);

                allData = null;
                allDataGrouped = null;
                rankStatsText.Clear();
                rankStatsText = null;
            }
        }


        private static async Task ServeIpcJobsAsync(ConnectionPool cp, IndexDataContainer indexDataContainer, List<(IndexData id, ConfusionMatrix cm)> iterationWholeResults, int iterationIndex, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return;
            
            Logging.LogEvent($"Reached start of {nameof(FsServer)}.{nameof(ServeIpcJobsAsync)} for iteration {iterationIndex}.", ModuleName);


            var serverCmFile = Path.Combine(ServerFolder, $@"_server_cache_{Program.GetIterationFilename(indexDataContainer.IndexesWhole, ct)}.csv");

            if (IoProxy.ExistsFile(true, serverCmFile, ModuleName, ct:ct) && IoProxy.FileLength(serverCmFile,ct) > 0)
            {
                var cache = await CacheLoad.LoadCacheFileAsync(serverCmFile, indexDataContainer, ct).ConfigureAwait(false);
                iterationWholeResults.AddRange(cache);
            }

            CacheLoad.UpdateMissing(iterationWholeResults, indexDataContainer, ct: ct);


            var startTotalMissing = indexDataContainer.IndexesMissingWhole.Length;
            var startTime = DateTime.UtcNow;
            var numComplete = 0;
            var numCompleteLock = new object();


            using var etaCts = new CancellationTokenSource();
            var etaCt = etaCts.Token;
            var etaTask = Task.Run(async () =>
                {
                    while (!ct.IsCancellationRequested && !etaCt.IsCancellationRequested)
                        try
                        {
                            await Task.Delay(TimeSpan.FromSeconds(30), etaCt).ConfigureAwait(false);
                            lock (numCompleteLock) { Routines.PrintETA(numComplete, startTotalMissing, startTime, ModuleName); }
                        }
                        catch (Exception e)
                        {
                            Logging.LogException(e, "", ModuleName);
                            break;
                        }
                },
                etaCt);

            //try { await Task.Delay(TimeSpan.FromMilliseconds(1), externalCt).ConfigureAwait(false); } catch (Exception) { }

            while (indexDataContainer.IndexesMissingWhole.Any())
            {
                //Logging.LogEvent("!!!!!!!!!!!!!!!!! START OF OUTER LOOP !!!!!!!!!!!!!!!!!!!!!!");
                while (!ct.IsCancellationRequested && cp.CountActive() == 0)
                {
                    Logging.LogEvent("Connection pool is empty... waiting...", ModuleName);
                    try { await Task.Delay(TimeSpan.FromMilliseconds(1), ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }

                if (ct.IsCancellationRequested)
                {
                    Logging.LogEvent($"Cancellation requested on {nameof(ct)}.", ModuleName);
                    break;
                }

                var tasks = new List<Task<(IndexData id, ConfusionMatrix[] CmList)>>();

                for (var index = 0; index < indexDataContainer.IndexesMissingWhole.Length; index++)
                {
                    var id = indexDataContainer.IndexesMissingWhole[index];

                    //Logging.LogEvent($"!!!!!!!!!!!!!!!!! START OF INNER LOOP {index} / {indexDataContainer.IndexesMissingWhole.Length} ... IdGroupArrayIndex = {id.IdGroupArrayIndex} !!!!!!!!!!!!!!!!!!!!!!");



                    if (id == null) continue;



                    var task = Task.Run(async () =>
                        {
                            Logging.LogEvent($"Server: Starting Inner Task - IdGroupArrayIndex = {id.IdGroupArrayIndex}", ModuleName);

                            if (ct.IsCancellationRequested)
                            {
                                Logging.LogEvent($"Server: Cancellation requested - IdGroupArrayIndex = {id.IdGroupArrayIndex}.", ModuleName);

                                return default;
                            }

                            var cpm = cp.GetNextClient();

                            try
                            {
                                if (cpm == default)
                                {
                                    Logging.LogEvent($"Server: Connection pool is empty - IdGroupArrayIndex = {id.IdGroupArrayIndex}");
                                    return default;
                                }

                                (IndexData id, ConfusionMatrix[] CmList) ret = default;



                                ret = await IpcMessaging.IpcAsync($"server_{cpm.LocalHost}:{cpm.LocalPort}", $"client_{cpm.RemoteHost}:{cpm.RemotePort}", cpm, null, true, id, ct).ConfigureAwait(false);

                                if (ret != default)
                                    lock (numCompleteLock) { numComplete++; }

                                return ct.IsCancellationRequested ? default :ret;
                            }
                            catch (Exception e)
                            {

                                Logging.LogException(e, $"Server: IdGroupArrayIndex = {id.IdGroupArrayIndex}", ModuleName);
                                return default;
                            }
                            finally
                            {
                                cpm?.JoinPool(cp);
                                
                                Logging.LogEvent($"Server: Exiting Inner Task - IdGroupArrayIndex = {id.IdGroupArrayIndex}", ModuleName);
                            }
                        },
                        ct);

                    tasks.Add(task);

                    //Logging.LogEvent($"!!!!!!!!!!!!!!!!! END OF INNER LOOP {index} / {indexDataContainer.IndexesMissingWhole.Length} ... IdGroupArrayIndex = {id.IdGroupArrayIndex} !!!!!!!!!!!!!!!!!!!!!!");
                }

                try { await Task.WhenAll(tasks).ConfigureAwait(false); }
                catch (Exception e) { Logging.LogException(e, "", ModuleName); }

                if (tasks.Any(a => a.Status != TaskStatus.RanToCompletion)) throw new Exception("????");
                //tasks.ForEach(a=> Logging.LogEvent($"Task {a.Id}: Status = {a.Status}"));

                var cache = tasks.Where(a => a != null && a.IsCompletedSuccessfully && a.Result != default).Select(a => a.Result).ToArray();
                if (cache.Length > 0)
                {
                    var cache2 = cache.Where(a => a.id != null && a.CmList != null && a.CmList.Length > 0).SelectMany(a => a.CmList.Select(b => (a.id, b)).ToArray()).ToArray();
                    if (cache2.Length > 0)
                    {

                        iterationWholeResults.AddRange(cache2);

                        var lines = iterationWholeResults.Select(a => $"{a.id?.CsvValuesString() ?? IndexData.Empty.CsvValuesString()},{a.cm?.CsvValuesString() ?? ConfusionMatrix.Empty.CsvValuesString()}").ToList();
                        lines.Insert(0, IndexData.CsvHeaderString + "," + ConfusionMatrix.CsvHeaderString);
                        await IoProxy.WriteAllLinesAsync(true, ct, serverCmFile, lines).ConfigureAwait(false);
                        CacheLoad.UpdateMissing(iterationWholeResults, indexDataContainer, ct: ct);
                        //await Task.Delay(TimeSpan.FromMilliseconds(1), ct:ct).ConfigureAwait(false);
                    }
                }

                //Logging.LogEvent("!!!!!!!!!!!!!!!!! END OF OUTER LOOP !!!!!!!!!!!!!!!!!!!!!!");
            }


            etaCts.Cancel();


            Logging.LogEvent($"Reached end of {nameof(FsServer)}.{nameof(ServeIpcJobsAsync)} for iteration {iterationIndex}.", ModuleName);
        }

        /*
        while (!ct.IsCancellationRequested && IndexDataContainer.indexes_missing_whole.Any())
        {
            // 1. get results clients have made

            var results = wait_results(ct, TimeSpan.Zero, IndexDataContainer);
            log($"{nameof(results)} = {results?.Length ?? 0}");
            if (results != null && results.Length > 0)
            {
                for (var results_index = 0; results_index < results.Length; results_index++)
                {
                    var result = results[results_index];
                    //var result_id = result.id_CmList.Select(a => a.id).ToArray();
                    //var work_list_item = work_list.FirstOrDefault(a => a.client_id == r.client_id && a.work_id == r.work_id);
                    if (result.id_CmList.Length > 0)
                    {
                        iteration_whole_results.AddRange(result.id_CmList);

                        await io_proxy.AppendAllLines(true, ct, server_cm_file, result.id_CmList.Select(a => string.Join(",", a.id.CsvValuesString(), a.cm.CsvValuesString())).ToArray(), _CallerModuleName: ModuleName, _CallerMethodName: MethodName).ConfigureAwait(false);
                    }
                }

                cache_load.update_missing(ct, iteration_whole_results, IndexDataContainer);
            }

            // 2. break out, if last set of results completed the whole iteration.
            if (!IndexDataContainer.indexes_missing_whole.Any()) break;

            // 3. timeout any work which hasn't had a response from client...
            if (work_timeout != TimeSpan.Zero && work_list.Count > 0)
            {
                var now = DateTime.UtcNow;
                var timed_out_work_list = work_list.Where(a => (now - a.time_sent) > work_timeout).ToArray();
                if (timed_out_work_list.Length > 0)
                {
                    work_list = work_list.Except(timed_out_work_list).ToList();

                    //for (var i = 0; i < timed_out_work_list.Length; i++)
                    //{
                    //var wtc = timed_out_work_list[i];
                    //var cancel_fn = Path.Combine(_server_folder, wtc.client_id, $@"cancel_{wtc.work_id}.csv");
                    //await io_proxy.WriteAllLines(true, ct, cancel_fn, new[] {$@"{wtc.work_id}"}, _CallerModuleName: ModuleName, _CallerMethodName: MethodName);
                    //}
                }

            }

            // 4. send any available clients work to do...
            var new_clients = wait_client(ct, TimeSpan.Zero);
            log($"{nameof(new_clients)} = {new_clients?.Length ?? 0}");

            if (new_clients != null && new_clients.Length > 0)
            {
                clients_waiting = clients_waiting.Concat(new_clients).ToArray();
                var awaiting = work_list.SelectMany(a => a.id).ToArray();
                var not_awaiting = IndexDataContainer.indexes_missing_whole.Except(awaiting).ToArray();

                if ( /*clients_waiting != null && * /clients_waiting.Length > 0 && not_awaiting.Length > 0)
                {
                    var work_to_client = clients_waiting.Select(a => (a.client_id, a.work_id, id_list: new List<index_data>())).ToArray();

                    var max_work = not_awaiting.Length <= (work_per_request * clients_waiting.Length) ? not_awaiting.Length : (work_per_request * clients_waiting.Length);

                    var i = 0;
                    var client_index = 0;
                    while (i < max_work)
                    {
                        if (work_to_client[client_index].id_list.Count < work_per_request) { work_to_client[client_index].id_list.Add(not_awaiting[i]); }

                        i++;
                        client_index = client_index == clients_waiting.Length - 1 ? 0 : client_index + 1;
                    }

                    work_to_client = work_to_client.Where(a => a.id_list.Count > 0).ToArray();
                    clients_waiting = clients_waiting.Except(work_to_client.Select(a => (a.client_id, a.work_id)).ToArray()).ToArray();

                    // note: if this causes i/o bottleneck, could store all responses in one file instead...
                    for (var work_to_client_index = 0; work_to_client_index < work_to_client.Length; work_to_client_index++)
                    {
                        var wtc = work_to_client[work_to_client_index];
                        var wtc_fn = Path.Combine(_server_folder, wtc.client_id, $@"server_response_{wtc.work_id}.csv");
                        var wtc_lines = new string[wtc.id_list.Count + 1];
                        wtc_lines[0] = index_data.csv_header_string;
                        for (var id_list_index = 0; id_list_index < wtc.id_list.Count; id_list_index++) { wtc_lines[id_list_index + 1] = wtc.id_list[id_list_index].CsvValuesString(); }

                        await io_proxy.WriteAllLines(true, ct, wtc_fn, wtc_lines, _CallerMethodName: ModuleName, _CallerModuleName: ModuleName);
                        work_list.Add((wtc.client_id, wtc.work_id, DateTime.UtcNow, wtc.id_list.ToArray()));
                    }
                }
            }

            // 5. print time remaining...
            routines.print_eta(IndexDataContainer.indexes_loaded_whole.Length, start_total_missing /*IndexDataContainer.IndexesWhole.Length* /, start_time, ModuleName, MethodName);

            // 6. pause delay until retry
            await  Logging.WaitAsync(ct, 5, 5, _CallerModuleName: ModuleName, _CallerMethodName: MethodName);
        

        }*/


        internal static (IndexData id, ConfusionMatrix cm, RankScore rs)[] SetRanks(List<(IndexData id, ConfusionMatrix cm, double fs_score)> idCmScore, List<(IndexData id, ConfusionMatrix cm, RankScore rs)[]> allIterationIdCmRs, (IndexData id, ConfusionMatrix cm, RankScore rs) bestScore, (IndexData id, ConfusionMatrix cm, RankScore rs) lastScore, bool asParallel = true, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            var lastIterationIdCmRs = allIterationIdCmRs?.LastOrDefault();

            var allIterationIdCmRsFlat = allIterationIdCmRs != null && allIterationIdCmRs.Count > 0
                ? asParallel ? allIterationIdCmRs.AsParallel().AsOrdered().WithCancellation(ct).SelectMany(a => a).ToArray() : allIterationIdCmRs.SelectMany(a => a).ToArray()
                : null;

            var allIterationIdFlat = allIterationIdCmRsFlat?.Select(a => a.id) /*.OrderBy(a=>a.group_array_index)*/
                .ToArray();

            //var last_winner_id_cm_rs = last_iteration_id_cm_rs?.FirstOrDefault();

            // ensure consistent reordering (i.e. for items with equal tied scores when processing may have been done out of order)
            idCmScore = idCmScore.OrderBy(a => a.id.IdGroupArrayIndex).ThenBy(a => a.cm.XClassId).ToList();

            // order descending by score
            idCmScore = idCmScore.OrderByDescending(a => a.fs_score).ThenBy(a => a.id.IdNumColumns).ToList();

            // unexpected but possible edge case, if winner is the same as last time (due to random variance), then take the next gkGroup instead.
            if (idCmScore[0].id.IdGroupArrayIndex == (lastIterationIdCmRs?.FirstOrDefault().id.IdGroupArrayIndex ?? -1) && idCmScore.Count > 1)
            {
                var ix0 = idCmScore[0];
                var ix1 = idCmScore[1];

                idCmScore[0] = ix1;
                idCmScore[1] = ix0;
            }


            var maxRank = idCmScore.Count - 1;

            var ranksList = Enumerable.Range(0, idCmScore.Count).ToArray();
            var ranksListScaling = new Scaling(ranksList) { RescaleScaleMin = 0, RescaleScaleMax = 1 };

            var scoresList = idCmScore.Select(a => a.fs_score).ToArray();
            var scoresListScaling = new Scaling(scoresList) { RescaleScaleMin = 0, RescaleScaleMax = 1 };

            // make rank_data instances, which track the ranks (performance) of each gkGroup over time, to allow for optimisation decisions and detection of variant features

            var idso = new IndexData.IndexDataSearchOptions
            {
                IdIterationIndex = false,
                IdGroupArrayIndex = true,
                IdTotalGroups = true,
                IdSelectionDirection = false,
                IdCalcElevenPointThresholds = true,
                IdSvmType = true,
                IdSvmKernel = true,
                IdScaleFunction = true,
                IdRepetitions = true,
                IdOuterCvFolds = true,
                IdOuterCvFoldsToRun = true,
                IdInnerCvFolds = true,
                IdGroupKey = true,
                IdExperimentName = true,
                IdNumGroups = false,
                IdNumColumns = false,
                IdGroupArrayIndexes = false,
                IdColumnArrayIndexes = false,
                IdClassWeights = true,
                IdGroupFolder = false
            };

            //if (all_iteration_id_flat != null && last_group == null)
            //{
            //    var f = all_iteration_id_flat.First(b => b.group_array_index == a.id.group_array_index);
            //    Console.WriteLine($@"{a.id.IterationIndex} == {f.IterationIndex}             " + (a.id.IterationIndex == f.IterationIndex));
            //    Console.WriteLine($@"{a.id.group_array_index} == {f.group_array_index}         " + (a.id.group_array_index == f.group_array_index));
            //    Console.WriteLine($@"{a.id.TotalGroups} == {f.TotalGroups}                   " + (a.id.TotalGroups == f.TotalGroups));
            //    Console.WriteLine($@"{a.id.selection_direction} == {f.selection_direction}     " + (a.id.selection_direction == f.selection_direction));
            //    Console.WriteLine($@"{a.id.calc_ElevenPoint_thresholds} == {f.calc_ElevenPoint_thresholds}     " + (a.id.calc_ElevenPoint_thresholds == f.calc_ElevenPoint_thresholds));
            //    Console.WriteLine($@"{a.id.svm_type} == {f.svm_type}                           " + (a.id.svm_type == f.svm_type));
            //    Console.WriteLine($@"{a.id.svm_kernel} == {f.svm_kernel}                       " + (a.id.svm_kernel == f.svm_kernel));
            //    Console.WriteLine($@"{a.id.scale_function} == {f.scale_function}               " + (a.id.scale_function == f.scale_function));
            //    Console.WriteLine($@"{a.id.repetitions} == {f.repetitions}                     " + (a.id.repetitions == f.repetitions));
            //    Console.WriteLine($@"{a.id.outer_cv_folds} == {f.outer_cv_folds}               " + (a.id.outer_cv_folds == f.outer_cv_folds));
            //    Console.WriteLine($@"{a.id.outer_cv_folds_to_run} == {f.outer_cv_folds_to_run} " + (a.id.outer_cv_folds_to_run == f.outer_cv_folds_to_run));
            //    Console.WriteLine($@"{a.id.inner_cv_folds} == {f.inner_cv_folds}               " + (a.id.inner_cv_folds == f.inner_cv_folds));
            //    Console.WriteLine($@"{a.id.GroupKey} == {f.GroupKey}                         " + (a.id.GroupKey == f.GroupKey));
            //    Console.WriteLine($@"{a.id.ExperimentName} == {f.ExperimentName}             " + (a.id.ExperimentName == f.ExperimentName));
            //    Console.WriteLine($@"{a.id.num_groups} == {f.num_groups}                       " + (a.id.num_groups == f.num_groups));
            //    Console.WriteLine($@"{a.id.num_columns} == {f.num_columns}                     " + (a.id.num_columns == f.num_columns));
            //    Console.WriteLine($@"{a.id.group_array_indexes} == {f.group_array_indexes}     " + (a.id.group_array_indexes == f.group_array_indexes));
            //    Console.WriteLine($@"{a.id.column_array_indexes} == {f.column_array_indexes}   " + (a.id.column_array_indexes == f.column_array_indexes));
            //    Console.WriteLine($@"{a.id.ClassWeights} == {f.ClassWeights}                 " + (a.id.ClassWeights == f.ClassWeights));
            //    throw new Exception();
            //}

            var idCmRs = asParallel
                ? idCmScore
                    //.AsParallel()
                    //.AsOrdered()
                    //.WithCancellation(ct)
                    .Select((a, index) =>
                    {
                        var lastGroup = allIterationIdFlat != null
                            ? IndexData.FindLastReference(allIterationIdFlat, a.id, idso)
                            : null;
                        var lastGroupRs = lastGroup != null
                            ? allIterationIdCmRsFlat?.LastOrDefault(c => c.id == lastGroup) ?? default
                            : default;

                        var rs = new RankScore
                        {
                            RsGroupArrayIndex = a.id.IdGroupArrayIndex,
                            RsIterationIndex = a.id.IdIterationIndex,
                            RsFsRankIndex = maxRank - index,
                            RsFsMaxRankIndex = maxRank,
                            RsFsRankIndexPercentile = ranksListScaling.Scale(maxRank - index, Scaling.ScaleFunction.Rescale),
                            RsFsScore = a.fs_score,
                            RsFsScorePercentile = scoresListScaling.Scale(a.fs_score, Scaling.ScaleFunction.Rescale),
                            RsFsScoreChangeBest = a.fs_score - (bestScore.rs?.RsFsScore ?? 0),
                            RsFsScoreChangeLast = a.fs_score - (lastScore.rs?.RsFsScore ?? 0),
                            RsFsScoreChangeGroup = lastGroupRs.rs != null
                                ? a.fs_score - (lastGroupRs.rs?.RsFsScore ?? 0)
                                : 0
                        };

                        return ct.IsCancellationRequested ? default :(a.id, a.cm, rs);
                    }).ToArray()
                : idCmScore.Select((a, index) =>
                {
                    var lastGroup = allIterationIdFlat != null
                        ? IndexData.FindLastReference(allIterationIdFlat, a.id, idso)
                        : null;
                    var lastGroupRs = lastGroup != null
                        ? allIterationIdCmRsFlat?.LastOrDefault(c => c.id == lastGroup) ?? default
                        : default;

                    var rs = new RankScore
                    {
                        RsGroupArrayIndex = a.id.IdGroupArrayIndex,
                        RsIterationIndex = a.id.IdIterationIndex,
                        RsFsRankIndex = maxRank - index,
                        RsFsMaxRankIndex = maxRank,
                        RsFsRankIndexPercentile = ranksListScaling.Scale(maxRank - index, Scaling.ScaleFunction.Rescale),
                        RsFsScore = a.fs_score,
                        RsFsScorePercentile = scoresListScaling.Scale(a.fs_score, Scaling.ScaleFunction.Rescale),
                        RsFsScoreChangeBest = a.fs_score - (bestScore.rs?.RsFsScore ?? 0),
                        RsFsScoreChangeLast = a.fs_score - (lastScore.rs?.RsFsScore ?? 0),
                        RsFsScoreChangeGroup = lastGroupRs.rs != null
                            ? a.fs_score - (lastGroupRs.rs?.RsFsScore ?? 0)
                            : 0
                    };

                    return ct.IsCancellationRequested ? default :(a.id, a.cm, rs);
                }).ToArray();

            return ct.IsCancellationRequested ? default :idCmRs;
        }


        /*internal static (string client_id, string work_id)[] wait_client(CancellationToken ct, TimeSpan timeout)
        {
            const string MethodName = nameof(wait_client);
            Logging.WriteLine($"{MethodName}()", ModuleName, MethodName);

            if (cts == null) cts = new CancellationTokenSource();
            var sw1 = Stopwatch.StartNew();
            while (!cts.IsCancellationRequested && (timeout == TimeSpan.Zero || sw1.Elapsed <= timeout))
            {
                if (!await io_proxy.exists_directory(true, _server_folder, ModuleName, MethodName))
                {
                    if (timeout == TimeSpan.Zero) return null;
                    var time_left = timeout - sw1.Elapsed;
                    if (time_left > TimeSpan.Zero) await  Logging.WaitAsync(ct, 10 <= time_left.TotalSeconds ? 10 : (int)Math.Floor(time_left.TotalSeconds), 20 <= time_left.TotalSeconds ? 20 : (int)Math.Floor(time_left.TotalSeconds), ModuleName, MethodName);
                }

                var client_request_files = await io_proxy.GetFiles(true, ct, _server_folder, "client_request_*.csv", SearchOption.AllDirectories, _CallerModuleName: ModuleName, _CallerMethodName: MethodName);

                if (client_request_files == null || client_request_files.Length == 0)
                {
                    if (timeout == TimeSpan.Zero) return null;
                    var time_left = timeout - sw1.Elapsed;
                    if (time_left > TimeSpan.Zero) await  Logging.WaitAsync(ct, 10 <= time_left.TotalSeconds ? 10 : (int)Math.Floor(time_left.TotalSeconds), 20 <= time_left.TotalSeconds ? 20 : (int)Math.Floor(time_left.TotalSeconds), ModuleName, MethodName);
                }

                var client_requests = client_request_files.Select(a =>
                    {
                        var client_id = Path.GetDirectoryName(a).Split(new char[] { '\\', '/' }, StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
                        var work_id = Path.GetFileNameWithoutExtension(a).Split('_', StringSplitOptions.RemoveEmptyEntries).LastOrDefault();

                        return ct.IsCancellationRequested ? default :(client_id, work_id);
                    })
                    .ToArray();

                for (var i = 0; i < client_request_files.Length; i++) { await io_proxy.delete_file(true, ct, client_request_files[i], _CallerModuleName: ModuleName, _CallerMethodName: MethodName); }

                return ct.IsCancellationRequested ? default :client_requests;
            }

            return null;
        }*/

        /*
        internal static (string client_id, string work_id, (index_data id, ConfusionMatrix cm)[] id_CmList)[] wait_results(CancellationToken ct, TimeSpan timeout, IndexDataContainer IndexDataContainer, bool as_parallel = true)
        {
            const string MethodName = nameof(wait_results);
            Logging.WriteLine($"{MethodName}()", ModuleName, MethodName);

            if (cts == null) cts = new CancellationTokenSource();
            var sw1 = Stopwatch.StartNew();
            while (!cts.IsCancellationRequested && (timeout == TimeSpan.Zero || sw1.Elapsed <= timeout))
            {
                if (!await io_proxy.exists_directory(true, _server_folder, ModuleName, MethodName))
                {
                    if (timeout == TimeSpan.Zero) return null;
                    var time_left = timeout - sw1.Elapsed;
                    if (time_left > TimeSpan.Zero) await  Logging.WaitAsync(ct, 10 <= time_left.TotalSeconds ? 10 : (int)Math.Floor(time_left.TotalSeconds), 20 <= time_left.TotalSeconds ? 20 : (int)Math.Floor(time_left.TotalSeconds), ModuleName, MethodName);
                }

                var client_request_files = await io_proxy.GetFiles(true, ct, _server_folder, "client_results_*.csv", SearchOption.AllDirectories, _CallerModuleName: ModuleName, _CallerMethodName: MethodName);

                if (client_request_files == null || client_request_files.Length == 0)
                {
                    if (timeout == TimeSpan.Zero) return null;
                    var time_left = timeout - sw1.Elapsed;
                    if (time_left > TimeSpan.Zero) await  Logging.WaitAsync(ct, 10 <= time_left.TotalSeconds ? 10 : (int)Math.Floor(time_left.TotalSeconds), 20 <= time_left.TotalSeconds ? 20 : (int)Math.Floor(time_left.TotalSeconds), ModuleName, MethodName);
                }

                var client_requests = as_parallel
                    ? client_request_files
                        .AsParallel()
                        .AsOrdered()
                        .Select(a =>
                        {
                            var client_id = Path.GetDirectoryName(a).Split(new char[] { '\\', '/' }, StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
                            var work_id = Path.GetFileNameWithoutExtension(a).Split('_', StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
                            var CmList = cache_load.load_cache_file(ct, a, IndexDataContainer); //ConfusionMatrix.load(ct, a, as_parallel: as_parallel);
                            return ct.IsCancellationRequested ? default :(client_id, work_id, CmList);
                        })
                        .ToArray()
                    : client_request_files
                        .Select(a =>
                        {
                            var client_id = Path.GetDirectoryName(a).Split(new char[] { '\\', '/' }, StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
                            var work_id = Path.GetFileNameWithoutExtension(a).Split('_', StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
                            var CmList = cache_load.load_cache_file(ct, a, IndexDataContainer); //ConfusionMatrix.load(ct, a, as_parallel: as_parallel);
                            return ct.IsCancellationRequested ? default :(client_id, work_id, CmList);
                        })
                        .ToArray();

                for (var i = 0; i < client_request_files.Length; i++) { await io_proxy.delete_file(true, ct, client_request_files[i], _CallerModuleName: ModuleName, _CallerMethodName: MethodName); }

                return ct.IsCancellationRequested ? default :client_requests;
            }

            return null;
        }*/
    }
}