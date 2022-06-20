using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SvmFsLib;

namespace SvmFsCtl
{
    public static class FsController
    {
        public const string ModuleName = nameof(FsController);

        //public static readonly string _server_id = program.program_args.server_id;//Guid.NewGuid().ToString();
        //public static readonly string ServerFolder = Path.Combine(Program.ProgramArgs.ResultsRootFolder, "_server", $"{Program.ProgramArgs.ServerGuid:N}");

        public static async Task FeatureSelectionInitializationAsync
            (
            //int instanceId,
            int numInstances, 
            DataSet baseLineDataSet, 
            int[] baseLineColumnIndexes, 
            DataSet dataSet, 
            int scoringClassId,
            string[] scoringMetrics, 
            string experimentName, 
            //int totalInstances,
            int repetitions, 
            int outerCvFolds,
            int outerCvFoldsToRun,
            int innerFolds, 
            Libsvm.LibsvmSvmType[] svmTypes, 
            Libsvm.LibsvmKernelType[] kernels,
            Scaling.ScaleFunction[] scales, 
            (int ClassId, double ClassWeight)[][] classWeightSets,
            bool calcElevenPointThresholds, 
            int limitIterationNotHigherThanAll = 14, 
            int limitIterationNotHigherThanLast = 7, 
            bool makeOuterCvConfusionMatrices = false,
            bool testFinalBestBias = false,
            ulong lvl = 0,
            CancellationToken ct = default
            )
        {
            Logging.LogCall(ModuleName);

            //using var methodCts = new CancellationTokenSource();
            //var methodCt = methodCts.Token;
            //using var methodLinkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, methodCt);
            //var methodLinkedCt = methodLinkedCts.Token;

            if (ct.IsCancellationRequested)
            {
                Logging.LogExit(ModuleName);
                return;
            }

            //const string MethodName = nameof(FeatureSelectionInitializationAsync);

            var startTime = DateTime.UtcNow;

            // -fs0 0/1/2 -fs2 0/1 -fs4 0/1
            var option0_findBestGroupFeaturesFirst =              SvmFsCtl.ProgramArgs.Option0 == 1 || SvmFsCtl.ProgramArgs.Option0 == 2;
            var option0_findBestGroupFeaturesFirstWithPreselect = SvmFsCtl.ProgramArgs.Option0 == 1;

            //var option1 = true;

            var option2_checkIndividualLast = SvmFsCtl.ProgramArgs.Option2 == 1;
            var option3_checkIndividualLast = SvmFsCtl.ProgramArgs.Option3 == 1;
            var option4_testFinalBestBias =   SvmFsCtl.ProgramArgs.Option4 == 1;

            //var serverGuid = Program.ProgramArgs.ServerGuid; // Guid.NewGuid().ToByteArray();
            //var server_guid_bytes = Program.program_args.server_guid.ToByteArray();// Guid.NewGuid().ToByteArray();


            // option 1: maintain 2 connections to every RPC server at all times
            // option 2: keep trying to establish an extra connection to each RPC server

            //var cp = new ConnectionPool(callChain: null, lvl: lvl + 1);
            //var cpConnecTask = RpcService.RpcConnectTask(cp, ct);

            //cp.Start(true, "Server", serverGuid, default, callChain: null, lvl: lvl + 1, ct: ct);
            //cp.Start(false, "Controller", Guid.NewGuid(), default, ct, ModuleName);





            // Get the feature groups within the DataSet
            var groups1 = DataSetGroupMethods.GetMainGroups(dataSet, true, true, true, true, true, true, true, false, false, ct: ct);

            // Limit for testing
            // todo: remove this
            //groups1 = groups1.Take(100).ToArray();

            var experimentDurations = new List<TimeSpan>();

            //var esn = 0;
            // Feature select within each group first, to reduce number of columns
            if (option0_findBestGroupFeaturesFirst)
            {
                var startTime0 = DateTime.UtcNow;

                Logging.WriteLine($@"Finding best of {groups1.Sum(a => a.columns.Length)} individual columns within the {groups1.Length} groups", ModuleName);//, MethodName);



                // get best features in each gkGroup
                var groups1ReduceInput = DataSetGroupMethods.GetSubGroups(groups1, true, true, true, true, true, true, true, true, true, ct: ct);

                // There is 1 performance test per instance (i.e. each nested cross validation performance test [1-repetition, 5-fold outer, 5-fold inner])
                // This means that if number of groups is less than number of instances, some instances could be idle... problem, but rare enough to ignore.


                // note: it doesn't make sense to go both backwards (i.e. preselect) and forwards (i.e. no preselect), as the results are unlikely to ever agree... and it would be an arbitrary choice.







                var groups1ReduceOutputTasks = groups1ReduceInput
                    //.AsParallel()
                    //.AsOrdered()
                    ///*.WithCancellation(ct)*/
                    .Select(async (groups0, groupIndex) =>
                    {
                        var experimentDescription0 = $@"Backwards feature selection on each feature-groups (features made of several columns) to reduce their sizes by removing surplus columns (dimensionality reduction).";
                        var experimentSequenceNumber0 = 0;
                        var experimentSubequenceNumber0 = groupIndex;
                        var experimentGroups0 = groups0;
                        var experimentName0 = $"{experimentName}_E{experimentSequenceNumber0}_S{experimentSubequenceNumber0}_G{experimentGroups0.Length}";

                        return await FeatureSelectionWorker(numInstances, scoringClassId,
                            scoringMetrics,
                            //cp,
                            baseLineDataSet,
                            baseLineColumnIndexes,
                            dataSet,
                            experimentGroups0,
                            option0_findBestGroupFeaturesFirstWithPreselect,
                            //save_status: true,
                            null,
                            experimentDescription0,
                            experimentName0,
                            //experimentSequenceNumber: 0,
                            //experimentSubequenceNumber: groupIndex,
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
                            lvl: lvl + 1,
                            ct: ct
                        //make_outer_cv_confusion_matrices: false
                        ).ConfigureAwait(false);
                    }).ToArray();

                var groups1ReduceOutput = await Task.WhenAll(groups1ReduceOutputTasks).ConfigureAwait(false);

                // ungroup (gkMember & gkPerspective)
                var groups1ReduceOutputUngrouped = DataSetGroupMethods.Ungroup(groups1ReduceOutput.Select(a => a.BestWinnerGroups).ToArray(), ct: ct);

                // regroup (without gkMember & gkPerspective)
                var groups1ReduceOutputRegrouped = DataSetGroupMethods.GetMainGroups(groups1ReduceOutputUngrouped, true, true, true, true, true, true, true, false, false, ct: ct);

                groups1 = groups1ReduceOutputRegrouped;



                experimentDurations.Add(DateTime.UtcNow - startTime0);
            }


            // Feature select between the DataSet groups

            var startTime1 = DateTime.UtcNow;
            var experimentDescription1 = $@"Forward feature selection with feature-groups (features made of several columns).";
            var experimentSequenceNumber1 = 1;
            var experimentSubequenceNumber1 = 0;
            var experimentGroups1 = groups1;
            var experimentName1 = $"{experimentName}_E{experimentSequenceNumber1}_S{experimentSubequenceNumber1}_G{experimentGroups1.Length}";

            Logging.WriteLine($@"Finding best of {groups1.Length} groups (made of {groups1.Sum(a => a.columns.Length)} columns)", ModuleName);//, MethodName);

            var winner = await FeatureSelectionWorker(numInstances, scoringClassId,
                scoringMetrics,
                //cp,
                baseLineDataSet,
                baseLineColumnIndexes,
                dataSet,
                experimentGroups1,
                false,
                //save_status: true,
                null,
                experimentDescription1,
                experimentName1,
                //experimentSequenceNumber: 1,
                //experimentSubequenceNumber: 0,
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
                lvl: lvl + 1,
                ct: ct
            //make_outer_cv_confusion_matrices: make_outer_cv_confusion_matrices
            ).ConfigureAwait(false);
            experimentDurations.Add(DateTime.UtcNow - startTime1);

            if (winner == default)
            {
                // todo: some problem happened
                if (!string.Equals(SvmFsCtl.ProgramArgs.LaunchMethod,"PBS",StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }
            }

            // Column based feature select from the winners
            if (option2_checkIndividualLast || option3_checkIndividualLast)
            {


                var bestWinnerColumns = DataSetGroupMethods.Ungroup(winner.BestWinnerGroups, ct: ct);
                var bestWinnerColumnsInput = DataSetGroupMethods.GetMainGroups(bestWinnerColumns, true, true, true, true, true, true, true, true, true, ct: ct);

                if (option2_checkIndividualLast)
                {
                    var startTime2 = DateTime.UtcNow;

                    var experimentDescription2 = $@"Preselect all group-selection-winner group columns, then test if feature selection goes backwards on a per column (not per group) basis (dimensionality reduction).";
                    var experimentSequenceNumber2 = 2;
                    var experimentSubequenceNumber2 = 0;
                    var experimentGroups2 = bestWinnerColumnsInput;
                    var experimentName2 = $"{experimentName}_E{experimentSequenceNumber2}_S{experimentSubequenceNumber2}_G{experimentGroups2.Length}";

                    var bestWinnerColumnsOutputStartBackwards = await FeatureSelectionWorker(numInstances, scoringClassId,
                        scoringMetrics,
                        //cp,
                        baseLineDataSet,
                        baseLineColumnIndexes,
                        dataSet,
                        experimentGroups2,
                        true,
                        //save_status: true,
                        null,
                        experimentDescription2,
                        experimentName2,
                        //experimentSequenceNumber: 2,
                        //experimentSubequenceNumber: 0,
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
                        lvl: lvl + 1,
                        ct: ct
                    //make_outer_cv_confusion_matrices: false
                    ).ConfigureAwait(false);

                    experimentDurations.Add(DateTime.UtcNow - startTime2);
                }

                if (option3_checkIndividualLast)
                {
                    var startTime3 = DateTime.UtcNow;

                    var experimentDescription3 = $@"Run forward feature selection on all group-selection-winner group columns, on a per column (not per group) basis to remove surplus columns (dimensionality reduction).";
                    var experimentSequenceNumber3 = 3;
                    var experimentSubequenceNumber3 = 0;
                    var experimentGroups3 = bestWinnerColumnsInput;
                    var experimentName3 = $"{experimentName}_E{experimentSequenceNumber3}_S{experimentSubequenceNumber3}_G{experimentGroups3.Length}";


                    var bestWinnerColumnsOutputStartForwards = await FeatureSelectionWorker(numInstances, scoringClassId,
                        scoringMetrics,
                        //cp,
                        baseLineDataSet,
                        baseLineColumnIndexes,
                        dataSet,
                        experimentGroups3,
                        false,
                        //save_status: true,
                        null,
                        experimentDescription3,
                        experimentName3,
                        //experimentSequenceNumber: 3,
                        //experimentSubequenceNumber: 0,
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
                        lvl: lvl + 1,
                        ct: ct
                    //make_outer_cv_confusion_matrices: false
                    ).ConfigureAwait(false);
                    experimentDurations.Add(DateTime.UtcNow - startTime3);
                }
            }

            // Check if result is approximately the same with other parameters values (i.e. variance number of repetitions, outer folds, inner folds, etc.)
            if (option4_testFinalBestBias)
            {
                // stage5 ...

                // 1. test variance of kernel & scale
                //feature_selection_worker(DataSet, winner.groups);

                // 2. test variance of repetitions, outer-cv, inner-cv

                // 3. test variance of class weight
            }

            //await cp.StopAsync(callChain: null, lvl: lvl + 1).ConfigureAwait(false);

            Logging.LogEvent($"Experiment durations: {string.Join(", ", experimentDurations.Select(a => $"[{a:dd\\:hh\\:mm\\:ss}]").ToArray())}.");
            Logging.LogEvent($"Total duration: [{(DateTime.UtcNow - startTime):dd\\:hh\\:mm\\:ss}].");

            Logging.LogExit(ModuleName);
        }

        public static int[] GetInstanceWorkSizes(int numInstances, int numWork)
        {
            if (numInstances == 0) return Array.Empty<int>();

            var y = numWork / numInstances;
            var r = numWork % numInstances;
            var list = Enumerable.Range(0, numInstances).Select((b, i) => y + (i < r ? 1 : 0)).ToArray();

            return list;
        }



        public static (int instanceId, IndexData[] instanceWork)[] RedistributeWork(IndexData[] work, IndexData[] workComplete, IndexData[] workIncomplete, int numInstances)
        {
            if (numInstances < 1) return default;

            if (workComplete == null && work != null && workIncomplete != null) workComplete = work.Except(workIncomplete).ToArray();
            if (workIncomplete == null && work != null && workComplete != null) workIncomplete = work.Except(workComplete).ToArray();

            var instanceWorkSizes = GetInstanceWorkSizes(numInstances, workIncomplete.Length);
            var list = new (int instanceId, IndexData[] instanceWork)[numInstances];
            for (var i = 0; i < list.Length; i++)
            {
                list[i].instanceId = i;
                list[i].instanceWork = new IndexData[instanceWorkSizes[i]];
            }

            var instanceWorkIndexes = new int[numInstances];

            var instanceIndex = -1;
            for (var workItemIndex = 0; workItemIndex < workIncomplete.Length; workItemIndex++)
            {
                do
                {
                    instanceIndex++;
                    if (instanceIndex >= numInstances) instanceIndex = 0;
                } while (instanceWorkIndexes[instanceIndex] >= instanceWorkSizes[instanceIndex]);


                list[instanceIndex].instanceWork[instanceWorkIndexes[instanceIndex]++] = workIncomplete[workItemIndex];
            }

            return list;
        }

        public static async Task<List<(IndexData id, ConfusionMatrix cm)>> LoadIterationCache(IndexData[] indexesWhole, string cacheFolder, List<string> cacheFilesLoaded = null, CancellationToken callerCt = default)
        {
            var cacheFiles1 = await IoProxy.GetFilesAsync(true, callerCt, cacheFolder, "_cache_*.csv", SearchOption.TopDirectoryOnly).ConfigureAwait(false);

            var result = new List<(IndexData id, ConfusionMatrix cm)>();
            do
            {
                var cacheFiles = cacheFiles1?.ToArray();

                if ((cacheFiles?.Length ?? 0) > 0 && (cacheFilesLoaded?.Count ?? 0) > 0)
                {
                    cacheFiles = cacheFiles.Except(cacheFilesLoaded).ToArray();

                    cacheFiles = cacheFiles.Where(a => { try { return File.Exists(a) && new FileInfo(a).Length > 0; } catch (Exception) { return false; } }).ToArray();
                }

                if ((cacheFiles?.Length ?? 0) > 0)
                {
                    Logging.LogEvent($"Loading cache files [{cacheFiles.Length}]: {string.Join(",", cacheFiles.Select((a, i) => $@"[{i}] ""{a}""").ToArray())}");

                    var cache = await CacheLoad.LoadCacheFileListAsync(indexesWhole, cacheFiles, true, callerCt).ConfigureAwait(false);
                    if (cache != default && cache.IdCmSd != default && cache.IdCmSd.Length > 0)
                    {
                        var cacheData = cache.IdCmSd.Where(a => a.cm != default && a.id != default).ToList();
                        result.AddRange(cacheData);
                        cacheFilesLoaded?.AddRange(cache.FilesLoaded);

                        //var masterIterationCmLoadedIds = masterIterationCmLoaded.Select(a => a.id.IdJobUid).OrderBy(a => a).Distinct().ToArray();
                    }

                    var cacheFiles2 = await IoProxy.GetFilesAsync(true, callerCt, cacheFolder, "_cache_*.csv", SearchOption.TopDirectoryOnly).ConfigureAwait(false);

                    if ((cacheFiles2?.Length ?? 0) > 0 && !cacheFiles1.SequenceEqual(cacheFiles2))
                    {
                        Logging.LogEvent("New cache files found.");
                        cacheFiles1 = cacheFiles2;
                        continue;
                    }
                }

                break;
            } while (true);

            

            return result;
        }

        public static async Task<((DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] BestWinnerGroups, (IndexData id, ConfusionMatrix cm, RankScore rs) BestWinnerData, List<(IndexData id, ConfusionMatrix cm, RankScore rs)> winners)> 
            
            FeatureSelectionWorker
            (
            int numInstances,
            int scoringClassId,
            string[] scoringMetrics, 
            DataSet baseLineDataSet,
            int[] baseLineColumnIndexes, 
            DataSet dataSet, 
            (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] groups, 
            bool preselectAllGroups, // preselect all groups
            int[] baseGroupIndexes, //always include these groups
            string experimentDescription, 
            string experimentName,            
            int repetitions,
            int outerCvFolds,
            int outerCvFoldsToRun,
            int innerFolds, 
            Libsvm.LibsvmSvmType[] svmTypes,
            Libsvm.LibsvmKernelType[] kernels, 
            Scaling.ScaleFunction[] scales,
            (int ClassId, double ClassWeight)[][] classWeightSets, 
            bool calcElevenPointThresholds, 
            double minScoreIncrease = 0.005,
            int maxIterations = 100, 
            int limitIterationNotHigherThanAll = 14, 
            int limitIterationNotHigherThanLast = 7,
            bool asParallel = true,
            ulong lvl = 0,
            CancellationToken ct = default
            )
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }

            var instanceGuid = Guid.NewGuid();

            
            const string methodName = nameof(FeatureSelectionWorker);
            const bool overwriteCache = false;

     


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
                Logging.LogEvent($@"[{experimentName}; iteration: {iterationIndex}] [{msg}]", ModuleName, methodName);
            }

            // todo: add rank positions (for each iteration) to the winning features summary output... 

            Logging.LogGap(2);
            Log($@"Start of iteration.");
            Logging.LogGap(2);

            Log($@"Total groups: {groups?.Length ?? 0}.");

            while (!featureSelectionFinished)
            {
                if (ct.IsCancellationRequested)
                {
                    Logging.LogExit(ModuleName);
                    return default;
                }

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
                var jobGroupSeries = CacheLoad.JobGroupSeries(SvmFsCtl.ProgramArgs.ResultsRootFolder, dataSet, groups, experimentName, iterationIndex, baseGroupIndexes, groupIndexesToTest, selectedGroups, previousWinnerGroupIndex, selectionExcludedGroups2, previousGroupTests, asParallel, ct);
                Log($@"{nameof(jobGroupSeries)}.{nameof(jobGroupSeries.Length)} = {jobGroupSeries?.Length ?? 0}.");
                if (jobGroupSeries == null || jobGroupSeries.Length == 0) break;

                // get folder and file names for this iteration (iteration_folder & iteration_whole_cm_filename are the same for all partitions; iteration_partition_cm_filename is specific to the partition)
                var iterationFolder = CacheLoad.GetIterationFolder(SvmFsCtl.ProgramArgs.ResultsRootFolder, experimentName, iterationIndex, ct: ct);


                IndexData[] indexesWhole = null;

                if ((indexesWhole?.Length ?? 0) == 0)
                {
                    indexesWhole = CacheLoad.GetFeatureSelectionInstructions(baseLineDataSet, baseLineColumnIndexes, dataSet, groups, jobGroupSeries, experimentName, iterationIndex, groups?.Length ?? 0, /*InstanceId, TotalInstances,*/ repetitions, outerCvFolds, outerCvFoldsToRun, innerFolds, svmTypes, kernels, scales, classWeightSets, calcElevenPointThresholds, baseGroupIndexes, groupIndexesToTest, selectedGroups, previousWinnerGroupIndex, selectionExcludedGroups2, previousGroupTests, ct: ct);

                    var indexesWholeFilename = Path.Combine(iterationFolder, $@"work_list_{iterationIndex}.csv");

                    var indexesWholeLines = indexesWhole.Select(a => a.CsvValuesString()).ToList();
                    indexesWholeLines.Insert(0, IndexData.CsvHeaderString);

                    await IoProxy.WriteAllLinesAsync(true, ct, indexesWholeFilename, indexesWholeLines);
                }
                



                allIndexData.AddRange(indexesWhole);

                Log($@"{nameof(indexesWhole)}.{nameof(indexesWhole.Length)} = {indexesWhole?.Length ?? 0}");
                if (indexesWhole == null || indexesWhole.Length == 0) break;


                // todo: 1. load cache
                var iterationWholeResults = await LoadIterationCache(indexesWhole, iterationFolder, callerCt: ct).ConfigureAwait(false);
                var (indexesLoaded, indexesNotLoaded) = CacheLoad.UpdateMissing(iterationWholeResults, indexesWhole, true, ct:ct);

                
                if (indexesNotLoaded.Any())
                {
                    // log missing indexes
                    Logging.LogEvent($"Missing work ({indexesNotLoaded.Length} items): {string.Join(", ", indexesNotLoaded?.Select(a => $"{a.IdGroupArrayIndex}:{a.IdJobUid}").ToArray() ?? Array.Empty<string>())}");

                    // redistribute all remaining work into segments
                    var workDistribution = RedistributeWork(indexesWhole, indexesLoaded, indexesNotLoaded, numInstances);

                    // log redistribution
                    foreach (var wd in workDistribution)
                    {
                        Logging.LogEvent($@"Work redistributed - instance {wd.instanceId}/{workDistribution.Length}: {string.Join(", ", wd.instanceWork?.Select(a => $"{a.IdGroupArrayIndex}:{a.IdJobUid}").ToArray() ?? Array.Empty<string>())}");
                    }

                    // write work segment files
                    var workQueueFolder = Path.Combine(CacheLoad.GetIterationFolder(SvmFsCtl.ProgramArgs.ResultsRootFolder, SvmFsCtl.ProgramArgs.ExperimentName), "work_queue");

                    try { Directory.Delete(workQueueFolder, true); } catch (Exception) { }

                    var workFiles = new List<string>() { experimentName }; // first line is the name of the experiment

                    foreach (var wd in workDistribution)
                    {
                        if (wd.instanceWork.Length == 0) continue;
                        
                        var lines = new List<string>();
                        lines.Add(IndexData.CsvHeaderString);
                        lines.AddRange(wd.instanceWork.Select(a => a.CsvValuesString()).ToArray());

                        // write new queues
                        var wfn = Path.Combine(workQueueFolder, $@"work_{wd.instanceId}.csv");
                        await IoProxy.WriteAllLinesAsync(true, ct, wfn, lines);

                        workFiles.Add(wfn);
                    }

                    // write a file which contains a list of work list files
                    await IoProxy.WriteAllLinesAsync(true, ct, Path.Combine(workQueueFolder, $@"work.csv"), workFiles);

                    // todo: notify SvmFsLdr to run msub for SvmFsWkr
                    // ...SvmFsLdr should be waiting for the exit notification, and then will read the work list file

                    // ???
                    // ???


                    // exit to free up node whilst waiting for results
                    if (string.Equals(SvmFsCtl.ProgramArgs.LaunchMethod, "PBS", StringComparison.OrdinalIgnoreCase))
                    {
                        Logging.LogEvent("Controller exiting to free up node...");
                        Environment.Exit(0);
                    }
                    return default;
                }

               

                var iterationWholeResultsFixedWithRanks = CalculateRanks(scoringClassId, scoringMetrics, iterationWholeResults, iterationIndex, allIterationIdCmRs, bestWinnerIdCmRs, lastWinnerIdCmRs, ct);

                var thisIterationWinnerIdCmRs = iterationWholeResultsFixedWithRanks[0];
                allWinnersIdCmRs.Add(thisIterationWinnerIdCmRs);

              

                var numAvailableGroups = NumGroupsAvailable(groups, baseGroupIndexes, calibrate, thisIterationWinnerIdCmRs, selectionExcludedGroups, ct);
                BanPoorPerformanceGroups(experimentName, iterationWholeResultsFixedWithRanks, selectionExcludedGroups, numAvailableGroups, iterationIndex, allIterationIdCmRs, ct);
                numAvailableGroups = NumGroupsAvailable(groups, baseGroupIndexes, calibrate, thisIterationWinnerIdCmRs, selectionExcludedGroups, ct);


                featureSelectionFinished = CheckWhetherFinished(experimentName, minScoreIncrease, maxIterations, limitIterationNotHigherThanAll, limitIterationNotHigherThanLast, featureSelectionFinished, thisIterationWinnerIdCmRs, lastWinnerIdCmRs, numAvailableGroups, iterationIndex, ref bestWinnerIdCmRs, ref iterationsNotHigherThanLast, ref iterationsNotHigherThanBest, ct);
                await SaveIterationSummaryAsync(experimentName, indexesWhole, iterationFolder, iterationIndex, overwriteCache, iterationWholeResultsFixedWithRanks, allWinnersIdCmRs, ct).ConfigureAwait(false);

                lastWinnerIdCmRs = thisIterationWinnerIdCmRs;
                allIterationIdCmRs.Add(iterationWholeResultsFixedWithRanks);
                foreach (var iterationWholeResultsFixedWithRank in iterationWholeResultsFixedWithRanks /*.Skip(1)*/)
                {
                    iterationWholeResultsFixedWithRank.cm.ClearSupplemental();
                    iterationWholeResultsFixedWithRank.id.ClearSupplemental();
                }

                Logging.LogGap(2);
                Log($@"Finished iteration: winning score = {bestWinnerIdCmRs.rs?.RsFsScore ?? 0}, total columns = {bestWinnerIdCmRs.id?.IdNumColumns ?? 0}.");
                Logging.LogGap(2);

                iterationIndex++;
                calibrate = false;
                preselectAllGroups = false;
                previousGroupTests.AddRange(jobGroupSeries.Select(a => a.GroupIndexes).ToArray());
            }

            Log($@"Finished all: all iterations - feature selection complete from {groups?.Length ?? 0} groups.");
            Log($@"Finished all: final winning score = {bestWinnerIdCmRs.rs?.RsFsScore ?? 0}, total columns = {bestWinnerIdCmRs.id?.IdNumColumns ?? 0}.");

            var bestWinnerGroups = bestWinnerIdCmRs.id.IdGroupArrayIndexes.Select(groupIndex => groups[groupIndex]).ToArray();

            await SaveResultsSummaryAsync(
                allIndexData: allIndexData.ToArray(),
                groups: groups,
                experimentDescription: experimentDescription,
                experimentName: experimentName,
                allWinnersIdCmRs: allWinnersIdCmRs,
                bestWinnerIdCmRs: bestWinnerIdCmRs,
                bestWinnerGroups: bestWinnerGroups,
                MethodName: methodName,
                allIterationIdCmRs: allIterationIdCmRs,
                ct: ct
                ).ConfigureAwait(false);

            // save selected columns as dataset (

            //await io_proxy.WriteAllLines(true, ct, Path.Combine(_server_folder, @"exit.csv"), new[] { "exit" }, _CallerModuleName: ModuleName, _CallerMethodName: MethodName).ConfigureAwait(false);
            Logging.LogExit(ModuleName);
            return ct.IsCancellationRequested ? default : (bestWinnerGroups, bestWinnerIdCmRs, allWinnersIdCmRs);
        }

        public static (IndexData id, ConfusionMatrix cm, RankScore rs)[] CalculateRanks(int scoringClassId, string[] scoringMetrics, List<(IndexData id, ConfusionMatrix cm)> iterationWholeResults, int iterationIndex, List<(IndexData id, ConfusionMatrix cm, RankScore rs)[]> allIterationIdCmRs, (IndexData id, ConfusionMatrix cm, RankScore rs) bestWinnerIdCmRs, (IndexData id, ConfusionMatrix cm, RankScore rs) lastWinnerIdCmRs, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested)
            {
                Logging.LogExit(ModuleName);
                return default;
            }

            var iterationWholeResultsFixed = iterationWholeResults.Where(cmSd => cmSd.id != null && cmSd.cm != null && cmSd.cm.XClassId != null && // class id exists
                                                                                 cmSd.cm.XClassId.Value == scoringClassId && // ...and is the scoring class id
                                                                                 cmSd.cm.XPredictionThreshold == null && // not a threshold altered metric
                                                                                 cmSd.cm.XRepetitionsIndex == -1 && // merged
                                                                                 cmSd.cm.XOuterCvIndex == -1 && // merged
                                                                                 cmSd.id.IdIterationIndex == iterationIndex // this iteration
            ).Select(a =>
            {
                var fsScore = a.cm.Metrics.GetValuesByNames(scoringMetrics).Average();

                Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default : (a.id, a.cm, fs_score: fsScore);
            }).ToList();

            var iterationWholeResultsFixedWithRanks = SetRanks(iterationWholeResultsFixed, allIterationIdCmRs, bestWinnerIdCmRs, lastWinnerIdCmRs, ct: ct);

            iterationWholeResults.Clear();
            iterationWholeResultsFixed.Clear();

            Logging.LogExit(ModuleName);

            return ct.IsCancellationRequested ? default : iterationWholeResultsFixedWithRanks;
        }

        public static int NumGroupsAvailable((DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] groups, int[] baseGroupIndexes, bool calibrate, (IndexData id, ConfusionMatrix cm, RankScore rs) thisIterationWinnerIdCmRs, List<int> selectionExcludedGroups, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }

            var availableGroups = Enumerable.Range(0, groups.Length).ToArray();
            if (!calibrate)
            {
                availableGroups = availableGroups.Except(thisIterationWinnerIdCmRs.id.IdGroupArrayIndexes).ToArray();
                if (baseGroupIndexes != null && baseGroupIndexes.Length > 0) availableGroups = availableGroups.Except(baseGroupIndexes).ToArray();
                if (selectionExcludedGroups != null && selectionExcludedGroups.Count > 0) availableGroups = availableGroups.Except(selectionExcludedGroups).ToArray();
            }

            var numAvailableGroups = availableGroups.Length;
            Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default : numAvailableGroups;
        }

        public static void BanPoorPerformanceGroups(string ExperimentName, (IndexData id, ConfusionMatrix cm, RankScore rs)[] iterationWholeResultsFixedWithRanks, List<int> selectionExcludedGroups, int numAvailableGroups, int iterationIndex, List<(IndexData id, ConfusionMatrix cm, RankScore rs)[]> allIterationIdCmRs, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

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

            Logging.LogExit(ModuleName);
        }

        public static bool CheckWhetherFinished(string ExperimentName, double minScoreIncrease, int maxIterations, int limitIterationNotHigherThanAll, int limitIterationNotHigherThanLast, bool featureSelectionFinished, (IndexData id, ConfusionMatrix cm, RankScore rs) thisIterationWinnerIdCmRs, (IndexData id, ConfusionMatrix cm, RankScore rs) lastWinnerIdCmRs, int numAvailableGroups, int iterationIndex, ref (IndexData id, ConfusionMatrix cm, RankScore rs) bestWinnerIdCmRs, ref int iterationsNotHigherThanLast, ref int iterationsNotHigherThanBest, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }

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

            Logging.LogExit(ModuleName); return featureSelectionFinished;
        }

        public static async Task SaveIterationSummaryAsync(string experimentName, IndexData[] indexesWhole, string iterationFolder, int iterationIndex, bool overwriteCache, (IndexData id, ConfusionMatrix cm, RankScore rs)[] iterationWholeResultsFixedWithRanks, List<(IndexData id, ConfusionMatrix cm, RankScore rs)> allWinnersIdCmRs, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested)
            {
                Logging.LogExit(ModuleName);
                return;
            }

            void Log(string msg)
            {
                Logging.WriteLine($"{experimentName}, iteration: {iterationIndex}, {msg}.");
            }

            var fn = CacheLoad.GetIterationFilename(indexesWhole, ct);


            var task1 = Task.Run(async () =>
                {
                    if (ct.IsCancellationRequested) return;

                    // Save the CM ranked for the current iteration (winner rank #0)
                    var iterationCmRanksFn1 = Path.Combine(iterationFolder, $@"iteration_ranks_cm_{fn}_full.csv");
                    //var iteration_cm_ranks_fn2 = Path.Combine(iteration_folder, $@"iteration_ranks_cm_{fn}_summary.csv");
                    if (await IoProxy.IsFileAvailableAsync(true, ct, iterationCmRanksFn1, false, callerModuleName: ModuleName).ConfigureAwait(false)) // && await io_proxy.IsFileAvailable(true, ct, iteration_cm_ranks_fn2, false, _CallerModuleName: ModuleName, _CallerMethodName: MethodName))
                    {
                        Log($@"Already saved for iteration {iterationIndex}. Files: {iterationCmRanksFn1}."); //, {iteration_cm_ranks_fn2}.");
                    }
                    else
                    {
                        Log($@"Unavailable for iteration {iterationIndex}. Files: {iterationCmRanksFn1}."); //, {iteration_cm_ranks_fn2}.");
                        await ConfusionMatrix.SaveAsync(iterationCmRanksFn1, /*iteration_cm_ranks_fn2,*/ overwriteCache, iterationWholeResultsFixedWithRanks, ct: ct).ConfigureAwait(false);
                        Log($@"Saved for iteration {iterationIndex}. Files: {iterationCmRanksFn1}."); //, {iteration_cm_ranks_fn2}.");
                    }
                },
                ct);

            var task2 = Task.Run(async () =>
                {
                    if (ct.IsCancellationRequested) return;

                    // Save the CM of winners from all iterations
                    var winnersCmFn1 = Path.Combine(iterationFolder, $@"winners_cm_{fn}_full.csv");
                    //var winners_cm_fn2 = Path.Combine(iteration_folder, $@"winners_cm_{fn}_summary.csv");
                    if (await IoProxy.IsFileAvailableAsync(true, ct, winnersCmFn1, false, callerModuleName: ModuleName).ConfigureAwait(false))
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
                },
                ct);

            var task3 = Task.Run(async () =>
                {
                    if (ct.IsCancellationRequested) return;

                    // Save the prediction list for misclassification analysis
                    var predictionListFilename = Path.Combine(iterationFolder, $@"iteration_prediction_list_{fn}.csv");
                    if (await IoProxy.IsFileAvailableAsync(true, ct, predictionListFilename, false, callerModuleName: ModuleName).ConfigureAwait(false))
                    {
                        Log($@"Already saved for iteration {iterationIndex}. File: {predictionListFilename}.");
                    }
                    else
                    {
                        Log($@"Unavailable for iteration {iterationIndex}. File: {predictionListFilename}.");
                        await Prediction.SaveAsync(ct, predictionListFilename, iterationWholeResultsFixedWithRanks).ConfigureAwait(false);
                        Log($@"Saved for iteration {iterationIndex}. File: {predictionListFilename}.");
                    }
                },
                ct);

            try { await Task.WhenAll(task1, task2, task3).ConfigureAwait(false); }
            catch (Exception e) { Logging.LogException(e); }

            Logging.LogExit(ModuleName);
        }


        public static async Task SaveResultsSummaryAsync(IndexData[] allIndexData, (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] groups, string experimentDescription, string experimentName, List<(IndexData id, ConfusionMatrix cm, RankScore rs)> allWinnersIdCmRs, (IndexData id, ConfusionMatrix cm, RankScore rs) bestWinnerIdCmRs, (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] bestWinnerGroups, string MethodName, List<(IndexData id, ConfusionMatrix cm, RankScore rs)[]> allIterationIdCmRs, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            var experimentFolder = CacheLoad.GetIterationFolder(SvmFsCtl.ProgramArgs.ResultsRootFolder, experimentName, ct: ct);

            var fn = CacheLoad.GetIterationFilename(allIndexData, ct);


            var task1 = Task.Run(async () =>
            {
                if (ct.IsCancellationRequested) return;
                var bestWinnerFn = Path.Combine(experimentFolder, $@"best_winner_{fn}.csv");
                var bestWinnerText = new List<string>();

                bestWinnerText.Add($@"Experiment description: {experimentDescription}");
                bestWinnerText.Add($@"Experiment name: {experimentName}");
                bestWinnerText.Add($@"");


                bestWinnerText.Add($@"Feature selection iterative winner history:");
                bestWinnerText.Add($@"");
                bestWinnerText.Add($@"index,{string.Join(",", RankScore.CsvHeaderValuesArray)},{string.Join(",", IndexData.CsvHeaderValuesArray)},{string.Join(",", ConfusionMatrix.CsvHeaderValuesArray)}");
                bestWinnerText.AddRange(allWinnersIdCmRs.Select((a, k1) => $@"{k1},{string.Join(",", a.rs?.CsvValuesArray() ?? RankScore.Empty.CsvValuesArray())},{string.Join(",", a.id?.CsvValuesArray() ?? IndexData.Empty.CsvValuesArray())},{string.Join(",", a.cm?.CsvValuesArray() ?? ConfusionMatrix.Empty.CsvValuesArray())}").ToArray());
                bestWinnerText.Add($@"");
                bestWinnerText.Add($@"");

                bestWinnerText.Add($@"Note: the final best winner iteration could be several iterations before the final feature selection iteration.");
                bestWinnerText.Add($@"");
                bestWinnerText.Add($@"Winning iteration:,{bestWinnerIdCmRs.id.IdIterationIndex}");
                bestWinnerText.Add($@"");
                bestWinnerText.Add($@"Final best winner score data:");
                bestWinnerText.Add($@"");
                bestWinnerText.Add($@"{string.Join(",", RankScore.CsvHeaderValuesArray)},{string.Join(",", IndexData.CsvHeaderValuesArray)},{string.Join(",", ConfusionMatrix.CsvHeaderValuesArray)}");
                bestWinnerText.Add($@"{string.Join(",", bestWinnerIdCmRs.rs?.CsvValuesArray() ?? RankScore.Empty.CsvValuesArray())},{string.Join(",", bestWinnerIdCmRs.id?.CsvValuesArray() ?? IndexData.Empty.CsvValuesArray())},{string.Join(",", bestWinnerIdCmRs.cm?.CsvValuesArray() ?? ConfusionMatrix.Empty.CsvValuesArray())}");
                bestWinnerText.Add($@"");
                bestWinnerText.Add($@"");

                bestWinnerText.Add($@"Final best winner group keys:");
                bestWinnerText.Add($@"");
                bestWinnerText.Add($@"index1,{string.Join(",", DataSetGroupKey.CsvHeaderValuesArray)},columns...");
                bestWinnerText.AddRange(bestWinnerGroups.Select((a, k1) => $"{k1},{string.Join(",", a.GroupKey?.CsvValuesArray() ?? DataSetGroupKey.Empty.CsvValuesArray())},{string.Join(";", a.columns ?? Array.Empty<int>())}").ToList());
                bestWinnerText.Add($@"");
                bestWinnerText.Add($@"");

                bestWinnerText.Add($@"Final best winner group column keys:");
                bestWinnerText.Add($@"");
                bestWinnerText.Add($@"index1,index2,{string.Join(",", DataSetGroupKey.CsvHeaderValuesArray)},columns...");
                bestWinnerText.AddRange(bestWinnerGroups.SelectMany((a, k1) => a.GroupColumnHeaders.Select((b, k2) => $"{k1},{k2},{string.Join(",", b.CsvValuesArray() ?? DataSetGroupKey.Empty.CsvValuesArray())}").ToList()).ToList());
                bestWinnerText.Add($@"");
                bestWinnerText.Add($@"");

                await IoProxy.WriteAllLinesAsync(true, ct, bestWinnerFn, bestWinnerText, callerModuleName: ModuleName, callerMethodName: MethodName).ConfigureAwait(false);
            }, ct);

            var allData = allIterationIdCmRs.SelectMany(a => a).ToArray();

            var task2 = Task.Run(async () =>
                {
                    if (ct.IsCancellationRequested) return;
                    var allDataFn = Path.Combine(experimentFolder, $@"all_data_{fn}.csv");

                    var allDataText = new List<string>();
                    allDataText.Add($@"index,{string.Join(",", RankScore.CsvHeaderValuesArray)},{string.Join(",", IndexData.CsvHeaderValuesArray)},{string.Join(",", ConfusionMatrix.CsvHeaderValuesArray)}");
                    allDataText.AddRange(allData.Select((a, k1) => $@"{k1},{string.Join(",", a.rs?.CsvValuesArray() ?? RankScore.Empty.CsvValuesArray())},{string.Join(",", a.id?.CsvValuesArray() ?? IndexData.Empty.CsvValuesArray())},{string.Join(",", a.cm?.CsvValuesArray() ?? ConfusionMatrix.Empty.CsvValuesArray())}").ToArray());
                    await IoProxy.WriteAllLinesAsync(true, ct, allDataFn, allDataText, callerModuleName: ModuleName, callerMethodName: MethodName).ConfigureAwait(false);
                },
                ct);

            var task3 = Task.Run(async () =>
            {
                if (ct.IsCancellationRequested) return;
                // group all data by values unique to each group and specific test of that group (i.e. svm kernel, scaling function, number of repetitions, folds, etc.).
                var allDataGrouped = allData.AsParallel().AsOrdered() /*.WithCancellation(ct)*/.GroupBy(a => (a.id.IdGroupArrayIndex, a.id.IdRepetitions, a.id.IdOuterCvFolds, a.id.IdOuterCvFoldsToRun, a.id.IdInnerCvFolds, a.id.IdExperimentName, a.id.IdTotalGroups, a.id.IdSvmType, a.id.IdSvmKernel, a.id.IdScaleFunction, a.id.IdCalcElevenPointThresholds, a.id.IdSelectionDirection, IdClassWeights: string.Join(";", a.id?.IdClassWeights?.Select(a => $"{a.ClassId}:{a.ClassWeight}").ToArray() ?? Array.Empty<string>()))).Select(a => (key: a.Key, list: a.ToList())).ToArray();
                var rankStatsFn = Path.Combine(experimentFolder, $@"rank_stats_{fn}.csv");
                var rankStatsText = new List<string>();
                rankStatsText.Add($@"{string.Join(",", "ListIndex", "ListCount", string.Join(",", DataSetGroupKey.CsvHeaderValuesArray), nameof(IndexData.IdGroupArrayIndex), nameof(IndexData.IdRepetitions), nameof(IndexData.IdOuterCvFolds), nameof(IndexData.IdOuterCvFoldsToRun), nameof(IndexData.IdInnerCvFolds), nameof(IndexData.IdExperimentName), nameof(IndexData.IdTotalGroups), nameof(IndexData.IdSvmType), nameof(IndexData.IdSvmKernel), nameof(IndexData.IdScaleFunction), nameof(IndexData.IdCalcElevenPointThresholds), nameof(IndexData.IdSelectionDirection), nameof(IndexData.IdClassWeights))},{string.Join(",", Stats.CsvHeaderValuesArray.Select(a => $"FsScore{a}").ToArray())},{string.Join(",", Stats.CsvHeaderValuesArray.Select(a => $"FsScorePercentile{a}").ToArray())}");
                rankStatsText.AddRange(allDataGrouped.Select((a, k1) =>
                {
                    var fsScore = new Stats(a.list.Select(b => b.rs.RsFsScore).ToArray());
                    var fsScorePercentile = new Stats(a.list.Select(b => b.rs.RsFsScorePercentile).ToArray());
                    //var fs_rank_index_percentile = new gkStats(a.list.Select(b => b.rs.fs_rank_index_percentile).ToArray());
                    //var fs_rank_index = new gkStats(a.list.Select(b => (double) b.rs.fs_rank_index).ToArray());
                    var gk = a.key.IdGroupArrayIndex > -1 && groups != null && groups.Length > 0
                        ? groups[a.key.IdGroupArrayIndex].GroupKey
                        : DataSetGroupKey.Empty;


                    return ct.IsCancellationRequested
                        ? default
                        : $"{k1},{a.list.Count},{string.Join(",", gk.CsvValuesArray())},{a.key.IdGroupArrayIndex},{a.key.IdRepetitions},{a.key.IdOuterCvFolds},{a.key.IdOuterCvFoldsToRun},{a.key.IdInnerCvFolds},{a.key.IdExperimentName},{a.key.IdTotalGroups},{a.key.IdSvmType},{a.key.IdSvmKernel},{a.key.IdScaleFunction},{(a.key.IdCalcElevenPointThresholds ? 1 : 0)},{a.key.IdSelectionDirection},{a.key.IdClassWeights},{string.Join(",", fsScore.CsvValuesArray())},{string.Join(",", fsScorePercentile.CsvValuesArray())}";
                }).ToArray());
                await IoProxy.WriteAllLinesAsync(true, ct, rankStatsFn, rankStatsText, callerModuleName: ModuleName, callerMethodName: MethodName).ConfigureAwait(false);
            }, ct);

            var task4 = Task.Run(async () =>
            {
                if (ct.IsCancellationRequested) return;
                var rankStatsFn2 = Path.Combine(experimentFolder, $@"rank_scores_{fn}.csv");
                var iterScores = allData.GroupBy(a => a.id.IdGroupArrayIndex).Select(a => (IdGroupArrayIndex: a.Key, a.ToArray())).ToArray();
                var iterLines = new List<string>();
                var iters = allIndexData.Select(a => a.IdIterationIndex).Distinct().OrderBy(a => a).ToArray();
                iterLines.Add($"{DataSetGroupKey.CsvHeaderString},g,i{iters[0] - 1},{string.Join(",", iters.Select(a => $"i{a}").ToArray())}");
                foreach (var iterScore in iterScores)
                {
                    var x = new double?[iters.Length];

                    for (var index = 0; index < iters.Length; index++)
                    {
                        var y = iterScore.Item2.FirstOrDefault(a => a.id.IdIterationIndex == iters[index]);
                        if (y != default && y.rs != default) { x[index] = (double?)y.rs.RsFsScore; }
                    }

                    iterLines.Add($"{iterScore.Item2.First().id.IdGroupKey?.CsvValuesString() ?? DataSetGroupKey.Empty.CsvValuesString()},g{iterScore.IdGroupArrayIndex},0,{string.Join(",", x)}");
                }

                await IoProxy.WriteAllLinesAsync(true, ct, rankStatsFn2, iterLines, callerModuleName: ModuleName, callerMethodName: MethodName).ConfigureAwait(false);
            }, ct);

            try { await Task.WhenAll(task1, task2, task3, task4).ConfigureAwait(false); }
            catch (Exception e) { Logging.LogException(e); }

            Logging.LogExit(ModuleName);
        }




        //public static async Task<List<(IndexData id, ConfusionMatrix cm)>> ServeIpcJobsAsync(DataSet dataSet, ConnectionPool cp, IndexDataContainer indexDataContainer, int iterationIndex, ulong lvl = 0, CancellationToken ct = default)
        //{

        //    Logging.LogCall(ModuleName);
        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }

        //    Logging.LogEvent($"Reached start of {nameof(FsServer)}.{nameof(ServeIpcJobsAsync)} for iteration {iterationIndex}.", ModuleName);

        //    // load cache
        //    var serverCmFile = Path.Combine(ServerFolder, $@"_server_cache_{Program.GetIterationFilename(indexDataContainer.IndexesWhole, ct)}.csv");
        //    var iterationWholeResults = new List<(IndexData id, ConfusionMatrix cm)>();
        //    var iterationWholeResultsLines = new List<string>();
        //    iterationWholeResultsLines.Add($"{IndexData.CsvHeaderString},{ConfusionMatrix.CsvHeaderString}");

        //    if (IoProxy.ExistsFile(true, serverCmFile, ModuleName, ct: ct) && IoProxy.FileLength(serverCmFile, ct) > 0)
        //    {
        //        var cache = await CacheLoad.LoadCacheFileAsync(serverCmFile, indexDataContainer, ct).ConfigureAwait(false);
        //        if (cache != null && cache.Length > 0)
        //        {
        //            iterationWholeResults.AddRange(cache);
        //            iterationWholeResultsLines.AddRange(cache.Select(a => $"{a.id?.CsvValuesString() ?? IndexData.Empty.CsvValuesString()},{a.cm?.CsvValuesString() ?? ConfusionMatrix.Empty.CsvValuesString()}").ToArray());
        //        }
        //    }
        //    else
        //    {
        //        // create cache file, if doesn't exist
        //        var header = $"{IndexData.CsvHeaderString},{ConfusionMatrix.CsvHeaderString}";
        //        await IoProxy.WriteAllLinesAsync(true, ct, serverCmFile, new[] { header }).ConfigureAwait(false);
        //    }

        //    CacheLoad.UpdateMissing(iterationWholeResults, indexDataContainer, ct: ct);

        //    // setup ETA task
        //    var etaNumTotal = indexDataContainer.IndexesMissingWhole.Length;
        //    var etaStartTime = DateTime.UtcNow;
        //    var etaNumComplete = 0;
        //    var etaNumCompleteLock = new object();

        //    using var etaCts = new CancellationTokenSource();
        //    var etaCt = etaCts.Token;

        //    var etaTask = Task.Run(async () =>
        //        {
        //            while (!ct.IsCancellationRequested && !etaCt.IsCancellationRequested)
        //                try
        //                {
        //                    await Task.Delay(TimeSpan.FromSeconds(30), etaCt).ConfigureAwait(false);
        //                    lock (etaNumCompleteLock) { Routines.PrintETA(etaNumComplete, etaNumTotal, etaStartTime, ModuleName); }
        //                }
        //                catch (Exception e)
        //                {
        //                    Logging.LogException(e, "", ModuleName);
        //                    break;
        //                }
        //        },
        //        etaCt);

        //    // 


        //    //var makeOuterCvInputs = indexDataContainer.IndexesMissingWhole.Select(id => {            }).ToArray();
        //    var size = 0;
        //    var size_lock = new object();
        //    // save to file
        //    Parallel.For(0, indexDataContainer.IndexesMissingWhole.Length, i =>
        //    {

        //        var id = indexDataContainer.IndexesMissingWhole[i];
        //        var mocvi = CrossValidate.MakeOuterCvInputs(dataSet, id, asParallel: true, ct: ct);

        //        var idJson = JsonSerializer.Serialize<IndexData>(id, new JsonSerializerOptions() { IncludeFields = true });
        //        var mergedCvInputJson = JsonSerializer.Serialize<OuterCvInput>(mocvi.mergedCvInput, new JsonSerializerOptions() { IncludeFields = true });
        //        var outerCvInputsJson = JsonSerializer.Serialize<OuterCvInput[]>(mocvi.outerCvInputs, new JsonSerializerOptions() { IncludeFields = true });

        //        lock (size_lock) size += idJson.Length + mergedCvInputJson.Length + outerCvInputsJson.Length;
        //        //var file
        //    });

        //    Console.WriteLine((size / 1024 / 1024) + " mb");
        // write all inputs ready for use... no need to load dataset each time.
        // if 4/7 instances run... what happens to the 3 that didn't?
        //todo: remove all tcp code... useless doesn't work, don't know why... go back to using hpc array indexes.
        // problem: how to solve issue of some array indexes not running... either because hpc doesn't run them, kills them, or not enough free cpus to run them... 
        // exit others?

        // on start, writing file giving stating which instances are running,
        // owhen a new instance starts, it must tell the other instances .....
        // other instances write file 

        //    var listen = new TcpListener(IPAddress.Any, 4455);
        //    listen.Start();
        //    // serve message queue
        //    while (!ct.IsCancellationRequested)
        //    {
        //        var q = new Queue<IndexData>(indexDataContainer.IndexesMissingWhole);

        //        while (indexDataContainer.IndexesMissingWhole.Any())
        //        {
        //            var client = await listen.AcceptTcpClientAsync();
        //            if (client == default) continue;

        //            var id = q.Dequeue();
        //            //var mocvi = makeOuterCvInputs.First(a => a.id == id);

        //        }

        //    }
        //    listen.Stop();

        //    return default;
        //}
        //    while (!ct.IsCancellationRequested && indexDataContainer.IndexesMissingWhole.Any())
        //    {
        //        var tasks = new List<Task<(IndexData id, ConfusionMatrix cm)[]>>();

        //        for (int i = 0; i < indexDataContainer.IndexesMissingWhole.Length; i++)
        //        {
        //            var k = i;
        //            var mocvi = makeOuterCvInputs.First(a => a.id == indexDataContainer.IndexesMissingWhole[k]).mocvi;
        //            var task = Task.Run(async () => await CrossValidate.CrossValidatePerformanceAsync(cp, CrossValidate.RpcPoint.CrossValidatePerformanceAsync, mocvi.outerCvInputs, mocvi.mergedCvInput, indexDataContainer.IndexesMissingWhole[k], ct: ct).ConfigureAwait(false), ct);
        //            tasks.Add(task);
        //        }

        //        var incompleteTasks = tasks.ToArray();// doing '.Where(a => !a.IsCompleted).ToArray();' here would add race condition.
        //        do
        //        {
        //            try { await Task.WhenAny(incompleteTasks).ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e, "", ModuleName); }

        //            var completeTasks = incompleteTasks.Where(a => a.IsCompleted).ToArray();
        //            if (completeTasks.Length == 0) continue;
        //            incompleteTasks = incompleteTasks.Except(completeTasks).ToArray();

        //            var completeTaskResults = completeTasks.Where(a => a.IsCompletedSuccessfully && a.Result != default && a.Result.Length > 0).Select(a => a.Result).ToArray();
        //            if (completeTaskResults.Length == 0) continue;

        //            var completeTaskResultsMany = completeTaskResults.SelectMany(a => a).ToArray();
        //            var completeTaskResultsManyLines = completeTaskResultsMany.Select(a => $"{a.id?.CsvValuesString() ?? IndexData.Empty.CsvValuesString()},{a.cm?.CsvValuesString() ?? ConfusionMatrix.Empty.CsvValuesString()}").ToArray();
        //            iterationWholeResultsLines.AddRange(completeTaskResultsManyLines);
        //            iterationWholeResults.AddRange(completeTaskResultsMany);

        //            await IoProxy.AppendAllLinesAsync(true, ct, serverCmFile, completeTaskResultsManyLines).ConfigureAwait(false);

        //            lock (etaNumCompleteLock)
        //            {
        //                etaNumComplete += completeTasks.Length;
        //            }

        //        } while (incompleteTasks.Length > 0);

        //        //try { await Task.WhenAll(tasks).ConfigureAwait(false); }catch (Exception e) { Logging.LogException(e, "", ModuleName); }
        //        CacheLoad.UpdateMissing(iterationWholeResults, indexDataContainer, ct: ct);
        //        //var taskResults = tasks.Where(a => a.IsCompletedSuccessfully && a.Result != default && a.Result.Length > 0).Select(a => a.Result).ToArray();
        //        //var taskResultsMany = taskResults.SelectMany(a => a).ToArray();
        //        //iterationWholeResults.AddRange(taskResultsMany);

        //        //var lines = iterationWholeResults.Select(a => $"{a.id?.CsvValuesString() ?? IndexData.Empty.CsvValuesString()},{a.cm?.CsvValuesString() ?? ConfusionMatrix.Empty.CsvValuesString()}").ToList();
        //        //lines.Insert(0, IndexData.CsvHeaderString + "," + ConfusionMatrix.CsvHeaderString);
        //        //await IoProxy.WriteAllLinesAsync(true, ct, serverCmFile, lines).ConfigureAwait(false);

        //    }

        //    try { etaCts.Cancel(); } catch (Exception e) { Logging.LogException(e, "", ModuleName); }
        //    Logging.LogEvent($"Reached end of {nameof(FsServer)}.{nameof(ServeIpcJobsAsync)} for iteration {iterationIndex}.", ModuleName);
        //    Logging.LogExit(ModuleName);
        //    return iterationWholeResults;

        //}


        //    var startTotalMissing = indexDataContainer.IndexesMissingWhole.Length;
        //    var startTime = DateTime.UtcNow;
        //    var numComplete = 0;
        //    var numCompleteLock = new object();


        //    using var etaCts = new CancellationTokenSource();
        //    var etaCt = etaCts.Token;
        //    var etaTask = Task.Run(async () =>
        //        {
        //            while (!ct.IsCancellationRequested && !etaCt.IsCancellationRequested)
        //                try
        //                {
        //                    await Task.Delay(TimeSpan.FromSeconds(30), etaCt).ConfigureAwait(false);
        //                    lock (numCompleteLock) { Routines.PrintETA(numComplete, startTotalMissing, startTime, ModuleName); }
        //                }
        //                catch (Exception e)
        //                {
        //                    Logging.LogException(e, "", ModuleName);
        //                    break;
        //                }
        //        },
        //        etaCt);

        //    //try { await Task.Delay(TimeSpan.FromMilliseconds(1), externalCt).ConfigureAwait(false); } catch (Exception) { }

        //    while (indexDataContainer.IndexesMissingWhole.Any())
        //    {
        //        if (ct.IsCancellationRequested)
        //        {
        //            Logging.LogEvent($"Cancellation requested on {nameof(ct)}.", ModuleName);
        //            break;
        //        }

        //        var workQueue = new Queue<IndexData>(indexDataContainer.IndexesMissingWhole);

        //        var tasks = new List<(IndexData id, Task<(IndexData id, ConfusionMatrix[] CmList)> task)>();
        //        var taskResults = new List<(IndexData id, ConfusionMatrix[] CmList)>();

        //        while (workQueue.Any() || tasks.Any())
        //        {
        //            // check if cancelled


        //            if (ct.IsCancellationRequested)
        //            {
        //                Logging.LogEvent($"Cancellation requested on {nameof(ct)}.", ModuleName);
        //                break;
        //            }

        //            // check if any work
        //            if (workQueue.Any())
        //            {
        //                var id = workQueue.Dequeue();
        //                if (id == null) continue;

        //                ConnectionPoolMember cpm = null;

        //                var lastCountActive = (0, 0, 0);
        //                do
        //                {
        //                    cpm = cp.GetNextClient(callChain: null, lvl: lvl + 1);

        //                    if (cpm != default) break;

        //                    var countActive = cp.CountActive(callChain: null, lvl: lvl + 1);

        //                    if (countActive != lastCountActive)
        //                    {
        //                        lastCountActive = countActive;
        //                        //if (countActive.unreserved > 0) break;
        //                        Logging.LogEvent($"Connection pool is empty. Waiting to run. Id: {id.IdGroupArrayIndex}. Pool reserved: {countActive.reserved}. Pool unreserved: {countActive.unreserved}. Pool total: {countActive.total}.", ModuleName);
        //                    }

        //                    try { await Task.Delay(TimeSpan.FromSeconds(1), ct); }catch (Exception) { break; }
        //                } while (!ct.IsCancellationRequested && cpm == default);

        //                if (cpm != default)
        //                {
        //                    var task = Task.Run(async () =>
        //                        {
        //                            if (cpm == default || !cpm.IsActive(callChain: null, lvl: lvl + 1)) { return default; }

        //                            Logging.LogEvent($"Server: Starting Inner Task - IdGroupArrayIndex = {id.IdGroupArrayIndex}", ModuleName);

        //                            if (ct.IsCancellationRequested)
        //                            {
        //                                Logging.LogEvent($"Server: Cancellation requested - IdGroupArrayIndex = {id.IdGroupArrayIndex}.", ModuleName);

        //                                Logging.LogExit(ModuleName);
        //                                return default;
        //                            }


        //                            try
        //                            {
        //                                (IndexData id, ConfusionMatrix[] CmList) ret = default;

        //                                ret = await IpcMessaging.IpcAsync($"server_{cpm.LocalHost}:{cpm.LocalPort}", $"client_{cpm.RemoteHost}:{cpm.RemotePort}", cpm, null, true, id, lvl: lvl + 1, ct: ct).ConfigureAwait(false);

        //                                if (ret != default)
        //                                {
        //                                    lock (numCompleteLock) { numComplete++; }
        //                                }
        //                                else { Logging.LogEvent($"{nameof(IpcMessaging.IpcAsync)} response was empty!"); }

        //                                Logging.LogExit(ModuleName);

        //                                return ct.IsCancellationRequested
        //                                    ? default
        //                                    : ret;
        //                            }
        //                            catch (Exception e)
        //                            {

        //                                Logging.LogException(e, $"Server: IdGroupArrayIndex = {id.IdGroupArrayIndex}", ModuleName);
        //                                Logging.LogExit(ModuleName);
        //                                return default;
        //                            }
        //                            finally
        //                            {
        //                                cpm?.Unreserve(callChain: null, lvl: lvl + 1);

        //                                Logging.LogEvent($"Server: Exiting Inner Task - IdGroupArrayIndex = {id.IdGroupArrayIndex}", ModuleName);
        //                            }
        //                        },
        //                        ct);

        //                    tasks.Add((id, task));
        //                }
        //            }

        //            var requeueTasks = tasks.Where(a => a.task.IsCompleted && (!a.task.IsCompletedSuccessfully || a.task.Result == default || a.task.Result.CmList == null || a.task.Result.CmList.Length == 0)).ToArray();
        //            if (requeueTasks.Length > 0)
        //            {
        //                tasks = tasks.Except(requeueTasks).ToList();
        //                for (var i = 0; i < requeueTasks.Length; i++) { workQueue.Enqueue(requeueTasks[i].id); }
        //            }

        //            while (!workQueue.Any() && tasks.Any())
        //            {
        //                if (ct.IsCancellationRequested)
        //                {
        //                    Logging.LogEvent($"Cancellation requested on {nameof(ct)}.", ModuleName);
        //                    break;
        //                }

        //                try
        //                {
        //                    var task = await Task.WhenAny(tasks.Select(a => a.task).ToArray());

        //                    var item = tasks.First(a => a.task == task);
        //                    tasks.Remove(item);

        //                    var taskSuccess = task.IsCompletedSuccessfully && task.Result != default && task.Result.CmList != null && task.Result.CmList.Length > 0;

        //                    if (taskSuccess)
        //                    {
        //                        taskResults.Add(item.task.Result);
        //                    }
        //                    else
        //                    {
        //                        workQueue.Enqueue(item.id);
        //                    }

        //                    continue;
        //                }
        //                catch (Exception e)
        //                {
        //                    Logging.LogException(e, "", ModuleName);
        //                }

        //                if (tasks.Count > 0)
        //                {
        //                    try { await Task.WhenAll(tasks.Select(a => a.task).ToArray()).ConfigureAwait(false); }
        //                    catch (Exception e) { Logging.LogException(e, "", ModuleName); }
        //                    var tasksOk = tasks.Where(a => a.task.IsCompletedSuccessfully && a.task.Result != default && a.task.Result.CmList != null && a.task.Result.CmList.Length > 0).ToArray();
        //                    var tasksNotOk = tasks.Except(tasksOk).ToArray();
        //                    taskResults.AddRange(tasksOk.Select(a=>a.task.Result).ToArray());
        //                    for (var i = 0; i < tasksNotOk.Length; i++) { workQueue.Enqueue(tasksNotOk[i].id); }
        //                    tasks.Clear();
        //                }
        //            }
        //        }

        //        if (taskResults.Count > 0)
        //        {
        //            var cache2 = taskResults.SelectMany(a => a.CmList.Select(cm => (a.id, cm)).ToArray()).ToArray();
        //            iterationWholeResults.AddRange(cache2);

        //            var lines = iterationWholeResults.Select(a => $"{a.id?.CsvValuesString() ?? IndexData.Empty.CsvValuesString()},{a.cm?.CsvValuesString() ?? ConfusionMatrix.Empty.CsvValuesString()}").ToList();
        //            lines.Insert(0, IndexData.CsvHeaderString + "," + ConfusionMatrix.CsvHeaderString);
        //            await IoProxy.WriteAllLinesAsync(true, ct, serverCmFile, lines).ConfigureAwait(false);
        //            CacheLoad.UpdateMissing(iterationWholeResults, indexDataContainer, ct: ct);
        //        }
        //    }


        //    etaCts.Cancel();


        //    Logging.LogEvent($"Reached end of {nameof(FsServer)}.{nameof(ServeIpcJobsAsync)} for iteration {iterationIndex}.", ModuleName);

        //    Logging.LogExit(ModuleName);
        //}

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


        public static (IndexData id, ConfusionMatrix cm, RankScore rs)[] SetRanks(List<(IndexData id, ConfusionMatrix cm, double fs_score)> idCmScore, List<(IndexData id, ConfusionMatrix cm, RankScore rs)[]> allIterationIdCmRs, (IndexData id, ConfusionMatrix cm, RankScore rs) bestScore, (IndexData id, ConfusionMatrix cm, RankScore rs) lastScore, bool asParallel = true, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }

            var lastIterationIdCmRs = allIterationIdCmRs?.LastOrDefault();

            var allIterationIdCmRsFlat = allIterationIdCmRs != null && allIterationIdCmRs.Count > 0
                ? asParallel ? allIterationIdCmRs.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.SelectMany(a => a).ToArray() : allIterationIdCmRs.SelectMany(a => a).ToArray()
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

            // make rank_data instances, which track the ranks (performance) of each group over time, to allow for optimisation decisions and detection of variant features

            //var idso = new IndexDataSearchOptions();//false)
            //{
            //    IdJobUid = true,
            //    IdIterationIndex = true,
            //    IdGroupArrayIndex = true,
            //    IdSelectionDirection = true,
            //    IdTotalGroups = true,
            //    IdCalcElevenPointThresholds = true,
            //    IdSvmType = true,
            //    IdSvmKernel = true,
            //    IdScaleFunction = true,
            //    IdRepetitions = true,
            //    IdOuterCvFolds = true,
            //    IdOuterCvFoldsToRun = true,
            //    IdInnerCvFolds = true,
            //    IdGroupKey = true,
            //    IdExperimentName = true,
            //    IdClassWeights = true,
            //    IdNumGroups = false,
            //    IdNumColumns = false,
            //    IdGroupArrayIndexes = false,
            //    IdColumnArrayIndexes = false,
            //    IdGroupFolder = false,
            //    IdClassFolds = false,
            //    IdDownSampledTrainClassFolds = false,
            //};

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
                    ///*.WithCancellation(ct)*/
                    .Select((a, index) =>
                    {
                        var lastGroup = allIterationIdFlat != null
                            ? IndexData.FindLastReference(allIterationIdFlat, a.id/*, idso*/)
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

                        Logging.LogExit(ModuleName);
                        return /*ct.IsCancellationRequested ? default :*/ (a.id, a.cm, rs);
                    }).ToArray()
                : idCmScore.Select((a, index) =>
                {
                    var lastGroup = allIterationIdFlat != null
                        ? IndexData.FindLastReference(allIterationIdFlat, a.id/*, idso*/)
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

                    Logging.LogExit(ModuleName);
                    return /*ct.IsCancellationRequested ? default :*/ (a.id, a.cm, rs);
                }).ToArray();

            Logging.LogExit(ModuleName);

            return ct.IsCancellationRequested ? default : idCmRs;
        }


        /*public static (string client_id, string work_id)[] wait_client(CancellationToken ct, TimeSpan timeout)
        {
            const string MethodName = nameof(wait_client);
            Logging.WriteLine($"{MethodName}()", ModuleName, MethodName);

            if (cts == null) cts = new CancellationTokenSource();
            var sw1 = Stopwatch.StartNew();
            while (!cts.IsCancellationRequested && (timeout == TimeSpan.Zero || sw1.Elapsed <= timeout))
            {
                if (!await io_proxy.exists_directory(true, _server_folder, ModuleName, MethodName))
                {
                    if (timeout == TimeSpan.Zero) { Logging.LogExit(ModuleName);  return null; }
                    var time_left = timeout - sw1.Elapsed;
                    if (time_left > TimeSpan.Zero) await  Logging.WaitAsync(ct, 10 <= time_left.TotalSeconds ? 10 : (int)Math.Floor(time_left.TotalSeconds), 20 <= time_left.TotalSeconds ? 20 : (int)Math.Floor(time_left.TotalSeconds), ModuleName, MethodName);
                }

                var client_request_files = await io_proxy.GetFiles(true, ct, _server_folder, "client_request_*.csv", SearchOption.AllDirectories, _CallerModuleName: ModuleName, _CallerMethodName: MethodName);

                if (client_request_files == null || client_request_files.Length == 0)
                {
                    if (timeout == TimeSpan.Zero) { Logging.LogExit(ModuleName);  return null; }
                    var time_left = timeout - sw1.Elapsed;
                    if (time_left > TimeSpan.Zero) await  Logging.WaitAsync(ct, 10 <= time_left.TotalSeconds ? 10 : (int)Math.Floor(time_left.TotalSeconds), 20 <= time_left.TotalSeconds ? 20 : (int)Math.Floor(time_left.TotalSeconds), ModuleName, MethodName);
                }

                var client_requests = client_request_files.Select(a =>
                    {
                        var client_id = Path.GetDirectoryName(a).Split(new char[] { '\\', '/' }, StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
                        var work_id = Path.GetFileNameWithoutExtension(a).Split('_', StringSplitOptions.RemoveEmptyEntries).LastOrDefault();

                        Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :(client_id, work_id);
                    })
                    .ToArray();

                for (var i = 0; i < client_request_files.Length; i++) { await io_proxy.delete_file(true, ct, client_request_files[i], _CallerModuleName: ModuleName, _CallerMethodName: MethodName); }

                Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :client_requests;
            }

            Logging.LogExit(ModuleName); return null;
        }*/

        /*
        public static (string client_id, string work_id, (index_data id, ConfusionMatrix cm)[] id_CmList)[] wait_results(CancellationToken ct, TimeSpan timeout, IndexDataContainer IndexDataContainer, bool as_parallel = true)
        {
            const string MethodName = nameof(wait_results);
            Logging.WriteLine($"{MethodName}()", ModuleName, MethodName);

            if (cts == null) cts = new CancellationTokenSource();
            var sw1 = Stopwatch.StartNew();
            while (!cts.IsCancellationRequested && (timeout == TimeSpan.Zero || sw1.Elapsed <= timeout))
            {
                if (!await io_proxy.exists_directory(true, _server_folder, ModuleName, MethodName))
                {
                    if (timeout == TimeSpan.Zero) { Logging.LogExit(ModuleName);  return null; }
                    var time_left = timeout - sw1.Elapsed;
                    if (time_left > TimeSpan.Zero) await  Logging.WaitAsync(ct, 10 <= time_left.TotalSeconds ? 10 : (int)Math.Floor(time_left.TotalSeconds), 20 <= time_left.TotalSeconds ? 20 : (int)Math.Floor(time_left.TotalSeconds), ModuleName, MethodName);
                }

                var client_request_files = await io_proxy.GetFiles(true, ct, _server_folder, "client_results_*.csv", SearchOption.AllDirectories, _CallerModuleName: ModuleName, _CallerMethodName: MethodName);

                if (client_request_files == null || client_request_files.Length == 0)
                {
                    if (timeout == TimeSpan.Zero) { Logging.LogExit(ModuleName);  return null; }
                    var time_left = timeout - sw1.Elapsed;
                    if (time_left > TimeSpan.Zero) await  Logging.WaitAsync(ct, 10 <= time_left.TotalSeconds ? 10 : (int)Math.Floor(time_left.TotalSeconds), 20 <= time_left.TotalSeconds ? 20 : (int)Math.Floor(time_left.TotalSeconds), ModuleName, MethodName).ConfigureAwait(false);
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
                            Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :(client_id, work_id, CmList);
                        })
                        .ToArray()
                    : client_request_files
                        .Select(a =>
                        {
                            var client_id = Path.GetDirectoryName(a).Split(new char[] { '\\', '/' }, StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
                            var work_id = Path.GetFileNameWithoutExtension(a).Split('_', StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
                            var CmList = cache_load.load_cache_file(ct, a, IndexDataContainer); //ConfusionMatrix.load(ct, a, as_parallel: as_parallel);
                            Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :(client_id, work_id, CmList);
                        })
                        .ToArray();

                for (var i = 0; i < client_request_files.Length; i++) { await io_proxy.delete_file(true, ct, client_request_files[i], _CallerModuleName: ModuleName, _CallerMethodName: MethodName).ConfigureAwait(false); }

                Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :client_requests;
            }

            Logging.LogExit(ModuleName); return null;
        }*/
    }
}