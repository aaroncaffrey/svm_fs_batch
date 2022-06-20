using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using SvmFsLib;

namespace SvmFsWkr
{
    public class SvmFsWkr
    {
        public const string ModuleName = nameof(SvmFsWkr);


        public static ProgramArgs ProgramArgs;

        public static async Task Main(string[] args)
        {
#if DEBUG
            System.Diagnostics.Debugger.Launch();
#endif

            // Parse command line parameters
            ProgramArgs = new ProgramArgs(args);

            await Main2(ProgramArgs).ConfigureAwait(false);

            ulong lvl = 0;
            Logging.LogEvent($"Reached end of {nameof(SvmFsWkr)}.{nameof(Main)}.", ModuleName);
            Logging.LogExit(ModuleName, lvl: lvl + 1);
        }

        public static async Task Main2(ProgramArgs programArgs)
        {

            if (programArgs != null)
            {
                SvmFsWkr.ProgramArgs = programArgs;
            }
            
            
            Logging.LogEvent($@"programArgs.InstanceId: {ProgramArgs.InstanceId}, programArgs.NodeArrayIndex: {ProgramArgs.NodeArrayIndex}, programArgs.TotalNodes: {ProgramArgs.TotalNodes}", ModuleName);

            var mainCts = new CancellationTokenSource();
            var mainCt = mainCts.Token;

            // Initialise
            Init.EnvInfo();
            Init.CloseNotifications(mainCt);
            Init.CheckX64();
            Init.SetGcMode();

            // Check all required parameters were specified
            var instanceId = ProgramArgs.InstanceId;
            var experimentName = ProgramArgs.ExperimentName;
            var resultsRootFolder = ProgramArgs.ResultsRootFolder;
            var dataSetDir = ProgramArgs.DataSetDir;
            var baseLineDataSetDir = ProgramArgs.BaseLineDataSetDir;
            var classNames = ProgramArgs.ClassNames;
            var libsvmTrainRuntime = ProgramArgs.LibsvmTrainRuntime;
            var libsvmPredictRuntime = ProgramArgs.LibsvmPredictRuntime;

            if (instanceId == -1) throw new ArgumentOutOfRangeException(nameof(instanceId));
            //if (iterationIndex == -1) throw new ArgumentOutOfRangeException(nameof(iterationIndex));
            if (string.IsNullOrEmpty(resultsRootFolder)) throw new Exception(nameof(resultsRootFolder));
            if (string.IsNullOrEmpty(experimentName)) throw new Exception(nameof(experimentName));
            if (string.IsNullOrEmpty(dataSetDir)) throw new Exception(nameof(dataSetDir));
            if (classNames == null || classNames.Length == 0) throw new Exception(nameof(classNames));
            if (string.IsNullOrEmpty(libsvmTrainRuntime)) throw new Exception(nameof(libsvmTrainRuntime));
            if (string.IsNullOrEmpty(libsvmPredictRuntime)) throw new Exception(nameof(libsvmPredictRuntime));

            // Run worker as threadpool task
            var wkrTask = Task.Run(async () => await Worker(instanceId, /*iterationIndex,*/ resultsRootFolder, experimentName, dataSetDir, baseLineDataSetDir, classNames, libsvmTrainRuntime, libsvmPredictRuntime, mainCt).ConfigureAwait(false));

            await Task.WhenAll(wkrTask).ConfigureAwait(false);
        }

        public static async Task<IndexData[]> LoadWork(int instanceId, /*int iterationIndex,*/ string resultsRootFolder, string experimentName)
        {
            if (instanceId == -1) throw new ArgumentOutOfRangeException(nameof(instanceId));
            //if (iterationIndex == -1) throw new ArgumentOutOfRangeException(nameof(iterationIndex));

            //var workQueueFolder = Path.Combine(CacheLoad.GetIterationFolder(resultsRootFolder, experimentName, iterationIndex), "work");
            var workQueueFolder = Path.Combine(CacheLoad.GetIterationFolder(resultsRootFolder, SvmFsWkr.ProgramArgs.ExperimentName), "work_queue");

            //var loadFilename = Path.Combine(workQueueFolder, $"work_{iterationIndex}_{instanceId}.csv");
            var workQueueInstanceFilename = Path.Combine(workQueueFolder, $"work_{instanceId}.csv"); // either work_{instanceId}, or read work.csv -> lines[instanceid] -> filename to read ...

            var lines = await IoProxy.ReadAllLinesAsync(true, default, workQueueInstanceFilename, 50);
            // skip first line because it is the csv header
            var indexDataList = lines.Skip(1).Select(a => new IndexData(new[] { lines[0], a })).ToArray();

            
            //if (indexDataList.Any(a => a.IdIterationIndex != iterationIndex || a.IdExperimentName != experimentName)) throw new Exception($"Wrong {nameof(experimentName)} value.");
            if (indexDataList.Any(a => !a.IdExperimentName.StartsWith(ProgramArgs.ExperimentName))) throw new Exception($"Wrong {nameof(experimentName)} value.");
            if (indexDataList.Select(a => a.IdIterationIndex).Distinct().Count() != 1) throw new Exception($"Wrong IdIterationIndex value.");

            Logging.LogEvent($@"Loaded {indexDataList.Length} work items from ""{workQueueInstanceFilename}"".");

            return indexDataList;
        }

        public static async Task<List<(IndexData id, ConfusionMatrix cm)>> LoadCache(int instanceId, /*int iterationIndex,*/ string resultsRootFolder, string experimentName, IndexData[] indexDataList, CancellationToken ct = default)
        {
            var cache = new List<(IndexData id, ConfusionMatrix cm)>();

           
            var iterationIndex = indexDataList[0].IdIterationIndex;
            

            var saveFolder = Path.Combine(CacheLoad.GetIterationFolder(resultsRootFolder, experimentName, iterationIndex, ct: ct), "cache");
            var saveFilename = Path.Combine(saveFolder, $"{iterationIndex}_{experimentName}.csv");

            try
            {
                if (File.Exists(saveFilename) && new FileInfo(saveFilename).Length > 0)
                {
                    var cacheFile = await CacheLoad.LoadCacheFileAsync(saveFilename, indexDataList, ct);
                    if ((cacheFile?.Length ?? 0) > 0 && cacheFile.All(a => a != default && a.id != default && a.cm != default))
                    {
                        cache.AddRange(cacheFile);
                    }
                }
            }
            catch (Exception e)
            {
                Logging.LogException(e, $@"[{instanceId}]", ModuleName);
            }


            return cache;
        }

        public static async Task Worker(int instanceId, /*int iterationIndex,*/ string resultsRootFolder, string experimentName, string dataSetDir, string baseLineDataSetDir, (int ClassId, string ClassName)[] classNames, string libsvmTrainRuntime, string libsvmPredictRuntime, CancellationToken ct)
        {
            //if (instanceId == -1)
            //{
            //    throw new ArgumentOutOfRangeException(nameof(instanceId));
            //}

            const bool asParallel = true;
            const bool save = true;

            // load work file for this instance id
            var indexDataList = await LoadWork(instanceId, /*iterationIndex,*/ resultsRootFolder, experimentName).ConfigureAwait(false);

            if ((indexDataList?.Length ?? 0) == 0)
            {
                Logging.LogEvent("Work list is empty. Exiting.");
                return;
            }

            if (indexDataList.Select(a => (a.IdIterationIndex, a.IdExperimentName)).Distinct().Count() != 1) throw new Exception();

            var iterationIndex = indexDataList[0].IdIterationIndex;

            var cache = await LoadCache(instanceId, /*iterationIndex,*/ resultsRootFolder, experimentName, indexDataList).ConfigureAwait(false);

            var (indexesLoaded, indexesNotLoaded) = CacheLoad.UpdateMissing(cache, indexDataList);

            if (!indexesNotLoaded.Any())
            {
                Logging.LogEvent("Work list is already complete. Exiting.");
                return;
            }


            var datasets = LoadDatasets(dataSetDir, baseLineDataSetDir, classNames, indexesLoaded, ct);

            var innerResultsTasks = asParallel
                       ? indexesNotLoaded.AsParallel().AsOrdered().Select(async (indexData, x) => await ProcessJob(instanceId, /*iterationIndex,*/ resultsRootFolder, experimentName, libsvmTrainRuntime, libsvmPredictRuntime, save, indexData, datasets, ct).ConfigureAwait(false)).ToArray()
                       : indexesNotLoaded.Select(async (indexData, x) => await ProcessJob(instanceId, /*iterationIndex,*/ resultsRootFolder, experimentName, libsvmTrainRuntime, libsvmPredictRuntime, save, indexData, datasets, ct).ConfigureAwait(false)).ToArray();

            try { await Task.WhenAll(innerResultsTasks).ConfigureAwait(false); }
            catch (Exception e) { Logging.LogException(e, $"[{instanceId}]"); }

            var successfulTasks = innerResultsTasks.Where(a => a.IsCompletedSuccessfully && a.Result != default && a.Result.Length > 0).ToArray();

            var successfulTaskResults = successfulTasks.SelectMany(a => a.Result).ToArray();
            successfulTaskResults = successfulTaskResults.Where(a => a != default && a.cm != default && a.id != default).ToArray();

            cache.AddRange(cache);

            await SaveAll(instanceId, iterationIndex, resultsRootFolder, experimentName, cache, ct).ConfigureAwait(false);


        }

        public static async Task SaveAll(int instanceId, int iterationIndex, string resultsRootFolder, string experimentName, List<(IndexData id, ConfusionMatrix cm)> cache, CancellationToken ct = default)
        {
            // save all results as single file

            Logging.LogEvent($"[Instance:{instanceId}] [Experiment:{experimentName}] [Iteration:{iterationIndex}] Tasks complete. Inner results: {(cache?.Count() ?? 0)} items. Ids: {string.Join(", ", cache.Select(a => $"{a.id.IdIterationIndex}:{a.id.IdJobUid}").Distinct().ToArray())}");


            var saveFolder   = Path.Combine(CacheLoad.GetIterationFolder(resultsRootFolder, experimentName, iterationIndex, ct: ct), "cache");
            var saveFilename = Path.Combine(saveFolder, $"{instanceId}_{iterationIndex}_{experimentName}.csv");
            await Save(instanceId, saveFilename, cache, ct);

        }

        public static (DataSet baseLineDataSet, DataSet dataSet) LoadDataset((int ClassId, string ClassName)[] ClassNames, string BaseLineDataSetDir, string[] BaseLineDataSetNames, string DataSetDir, string[] DataSetNames, CancellationToken ct = default)
        {
            DataSet baseLineDataSet = null;

            if ((BaseLineDataSetNames?.Length ?? 0) > 0)
            {
                baseLineDataSet = new DataSet();
                baseLineDataSet.LoadDataSet(BaseLineDataSetDir, BaseLineDataSetNames, ClassNames, ct);
            }

            var dataSet = new DataSet();
            dataSet.LoadDataSet(DataSetDir, DataSetNames, ClassNames, ct);

            return (baseLineDataSet, dataSet);
        }

        public static List<(string[] datasetFileTags, DataSet dataSet)> LoadDatasets(string dataSetDir, string baseLineDataSetDir, (int ClassId, string ClassName)[] classNames, IndexData[] indexDataList, CancellationToken ct = default)
        {

            if (string.IsNullOrEmpty(dataSetDir)) throw new Exception(nameof(dataSetDir));
            if (string.IsNullOrEmpty(baseLineDataSetDir) && indexDataList.Any(a => (a.IdBaseLineDatasetFileTags?.Length ?? 0) > 0 && (a.IdBaseLineColumnArrayIndexes?.Length ?? 0) > 0)) throw new Exception(nameof(baseLineDataSetDir));
            if (classNames == null || classNames.Length == 0) throw new Exception(nameof(classNames));


            var datasets = new List<(string[] datasetFileTags, DataSet dataSet)>();

            foreach (var x in indexDataList)
            {
                if (!datasets.Any(a => Enumerable.SequenceEqual(a.datasetFileTags, x.IdDatasetFileTags)))
                {
                    Logging.LogEvent($"Loading dataset: {string.Join(",", x.IdDatasetFileTags ?? Array.Empty<string>())}");

                    var dataSet = new DataSet();
                    dataSet.LoadDataSet(dataSetDir, x.IdDatasetFileTags, classNames, ct);
                    datasets.Add((x.IdDatasetFileTags, dataSet));
                }

                if (!datasets.Any(a => Enumerable.SequenceEqual(a.datasetFileTags, x.IdBaseLineDatasetFileTags)))
                {
                    Logging.LogEvent($"Loading baseline dataset: {string.Join(",", x.IdBaseLineDatasetFileTags ?? Array.Empty<string>())}");
                    
                    var baseLineDataSet = new DataSet();
                    baseLineDataSet.LoadDataSet(baseLineDataSetDir, x.IdBaseLineDatasetFileTags, classNames, ct);
                    datasets.Add((x.IdBaseLineDatasetFileTags, baseLineDataSet));
                }
            }

            return datasets;
        }

        public static async Task<(IndexData id, ConfusionMatrix cm)[]> ProcessJob(int instanceId, /*int iterationIndex,*/ string resultsRootFolder, string experimentName, string libsvmTrainRuntime, string libsvmPredictRuntime, bool save, IndexData indexData, List<(string[] datasetFileTags, DataSet dataSet)> datasets, CancellationToken ct)
        {

            try
            {
                if (indexData == default || ct.IsCancellationRequested)
                {
                    Logging.LogEvent($@"[{instanceId}] {nameof(ProcessJob)}(): Exiting:{(indexData == default ? " indexData was default. " : "")}{(ct.IsCancellationRequested ? " Cancellation requested. " : "")} IdExperimentName=[{indexData.IdExperimentName}] IdIterationIndex=[{indexData.IdIterationIndex}] IdJobUid=[{indexData.IdJobUid}], IdGroupArrayIndex=[{indexData.IdGroupArrayIndex}].");
                    return default;
                }

                var saveFolder = Path.Combine(CacheLoad.GetIterationFolder(resultsRootFolder, indexData.IdExperimentName, indexData.IdIterationIndex, ct: ct), "queue_cache");
                var jobSaveFilename = Path.Combine(saveFolder, $"{indexData.IdIterationIndex}_{indexData.IdGroupArrayIndex}_{indexData.IdJobUid}.csv");

                try
                {
                    if (File.Exists(jobSaveFilename) && new FileInfo(jobSaveFilename).Length > 0)
                    {
                        var cacheFile = await CacheLoad.LoadCacheFileAsync(jobSaveFilename, new[] { indexData }, ct).ConfigureAwait(false);
                        if ((cacheFile?.Length ?? 0) > 0 && cacheFile.All(a => a.id != default && a.cm != default))
                        {
                            Logging.LogEvent($@"[{instanceId}] {nameof(ProcessJob)}(): Completed job - already cached. IdExperimentName=[{indexData.IdExperimentName}] IdIterationIndex=[{indexData.IdIterationIndex}] IdJobUid=[{indexData.IdJobUid}], IdGroupArrayIndex=[{indexData.IdGroupArrayIndex}].");
                            return cacheFile;
                        }
                    }
                }
                catch (Exception e)
                {
                    Logging.LogException(e, $@"[{instanceId}]", ModuleName);
                }

                var dataSet = (indexData.IdDatasetFileTags?.Length ?? 0) > 0 ? datasets.FirstOrDefault(a => Enumerable.SequenceEqual(a.datasetFileTags, indexData.IdDatasetFileTags)).dataSet : default;
                var baseLineDataSet = (indexData.IdBaseLineDatasetFileTags?.Length ?? 0) > 0 ? datasets.FirstOrDefault(a => Enumerable.SequenceEqual(a.datasetFileTags, indexData.IdBaseLineDatasetFileTags)).dataSet : default;

                if (dataSet == null)
                {
                    Logging.LogEvent("Dataset not found: " + string.Join(", ", indexData.IdDatasetFileTags ?? Array.Empty<string>()));
                    //throw new Exception();
                    return default;
                }

                // MakeOuterCvInputs
                var mocvi = CrossValidate.MakeOuterCvInputs(baseLineDataSet, indexData.IdBaseLineColumnArrayIndexes, dataSet, indexData, ct: ct);
                if (mocvi == default || mocvi.outerCvInputs.Length == 0)
                {
                    Logging.LogEvent($@"[{instanceId}] {nameof(ProcessJob)}(): Exiting: {nameof(CrossValidate.MakeOuterCvInputs)}() returned default. IdExperimentName=[{indexData.IdExperimentName}] IdIterationIndex=[{indexData.IdIterationIndex}] IdJobUid=[{indexData.IdJobUid}], IdGroupArrayIndex=[{indexData.IdGroupArrayIndex}].");
                    return default;
                }

                // CrossValidatePerformanceAsync
                var ret = await CrossValidate.CrossValidatePerformanceAsync(libsvmTrainRuntime, libsvmPredictRuntime, mocvi.outerCvInputs, mocvi.mergedCvInput, indexData, ct: ct).ConfigureAwait(false);
                if (ret == default || ret.Length == 0 || ret.Any(a => a.id == default || a.cm == default))
                {
                    Logging.LogEvent($@"[{instanceId}] {nameof(ProcessJob)}(): Exiting: {nameof(CrossValidate.CrossValidatePerformanceAsync)}() returned default. IdExperimentName=[{indexData.IdExperimentName}] IdIterationIndex=[{indexData.IdIterationIndex}] IdJobUid=[{indexData.IdJobUid}], IdGroupArrayIndex=[{indexData.IdGroupArrayIndex}].");
                    return default;
                }

                Logging.LogEvent($@"[{instanceId}] {nameof(ProcessJob)}(): Completed job. IdExperimentName=[{indexData.IdExperimentName}] IdIterationIndex=[{indexData.IdIterationIndex}] IdJobUid=[{indexData.IdJobUid}], IdGroupArrayIndex=[{indexData.IdGroupArrayIndex}].");

                // Save
                if (save)
                {

                    await Save(instanceId, jobSaveFilename, ret, ct).ConfigureAwait(false);
                }

                return ret;
            }
            catch (Exception e)
            {
                Logging.LogException(e, $@"[{instanceId}]", ModuleName);
                return default;
            }
        }

        public static async Task Save(int instanceId, string saveFilename, IList<(IndexData id, ConfusionMatrix cm)> data, CancellationToken ct = default)
        {
            try
            {
                var cacheSaveLines = data.AsParallel().AsOrdered().Select(a => $@"{a.id?.CsvValuesString() ?? IndexData.Empty.CsvValuesString()},{a.cm.CsvValuesString() ?? ConfusionMatrix.Empty.CsvValuesString()}").ToList();
                cacheSaveLines.Insert(0, $@"{IndexData.CsvHeaderString},{ConfusionMatrix.CsvHeaderString}");
                await IoProxy.WriteAllLinesAsync(true, ct, saveFilename, cacheSaveLines, 50).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logging.LogException(e, $@"[{instanceId}] [{saveFilename}] [{(data?.Count ?? 0)}]", ModuleName);
                //throw;
            }
        }
    }
}
