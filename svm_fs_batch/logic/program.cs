using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal static class Program
    {
        public const string ModuleName = nameof(Program);


        internal static ProgramArgs ProgramArgs;

        internal static async Task Main(string[] args)
        {
            //var id1 = new IndexData();
            //id1.IdExperimentName = "test";
            //id1.IdColumnArrayIndexes = new int[] { 1, 2, 3 };
            //id1.IdGroupArrayIndexes = new int[] { 7, 8, 9 };
            //id1.IdGroupKey = new DataSetGroupKey("file tag", "alpha", "stat", "dim", "cat", "src", "grp", "mem", "per", 10, 20);
            //var id2 = new IndexData(string.Join(",", id1.CsvValuesArray()));

            //var eq = IndexData.CompareReferenceData2(id1, id2);
            //return;


            //var rnd = new metrics_box();
            //rnd.set_cm(null, null, 20, 11, 126, 30);
            //Console.WriteLine(string.Join("\r\n", rnd.CsvValuesArray().Select((a, i) => $"{metrics_box.CsvHeaderValuesArray[i]} = '{a}'").ToArray()));
            //Console.WriteLine();

            //rnd.apply_imbalance_correction1();
            //Console.WriteLine(string.Join("\r\n", rnd.CsvValuesArray().Select((a, i) => $"{metrics_box.CsvHeaderValuesArray[i]} = '{a}'").ToArray()));
            //Console.WriteLine();

            //rnd.set_random_perf();
            //Console.WriteLine(string.Join("\r\n", rnd.CsvValuesArray().Select((a, i) => $"{metrics_box.CsvHeaderValuesArray[i]} = '{a}'").ToArray()));
            //Console.WriteLine();

            //const string methodName = nameof(Main);
            //-ExperimentName _20201028084510741 -job_id _ -job_name _ -instance_array_index_start 0 -array_instances 1 -array_start 0 -array_end 6929 -array_step 6930 -inner_folds 1 -outer_cv_folds 5 -outer_cv_folds_to_run 1 -repetitions 1
            //-ExperimentName test_20201025014739579 -job_id _ -job_name _ -array_index _ -array_instances _ -array_start 0 -array_end 6929 -array_step 385
            //var x=ConfusionMatrix.load($@"C:\mmfs1\data\scratch\k1040015\SvmFsBatch\results\test\it_5\x_it-5_gr-5_sv-1_kr-3_sc-2_rn-1_oc-10_ic-10_ix-1-5.cm.csv");
            // debug cmd line parameters: -ExperimentName test2 -array_start 0 -array_end 4 -array_index 0 -array_step 5 -array_instances 1 -array_last_index -1


            ThreadPool.GetMinThreads(out var minWorkerThreads, out var minCompletionPortThreads);
            ThreadPool.GetMaxThreads(out var maxWorkerThreads, out var maxCompletionPortThreads);

            Logging.WriteLine($@"Environment.CommandLine: {Environment.CommandLine}", ModuleName);
            Logging.WriteLine($@"Environment.ProcessorCount: {Environment.ProcessorCount}", ModuleName);
            Logging.WriteLine($@"GetMinThreads: minWorkerThreads = {minWorkerThreads}, minCompletionPortThreads = {minCompletionPortThreads}.", ModuleName);
            Logging.WriteLine($@"GetMaxThreads: maxWorkerThreads = {maxWorkerThreads}, maxCompletionPortThreads = {maxCompletionPortThreads}.", ModuleName);

            var mainCts = new CancellationTokenSource();
            var mainCt = mainCts.Token;

            Init.CloseNotifications(mainCt); //, mainCts);
            Init.CheckX64();
            Init.SetGcMode();
            //Init.SetThreadCounts();

            //var fake_args = $"-ExperimentName=test -whole_array_index_first=0 -whole_array_index_last=9 -whole_array_step_size=2 -whole_array_length=5 -partition_array_index_first=4 -partition_array_index_last=5";
            var fakeArgsList = new List<(string name, string value)>
            {
                (nameof(ProgramArgs.ExperimentName), "test"),
                (nameof(ProgramArgs.WholeArrayIndexFirst), "0"),
                (nameof(ProgramArgs.WholeArrayIndexLast), "0"),
                (nameof(ProgramArgs.WholeArrayStepSize), "1"),
                (nameof(ProgramArgs.WholeArrayLength), "1"),
                (nameof(ProgramArgs.PartitionArrayIndexFirst), "0"),
                (nameof(ProgramArgs.PartitionArrayIndexLast), "0"),

                (nameof(ProgramArgs.Client), "1"),
                (nameof(ProgramArgs.Server), "1"),

                (nameof(ProgramArgs.DataSetNames), "[1i.aaindex]"),

                (nameof(ProgramArgs.InnerFolds), "0"),
                (nameof(ProgramArgs.OuterCvFolds), "10"),
                (nameof(ProgramArgs.OuterCvFoldsToRun), "1"),
                (nameof(ProgramArgs.Repetitions), "1")
            };

            var fakeArgs = string.Join(" ", fakeArgsList.Select(a => $"-{a.name}={a.value}").ToArray());
            args = fakeArgs.Split();

            ProgramArgs = new ProgramArgs(args);

            if (ProgramArgs.Setup)
            {
                await Setup.SetupPbsJobAsync(ProgramArgs, mainCt).ConfigureAwait(false);
                return;
            }


            // check experiment name is valid
            if (string.IsNullOrWhiteSpace(ProgramArgs.ExperimentName)) throw new ArgumentOutOfRangeException(nameof(args), $"{nameof(ProgramArgs.ExperimentName)}: must specify experiment name");

            // check whole array indexes are valid
            if (ProgramArgs.WholeArrayIndexFirst <= -1) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.WholeArrayIndexFirst)} = {ProgramArgs.WholeArrayIndexFirst}");
            if (ProgramArgs.WholeArrayIndexLast <= -1) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.WholeArrayIndexLast)} = {ProgramArgs.WholeArrayIndexLast}");
            if (ProgramArgs.WholeArrayStepSize <= 0) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.WholeArrayStepSize)} = {ProgramArgs.WholeArrayStepSize}");
            if (ProgramArgs.WholeArrayLength <= 0) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.WholeArrayLength)} = {ProgramArgs.WholeArrayLength}");

            // check partition array indexes are valid
            if (!Routines.IsInRange(ProgramArgs.WholeArrayIndexFirst, ProgramArgs.WholeArrayIndexLast, ProgramArgs.PartitionArrayIndexFirst)) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.PartitionArrayIndexFirst)} = {ProgramArgs.PartitionArrayIndexFirst}");
            if (!Routines.IsInRange(ProgramArgs.WholeArrayIndexFirst, ProgramArgs.WholeArrayIndexLast, ProgramArgs.PartitionArrayIndexLast)) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.PartitionArrayIndexLast)} = {ProgramArgs.PartitionArrayIndexLast}");


            //var InstanceId = GetInstanceId(program_args.whole_array_index_first, program_args.whole_array_index_last, program_args.whole_array_step_size, program_args.partition_array_index_first, program_args.partition_array_index_last);

            var instanceId = Routines.ForIterations(ProgramArgs.WholeArrayIndexFirst, ProgramArgs.PartitionArrayIndexFirst, ProgramArgs.WholeArrayStepSize) - 1;
            var totalInstance = Routines.ForIterations(ProgramArgs.WholeArrayIndexFirst, ProgramArgs.WholeArrayIndexLast, ProgramArgs.WholeArrayStepSize);

            if (instanceId < 0) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(instanceId)} = {instanceId}");
            if (totalInstance != ProgramArgs.WholeArrayLength) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.WholeArrayLength)} = {ProgramArgs.WholeArrayLength}, {nameof(totalInstance)} = {totalInstance}");


            Logging.WriteLine($"Array job index: {instanceId} / {totalInstance}. Partition array indexes: {ProgramArgs.PartitionArrayIndexFirst}..{ProgramArgs.PartitionArrayIndexLast}.  Whole array indexes: {ProgramArgs.WholeArrayIndexFirst}..{ProgramArgs.WholeArrayIndexLast}:{ProgramArgs.WholeArrayStepSize} (length: {ProgramArgs.WholeArrayLength}).");

            // Load DataSet

            var DataSet = new DataSet();
            await DataSet.LoadDataSetAsync(ProgramArgs.DataSetDir, ProgramArgs.DataSetNames, ProgramArgs.ClassNames, mainCt).ConfigureAwait(false);

            var tasks = new List<Task>();
            //var threads = new List<Thread>();

            if ( /*InstanceId == 0 ||*/ ProgramArgs.Server)
            {
                //var fss = Task.Run(async () => await fs_server.feature_selection_initialization(
                var fsServerTask = Task.Run(async () => await FsServer.FeatureSelectionInitializationAsync(DataSet,
                        ProgramArgs.ScoringClassId,
                        ProgramArgs.ScoringMetrics,
                        ProgramArgs.ExperimentName,
                        instanceId,
                        ProgramArgs.WholeArrayLength,
                        //program_args.instance_array_index_start,
                        //program_args.array_step,
                        //program_args.instance_array_index_end,
                        ProgramArgs.Repetitions,
                        ProgramArgs.OuterCvFolds,
                        ProgramArgs.OuterCvFoldsToRun,
                        ProgramArgs.InnerFolds,
                        ProgramArgs.SvmTypes,
                        ProgramArgs.Kernels,
                        ProgramArgs.Scales,
                        ProgramArgs.ClassWeights,
                        ProgramArgs.CalcElevenPointThresholds,
                        ct: mainCt).ConfigureAwait(false),
                    mainCt);

                tasks.Add(fsServerTask);
                //threads.Add(fss);
            }

            if ( /*InstanceId != 0 ||*/ ProgramArgs.Client)
            {
                //var fsc = Task.Run(async () => await fs_client.x0_feature_selection_client_initialization
                var fsClientTask = Task.Run(async () => await FsClient.FeatureSelectionClientInitializationAsync(DataSet, ProgramArgs.ExperimentName, instanceId, ProgramArgs.WholeArrayLength, mainCt).ConfigureAwait(false), mainCt);

                tasks.Add(fsClientTask);
                //threads.Add(fsc);
            }

            if (tasks.Count > 0)
            {
                try { await Task.WhenAny(tasks).ConfigureAwait(false); }
                catch (Exception e) { Logging.LogException(e, ModuleName); }

                try
                {
                    Logging.LogEvent($"Cancelling {nameof(mainCts)}", ModuleName);
                    mainCts.Cancel();
                }
                catch (Exception e) { Logging.LogException(e, ModuleName); }

                try { await Task.WhenAll(tasks).ConfigureAwait(false); }
                catch (Exception e) { Logging.LogException(e, ModuleName); }
            }

            //if (threads.Count > 0)
            //{
            //    for (var i = 0; i < threads.Count; i++)
            //    {
            //        try{threads[i].Join();} catch (Exception e){ Logging.LogException( e, ModuleName); }
            //    }
            //}

            Logging.LogEvent($"Reached end of {nameof(Program)}.{nameof(Main)}...", ModuleName);
        }


        internal static string GetIterationFolder(string resultsRootFolder, string ExperimentName, int? iterationIndex = null, int? groupIndex = null, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            const bool hr = false;

            if (iterationIndex == null) return ct.IsCancellationRequested ? default :Path.Combine(resultsRootFolder, ExperimentName);
            if (groupIndex == null) return ct.IsCancellationRequested ? default :Path.Combine(resultsRootFolder, ExperimentName, $@"it_{iterationIndex + (hr ? 1 : 0)}");
            return ct.IsCancellationRequested ? default :Path.Combine(resultsRootFolder, ExperimentName, $@"it_{iterationIndex + (hr ? 1 : 0)}", $@"gr_{groupIndex + (hr ? 1 : 0)}");
        }


        internal static void UpdateMergedCm(DataSet DataSet, (Prediction[] prediction_list, ConfusionMatrix[] CmList) predictionFileData, IndexData unrolledIndexData, OuterCvInput mergedCvInput, (TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText)[] predictionDataList, bool asParallel = false, CancellationToken ct = default)
        {
            const string methodName = nameof(UpdateMergedCm);

            if (ct.IsCancellationRequested) return;

            if (predictionFileData.CmList == null) throw new ArgumentOutOfRangeException(nameof(predictionFileData), $@"{ModuleName}.{methodName}.{nameof(predictionFileData)}.{nameof(predictionFileData.CmList)}");

            if (asParallel)
                Parallel.ForEach(predictionFileData.CmList,
                    cm =>
                    {
                        UpdateMergedCmFromVector(predictionDataList, cm, ct);

                        UpdateMergedCmSingle(DataSet, unrolledIndexData, mergedCvInput, cm, ct);
                    });
            else
                foreach (var cm in predictionFileData.CmList)
                {
                    UpdateMergedCmFromVector(predictionDataList, cm, ct);

                    UpdateMergedCmSingle(DataSet, unrolledIndexData, mergedCvInput, cm, ct);
                }
        }

        private static void UpdateMergedCmFromVector((TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText)[] predictionDataList, ConfusionMatrix cm, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return;

            if (cm.GridPoint == null) cm.GridPoint = new GridPoint(predictionDataList?.Select(a => a.GridPoint).ToArray());

            if ((cm.XTimeGrid == null || cm.XTimeGrid == TimeSpan.Zero) && (predictionDataList?.Any(a => a.gridDur != null) ?? false)) cm.XTimeGrid = new TimeSpan(predictionDataList?.Select(a => a.gridDur?.Ticks ?? 0).DefaultIfEmpty(0).Sum() ?? 0);
            if ((cm.XTimeTrain == null || cm.XTimeTrain == TimeSpan.Zero) && (predictionDataList?.Any(a => a.trainDur != null) ?? false)) cm.XTimeTrain = new TimeSpan(predictionDataList?.Select(a => a.trainDur?.Ticks ?? 0).DefaultIfEmpty(0).Sum() ?? 0);
            if ((cm.XTimeTest == null || cm.XTimeTest == TimeSpan.Zero) && (predictionDataList?.Any(a => a.predictDur != null) ?? false)) cm.XTimeTest = new TimeSpan(predictionDataList?.Select(a => a.predictDur?.Ticks ?? 0).DefaultIfEmpty(0).Sum() ?? 0);
        }

        internal static void UpdateMergedCmSingle(DataSet DataSet, IndexData unrolledIndexData, OuterCvInput mergedCvInput, ConfusionMatrix cm, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return;

            cm.XClassName = ProgramArgs.ClassNames?.FirstOrDefault(b => cm.XClassId == b.ClassId).ClassName;
            cm.XClassSize = DataSet.ClassSizes?.First(b => b.ClassId == cm.XClassId).class_size ?? -1;
            cm.XClassTestSize = mergedCvInput.TestSizes?.First(b => b.ClassId == cm.XClassId).test_size ?? -1;
            cm.XClassTrainSize = mergedCvInput.TrainSizes?.First(b => b.ClassId == cm.XClassId).train_size ?? -1;
            cm.XClassWeight = unrolledIndexData.IdClassWeights?.FirstOrDefault(b => cm.XClassId == b.ClassId).ClassWeight;
            //cm.x_time_grid_search =  PredictionData_list?.Select(a => a.dur.gridDur).DefaultIfEmpty(0).Sum();
            //cm.x_time_test = PredictionData_list?.Select(a => a.dur.predictDur).DefaultIfEmpty(0).Sum();
            //cm.x_time_train = PredictionData_list?.Select(a => a.dur.trainDur).DefaultIfEmpty(0).Sum();
            cm.XOuterCvIndex = mergedCvInput.OuterCvIndex;
            cm.XRepetitionsIndex = mergedCvInput.RepetitionsIndex;
        }


        internal static string GetItemFilename(IndexData unrolledIndex, int repetitionIndex, int outerCvIndex, CancellationToken ct)
        {
            return ct.IsCancellationRequested ? default :$@"{GetIterationFilename(new[] {unrolledIndex},ct)}_ri[{repetitionIndex}]_oi[{outerCvIndex}]";
        }

        //internal static string GetIterationFilename(program_args pa)
        //{
        //    var id = new index_data()
        //    {
        //        id_ExperimentName = pa.ExperimentName,
        //        id_IterationIndex = -1,
        //        id_outer_cv_folds = pa.outer_cv_folds,
        //        id_outer_cv_folds_to_run = pa.outer_cv_folds_to_run,
        //        id_repetitions = pa.repetitions,
        //        id_calc_ElevenPoint_thresholds = pa.calc_ElevenPoint_thresholds,
        //    };
        //    return ct.IsCancellationRequested ? default :GetIterationFilename(new[] {id});
        //}

        internal static string GetIterationFilename(IndexData[] indexes, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            static string GetInitials(string name)
            {
                var initials = string.Join("", name.Replace("_", " ", StringComparison.Ordinal).Split().Where(a => a.Length > 0).Select(a => a.First()).ToList());
                return initials.Length > 2
                    ? /* initials.Substring(0, 2) */ $@"{initials.First()}{initials.Last()}"
                    : initials;
            }

            static string Reduce(string text, int max = 30)
            {
                return text != null && text.Length > max
                    ? $"{text.Substring(0, max / 3)}_{text.Substring(text.Length / 2 - (max / 3 - 2) / 2, max / 3 - 2)}_{text.Substring(text.Length - max / 3, max / 3)}"
                    : text;
            }

            var experimentGroupName = Reduce(string.Join(@"_", indexes.Select(a => a.IdExperimentName).Distinct().ToArray()));
            var iterationIndex = Reduce(Routines.FindRangesStr(indexes.Select(a => a.IdIterationIndex).ToList()));
            var groupIndex = Reduce(Routines.FindRangesStr(indexes.Select(a => a.IdGroupArrayIndex).ToList()));
            var totalGroups = Reduce(Routines.FindRangesStr(indexes.Select(a => a.IdTotalGroups).ToList()));
            var calcElevenPointThresholds = Reduce(Routines.FindRangesStr(indexes.Select(a => a.IdCalcElevenPointThresholds
                ? 1
                : 0).ToList()));
            var repetitions = Reduce(Routines.FindRangesStr(indexes.Select(a => a.IdRepetitions).ToList()));
            var outerCvFolds = Reduce(Routines.FindRangesStr(indexes.Select(a => a.IdOuterCvFolds).ToList()));
            var outerCvFoldsToRun = Reduce(Routines.FindRangesStr(indexes.Select(a => a.IdOuterCvFoldsToRun).ToList()));
            var classWeights = string.Join("_", indexes.Where(a => a.IdClassWeights != null).SelectMany(a => a.IdClassWeights).GroupBy(a => a.ClassId).Select(a => $@"{a.Key}_{Reduce(Routines.FindRangesStr(a.Select(b => (int) (b.ClassWeight * 100)).ToList()))}").ToList());
            var svmType = Reduce(Routines.FindRangesStr(indexes.Select(a => (int) a.IdSvmType).ToList()));
            var svmKernel = Reduce(Routines.FindRangesStr(indexes.Select(a => (int) a.IdSvmKernel).ToList()));
            var scaleFunction = Reduce(Routines.FindRangesStr(indexes.Select(a => (int) a.IdScaleFunction).ToList()));
            var innerCvFolds = Reduce(Routines.FindRangesStr(indexes.Select(a => a.IdInnerCvFolds).ToList()));

            var p = new List<(string name, string value)>
            {
                (GetInitials(nameof(experimentGroupName)), experimentGroupName), //it
                (GetInitials(nameof(iterationIndex)), iterationIndex), //it
                (GetInitials(nameof(groupIndex)), groupIndex), //gi
                (GetInitials(nameof(totalGroups)), totalGroups), //tg
                (GetInitials(nameof(calcElevenPointThresholds)), calcElevenPointThresholds), //ot
                (GetInitials(nameof(repetitions)), repetitions), //r
                (GetInitials(nameof(outerCvFolds)), outerCvFolds), //oc
                (GetInitials(nameof(outerCvFoldsToRun)), outerCvFoldsToRun), //oc
                (GetInitials(nameof(classWeights)), classWeights), //cw
                (GetInitials(nameof(svmType)), svmType), //st
                (GetInitials(nameof(svmKernel)), svmKernel), //sk
                (GetInitials(nameof(scaleFunction)), scaleFunction), //sf
                (GetInitials(nameof(innerCvFolds)), innerCvFolds) //ic
            };

            var iterFn = string.Join(@"_", p.Select(a => $@"{a.name}[{a.value ?? ""}]").ToList());

            const string fnChars = @"0123456789[]{}()_+-.;qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";

            if (!iterFn.All(a => fnChars.Contains(a))) throw new Exception();

            return ct.IsCancellationRequested ? default :iterFn;
        }


        internal enum Direction
        {
            None, Forwards, Neutral,
            Backwards
        }
    }
}