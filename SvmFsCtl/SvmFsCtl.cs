using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SvmFsLib;


namespace SvmFsCtl
{
    public static class SvmFsCtl
    {
        public const string ModuleName = nameof(SvmFsCtl);

        public static ProgramArgs ProgramArgs;

        public static async Task Main(string[] args)
        {
#if DEBUG
            System.Diagnostics.Debugger.Launch();
#endif

            ulong lvl = 0;
            Logging.LogCall(ModuleName, lvl: lvl + 1);

            ProgramArgs = new ProgramArgs(args);

            await Main2(lvl, ProgramArgs).ConfigureAwait(false);
            Logging.LogEvent($"Reached end of {nameof(SvmFsCtl)}.{nameof(Main)}.", ModuleName);
            Logging.LogExit(ModuleName, lvl: lvl + 1);
        }

        public static async Task Main2(ulong lvl, ProgramArgs programArgs)
        {
            if (programArgs != null) {SvmFsCtl.ProgramArgs = programArgs;}

            var mainCts = new CancellationTokenSource();
            var mainCt = mainCts.Token;

            Init.EnvInfo();
            Init.CloseNotifications(mainCt);
            Init.CheckX64();
            Init.SetGcMode();

            var ctlTask = Task.Run(async () => await Controller(lvl, mainCt).ConfigureAwait(false));

            await Task.WhenAll(ctlTask).ConfigureAwait(false);
        }

        public static async Task Controller(ulong lvl = 0, CancellationToken ct = default)
        {
            // check experiment name is valid
            if (string.IsNullOrWhiteSpace(ProgramArgs.ExperimentName)) throw new ArgumentOutOfRangeException(nameof(ProgramArgs.ExperimentName), $"{nameof(ProgramArgs.ExperimentName)}: must specify experiment name");

            //// check whole array indexes are valid
            //if (ProgramArgs.WholeArrayIndexFirst <= -1) throw new ArgumentOutOfRangeException(nameof(ProgramArgs.WholeArrayIndexFirst), $@"{nameof(ProgramArgs.WholeArrayIndexFirst)} = {ProgramArgs.WholeArrayIndexFirst}");
            //if (ProgramArgs.WholeArrayIndexLast <= -1) throw new ArgumentOutOfRangeException(nameof(ProgramArgs.WholeArrayIndexLast), $@"{nameof(ProgramArgs.WholeArrayIndexLast)} = {ProgramArgs.WholeArrayIndexLast}");
            //if (ProgramArgs.WholeArrayStepSize <= 0) throw new ArgumentOutOfRangeException(nameof(ProgramArgs.WholeArrayStepSize), $@"{nameof(ProgramArgs.WholeArrayStepSize)} = {ProgramArgs.WholeArrayStepSize}");
            //if (ProgramArgs.WholeArrayLength <= 0) throw new ArgumentOutOfRangeException(nameof(ProgramArgs.WholeArrayLength), $@"{nameof(ProgramArgs.WholeArrayLength)} = {ProgramArgs.WholeArrayLength}");

            //// check partition array indexes are valid
            //if (!Routines.IsInRange(ProgramArgs.WholeArrayIndexFirst, ProgramArgs.WholeArrayIndexLast, ProgramArgs.PartitionArrayIndexFirst)) throw new ArgumentOutOfRangeException(nameof(ProgramArgs.PartitionArrayIndexFirst), $@"{nameof(ProgramArgs.PartitionArrayIndexFirst)} = {ProgramArgs.PartitionArrayIndexFirst}");
            //if (!Routines.IsInRange(ProgramArgs.WholeArrayIndexFirst, ProgramArgs.WholeArrayIndexLast, ProgramArgs.PartitionArrayIndexLast)) throw new ArgumentOutOfRangeException(nameof(ProgramArgs.PartitionArrayIndexLast), $@"{nameof(ProgramArgs.PartitionArrayIndexLast)} = {ProgramArgs.PartitionArrayIndexLast}");


            ////var InstanceId = GetInstanceId(program_args.whole_array_index_first, program_args.whole_array_index_last, program_args.whole_array_step_size, program_args.partition_array_index_first, program_args.partition_array_index_last);

            //var instanceId = Routines.ForIterations(ProgramArgs.WholeArrayIndexFirst, ProgramArgs.PartitionArrayIndexFirst, ProgramArgs.WholeArrayStepSize) - 1;
            //var totalInstance = Routines.ForIterations(ProgramArgs.WholeArrayIndexFirst, ProgramArgs.WholeArrayIndexLast, ProgramArgs.WholeArrayStepSize);

            //if (instanceId < 0) throw new ArgumentOutOfRangeException(nameof(instanceId), $@"{nameof(instanceId)} = {instanceId}");
            //if (totalInstance != ProgramArgs.WholeArrayLength) throw new ArgumentOutOfRangeException(nameof(ProgramArgs.WholeArrayLength), $@"{nameof(ProgramArgs.WholeArrayLength)} = {ProgramArgs.WholeArrayLength}, {nameof(totalInstance)} = {totalInstance}");


            //Logging.WriteLine($"Array job index: {instanceId} / {totalInstance}. Partition array Indexes: {ProgramArgs.PartitionArrayIndexFirst}..{ProgramArgs.PartitionArrayIndexLast}.  Whole array Indexes: {ProgramArgs.WholeArrayIndexFirst}..{ProgramArgs.WholeArrayIndexLast}:{ProgramArgs.WholeArrayStepSize} (length: {ProgramArgs.WholeArrayLength}).");



            



            DataSet baseLineDataSet = null;


            if ((ProgramArgs.BaseLineDataSetNames?.Length ?? 0) > 0)
            {
                baseLineDataSet = new DataSet();
                baseLineDataSet.LoadDataSet(ProgramArgs.BaseLineDataSetDir, ProgramArgs.BaseLineDataSetNames, ProgramArgs.ClassNames, ct);
            }

            var dataSet = new DataSet();
            dataSet.LoadDataSet(ProgramArgs.DataSetDir, ProgramArgs.DataSetNames, ProgramArgs.ClassNames, ct);

            if (baseLineDataSet != null)
            {
                if (!baseLineDataSet.ClassSizes.Select(a => (a.ClassId, a.ClassName, a.ClassSize, a.DownSampledClassSize)).SequenceEqual(dataSet.ClassSizes.Select(a => (a.ClassId, a.ClassName, a.ClassSize, a.DownSampledClassSize))))
                {
                    throw new Exception();
                }

                var x = dataSet.ValueList.SelectMany(a => a.ClassValueList.Select(b => b.RowColumns[0].RowColumnValue).ToArray()).ToArray();
                var y = baseLineDataSet.ValueList.SelectMany(a => a.ClassValueList.Select(b => b.RowColumns[0].RowColumnValue).ToArray()).ToArray();

                if (!x.SequenceEqual(y)) throw new Exception();
            }

            
            var fsControllerTask = Task.Run(async () => await FsController.FeatureSelectionInitializationAsync(
                    //ProgramArgs.InstanceId,
                    ProgramArgs.TotalNodes,
                    baseLineDataSet,
                    ProgramArgs.BaseLineDataSetColumnIndexes,
                    dataSet,
                    ProgramArgs.ScoringClassId,
                    ProgramArgs.ScoringMetrics,
                    ProgramArgs.ExperimentName,
                    //ProgramArgs.WholeArrayLength,
                    ProgramArgs.Repetitions,
                    ProgramArgs.OuterCvFolds,
                    ProgramArgs.OuterCvFoldsToRun,
                    ProgramArgs.InnerFolds,
                    ProgramArgs.SvmTypes,
                    ProgramArgs.Kernels,
                    ProgramArgs.Scales,
                    ProgramArgs.ClassWeights,
                    ProgramArgs.CalcElevenPointThresholds,
                    lvl: lvl + 1,
                    ct: ct

                    ).ConfigureAwait(false),
                ct);

            await Task.WhenAll(fsControllerTask).ConfigureAwait(false);
        }
    }
}