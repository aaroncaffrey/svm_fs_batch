using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace SvmFsLib
{
    public class ProgramArgs
    {
        public const string ModuleName = nameof(ProgramArgs);

        public static string[] CsvHeaderValuesArray =
        { 
            nameof(LaunchMethod),
            //nameof(WorkListInputFile),
            //nameof(WorkListOutputFile),
            nameof(NodeArrayIndex),
            nameof(TotalNodes),
            nameof(ProcessorsPerNode),

             nameof(Option0),
             nameof(Option1),
             nameof(Option2),
             nameof(Option3),
             nameof(Option4),
             nameof(InstanceId),
             nameof(IterationIndex),
             nameof(Folds),
             nameof(Repetitions),
             nameof(OuterCvFolds),
             nameof(OuterCvFoldsToRun),
             nameof(InnerFolds),
             nameof(ExperimentName),
             nameof(JobId),
             nameof(JobName),
             //nameof(WholeArrayIndexFirst),
             //nameof(WholeArrayIndexLast),
             //nameof(WholeArrayStepSize),
             //nameof(WholeArrayLength),
             //nameof(PartitionArrayIndexFirst),
             //nameof(PartitionArrayIndexLast),
             //nameof(Setup),
             //nameof(SetupTotalVcpus),
             //nameof(SetupInstanceVcpus),
             nameof(ScoringClassId),
             nameof(ScoringMetrics),
             nameof(ResultsRootFolder),
             nameof(LibsvmPredictRuntime),
             nameof(LibsvmTrainRuntime),
             nameof(ClassNames),
             nameof(ClassWeights),
             nameof(SvmTypes),
             nameof(Kernels),
             nameof(Scales),
             nameof(CalcElevenPointThresholds),
             nameof(DataSetDir),
             nameof(BaseLineDataSetDir),
             nameof(DataSetNames),
             nameof(BaseLineDataSetNames),
             nameof(BaseLineDataSetColumnIndexes),
             nameof(NegativeClassId),
             nameof(PositiveClassId),
             nameof(NegativeClassName),
             nameof(PositiveClassName)
        };


        public (string key, string asStr, int? asInt, double? asDouble, bool? asBool)[] Args;

        public string LaunchMethod = ProgramArgsEnv.LaunchMethod;

        //public string WorkListInputFile = ProgramArgsEnv.WorkListInputFile;
        //public string WorkListOutputFile = ProgramArgsEnv.WorkListOutputFile;

        public int TotalNodes = ProgramArgsEnv.TotalNodes;
        public int ProcessorsPerNode = ProgramArgsEnv.ProcessorsPerNode;
        public int NodeArrayIndex = ProgramArgsEnv.NodeIndex; // The Array Index (array size can be different from total nodes, in which case, each node is assigned work indexes of X..X+(step-1))
        public int InstanceId = ProgramArgsEnv.InstanceId; // the Instance Index


        public int IterationIndex = ProgramArgsEnv.IterationIndex;
        

        public int Option0 = ProgramArgsEnv.Option0;
        public int Option1 = ProgramArgsEnv.Option1;
        public int Option2 = ProgramArgsEnv.Option2;
        public int Option3 = ProgramArgsEnv.Option3;
        public int Option4 = ProgramArgsEnv.Option4;

        public bool CalcElevenPointThresholds = ProgramArgsEnv.CalcElevenPointThresholds;

        //public string ProtocolKey = "232e91fb28044ef483a65ff45a6b0f95";

        public (int ClassId, string ClassName)[] ClassNames = ProgramArgsEnv.ClassNames;
        public (int ClassId, double ClassWeight)[][] ClassWeights = ProgramArgsEnv.ClassWeights;

        //public bool Client;
        //public int ClientConnectionPoolSize = 10;
        //public Guid ClientGuid = Guid.NewGuid();


        public string PathSvmFsLdr = ProgramArgsEnv.PathSvmFsLdr;
        public string PathSvmFsCtl = ProgramArgsEnv.PathSvmFsCtl;
        public string PathSvmFsWkr = ProgramArgsEnv.PathSvmFsWkr;

        public string DataSetDir = ProgramArgsEnv.DataSetDir;
        public string BaseLineDataSetDir = ProgramArgsEnv.BaseLineDataSetDir;
        public string[] DataSetNames = ProgramArgsEnv.DataSetNames;
        public string[] BaseLineDataSetNames = ProgramArgsEnv.BaseLineDataSetNames;
        public int[] BaseLineDataSetColumnIndexes = ProgramArgsEnv.BaseLineDataSetColumnIndexes;

        //public bool run_local = false;
        public string ExperimentName = ProgramArgsEnv.ExperimentName;

        // note: use 'folds' to set repetitions, outer_cv_folds and inner_folds to the same value
        public int? Folds = ProgramArgsEnv.Folds;
        public int InnerFolds = ProgramArgsEnv.InnerFolds;
        //public bool IsUnix = !RuntimeInformation.IsOSPlatform(OSPlatform.Windows); //RuntimeInformation.IsOSPlatform(OSPlatform.Linux) || RuntimeInformation.IsOSPlatform(OSPlatform.FreeBSD) || RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

        //public bool IsWin = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

        public string JobId = ProgramArgsEnv.JobId;
        public string JobName = ProgramArgsEnv.JobName;
        public Libsvm.LibsvmKernelType[] Kernels = ProgramArgsEnv.Kernels;
        public string LibsvmPredictRuntime = ProgramArgsEnv.LibsvmPredictRuntime;
        public string LibsvmTrainRuntime = ProgramArgsEnv.LibsvmTrainRuntime;

        public int NegativeClassId = ProgramArgsEnv.NegativeClassId;
        public string NegativeClassName = ProgramArgsEnv.NegativeClassName;
        public int OuterCvFolds = ProgramArgsEnv.OuterCvFolds;
        public int OuterCvFoldsToRun = ProgramArgsEnv.OuterCvFoldsToRun;

        // partition array parameters (pbs only provides partition_array_index_first, whole_array_step_size is given by the script, and partition_array_index_last is calculated from it, if not provided)
        //public int PartitionArrayIndexFirst = -1; // start index for current instance
        //public int PartitionArrayIndexLast = -1; // last index for current instance
        public int PositiveClassId = ProgramArgsEnv.PositiveClassId;
        public string PositiveClassName = ProgramArgsEnv.PositiveClassName;

        public int Repetitions = ProgramArgsEnv.Repetitions;
        public string ResultsRootFolder = ProgramArgsEnv.ResultsRootFolder;
        public Scaling.ScaleFunction[] Scales = ProgramArgsEnv.Scales;

        public int ScoringClassId = ProgramArgsEnv.ScoringClassId;

        public string[] ScoringMetrics = ProgramArgsEnv.ScoringMetrics;

        //public bool Server = true;
        //public int ServerBacklog = 1000;
        //public Guid ServerGuid = new Guid("d2d95e16-d086-4b58-ba29-8b9b84e41be5");

        //public string ServerIp = "127.0.0.1";
        //public int ServerPort = 64727;

        // parameters for -setup only to generate pbs script to run array job
        //public bool Setup;
        //public int SetupInstanceVcpus = -1;
        //public int SetupTotalVcpus = -1;
        //public string SvmFsBatchHome=RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\mmfs1\data\scratch\k1040015\{nameof(SvmFsBatch)}" : @"/mmfs1/data/scratch/k1040015/{nameof(SvmFsBatch)}";

        public Libsvm.LibsvmSvmType[] SvmTypes = ProgramArgsEnv.SvmTypes;
        //public string UserHome = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\home\k1040015" : @"/home/k1040015";


        // whole array parameters
        //public int WholeArrayIndexFirst = -1; // start index of whole array
        //public int WholeArrayIndexLast = -1; // end index of whole array
        //public int WholeArrayLength; // number of jobs in array
        //public int WholeArrayStepSize; // step size for whole array

        public ProgramArgs()
        {

        }

        public ProgramArgs(ProgramArgs programArgs)
        {
            LaunchMethod = programArgs.LaunchMethod;
            ClassWeights = programArgs.ClassWeights?.Select(a => a?.ToArray()).ToArray();
            ClassNames = programArgs.ClassNames?.ToArray();
            CalcElevenPointThresholds = programArgs.CalcElevenPointThresholds;
            InnerFolds = programArgs.InnerFolds;
            NegativeClassId = programArgs.NegativeClassId;
            OuterCvFolds = programArgs.OuterCvFolds;
            OuterCvFoldsToRun = programArgs.OuterCvFoldsToRun;
            PositiveClassId = programArgs.PositiveClassId;
            Repetitions = programArgs.Repetitions;
            ScoringClassId = programArgs.ScoringClassId;
            Folds = programArgs.Folds;
            BaseLineDataSetColumnIndexes = programArgs.BaseLineDataSetColumnIndexes?.ToArray();
            Kernels = programArgs.Kernels?.ToArray();
            SvmTypes = programArgs.SvmTypes?.ToArray();
            Args = programArgs.Args?.ToArray();
            InstanceId = programArgs.InstanceId;
            IterationIndex = programArgs.IterationIndex;
            NodeArrayIndex = programArgs.NodeArrayIndex;
            Option0 = programArgs.Option0;
            Option1 = programArgs.Option1;
            Option2 = programArgs.Option2;
            Option3 = programArgs.Option3;
            Option4 = programArgs.Option4;
            ProcessorsPerNode = programArgs.ProcessorsPerNode;
            TotalNodes = programArgs.TotalNodes;
            //WorkListInputFile = programArgs.WorkListInputFile;
            //WorkListOutputFile = programArgs.WorkListOutputFile;
            Scales = programArgs.Scales?.ToArray();
            PathSvmFsCtl = programArgs.PathSvmFsCtl;
            PathSvmFsLdr = programArgs.PathSvmFsLdr;
            PathSvmFsWkr = programArgs.PathSvmFsWkr;
            BaseLineDataSetDir = programArgs.BaseLineDataSetDir;
            DataSetDir = programArgs.DataSetDir;
            ExperimentName = programArgs.ExperimentName;
            JobId = programArgs.JobId;
            JobName = programArgs.JobName;
            LibsvmPredictRuntime = programArgs.LibsvmPredictRuntime;
            LibsvmTrainRuntime = programArgs.LibsvmTrainRuntime;
            NegativeClassName = programArgs.NegativeClassName;
            PositiveClassName = programArgs.PositiveClassName;
            ResultsRootFolder = programArgs.ResultsRootFolder;
            BaseLineDataSetNames = programArgs.BaseLineDataSetNames?.ToArray();
            DataSetNames = programArgs.DataSetNames?.ToArray();
            ScoringMetrics = programArgs.ScoringMetrics?.ToArray();
        }

        public ProgramArgs(string[] args)
        {
            Logging.LogCall(ModuleName);

            const string methodName = nameof(ProgramArgs);


            var argNames = CsvHeaderValuesArray;

            Args = GetParams(args);
            var argsGiven = Args.Select(a => a.key).ToArray();
            var argsMissing = argNames.Except(argsGiven, StringComparer.OrdinalIgnoreCase).ToArray();
            var argsKnown = argsGiven.Intersect(argNames, StringComparer.OrdinalIgnoreCase).ToArray();
            var argsUnknown = argsGiven.Except(argNames, StringComparer.OrdinalIgnoreCase).ToArray();
            var argsCount = argsGiven.Distinct().Select(a => (key: a, count: Args.Count(b => string.Equals(a, b.key, StringComparison.OrdinalIgnoreCase)))).ToArray();

            Logging.WriteLine($@"{nameof(argsGiven)} = {string.Join(", ", argsGiven)}", ModuleName, methodName);
            Logging.WriteLine($@"{nameof(argsMissing)} = {string.Join(", ", argsMissing)}", ModuleName, methodName);
            Logging.WriteLine($@"{nameof(argsKnown)} = {string.Join(", ", argsKnown)}", ModuleName, methodName);
            Logging.WriteLine($@"{nameof(argsUnknown)} = {string.Join(", ", argsUnknown)}", ModuleName, methodName);

            if (argsUnknown.Any()) throw new ArgumentOutOfRangeException(nameof(args), $@"{ModuleName}.{methodName}: Invalid arguments: {string.Join(", ", argsUnknown)}");
            if (argsCount.Any(a => a.count > 1)) throw new ArgumentOutOfRangeException(nameof(args), $@"{ModuleName}.{methodName}: Arguments specified more than once: {string.Join(", ", argsCount.Where(a => a.count > 1).ToArray())}");

            

            if (ArgsKeyExists(nameof(LaunchMethod))) LaunchMethod = ArgsValue(nameof(LaunchMethod)).asStr;

            //if (ArgsKeyExists(nameof(WorkListInputFile))) WorkListInputFile = ArgsValue(nameof(WorkListInputFile)).asStr;
            //if (ArgsKeyExists(nameof(WorkListOutputFile))) WorkListOutputFile = ArgsValue(nameof(WorkListOutputFile)).asStr;


            if (ArgsKeyExists(nameof(NodeArrayIndex))) NodeArrayIndex = ArgsValue(nameof(NodeArrayIndex)).asInt ?? -1;
            if (ArgsKeyExists(nameof(TotalNodes))) TotalNodes = ArgsValue(nameof(TotalNodes)).asInt ?? -1;
            if (ArgsKeyExists(nameof(ProcessorsPerNode))) ProcessorsPerNode = ArgsValue(nameof(ProcessorsPerNode)).asInt ?? -1;



            if (ArgsKeyExists(nameof(Option0))) Option0 = ArgsValue(nameof(Option0)).asInt ?? -1;
            if (ArgsKeyExists(nameof(Option1))) Option1 = ArgsValue(nameof(Option1)).asInt ?? -1;
            if (ArgsKeyExists(nameof(Option2))) Option2 = ArgsValue(nameof(Option2)).asInt ?? -1;
            if (ArgsKeyExists(nameof(Option3))) Option3 = ArgsValue(nameof(Option3)).asInt ?? -1;
            if (ArgsKeyExists(nameof(Option4))) Option4 = ArgsValue(nameof(Option4)).asInt ?? -1;

            if (ArgsKeyExists(nameof(InstanceId))) Option4 = ArgsValue(nameof(InstanceId)).asInt ?? -1;
            if (ArgsKeyExists(nameof(IterationIndex))) Option4 = ArgsValue(nameof(IterationIndex)).asInt ?? -1;



            if (ArgsKeyExists(nameof(ExperimentName))) ExperimentName = ArgsValue(nameof(ExperimentName)).asStr;
            if (ArgsKeyExists(nameof(JobId))) JobId = ArgsValue(nameof(JobId)).asStr;
            if (ArgsKeyExists(nameof(JobName))) JobName = ArgsValue(nameof(JobName)).asStr;

            //if (args_key_exists(nameof(run_local))) run_local = ArgsValue(nameof(run_local)).asBool ?? default;
            //if (ArgsKeyExists(nameof(Setup))) Setup = ArgsValue(nameof(Setup)).asBool ?? default;

            if (ArgsKeyExists(nameof(Repetitions))) Repetitions = ArgsValue(nameof(Repetitions)).asInt ?? -1;
            if (ArgsKeyExists(nameof(OuterCvFolds))) OuterCvFolds = ArgsValue(nameof(OuterCvFolds)).asInt ?? -1;
            if (ArgsKeyExists(nameof(OuterCvFoldsToRun))) OuterCvFoldsToRun = ArgsValue(nameof(OuterCvFoldsToRun)).asInt ?? -1;
            if (ArgsKeyExists(nameof(InnerFolds))) InnerFolds = ArgsValue(nameof(InnerFolds)).asInt ?? -1;

            //if (ArgsKeyExists(nameof(PartitionArrayIndexFirst))) PartitionArrayIndexFirst = ArgsValue(nameof(PartitionArrayIndexFirst)).asInt ?? -1;
            //if (ArgsKeyExists(nameof(PartitionArrayIndexLast))) PartitionArrayIndexLast = ArgsValue(nameof(PartitionArrayIndexLast)).asInt ?? -1;

            //if (ArgsKeyExists(nameof(WholeArrayLength))) WholeArrayLength = ArgsValue(nameof(WholeArrayLength)).asInt ?? 0;
            //if (ArgsKeyExists(nameof(WholeArrayStepSize))) WholeArrayStepSize = ArgsValue(nameof(WholeArrayStepSize)).asInt ?? 0;
            //if (ArgsKeyExists(nameof(WholeArrayIndexFirst))) WholeArrayIndexFirst = ArgsValue(nameof(WholeArrayIndexFirst)).asInt ?? -1;
            //if (ArgsKeyExists(nameof(WholeArrayIndexLast))) WholeArrayIndexLast = ArgsValue(nameof(WholeArrayIndexLast)).asInt ?? -1;

            //if (ArgsKeyExists(nameof(SetupTotalVcpus))) SetupTotalVcpus = ArgsValue(nameof(SetupTotalVcpus)).asInt ?? -1;
            //if (ArgsKeyExists(nameof(SetupInstanceVcpus))) SetupInstanceVcpus = ArgsValue(nameof(SetupInstanceVcpus)).asInt ?? -1;



            if (ArgsKeyExists(nameof(ResultsRootFolder))) ResultsRootFolder = ArgsValue(nameof(ResultsRootFolder)).asStr;
            if (ArgsKeyExists(nameof(LibsvmPredictRuntime))) LibsvmPredictRuntime = ArgsValue(nameof(LibsvmPredictRuntime)).asStr;
            if (ArgsKeyExists(nameof(LibsvmTrainRuntime))) LibsvmTrainRuntime = ArgsValue(nameof(LibsvmTrainRuntime)).asStr;


            if (ArgsKeyExists(nameof(DataSetDir))) DataSetDir = ArgsValue(nameof(DataSetDir)).asStr;
            if (ArgsKeyExists(nameof(BaseLineDataSetDir))) BaseLineDataSetDir = ArgsValue(nameof(BaseLineDataSetDir)).asStr;
            if (ArgsKeyExists(nameof(DataSetNames))) DataSetNames = ArgsValue(nameof(DataSetNames)).asStr.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries);

            if (ArgsKeyExists(nameof(BaseLineDataSetNames))) BaseLineDataSetNames = ArgsValue(nameof(BaseLineDataSetNames)).asStr.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries);
            if (ArgsKeyExists(nameof(BaseLineDataSetColumnIndexes))) BaseLineDataSetColumnIndexes = ArgsValue(nameof(BaseLineDataSetColumnIndexes)).asStr.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries).Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();

            if ((BaseLineDataSetNames?.Length ?? 0) == 0 && (BaseLineDataSetColumnIndexes?.Length ?? 0) != 0) throw new Exception();

            if (ArgsKeyExists(nameof(NegativeClassId))) NegativeClassId = ArgsValue(nameof(NegativeClassId)).asInt ?? throw new ArgumentNullException(nameof(args));
            if (ArgsKeyExists(nameof(PositiveClassId))) PositiveClassId = ArgsValue(nameof(PositiveClassId)).asInt ?? throw new ArgumentNullException(nameof(args));
            if (ArgsKeyExists(nameof(NegativeClassName))) NegativeClassName = ArgsValue(nameof(NegativeClassName)).asStr;
            if (ArgsKeyExists(nameof(PositiveClassName))) PositiveClassName = ArgsValue(nameof(PositiveClassName)).asStr;

            if (ArgsKeyExists(nameof(ScoringClassId))) ScoringClassId = ArgsValue(nameof(ScoringClassId)).asInt ?? throw new ArgumentNullException(nameof(args));
            if (ArgsKeyExists(nameof(ScoringMetrics))) ScoringMetrics = ArgsValue(nameof(ScoringMetrics)).asStr.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries);

            if (ArgsKeyExists(nameof(SvmTypes))) SvmTypes = ArgsValue(nameof(SvmTypes)).asStr.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries).Select(a => Enum.Parse<Libsvm.LibsvmSvmType>(a, true)).ToArray();
            if (ArgsKeyExists(nameof(Kernels))) Kernels = ArgsValue(nameof(Kernels)).asStr.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries).Select(a => Enum.Parse<Libsvm.LibsvmKernelType>(a, true)).ToArray();
            if (ArgsKeyExists(nameof(Scales))) Scales = ArgsValue(nameof(Scales)).asStr.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries).Select(a => Enum.Parse<Scaling.ScaleFunction>(a, true)).ToArray();
            if (ArgsKeyExists(nameof(CalcElevenPointThresholds))) CalcElevenPointThresholds = ArgsValue(nameof(CalcElevenPointThresholds)).asBool ?? default;


            if (ArgsKeyExists(nameof(ClassNames)))
                ClassNames = ArgsValue(nameof(ClassNames)).asStr.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries).Select(a =>
                {
                    var x = a.Split(':');
                    return (ClassId: int.Parse(x[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo), ClassName: x[1].Trim());
                }).ToArray();
            else
                ClassNames = new[]
                {
                 (NegativeClassId, NegativeClassName),
                 (PositiveClassId, PositiveClassName)
                 };

            // ClassWeights=+1:0.1,-1:0.9;+1:0.1,-1:0.9;
            if (ArgsKeyExists(nameof(ClassWeights)))
                ClassWeights = ArgsValue(nameof(ClassWeights)).asStr.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries).Select(a => a.Split(',').Select(b =>
                {
                    var y = b.Split(':');
                    return (ClassId: int.Parse(y[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo), ClassWeight: double.Parse(y[1], NumberStyles.Float, NumberFormatInfo.InvariantInfo));
                }).ToArray()).ToArray();

            if (ArgsKeyExists(nameof(Folds)))
            {
                Folds = ArgsValue(nameof(Folds)).asInt ?? -1;

                if (Folds != null && Folds >= 2)
                {
                    InnerFolds = Folds.Value;
                    OuterCvFolds = Folds.Value;
                    OuterCvFoldsToRun = Folds.Value;
                    Repetitions = Folds.Value;
                }
            }
            else if (new[] { InnerFolds, OuterCvFolds, OuterCvFoldsToRun, Repetitions }.Distinct().Count() == 1) { Folds = InnerFolds; }

            //if (PartitionArrayIndexLast <= -1) PartitionArrayIndexLast = PartitionArrayIndexFirst + (WholeArrayStepSize - 1);


            var v = CsvValuesArray();
            var vHeaders = v.Select(a => a.key).ToArray();
            if (!Enumerable.SequenceEqual(vHeaders, CsvHeaderValuesArray)) throw new Exception();

            Logging.WriteLine(string.Join(" ", v?.Select(a => $"-{a.key}={a.value}").ToArray() ?? Array.Empty<string>()), ModuleName, methodName);
        }

        public (string key, string value)[] CsvValuesArray()
        {
            Logging.LogCall(ModuleName);

            var ret = new (string key, string value)[]
            {
                
                 (nameof(LaunchMethod),$"{LaunchMethod}"),

                 //(nameof(WorkListInputFile),$"{WorkListInputFile}"),
                 //(nameof(WorkListOutputFile),$"{WorkListOutputFile}"),

                (nameof(NodeArrayIndex),$"{NodeArrayIndex}"),
                 (nameof(TotalNodes),$"{TotalNodes}"),
                 (nameof(ProcessorsPerNode),$"{ProcessorsPerNode}"),

                 (nameof(Option0), $"{Option0}"),
                 (nameof(Option1), $"{Option1}"),
                 (nameof(Option2), $"{Option2}"),
                 (nameof(Option3), $"{Option3}"),
                 (nameof(Option4), $"{Option4}"),
                 (nameof(InstanceId), $"{InstanceId}"),
                 (nameof(IterationIndex), $"{IterationIndex}"),
                 (nameof(Folds), $"{Folds}"),
                 (nameof(Repetitions), $"{Repetitions}"),
                 (nameof(OuterCvFolds), $"{OuterCvFolds}"),
                 (nameof(OuterCvFoldsToRun), $"{OuterCvFoldsToRun}"),
                 (nameof(InnerFolds), $"{InnerFolds}"),
                 (nameof(ExperimentName), $"{ExperimentName}"),
                 (nameof(JobId), $"{JobId}"),
                 (nameof(JobName), $"{JobName}"),
                 //(nameof(WholeArrayIndexFirst), $"{WholeArrayIndexFirst}"),
                 //(nameof(WholeArrayIndexLast), $"{WholeArrayIndexLast}"),
                 //(nameof(WholeArrayStepSize), $"{WholeArrayStepSize}"),
                 //(nameof(WholeArrayLength), $"{WholeArrayLength}"),
                 //(nameof(PartitionArrayIndexFirst), $"{PartitionArrayIndexFirst}"),
                 //(nameof(PartitionArrayIndexLast), $"{PartitionArrayIndexLast}"),
                 //(nameof(Setup), $"{Setup}"),
                 //(nameof(SetupTotalVcpus), $"{SetupTotalVcpus}"),
                 //(nameof(SetupInstanceVcpus), $"{SetupInstanceVcpus}"),
                 (nameof(ScoringClassId), $"{ScoringClassId}"),
                 (nameof(ScoringMetrics), $"{string.Join(",", ScoringMetrics ?? Array.Empty<string>())}"),
                 (nameof(ResultsRootFolder), $"{ResultsRootFolder}"),
                 (nameof(LibsvmPredictRuntime), $"{LibsvmPredictRuntime}"),
                 (nameof(LibsvmTrainRuntime), $"{LibsvmTrainRuntime}"),
                 (nameof(ClassNames), $"{string.Join(",", ClassNames?.Select(a => $"{a.ClassId}:{a.ClassName}").ToArray() ?? Array.Empty<string>())}"), // ClassNames=+1:dimorphic;-1:coil; 
                 (nameof(ClassWeights), $"{string.Join(";", ClassWeights?.Select(a => string.Join(",", a?.Select(b => $"{b.ClassId}:{b.ClassWeight}").ToArray() ?? Array.Empty<string>())).ToArray() ?? Array.Empty<string>())}"),
                 (nameof(SvmTypes), $"{string.Join(",", SvmTypes ?? Array.Empty<Libsvm.LibsvmSvmType>())}"),
                 (nameof(Kernels), $"{string.Join(",", Kernels ?? Array.Empty<Libsvm.LibsvmKernelType>())}"),
                 (nameof(Scales), $"{string.Join(",", Scales ?? Array.Empty<Scaling.ScaleFunction>())}"),
                 (nameof(CalcElevenPointThresholds), $"{(CalcElevenPointThresholds ? 1 : 0)}"),
                 (nameof(DataSetDir), $"{DataSetDir}"),
                 (nameof(BaseLineDataSetDir), $"{BaseLineDataSetDir}"),
                 (nameof(DataSetNames), $"{string.Join(";", DataSetNames ?? Array.Empty<string>())}"),
                 (nameof(BaseLineDataSetNames), $"{string.Join(";", BaseLineDataSetNames ?? Array.Empty<string>())}"),
                 (nameof(BaseLineDataSetColumnIndexes), $"{string.Join(";", BaseLineDataSetColumnIndexes ?? Array.Empty<int>())}"),
                 (nameof(NegativeClassId), $"{NegativeClassId}"),
                 (nameof(PositiveClassId), $"{PositiveClassId}"),
                 (nameof(NegativeClassName), $"{NegativeClassName}"),
                 (nameof(PositiveClassName), $"{PositiveClassName}")
                 };

            Logging.LogExit(ModuleName);
            return ret;
        }

        public bool ArgsKeyExists(string key)
        {
            Logging.LogCall(ModuleName);

            if (Args == null || Args.Length == 0) { Logging.LogExit(ModuleName); return false; }

            var ret = Args.Any(a => string.Equals(a.key, key, StringComparison.OrdinalIgnoreCase));

            Logging.LogExit(ModuleName);
            return ret;
        }

        public (string key, string asStr, int? asInt, double? asDouble, bool? asBool) ArgsValue(string key)
        {
            Logging.LogCall(ModuleName);

            if (Args == null || Args.Length == 0)
            {
                Logging.LogExit(ModuleName);
                return default;
            }

            var ret = Args.FirstOrDefault(a => string.Equals(a.key, key, StringComparison.OrdinalIgnoreCase));

            Logging.LogExit(ModuleName);
            return ret;
        }

        public static (string key, string asStr, int? asInt, double? asDouble, bool? asBool)[] GetParams(string[] args)
        {
            Logging.LogCall(ModuleName);

            var x = new List<(string key, string value)>();

            if (args == null || args.Length == 0)
            {
                Logging.LogExit(ModuleName);
                return Array.Empty<(string key, string asStr, int? asInt, double? asDouble, bool? asBool)>();
            }

            args = args.SelectMany(a => a.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries)).Where(a => !string.IsNullOrWhiteSpace(a)).ToArray();

            var name = "";
            var value = "";

            for (var i = 0; i < args.Length; i++)
            {
                var arg = args[i];

                var startsDash = arg[0] == '-';
                var isNum = double.TryParse(arg, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var valOut);

                var isName = startsDash && (!isNum || string.IsNullOrEmpty(name)) && arg.Length > 1;
                var isValue = !isName;
                var isFinalIndex = i == args.Length - 1;

                if (isName)
                {
                    if (!string.IsNullOrWhiteSpace(name) || !string.IsNullOrWhiteSpace(value)) x.Add((name, value));
                    ;

                    name = arg[1..];
                    value = "";
                }

                if (isValue)
                    value += (value.Length > 0
                    ? " "
                    : "") + arg;

                if (isFinalIndex) x.Add((name, value));
            }

            //var x2 = new List<(string key, string asStr, int? asInt, double? asDouble, bool? asBool)>();

            var ret = x.Select(a =>
            {
                var asInt = int.TryParse(a.value, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var asIntOut)
     ? asIntOut
     : (int?)null;
                //var asLong = long.TryParse(a.value, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var asLong_out) ? (long?)asLong_out : (long?)null;
                var asDouble = double.TryParse(a.value, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var asDoubleOut)
                ? asDoubleOut
                : (double?)null;
                var asBool = bool.TryParse(a.value, out var asBoolOut)
     ? asBoolOut
     : (bool?)null;

                if (asBool == null)
                {
                    if (string.IsNullOrWhiteSpace(a.value)) asBool = true;
                    else if (asInt == 0 && asDouble == 0) asBool = false;
                    else if (asInt == 1 && asDouble == 1) asBool = true;
                }

                return (a.key, asStr: a.value, asInt, asDouble, asBool);
            }).ToArray();

            Logging.LogExit(ModuleName);
            return ret;
        }
    }
}