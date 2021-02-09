using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;

namespace SvmFsBatch
{
    internal class ProgramArgs
    {
        public const string ModuleName = nameof(ProgramArgs);

        internal static string[] CsvHeaderValuesArray =
        {
            nameof(Folds),
            nameof(Repetitions),
            nameof(OuterCvFolds),
            nameof(OuterCvFoldsToRun),
            nameof(InnerFolds),
            nameof(ExperimentName),
            nameof(JobId),
            nameof(JobName),
            nameof(WholeArrayIndexFirst),
            nameof(WholeArrayIndexLast),
            nameof(WholeArrayStepSize),
            nameof(WholeArrayLength),
            nameof(PartitionArrayIndexFirst),
            nameof(PartitionArrayIndexLast),
            nameof(Setup),
            nameof(SetupTotalVcpus),
            nameof(SetupInstanceVcpus),
            nameof(ScoringClassId),
            nameof(ScoringMetrics),
            nameof(IsWin),
            nameof(UserHome),
            nameof(SvmFsBatchHome),
            nameof(ResultsRootFolder),
            nameof(LibsvmPredictRuntime),
            nameof(LibsvmTrainRuntime),

            nameof(ClassNames),
            nameof(ClassWeights),
            nameof(SvmTypes),
            nameof(Kernels),
            nameof(Scales),
            nameof(CalcElevenPointThresholds),

            nameof(Client),
            nameof(ClientGuid),
            nameof(ClientConnectionPoolSize),
            nameof(Server),
            nameof(ServerBacklog),
            nameof(ServerGuid),
            nameof(DataSetDir),
            nameof(DataSetNames),
            nameof(NegativeClassId),
            nameof(PositiveClassId),
            nameof(NegativeClassName),
            nameof(PositiveClassName)
        };


        internal (string key, string asStr, int? asInt, double? asDouble, bool? asBool)[] Args;

        internal bool CalcElevenPointThresholds;


        internal (int ClassId, string ClassName)[] ClassNames;
        internal (int ClassId, double ClassWeight)[][] ClassWeights;

        internal bool Client;
        internal int ClientConnectionPoolSize = 10;
        internal Guid ClientGuid = Guid.NewGuid();

        internal string DataSetDir;
        internal string[] DataSetNames;

        //internal bool run_local = false;
        internal string ExperimentName = "";

        // note: use 'folds' to set repetitions, outer_cv_folds and inner_folds to the same value
        internal int? Folds;
        internal int InnerFolds = 5;
        internal bool IsUnix = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) || RuntimeInformation.IsOSPlatform(OSPlatform.FreeBSD) || RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

        internal bool IsWin = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

        internal string JobId = "";
        internal string JobName = "";
        internal Routines.LibsvmKernelType[] Kernels = {Routines.LibsvmKernelType.Rbf};
        internal string LibsvmPredictRuntime;
        internal string LibsvmTrainRuntime;

        internal int NegativeClassId = -1;
        internal string NegativeClassName = @"standard_coil";
        internal int OuterCvFolds = 5;
        internal int OuterCvFoldsToRun;

        // partition array parameters (pbs only provides partition_array_index_first, whole_array_step_size is given by the script, and partition_array_index_last is calculated from it, if not provided)
        internal int PartitionArrayIndexFirst = -1; // start index for current instance
        internal int PartitionArrayIndexLast = -1; // last index for current instance
        internal int PositiveClassId = +1;
        internal string PositiveClassName = @"dimorphic_coil";

        internal int Repetitions = 1;
        internal string ResultsRootFolder;
        internal Scaling.ScaleFunction[] Scales = {Scaling.ScaleFunction.Rescale};

        internal int ScoringClassId = +1;

        internal string[] ScoringMetrics = {nameof(MetricsBox.PF1S) /*nameof(metrics_box.p_MCC),*/ /*nameof(metrics_box.p_API_All)*/};

        internal bool Server;
        internal int ServerBacklog = 1000;
        internal Guid ServerGuid = new Guid("d2d95e16-d086-4b58-ba29-8b9b84e41be5");

        internal string ServerIp = "127.0.0.1";
        internal int ServerPort = 64727;

        // parameters for -setup only to generate pbs script to run array job
        internal bool Setup;
        internal int SetupInstanceVcpus = -1;
        internal int SetupTotalVcpus = -1;
        internal string SvmFsBatchHome;

        internal Routines.LibsvmSvmType[] SvmTypes = {Routines.LibsvmSvmType.CSvc};
        internal string UserHome;


        // whole array parameters
        internal int WholeArrayIndexFirst = -1; // start index of whole array
        internal int WholeArrayIndexLast = -1; // end index of whole array
        internal int WholeArrayLength; // number of jobs in array
        internal int WholeArrayStepSize; // step size for whole array

        public ProgramArgs(string[] args)
        {
            Logging.LogCall(ModuleName);

            const string methodName = nameof(ProgramArgs);

            var argNames = new[]
            {
                nameof(Folds), nameof(Repetitions), nameof(OuterCvFolds), nameof(OuterCvFoldsToRun),
                nameof(InnerFolds), nameof(Setup), /*nameof(run_local),*/ nameof(ExperimentName), nameof(JobId),
                nameof(JobName),
                nameof(PartitionArrayIndexFirst), nameof(PartitionArrayIndexLast), nameof(WholeArrayLength),
                nameof(WholeArrayStepSize),
                nameof(WholeArrayIndexFirst), nameof(WholeArrayIndexLast), nameof(SetupTotalVcpus),
                nameof(SetupInstanceVcpus),


                nameof(IsWin),
                nameof(UserHome),
                nameof(SvmFsBatchHome),
                nameof(ResultsRootFolder),
                nameof(LibsvmPredictRuntime),
                nameof(LibsvmTrainRuntime),
                nameof(Client),
                nameof(ClientGuid),
                nameof(ClientConnectionPoolSize),
                nameof(Server),
                nameof(ServerBacklog),
                nameof(ServerGuid),
                nameof(ServerIp),
                nameof(ServerPort),
                nameof(DataSetDir),
                nameof(DataSetNames),
                nameof(NegativeClassId),
                nameof(PositiveClassId),
                nameof(NegativeClassName),
                nameof(PositiveClassName),
                nameof(ScoringClassId),
                nameof(ScoringMetrics),

                nameof(ClassNames),
                nameof(ClassWeights),
                nameof(SvmTypes),
                nameof(Kernels),
                nameof(Scales),
                nameof(CalcElevenPointThresholds)
            };

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

            IsWin = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

            if (!IsWin)
            {
                UserHome = @"/home/k1040015";
                SvmFsBatchHome = $@"/mmfs1/data/scratch/k1040015/{nameof(SvmFsBatch)}";
                ResultsRootFolder = $@"{SvmFsBatchHome}/results/";
                LibsvmPredictRuntime = $@"{UserHome}/libsvm/svm-predict";
                LibsvmTrainRuntime = $@"{UserHome}/libsvm/svm-train";
                DataSetDir = $@"{UserHome}/DataSet/";
            }
            else
            {
                UserHome = @"C:\home\k1040015";
                SvmFsBatchHome = $@"C:\mmfs1\data\scratch\k1040015\{nameof(SvmFsBatch)}";
                ResultsRootFolder = $@"{SvmFsBatchHome}\results\";
                LibsvmPredictRuntime = @"C:\libsvm\windows\svm-predict.exe";
                LibsvmTrainRuntime = @"C:\libsvm\windows\svm-train.exe";
                DataSetDir = @"E:\DataSet7\merged_files\";
            }

            if (ArgsKeyExists(nameof(ExperimentName))) ExperimentName = ArgsValue(nameof(ExperimentName)).asStr;
            if (ArgsKeyExists(nameof(JobId))) JobId = ArgsValue(nameof(JobId)).asStr;
            if (ArgsKeyExists(nameof(JobName))) JobName = ArgsValue(nameof(JobName)).asStr;

            //if (args_key_exists(nameof(run_local))) run_local = ArgsValue(nameof(run_local)).asBool ?? default;
            if (ArgsKeyExists(nameof(Setup))) Setup = ArgsValue(nameof(Setup)).asBool ?? default;

            if (ArgsKeyExists(nameof(Repetitions))) Repetitions = ArgsValue(nameof(Repetitions)).asInt ?? -1;
            if (ArgsKeyExists(nameof(OuterCvFolds))) OuterCvFolds = ArgsValue(nameof(OuterCvFolds)).asInt ?? -1;
            if (ArgsKeyExists(nameof(OuterCvFoldsToRun))) OuterCvFoldsToRun = ArgsValue(nameof(OuterCvFoldsToRun)).asInt ?? -1;
            if (ArgsKeyExists(nameof(InnerFolds))) InnerFolds = ArgsValue(nameof(InnerFolds)).asInt ?? -1;

            if (ArgsKeyExists(nameof(PartitionArrayIndexFirst))) PartitionArrayIndexFirst = ArgsValue(nameof(PartitionArrayIndexFirst)).asInt ?? -1;
            if (ArgsKeyExists(nameof(PartitionArrayIndexLast))) PartitionArrayIndexLast = ArgsValue(nameof(PartitionArrayIndexLast)).asInt ?? -1;

            if (ArgsKeyExists(nameof(WholeArrayLength))) WholeArrayLength = ArgsValue(nameof(WholeArrayLength)).asInt ?? 0;
            if (ArgsKeyExists(nameof(WholeArrayStepSize))) WholeArrayStepSize = ArgsValue(nameof(WholeArrayStepSize)).asInt ?? 0;
            if (ArgsKeyExists(nameof(WholeArrayIndexFirst))) WholeArrayIndexFirst = ArgsValue(nameof(WholeArrayIndexFirst)).asInt ?? -1;
            if (ArgsKeyExists(nameof(WholeArrayIndexLast))) WholeArrayIndexLast = ArgsValue(nameof(WholeArrayIndexLast)).asInt ?? -1;

            if (ArgsKeyExists(nameof(SetupTotalVcpus))) SetupTotalVcpus = ArgsValue(nameof(SetupTotalVcpus)).asInt ?? -1;
            if (ArgsKeyExists(nameof(SetupInstanceVcpus))) SetupInstanceVcpus = ArgsValue(nameof(SetupInstanceVcpus)).asInt ?? -1;


            if (ArgsKeyExists(nameof(IsWin))) IsWin = ArgsValue(nameof(IsWin)).asBool ?? default;
            if (ArgsKeyExists(nameof(UserHome))) UserHome = ArgsValue(nameof(UserHome)).asStr;
            if (ArgsKeyExists(nameof(SvmFsBatchHome))) SvmFsBatchHome = ArgsValue(nameof(SvmFsBatchHome)).asStr;
            if (ArgsKeyExists(nameof(ResultsRootFolder))) ResultsRootFolder = ArgsValue(nameof(ResultsRootFolder)).asStr;
            if (ArgsKeyExists(nameof(LibsvmPredictRuntime))) LibsvmPredictRuntime = ArgsValue(nameof(LibsvmPredictRuntime)).asStr;
            if (ArgsKeyExists(nameof(LibsvmTrainRuntime))) LibsvmTrainRuntime = ArgsValue(nameof(LibsvmTrainRuntime)).asStr;

            if (ArgsKeyExists(nameof(Client))) Client = ArgsValue(nameof(Client)).asBool ?? default;
            if (ArgsKeyExists(nameof(ClientGuid))) ClientGuid = new Guid(ArgsValue(nameof(ClientGuid)).asStr ?? throw new ArgumentNullException(nameof(args)));
            if (ArgsKeyExists(nameof(ClientConnectionPoolSize))) ClientConnectionPoolSize = ArgsValue(nameof(ClientConnectionPoolSize)).asInt ?? default;

            if (ArgsKeyExists(nameof(Server))) Server = ArgsValue(nameof(Server)).asBool ?? default;
            if (ArgsKeyExists(nameof(ServerBacklog))) ServerBacklog = ArgsValue(nameof(ServerBacklog)).asInt ?? throw new ArgumentNullException(nameof(args));
            if (ArgsKeyExists(nameof(ServerGuid))) ServerGuid = new Guid(ArgsValue(nameof(ServerGuid)).asStr ?? throw new ArgumentNullException(nameof(args)));
            if (ArgsKeyExists(nameof(ServerIp))) ServerIp = ArgsValue(nameof(ServerIp)).asStr ?? throw new ArgumentNullException(nameof(args));
            if (ArgsKeyExists(nameof(ServerPort))) ServerPort = ArgsValue(nameof(ServerPort)).asInt ?? throw new ArgumentNullException(nameof(args));


            if (ArgsKeyExists(nameof(DataSetDir))) DataSetDir = ArgsValue(nameof(DataSetDir)).asStr;
            if (ArgsKeyExists(nameof(DataSetNames))) DataSetNames = ArgsValue(nameof(DataSetNames)).asStr.Split(new[] {',', ';'}, StringSplitOptions.RemoveEmptyEntries);

            if (ArgsKeyExists(nameof(NegativeClassId))) NegativeClassId = ArgsValue(nameof(NegativeClassId)).asInt ?? throw new ArgumentNullException(nameof(args));
            if (ArgsKeyExists(nameof(PositiveClassId))) PositiveClassId = ArgsValue(nameof(PositiveClassId)).asInt ?? throw new ArgumentNullException(nameof(args));
            if (ArgsKeyExists(nameof(NegativeClassName))) NegativeClassName = ArgsValue(nameof(NegativeClassName)).asStr;
            if (ArgsKeyExists(nameof(PositiveClassName))) PositiveClassName = ArgsValue(nameof(PositiveClassName)).asStr;

            if (ArgsKeyExists(nameof(ScoringClassId))) ScoringClassId = ArgsValue(nameof(ScoringClassId)).asInt ?? throw new ArgumentNullException(nameof(args));
            if (ArgsKeyExists(nameof(ScoringMetrics))) ScoringMetrics = ArgsValue(nameof(ScoringMetrics)).asStr.Split(new[] {',', ';'}, StringSplitOptions.RemoveEmptyEntries);

            if (ArgsKeyExists(nameof(SvmTypes))) SvmTypes = ArgsValue(nameof(SvmTypes)).asStr.Split(new[] {',', ';'}, StringSplitOptions.RemoveEmptyEntries).Select(a => Enum.Parse<Routines.LibsvmSvmType>(a, true)).ToArray();
            if (ArgsKeyExists(nameof(Kernels))) Kernels = ArgsValue(nameof(Kernels)).asStr.Split(new[] {',', ';'}, StringSplitOptions.RemoveEmptyEntries).Select(a => Enum.Parse<Routines.LibsvmKernelType>(a, true)).ToArray();
            if (ArgsKeyExists(nameof(Scales))) Scales = ArgsValue(nameof(Scales)).asStr.Split(new[] {',', ';'}, StringSplitOptions.RemoveEmptyEntries).Select(a => Enum.Parse<Scaling.ScaleFunction>(a, true)).ToArray();
            if (ArgsKeyExists(nameof(CalcElevenPointThresholds))) CalcElevenPointThresholds = ArgsValue(nameof(CalcElevenPointThresholds)).asBool ?? default;


            if (ArgsKeyExists(nameof(ClassNames)))
                ClassNames = ArgsValue(nameof(ClassNames)).asStr.Split(new[] {',', ';'}, StringSplitOptions.RemoveEmptyEntries).Select(a =>
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
                ClassWeights = ArgsValue(nameof(ClassWeights)).asStr.Split(new[] {';'}, StringSplitOptions.RemoveEmptyEntries).Select(a => a.Split(',').Select(b =>
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
            else if (new[] {InnerFolds, OuterCvFolds, OuterCvFoldsToRun, Repetitions}.Distinct().Count() == 1) { Folds = InnerFolds; }

            if (PartitionArrayIndexLast <= -1) PartitionArrayIndexLast = PartitionArrayIndexFirst + (WholeArrayStepSize - 1);


            //if (!client && !server)
            //{
            //    client = true;
            //}

            var v = CsvValuesArray();
            Logging.WriteLine(string.Join(" ", CsvHeaderValuesArray.Select((header, i) => $"{header}={v[i]}").ToArray()), ModuleName, methodName);
        }

        internal string[] CsvValuesArray()
        {
            Logging.LogCall(ModuleName);

            var ret = new[]
            {
                $"{Folds}",
                $"{Repetitions}",
                $"{OuterCvFolds}",
                $"{OuterCvFoldsToRun}",
                $"{InnerFolds}",
                $"{ExperimentName}",
                $"{JobId}",
                $"{JobName}",
                $"{WholeArrayIndexFirst}",
                $"{WholeArrayIndexLast}",
                $"{WholeArrayStepSize}",
                $"{WholeArrayLength}",
                $"{PartitionArrayIndexFirst}",
                $"{PartitionArrayIndexLast}",
                $"{(Setup ? 1 : 0)}",
                $"{SetupTotalVcpus}",
                $"{SetupInstanceVcpus}",
                $"{ScoringClassId}",
                $"{string.Join(",", ScoringMetrics ?? Array.Empty<string>())}",
                $"{(IsWin ? 1 : 0)}",
                $"{UserHome}",
                $"{SvmFsBatchHome}",
                $"{ResultsRootFolder}",
                $"{LibsvmPredictRuntime}",
                $"{LibsvmTrainRuntime}",

                $"{string.Join(",", ClassNames?.Select(a => $"{a.ClassId}:{a.ClassName}").ToArray() ?? Array.Empty<string>())}", // ClassNames=+1:dimorphic;-1:coil;

                $"{string.Join(";", ClassWeights?.Select(a => string.Join(",", a?.Select(b => $"{b.ClassId}:{b.ClassWeight}").ToArray() ?? Array.Empty<string>())).ToArray() ?? Array.Empty<string>())}", // ClassWeight=+1:0.1,-1:0.9;+1:0.2,-1:0.8
                $"{string.Join(",", SvmTypes ?? Array.Empty<Routines.LibsvmSvmType>())}",
                $"{string.Join(",", Kernels ?? Array.Empty<Routines.LibsvmKernelType>())}",
                $"{string.Join(",", Scales ?? Array.Empty<Scaling.ScaleFunction>())}",
                $"{(CalcElevenPointThresholds ? 1 : 0)}",

                $"{(Client ? 1 : 0)}",
                $"{ClientGuid}",
                $"{ClientConnectionPoolSize}",
                $"{(Server ? 1 : 0)}",
                $"{ServerBacklog}",
                $"{ServerGuid}",
                $"{ServerIp}",
                $"{ServerPort}",

                $"{DataSetDir}",
                $"{string.Join(";", DataSetNames ?? Array.Empty<string>())}",
                $"{NegativeClassId}",
                $"{PositiveClassId}",
                $"{NegativeClassName}",
                $"{PositiveClassName}"
            };

            Logging.LogExit(ModuleName);
            return ret;
        }

        internal bool ArgsKeyExists(string key)
        {
            Logging.LogCall(ModuleName);

            if (Args == null || Args.Length == 0) {Logging.LogExit(ModuleName); return false; }

            var ret = Args.Any(a => string.Equals(a.key, key, StringComparison.OrdinalIgnoreCase));

            Logging.LogExit(ModuleName);
            return ret;
        }

        internal (string key, string asStr, int? asInt, double? asDouble, bool? asBool) ArgsValue(string key)
        {
            Logging.LogCall(ModuleName);

            if (Args == null || Args.Length == 0) { Logging.LogExit(ModuleName);  return default; }

            var ret = Args.FirstOrDefault(a => string.Equals(a.key, key, StringComparison.OrdinalIgnoreCase));
            
            Logging.LogExit(ModuleName);
            return ret;
        }

        internal static (string key, string asStr, int? asInt, double? asDouble, bool? asBool)[] GetParams(string[] args)
        {
            Logging.LogCall(ModuleName);

            var x = new List<(string key, string value)>();

            if (args == null || args.Length == 0) { Logging.LogExit(ModuleName);  return null; }

            args = args.SelectMany(a => a.Split(new[] {'='}, StringSplitOptions.RemoveEmptyEntries)).Where(a => !string.IsNullOrWhiteSpace(a)).ToArray();

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
                    : (int?) null;
                //var asLong = long.TryParse(a.value, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var asLong_out) ? (long?)asLong_out : (long?)null;
                var asDouble = double.TryParse(a.value, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var asDoubleOut)
                    ? asDoubleOut
                    : (double?) null;
                var asBool = bool.TryParse(a.value, out var asBoolOut)
                    ? asBoolOut
                    : (bool?) null;

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