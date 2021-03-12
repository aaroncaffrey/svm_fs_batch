using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    public static class Libsvm
    {
        public const string ModuleName = nameof(Libsvm);

        public static async Task<(string CmdLine, string stdout, string stderr)> TrainAsync(string libsvmTrainExeFile, string trainFile, string modelOutFile, string stdoutFile = null, string stderrFile = null, double? cost = null, double? gamma = null, double? epsilon = null, double? coef0 = null, double? degree = null, (int ClassId, double ClassWeight)[] classWeights = null, Routines.LibsvmSvmType svmType = Routines.LibsvmSvmType.CSvc, Routines.LibsvmKernelType svmKernel = Routines.LibsvmKernelType.Rbf, int? innerCvFolds = null, bool probabilityEstimates = false, bool shrinkingHeuristics = true, TimeSpan? processMaxTime = null, bool quietMode = true, int memoryLimitMb = 1024, bool log = true, int maxTries = 1_000_000, bool rethrow = true, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            const string MethodName = nameof(TrainAsync);

            (string key, string value)[] GetParams()
            {
                try
                {
                    Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :new (string key, string value)[]
                    {
                        (nameof(libsvmTrainExeFile), libsvmTrainExeFile), (nameof(trainFile), trainFile),
                        (nameof(modelOutFile), modelOutFile), (nameof(stdoutFile), stdoutFile),
                        (nameof(stderrFile), stderrFile), (nameof(cost), cost?.ToString() ?? ""),
                        (nameof(gamma), gamma?.ToString() ?? ""), (nameof(epsilon), epsilon?.ToString() ?? ""),
                        (nameof(coef0), coef0?.ToString() ?? ""), (nameof(degree), degree?.ToString() ?? ""),
                        (nameof(classWeights), classWeights != null
                            ? string.Join(";", classWeights.Select(a => $@"{a.ClassId}={a.ClassWeight}").ToList())
                            : ""),
                        (nameof(svmType), svmType.ToString()), (nameof(svmKernel), svmKernel.ToString()),
                        (nameof(innerCvFolds), innerCvFolds?.ToString() ?? ""),
                        (nameof(probabilityEstimates), probabilityEstimates.ToString()),
                        (nameof(shrinkingHeuristics), shrinkingHeuristics.ToString()),
                        (nameof(processMaxTime), processMaxTime?.ToString() ?? ""),
                        (nameof(quietMode), quietMode.ToString()),
                        (nameof(memoryLimitMb), memoryLimitMb.ToString()), (nameof(log), log.ToString())
                    };
                }
                catch (Exception e) { Logging.LogException(e, "", ModuleName); }

                Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :Array.Empty<(string key, string value)>();
            }

            string GetParamsStr()
            {
                try { Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :string.Join(", ", GetParams().Select(a => $@"{a.key}=""{a.value}""").ToList()); }
                catch (Exception e) { Logging.LogException(e, "", ModuleName); }

                Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :"";
            }

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            var libsvmParams = new List<string>();
            if (quietMode) libsvmParams.Add("-q");
            if (memoryLimitMb != 100) libsvmParams.Add($@"-m {memoryLimitMb}");
            if (probabilityEstimates) libsvmParams.Add($@"-b {(probabilityEstimates ? "1" : "0")}");
            if (svmType != Routines.LibsvmSvmType.CSvc) libsvmParams.Add($@"-s {(int) svmType}");
            if (svmKernel != Routines.LibsvmKernelType.Rbf) libsvmParams.Add($@"-t {(int) svmKernel}");
            if (innerCvFolds != null && innerCvFolds >= 2) libsvmParams.Add($@"-v {innerCvFolds}");
            if (cost != null) libsvmParams.Add($@"-c {cost.Value}");
            if (gamma != null && svmKernel != Routines.LibsvmKernelType.Linear) libsvmParams.Add($@"-g {gamma.Value}");
            if (epsilon != null && (svmType == Routines.LibsvmSvmType.EpsilonSvr || svmType == Routines.LibsvmSvmType.NuSvr)) libsvmParams.Add($@"-p {epsilon.Value}");
            if (coef0 != null && (svmKernel == Routines.LibsvmKernelType.Sigmoid || svmKernel == Routines.LibsvmKernelType.Polynomial)) libsvmParams.Add($@"-r {coef0.Value}");
            if (degree != null && svmKernel == Routines.LibsvmKernelType.Polynomial) libsvmParams.Add($@"-d {degree.Value}");

            if (classWeights != null && classWeights.Length > 0)
            {
                classWeights = classWeights.OrderBy(a => a.ClassId).ToArray();

                for (var classWeightIndex = 0; classWeightIndex < classWeights.Length; classWeightIndex++)
                {
                    var classWeight = classWeights[classWeightIndex];
                    libsvmParams.Add($@"-w{classWeight.ClassId} {classWeight.ClassWeight}");
                }
            }

            if (!shrinkingHeuristics) libsvmParams.Add($@"-h {(shrinkingHeuristics ? "1" : "0")}");
            libsvmParams = libsvmParams.OrderBy(a => a).ToList();
            var trainFileParam = trainFile;
            var modelFileParam = modelOutFile;
            if (!string.IsNullOrWhiteSpace(trainFile)) libsvmParams.Add($@"{trainFileParam}");
            if (!string.IsNullOrWhiteSpace(modelOutFile) && (innerCvFolds == null || innerCvFolds <= 1)) libsvmParams.Add($@"{modelFileParam}");
            var wd = Path.GetDirectoryName(trainFile);

            var args = string.Join(" ", libsvmParams);

            var start = new ProcessStartInfo
            {
                FileName = libsvmTrainExeFile,
                Arguments = args,
                UseShellExecute = false,
                CreateNoWindow = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true
            };

            var cmdLine = string.Join(" ", libsvmTrainExeFile, args);

            var tries = 0;
            while (tries < maxTries)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested)
                    {
                        Logging.LogExit(ModuleName);  
                        return default;
                    }

                    using var process = Process.Start(start);

                    if (process == null)
                    {
                        Logging.WriteLine($@"""{start.FileName}"" failed to run. {nameof(tries)} = {tries}/{maxTries}.", ModuleName, MethodName);
                        await Logging.WaitAsync(25, 50, ct: ct).ConfigureAwait(false);
                        continue;
                    }

                    if (log) Logging.WriteLine($"Spawned process {Path.GetFileName(start.FileName)}: {process.Id}", ModuleName, MethodName);
                    //var exited = process.WaitForExit((int) Math.Ceiling(new TimeSpan(0, 45, 0).TotalMilliseconds));

                    using var delayCts = new CancellationTokenSource();
                    var delayCt = delayCts.Token;
                    try{await Task.WhenAny(process.WaitForExitAsync(ct), Task.Delay(TimeSpan.FromMinutes(45), delayCt)).ConfigureAwait(false);}
                    catch (Exception e) { Logging.LogException(e);}
                    delayCts.Cancel();
                    var exited = process.HasExited;
                    if (!exited)
                    {
                        Logging.WriteLine($@"""{start.FileName}"" {process.Id} failed to exit. {nameof(tries)} = {tries}/{maxTries}.", ModuleName, MethodName);
                        try {
                            process.Kill();
                            await process.WaitForExitAsync(ct).ConfigureAwait(false);
                        }
                        catch (Exception e) { Logging.LogException(e, GetParamsStr(), ModuleName, MethodName); }

                        await Logging.WaitAsync(25, 50, ct: ct).ConfigureAwait(false);
                        continue;
                    }

                    var stdoutResult = process?.StandardOutput?.ReadToEnd() ?? "";
                    var stderrResult = process?.StandardError?.ReadToEnd() ?? "";
                    if (log) Logging.WriteLine($"Exited process {Path.GetFileName(start.FileName)}: {process.Id}", ModuleName, MethodName);
                    var exitCode = process.ExitCode;
                    if (!string.IsNullOrWhiteSpace(stdoutFile) && !string.IsNullOrWhiteSpace(stdoutResult)) await IoProxy.AppendAllTextAsync(true, ct, stdoutFile, stdoutResult, callerModuleName: ModuleName, callerMethodName: MethodName).ConfigureAwait(false);
                    if (!string.IsNullOrWhiteSpace(stderrFile) && !string.IsNullOrWhiteSpace(stderrResult)) await IoProxy.AppendAllTextAsync(true, ct, stderrFile, stderrResult, callerModuleName: ModuleName, callerMethodName: MethodName).ConfigureAwait(false);

                    if (exitCode == 0) {
                        Logging.LogExit(ModuleName); 
                        return ct.IsCancellationRequested ? default :(cmdLine, stdoutResult, stderrResult);
                    }

                    Logging.WriteLine($@"""{start.FileName}"" {process.Id} failed to run. {nameof(exitCode)} = {exitCode}. {nameof(tries)} = {tries}/{maxTries}.", ModuleName, MethodName);
                    if (!string.IsNullOrWhiteSpace(stdoutResult)) Logging.WriteLine($"@{nameof(stdoutResult)} = {stdoutResult}", ModuleName, MethodName);
                    if (!string.IsNullOrWhiteSpace(stderrResult)) Logging.WriteLine($"@{nameof(stderrResult)} = {stderrResult}", ModuleName, MethodName);
                    await Logging.WaitAsync(25, 50, ct: ct).ConfigureAwait(false);
                }
                catch (Exception e1)
                {
                    Logging.LogException(e1, $@"{callerModuleName}.{callerMethodName} -> ( ""{GetParamsStr()}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, MethodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        Logging.LogExit(ModuleName); 
                        return ct.IsCancellationRequested ? default :(cmdLine, null, null);
                    }

                    await Logging.WaitAsync(25, 50, ct: ct).ConfigureAwait(false);
                }

            Logging.LogExit(ModuleName); 
            return ct.IsCancellationRequested ? default :(cmdLine, null, null);
        }

        public static async Task<(string CmdLine, string stdout, string stderr)> PredictAsync(string libsvmPredictExeFile, string testFile, string modelFile, string predictionsOutFile, bool probabilityEstimates, string stdoutFile = null, string stderrFile = null, bool log = true, int maxTries = 1_000_000, bool rethrow = true, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            const string MethodName = nameof(PredictAsync);

            (string key, string value)[] GetParams()
            {
                try
                {
                    Logging.LogExit(ModuleName); return new[]
                    {
                        (nameof(libsvmPredictExeFile), libsvmPredictExeFile), (nameof(testFile), testFile),
                        (nameof(modelFile), modelFile), (nameof(predictionsOutFile), predictionsOutFile),
                        (nameof(stdoutFile), stdoutFile), (nameof(stderrFile), stderrFile),
                        (nameof(log), log.ToString())
                    };
                }
                catch (Exception e) { Logging.LogException(e, "", ModuleName); }

                Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :Array.Empty<(string key, string value)>();
            }

            string GetParamsStr()
            {
                try { Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :string.Join(", ", GetParams()); }
                catch (Exception e) { Logging.LogException(e, "", ModuleName); }

                Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :"";
            }

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            var libsvmParams = new List<string>();
            if (probabilityEstimates) libsvmParams.Add(@"-b 1");
            libsvmParams = libsvmParams.OrderBy(a => a).ToList();
            var testFileParam = testFile;
            var modelFileParam = modelFile;
            var predictionFileParam = predictionsOutFile;
            if (!string.IsNullOrWhiteSpace(testFile)) libsvmParams.Add($@"{testFileParam}");
            if (!string.IsNullOrWhiteSpace(modelFile)) libsvmParams.Add($@"{modelFileParam}");
            if (!string.IsNullOrWhiteSpace(predictionsOutFile)) libsvmParams.Add($@"{predictionFileParam}");
            var args = string.Join(" ", libsvmParams);

            var start = new ProcessStartInfo
            {
                FileName = libsvmPredictExeFile,
                Arguments = args,
                UseShellExecute = false,
                CreateNoWindow = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true
            };

            var cmdLine = string.Join(" ", libsvmPredictExeFile, args);
            var tries = 0;
            while (tries < maxTries)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested)
                    {
                        Logging.LogExit(ModuleName);  
                        return default;
                    }
                    using var process = Process.Start(start);
                    if (process == null)
                    {
                        Logging.WriteLine($@"""{start.FileName}"" failed to run. {nameof(tries)} = {tries}/{maxTries}.", ModuleName, MethodName);
                        await Logging.WaitAsync(25, 50, ct: ct).ConfigureAwait(false);
                        continue;
                    }

                    if (log) Logging.WriteLine($"Spawned process {Path.GetFileName(start.FileName)}: {process.Id}", ModuleName, MethodName);
                    //var exited = process.WaitForExit((int) Math.Ceiling(new TimeSpan(0, 45, 0).TotalMilliseconds));
                    using var delayCts = new CancellationTokenSource();
                    var delayCt = delayCts.Token;
                    try{await Task.WhenAny(process.WaitForExitAsync(ct), Task.Delay(TimeSpan.FromMinutes(45), delayCt)).ConfigureAwait(false);}
                    catch (Exception e) { Logging.LogException(e);}
                    delayCts.Cancel();

                    var exited = process.HasExited;

                    if (!exited)
                    {
                        Logging.WriteLine($@"""{start.FileName}"" {process.Id} failed to exit. {nameof(tries)} = {tries}/{maxTries}.", ModuleName, MethodName);
                        try { process.Kill(); await process.WaitForExitAsync(ct).ConfigureAwait(false); }
                        catch (Exception e) { Logging.LogException(e, GetParamsStr(), ModuleName, MethodName); }

                        await Logging.WaitAsync(25, 50, ct: ct).ConfigureAwait(false);
                        continue;
                    }

                    var stdoutResult = process?.StandardOutput?.ReadToEnd() ?? "";
                    var stderrResult = process?.StandardError?.ReadToEnd() ?? "";
                    if (log) Logging.WriteLine($"Exited process {Path.GetFileName(start.FileName)}: {process.Id}", ModuleName, MethodName);
                    var exitCode = process.ExitCode;
                    if (!string.IsNullOrWhiteSpace(stdoutFile) && !string.IsNullOrWhiteSpace(stdoutResult)) await IoProxy.AppendAllTextAsync(true, ct, stdoutFile, stdoutResult, callerModuleName: ModuleName, callerMethodName: MethodName).ConfigureAwait(false);
                    if (!string.IsNullOrWhiteSpace(stderrFile) && !string.IsNullOrWhiteSpace(stderrResult)) await IoProxy.AppendAllTextAsync(true, ct, stderrFile, stderrResult, callerModuleName: ModuleName, callerMethodName: MethodName).ConfigureAwait(false);

                    if (exitCode == 0) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :(cmdLine, stdoutResult, stderrResult); }

                    Logging.WriteLine($@"""{start.FileName}"" {process.Id} failed to run. {nameof(exitCode)} = {exitCode}. {nameof(tries)} = {tries}/{maxTries}.", ModuleName, MethodName);
                    if (!string.IsNullOrWhiteSpace(stdoutResult)) Logging.WriteLine($"@{nameof(stdoutResult)} = {stdoutResult}", ModuleName, MethodName);
                    if (!string.IsNullOrWhiteSpace(stderrResult)) Logging.WriteLine($"@{nameof(stderrResult)} = {stderrResult}", ModuleName, MethodName);
                    await Logging.WaitAsync(25, 50, ct: ct).ConfigureAwait(false);
                }
                catch (Exception e1)
                {
                    Logging.LogException(e1, $@"{callerModuleName}.{callerMethodName} -> ( ""{GetParamsStr()}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, MethodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default : (cmdLine, null, null);
                    }

                    await Logging.WaitAsync(25, 50, ct: ct).ConfigureAwait(false);
                }

            Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default : (cmdLine, null, null);
        }
    }
}