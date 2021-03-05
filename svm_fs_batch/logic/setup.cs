using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    public class Setup
    {
        public const string ModuleName = nameof(Setup);

        public Setup()
        {

        }

        public static async Task SetupPbsJobAsync(ProgramArgs programArgs, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            const string MethodName = nameof(Setup);

            
            programArgs.Setup = false;
            //program_args.args.RemoveAll(a => string.Equals(a.key, nameof(program_args.setup), StringComparison.OrdinalIgnoreCase));

            programArgs.ExperimentName += $@"_{DateTime.UtcNow:yyyy_MM_dd_HH_mm_ss_fff}";

            programArgs.SetupTotalVcpus = programArgs.SetupTotalVcpus <= 0
                ? 1504
                : programArgs.SetupTotalVcpus;
            programArgs.SetupInstanceVcpus = programArgs.SetupInstanceVcpus <= 0
                ? 64
                : programArgs.SetupInstanceVcpus;
            var setupTotalInstances = (int) Math.Floor(programArgs.SetupTotalVcpus / (double) programArgs.SetupInstanceVcpus);

            var setupArrayStart = 0;
            var setupArrayEnd = setupTotalInstances - 1;
            var setupArrayStep = 1;

            Logging.WriteLine($@"{programArgs.ExperimentName}: {nameof(programArgs.SetupTotalVcpus)} = {programArgs.SetupTotalVcpus}", ModuleName, MethodName);
            Logging.WriteLine($@"{programArgs.ExperimentName}: {nameof(programArgs.SetupInstanceVcpus)} = {programArgs.SetupInstanceVcpus}", ModuleName, MethodName);
            Logging.WriteLine($@"{programArgs.ExperimentName}: {nameof(setupTotalInstances)} = {setupTotalInstances}", ModuleName, MethodName);
            Logging.WriteLine();

            Logging.WriteLine($@"{programArgs.ExperimentName}: {nameof(setupArrayStart)} = {setupArrayStart}", ModuleName, MethodName);
            Logging.WriteLine($@"{programArgs.ExperimentName}: {nameof(setupArrayEnd)} = {setupArrayEnd}", ModuleName, MethodName);
            Logging.WriteLine($@"{programArgs.ExperimentName}: {nameof(setupArrayStep)} = {setupArrayStep}", ModuleName, MethodName);
            Logging.WriteLine();


            var pbsScript = await MakePbsScriptAsync(programArgs, programArgs.SetupInstanceVcpus, true, setupArrayStart, setupArrayEnd, setupArrayStep, ct: ct).ConfigureAwait(false);

            for (var index = 0; index < pbsScript.pbs_script_lines.Count; index++) Logging.WriteLine($"{index,3}: {pbsScript.pbs_script_lines[index]}", ModuleName, MethodName);

            Logging.WriteLine();

            Logging.LogExit(ModuleName);
        }

        public static async Task<(List<string> pbs_script_lines, string run_line)> MakePbsScriptAsync(ProgramArgs programArgs, int pbsPpn = 1, bool isJobArray = false, int arrayIndexFirst = 0, int arrayIndexLast = 0, int arrayStepSize = 1, bool rerunnable = true, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            const string MethodName = nameof(MakePbsScriptAsync);

            if (isJobArray && arrayStepSize == 0) throw new ArgumentOutOfRangeException(nameof(arrayStepSize));
            //-ExperimentName test2 -job_id _ -job_name _ -instance_array_index_start 0 -array_instances 1 -array_start 0 -array_end 6929 -array_step 6930 -inner_folds 5 -outer_cv_folds 5 -outer_cv_folds_to_run 1 -repetitions 5


            var pbsScriptLines = new List<string>();

            var programRuntime = Process.GetCurrentProcess().MainModule?.FileName;

            if (string.IsNullOrWhiteSpace(programRuntime)) throw new Exception();

            var isWin = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

            //is_win = false;

            var envPbsArray = !isWin
                ? @"%J_%I"
                : @"_";

            var envJobid = !isWin ? @"${PBS_JOBID}${MOAB_JOBID}" :
                programArgs.ArgsKeyExists(nameof(programArgs.JobId)) ? $"{programArgs.JobId}" : "_";
            var envJobname = !isWin ? @"${PBS_JOBNAME}${MOAB_JOBNAME}" :
                programArgs.ArgsKeyExists(nameof(programArgs.JobName)) ? $"{programArgs.JobName}" : "_";


            var envJobArrayIndex = !isWin ? @"${PBS_ARRAYID}${MOAB_JOBARRAYINDEX}" :
                programArgs.ArgsKeyExists(nameof(programArgs.PartitionArrayIndexFirst)) ? $"{programArgs.PartitionArrayIndexFirst}" : "0";
            var envJobArrayLength = !isWin ? @"${MOAB_JOBARRAYRANGE}" :
                programArgs.ArgsKeyExists(nameof(programArgs.WholeArrayLength)) ? $"{programArgs.WholeArrayLength}" : "1";


            //var pbs_args = new program_args();


            TimeSpan? pbsWalltime = TimeSpan.FromHours(240);

            var pbsExecutionDirectory = string.Join(Path.DirectorySeparatorChar, Program.ProgramArgs.SvmFsBatchHome, "pbs", programArgs.ExperimentName);

            var pbsJobname = $@"{programArgs.ExperimentName}_{nameof(SvmFsBatch)}";
            var pbsMailAddr = "";
            var pbsMailOpt = "n";
            var pbsMem = "";

            // for each job, request 1 node with 64 vcpu, by default, if ppn not specified
            var pbsNodes = 1;
            if (pbsPpn <= 0) pbsPpn = 64;

            var pbsStdoutFilename = $@"{string.Join("_", new[] {programArgs.ExperimentName, nameof(SvmFsBatch), isJobArray ? envPbsArray : ""}.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.pbs.stdout";
            var pbsStderrFilename = $@"{string.Join("_", new[] {programArgs.ExperimentName, nameof(SvmFsBatch), isJobArray ? envPbsArray : ""}.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.pbs.stderr";

            var programStdoutFilename = $@"{string.Join("_", new[] {programArgs.ExperimentName, nameof(SvmFsBatch), envJobid, envJobname, envJobArrayIndex}.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.program.stdout";
            var programStderrFilename = $@"{string.Join("_", new[] {programArgs.ExperimentName, nameof(SvmFsBatch), envJobid, envJobname, envJobArrayIndex}.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.program.stderr";


            // 1. pbs directives
            if (isJobArray) pbsScriptLines.Add($@"#PBS -t {arrayIndexFirst}-{arrayIndexLast}:{arrayStepSize}");

            if (pbsWalltime != null && pbsWalltime.Value.TotalSeconds > 0) pbsScriptLines.Add($@"#PBS -l walltime={Math.Floor(pbsWalltime.Value.TotalHours):00}:{pbsWalltime.Value.Minutes:00}:{pbsWalltime.Value.Seconds:00}");
            if (pbsNodes > 0) pbsScriptLines.Add($@"#PBS -l nodes={pbsNodes}{(pbsPpn > 0 ? $@":ppn={pbsPpn}" : @"")}");
            if (!string.IsNullOrWhiteSpace(pbsMem)) pbsScriptLines.Add($@"#PBS -l mem={pbsMem}");
            pbsScriptLines.Add($@"#PBS -r {(rerunnable ? "y" : "n")}");
            if (!string.IsNullOrWhiteSpace(pbsJobname)) pbsScriptLines.Add($@"#PBS -N {pbsJobname}");
            if (!string.IsNullOrWhiteSpace(pbsMailOpt)) pbsScriptLines.Add($@"#PBS -m {pbsMailOpt}");
            if (!string.IsNullOrWhiteSpace(pbsMailAddr)) pbsScriptLines.Add($@"#PBS -M {pbsMailAddr}");
            if (!string.IsNullOrWhiteSpace(pbsStdoutFilename)) pbsScriptLines.Add($@"#PBS -o {pbsStdoutFilename}");
            if (!string.IsNullOrWhiteSpace(pbsStderrFilename)) pbsScriptLines.Add($@"#PBS -e {pbsStderrFilename}");
            if (!string.IsNullOrWhiteSpace(pbsExecutionDirectory)) pbsScriptLines.Add($@"#PBS -d {pbsExecutionDirectory}");

            // 2. program directives
            var pbsProgramArgs = new List<(string key, string value)>();

            // experiment name
            pbsProgramArgs.Add((nameof(programArgs.ExperimentName), programArgs.ExperimentName));

            // job id, job name
            pbsProgramArgs.Add((nameof(programArgs.JobId), envJobid));
            pbsProgramArgs.Add((nameof(programArgs.JobName), envJobname));

            if (isJobArray)
            {
                // first index for the hpc instance job to start from
                pbsProgramArgs.Add((nameof(programArgs.PartitionArrayIndexFirst), envJobArrayIndex));

                // total number of jobs in the array
                pbsProgramArgs.Add((nameof(programArgs.WholeArrayLength), envJobArrayLength));

                // pbs -t array start, end, and step (requested to the scheduler with #pbs -t start-end:step)
                pbsProgramArgs.Add((nameof(programArgs.WholeArrayIndexFirst), $@"{arrayIndexFirst}"));
                pbsProgramArgs.Add((nameof(programArgs.WholeArrayIndexLast), $@"{arrayIndexLast}"));
                pbsProgramArgs.Add((nameof(programArgs.WholeArrayStepSize), $@"{arrayStepSize}"));

                var arrayRange = Routines.Range(arrayIndexFirst, arrayIndexLast, arrayStepSize);
                Logging.WriteLine($@"{nameof(arrayRange)}: {string.Join(@", ", arrayRange)}", ModuleName, MethodName);
            }

            foreach (var programArg in programArgs.Args)
            {
                var keyExists = pbsProgramArgs.All(pbsProgramArg => !string.Equals(pbsProgramArg.key, programArg.key, StringComparison.OrdinalIgnoreCase));

                if (!keyExists) pbsProgramArgs.Add((programArg.key, $@"{programArg.asStr}"));
            }

            if (!string.IsNullOrEmpty(programStdoutFilename)) pbsProgramArgs.Add((@"1>", programStdoutFilename));
            if (!string.IsNullOrEmpty(programStderrFilename)) pbsProgramArgs.Add((@"2>", programStderrFilename));

            var runLine = $@"{programRuntime} {string.Join(" ", pbsProgramArgs.Select(a => string.Join(@" ", new[] {$@"-{a.key}", a.value}.Where(c => !string.IsNullOrWhiteSpace(c)).ToArray())).Where(b => !string.IsNullOrWhiteSpace(b)).ToArray())}";

            if (!string.IsNullOrWhiteSpace(pbsExecutionDirectory)) pbsScriptLines.Add($@"cd {pbsExecutionDirectory}");
            pbsScriptLines.Add(@"module load GCCcore");
            pbsScriptLines.Add(runLine);

            var pbsFn = Path.Combine(pbsExecutionDirectory, $@"{pbsJobname}.pbs");

            await IoProxy.WriteAllLinesAsync(true, ct, pbsFn, pbsScriptLines).ConfigureAwait(false);
            Logging.WriteLine($@"{programArgs.ExperimentName}: Saved PBS script. File: {pbsFn}.");

            Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :(pbsScriptLines, runLine);
        }
    }
}