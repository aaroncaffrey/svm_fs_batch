using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SvmFsLib;

namespace SvmFsLdr
{
    public class SvmFsLdr
    {
        // Program Description:
        // This program bootstraps the whole algorithm. It requests that the scheduler runs the feature-selection algorithm (controller).
        // It accepts parameters specifying the dataset on which to perform feature-selection.

        public const string ModuleName = nameof(SvmFsLdr);

        public static ProgramArgs ProgramArgs;

        public static async Task Main(string[] args)
        {
            // listen for work, run msub for array with new work.

            var mainCts = new CancellationTokenSource();
            var mainCt = mainCts.Token;

            Init.EnvInfo();
            Init.CloseNotifications(mainCt);
            Init.CheckX64();
            Init.SetGcMode();

            ProgramArgs = new ProgramArgs(args);

            if (string.IsNullOrWhiteSpace(ProgramArgs.ResultsRootFolder))
            {
                throw new Exception($@"Must specify argument: {nameof(ProgramArgs.ResultsRootFolder)}.");
            }

            var ldrTask = Task.Run(async () => await Loader(mainCt).ConfigureAwait(false));

            await Task.WhenAll(ldrTask).ConfigureAwait(false);
        }

        public static async Task Loader(CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested)
            {
                Logging.LogEvent("Cancellation requested.");
                return;
            }


            while (true)
            {
                if (ct.IsCancellationRequested)
                {
                    Logging.LogEvent("Cancellation requested.");
                    return;
                }

                // 1. run controller - controller will find work position by itself
                var controllerExitNotifyFn = Path.Combine(ProgramArgs.ResultsRootFolder, "pbs", $@"{Guid.NewGuid():N}");

                // generate pbs script (script will write file 'controllerExitNotifyFn' once controller has exited)
                var controllerPbsScript = await MakePbsScriptAsync(controllerExitNotifyFn, nameof(SvmFsCtl), ProgramArgs, 64, isJobArray: false, ct: ct).ConfigureAwait(false);

                // submit pbs script
                var controllerJobId = await Msub(controllerPbsScript.pbsFn, ct: ct).ConfigureAwait(false);

                // 2. controller will now:
                // (2.1) computes list of work to be done,
                // (2.2) write the work list to files (which are lists of indexdata jobs),
                // (2.3) write an exit notification file and exits

                var controllerExitStatus = await WaitForFile(controllerExitNotifyFn, timeout: TimeSpan.FromDays(2), delete: true, ct: ct).ConfigureAwait(false);

                // check success - exit status code file should exist and be set to "0"
                if (!controllerExitStatus.found)
                {
                    Logging.LogEvent($@"Controller: Exit status file not found: {controllerExitNotifyFn}");
                    continue;
                }

                if ((controllerExitStatus.lines?.Length ?? 0 ) == 0)
                {
                    Logging.LogEvent($@"Controller: Exit status file found but empty: {controllerExitNotifyFn}");
                    continue;
                }

                if (controllerExitStatus.lines.First().Trim() != "0")
                {
                    Logging.LogEvent($@"Controller: Exit status file exit code not zero: {controllerExitNotifyFn}");
                    continue;
                }

                if ((controllerExitStatus.files?.Length ?? 0) == 0)
                {
                    Logging.LogEvent($"Controller: Work queue list file was found, but queue is empty. Exiting.", ModuleName);
                    return;
                }

                // 5. if controller exited without error, and there is work to do, then submit array job to hpc scheduler...

                var workFilesFn = "";
                var workFiles = await IoProxy.ReadAllLinesAsync(true, ct, workFilesFn);

                var wkrPbs = await MakePbsScriptAsync(controllerExitNotifyFn, nameof(SvmFsWkr), ProgramArgs, 0, isJobArray: true, 0, controllerExitStatus.files.Length - 1, ct: ct);
                var wkrJobId = await Msub(wkrPbs.pbsFn, ct).ConfigureAwait(false);

                // 6. wait for all of the submitted work to complete...
                var exitFiles = Enumerable.Range(0, controllerExitStatus.files.Length).Select(a => $"{controllerExitNotifyFn}_{a}").ToArray();
                await Task.WhenAll(exitFiles.Select(async a => await WaitForFile(a, true, TimeSpan.FromDays(2), ct).ConfigureAwait(false)).ToArray()).ConfigureAwait(false);

                // 7. delay to not bottleneck login node
                try { await Task.Delay(TimeSpan.FromSeconds(10), ct).ConfigureAwait(false); } catch (Exception) { }

            }
        }

        public static async Task<(bool found, string[] lines)> WaitForFile(string fn, bool delete, TimeSpan timeout, CancellationToken ct = default)
        {
            // this method waits until a specific file exists before returning the contents of the file

            if (ct.IsCancellationRequested)
            {
                Logging.LogEvent("Cancellation requested.");
                return default;
            }


            var timeStart = DateTime.UtcNow;

            while (true)
            {
                if (ct.IsCancellationRequested)
                {
                    Logging.LogEvent("Cancellation requested.");
                    return default;
                }

                if (timeout != default)
                {
                    var elapsed = DateTime.UtcNow - timeStart;

                    var expired = timeout != TimeSpan.Zero && elapsed >= timeout;

                    if (expired)
                    {
                        Logging.LogEvent($@"Timeout: {timeout:dd\:hh\:mm\:ss\.fff}", ModuleName);
                        return default;
                    }
                }

                try
                {
                    if (File.Exists(fn) && new FileInfo(fn).Length > 0)
                    {
                        Logging.LogEvent($"File found: {fn}");

                        var lines = await IoProxy.ReadAllLinesAsync(true, ct, fn, 50);

                        if (delete)
                        {
                            try
                            {
                                File.Delete(fn);
                            }
                            catch (Exception e2)
                            {
                                Logging.LogException(e2, "", ModuleName);
                            }
                        }

                        return (true, lines);
                    }
                }
                catch (Exception e)
                {
                    Logging.LogException(e, "", ModuleName);
                }


                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(10), ct).ConfigureAwait(false);
                }
                catch (Exception)
                {

                }
            }
        }

        /*public static async Task<string[]> FindWork(CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested)
            {
                Logging.LogEvent("Cancellation requested.");
                return default;
            }

            var loadFolder = Path.Combine(CacheLoad.GetIterationFolder(ProgramArgs.ResultsRootFolder, ProgramArgs.ExperimentName), "work_queue");
            

            var workFiles = await IoProxy.GetFilesAsync(true, ct, $@"", "work_*.csv", SearchOption.TopDirectoryOnly).ConfigureAwait(false);

            return workFiles;
            // check iteration directory for work file?
        }*/





        internal static async Task<string> Msub(string pbsScriptFn, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested)
            {
                Logging.LogEvent("Cancellation requested.");
                return default;
            }

            var psi = new ProcessStartInfo()
            {
                FileName = $@"msub",
                //Arguments = (array ? $@"-t {array_start}-{array_end}:{array_step} " : "") + pbs_script_filename,
                Arguments = pbsScriptFn,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true,
            };

            var cmd_line = $"{psi.FileName} {psi.Arguments}";
            var jobId = "";
            var exitCode = (int?)null;
            var tries = 0;
            var maxTries = 1_000_000;

            Logging.LogEvent(cmd_line, ModuleName);

            while (tries < maxTries)
            {
                if (ct.IsCancellationRequested)
                {
                    Logging.LogEvent("Cancellation requested.");
                    return default;
                }

                try
                {
                    tries++;

                    Logging.LogEvent($"Starting process: {psi.FileName} {psi.Arguments}");
                    using var process = Process.Start(psi);

                    if (process != null)
                    {
                        //var stdout_task = process.StandardOutput.ReadToEndAsync();
                        //var stderr_task = process.StandardError.ReadToEndAsync();

                        try { await Task.WhenAny(process.WaitForExitAsync(ct), Task.Delay(TimeSpan.FromSeconds(60), ct)).ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e); }

                        var killed = false;

                        if (!process.HasExited)
                        {
                            try
                            {
                                process.Kill(true);

                                if (!process.HasExited)
                                {
                                    await Task.WhenAny(process.WaitForExitAsync(ct), Task.Delay(TimeSpan.FromSeconds(60), ct)).ConfigureAwait(false);
                                }
                            }
                            catch (Exception e)
                            {
                                Logging.LogException(e, "", ModuleName);
                            }

                            killed = true;
                        }

                        //Task.WaitAll(new Task[] { stdout_task, stderr_task }, new TimeSpan(0, 1, 0));

                        // todo: check libsvm calling code to compare with this code
                        //await Task.WhenAll(stdout_task, stderr_task);

                        //WaitAll(new Task[] { stdout_task, stderr_task }, new TimeSpan(0, 1, 0));

                        var stdoutResult = process?.StandardOutput?.ReadToEnd() ?? "";
                        var stderrResult = process?.StandardError?.ReadToEnd() ?? "";

                        if (!string.IsNullOrWhiteSpace(stdoutResult)) stdoutResult.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => Logging.LogEvent($@"{nameof(stdoutResult)}: {a}", ModuleName));
                        if (!string.IsNullOrWhiteSpace(stderrResult)) stderrResult.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => Logging.LogEvent($@"{nameof(stderrResult)}: {a}", ModuleName));

                        exitCode = process.HasExited ? (int?)process.ExitCode : null;

                        if (process.ExitCode == 0 && !string.IsNullOrWhiteSpace(stdoutResult))
                        {
                            jobId = stdoutResult.Trim().Split().LastOrDefault() ?? "";

                            if (jobId.StartsWith($@"Moab.", StringComparison.OrdinalIgnoreCase))
                            {
                                Logging.LogEvent($@"Success: Job submitted. Job ID: {nameof(jobId)} = ""{jobId}"". (""{cmd_line}"").", ModuleName);

                                break;
                            }
                            else
                            {
                                Logging.LogEvent($@"Error: Invalid Job ID: {nameof(jobId)} = ""{jobId}"". ""{cmd_line}"" failed. Exit code: {exitCode}. ( ""{pbsScriptFn}"" )", ModuleName);

                                break;
                            }
                        }
                        else if (killed)
                        {
                            Logging.LogEvent($@"Error: Killed process. Process did not respond. Tries: {tries}. Exit code: {exitCode}. Stdout: {(stdoutResult?.Length ?? 0)}. Stderr: {(stderrResult?.Length ?? 0)}. ( ""{pbsScriptFn}"" )", ModuleName);
                        }
                        else
                        {
                            Logging.LogEvent($@"Error: Non zero exit code / no stdout. Tries: {tries}. Exit code: {exitCode}. ( ""{pbsScriptFn}"" )", ModuleName);
                        }
                    }
                    else
                    {
                        Logging.LogEvent($@"Error: Process could not be launched. Tries: {tries}. ( ""{pbsScriptFn}"" )", ModuleName);
                    }

                }
                catch (Exception e)
                {
                    Logging.LogException(e, $"{nameof(pbsScriptFn)} = {pbsScriptFn}", ModuleName);
                }

                Logging.LogEvent($@"Error: process could not start. Tries: {tries}. Exit code: {exitCode}. ( ""{pbsScriptFn}"" )", ModuleName);

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(15), ct).ConfigureAwait(false);
                }
                catch (Exception)
                {
                }
            }

            return jobId;
        }

        public static async Task<(string pbsFn, List<string> pbs_script_lines, string run_line)> MakePbsScriptAsync
           (
               string exitNotifyFn,
               string programName,
               ProgramArgs programArgs,
               int pbsPpn = 1,
               bool isJobArray = false,
               int arrayIndexFirst = 0,
               int arrayIndexLast = 0,
               int arrayStepSize = 1,
               int arrayConcurrentLimit = 0,
               bool rerunnable = true,
               CancellationToken ct = default
           )
        {
            //var jobId = programArgs.JobId;
            //var jobName = programArgs.JobName;
            //var wholeArrayLength = programArgs.WholeArrayLength;
            //var partitionArrayIndexFirst = programArgs.PartitionArrayIndexFirst;
            //var wholeArrayIndexFirst = programArgs.WholeArrayIndexFirst;
            //var wholeArrayIndexLast = programArgs.WholeArrayIndexLast;
            //var wholeArrayStepSize = programArgs.WholeArrayStepSize;
            //var resultsRootFolder = programArgs.ResultsRootFolder;


            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested)
            {
                Logging.LogExit(ModuleName);
                return default;
            }

            if (programArgs == null) throw new Exception();
            if (string.IsNullOrEmpty(programArgs.ExperimentName)) throw new Exception();



            //const string MethodName = nameof(MakePbsScriptAsync);

            if (isJobArray && arrayStepSize == 0) throw new ArgumentOutOfRangeException(nameof(arrayStepSize));
            //-ExperimentName test2 -job_id _ -job_name _ -instance_array_index_start 0 -array_instances 1 -array_start 0 -array_end 6929 -array_step 6930 -inner_folds 5 -outer_cv_folds 5 -outer_cv_folds_to_run 1 -repetitions 5


            var pbsScriptLines = new List<string>();

            //var runLocation = Assembly.GetExecutingAssembly().Location;

            var programRuntime = "";
            if (string.Equals(programName, nameof(SvmFsLdr), StringComparison.OrdinalIgnoreCase)) programRuntime = ProgramArgs.PathSvmFsLdr;
            else if (string.Equals(programName, nameof(SvmFsCtl), StringComparison.OrdinalIgnoreCase)) programRuntime = ProgramArgs.PathSvmFsCtl;
            else if (string.Equals(programName, nameof(SvmFsWkr), StringComparison.OrdinalIgnoreCase)) programRuntime = ProgramArgs.PathSvmFsWkr;
            else throw new ArgumentOutOfRangeException(nameof(programName));

            
            if (string.IsNullOrWhiteSpace(programRuntime)) 
            {
                throw new Exception($@"{nameof(programRuntime)} is empty."); 
            }


            //var isWin = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
                                                                                               

            var envPbsArray = @"%J_%I";
            var envJobid = @"${PBS_JOBID}${MOAB_JOBID}";
            var envJobname = @"${PBS_JOBNAME}${MOAB_JOBNAME}";
            var envJobArrayIndex = @"${PBS_ARRAYID}${MOAB_JOBARRAYINDEX}";
            //var envJobArrayLength = @"${MOAB_JOBARRAYRANGE}";


            //var pbs_args = new program_args();


            TimeSpan? pbsWalltime = TimeSpan.FromHours(240);

            var pbsExecutionDirectory = string.Join(Path.DirectorySeparatorChar, programArgs.ResultsRootFolder, "pbs", programArgs.ExperimentName);

            var pbsJobname = $@"{programArgs.ExperimentName}_{Path.GetFileNameWithoutExtension(programRuntime)}";
            var pbsMailAddr = "";
            var pbsMailOpt = "n";
            var pbsMem = "";

            // for each job, request 1 node with 64 vcpu, by default, if ppn not specified
            var pbsNodes = 1; // set to 1 because these workloads cannot be split over multiple nodes
            if (pbsPpn <= 0) pbsPpn = 64;

            var pbsStdoutFilename = $@"{string.Join("_", new[] { programArgs.ExperimentName, nameof(SvmFsCtl), isJobArray ? envPbsArray : "" }.Where(a => !string.IsNullOrWhiteSpace(a)).ToArray())}.pbs.stdout";
            var pbsStderrFilename = $@"{string.Join("_", new[] { programArgs.ExperimentName, nameof(SvmFsCtl), isJobArray ? envPbsArray : "" }.Where(a => !string.IsNullOrWhiteSpace(a)).ToArray())}.pbs.stderr";

            var programStdoutFilename = $@"{string.Join("_", new[] { programArgs.ExperimentName, nameof(SvmFsCtl), envJobid, envJobname, envJobArrayIndex }.Where(a => !string.IsNullOrWhiteSpace(a)).ToArray())}.program.stdout";
            var programStderrFilename = $@"{string.Join("_", new[] { programArgs.ExperimentName, nameof(SvmFsCtl), envJobid, envJobname, envJobArrayIndex }.Where(a => !string.IsNullOrWhiteSpace(a)).ToArray())}.program.stderr";


            // 1. pbs directives
            if (isJobArray) pbsScriptLines.Add($@"#PBS -t {arrayIndexFirst}-{arrayIndexLast}" + (arrayStepSize != 1 ? $@":{arrayStepSize}" : "") + (arrayConcurrentLimit != 0 ? $@"%{arrayConcurrentLimit}" : "")); // Notify scheduler that this is submission should be run as an array
            if (pbsWalltime != null && pbsWalltime.Value.TotalSeconds > 0) pbsScriptLines.Add($@"#PBS -l walltime={Math.Floor(pbsWalltime.Value.TotalHours):00}:{pbsWalltime.Value.Minutes:00}:{pbsWalltime.Value.Seconds:00}"); // TotalHours because anything larger than an hour (e.g. a day, week, year) wouldn't be included if using Hours
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
                pbsProgramArgs.Add((nameof(programArgs.NodeIndex), envJobArrayIndex));

                // total number of jobs in the array
                //pbsProgramArgs.Add((nameof(programArgs.TotalNodes), envJobArrayLength));
                pbsProgramArgs.Add((nameof(programArgs.TotalNodes), $"{programArgs.TotalNodes}"));

                pbsProgramArgs.Add((nameof(programArgs.ProcessorsPerNode), $"{programArgs.ProcessorsPerNode}"));


                //// first index for the hpc instance job to start from
                //pbsProgramArgs.Add((nameof(programArgs.PartitionArrayIndexFirst), envJobArrayIndex));

                //// total number of jobs in the array
                //pbsProgramArgs.Add((nameof(programArgs.WholeArrayLength), envJobArrayLength));

                //// pbs -t array start, end, and step (requested to the scheduler with #pbs -t start-end:step)
                //pbsProgramArgs.Add((nameof(programArgs.WholeArrayIndexFirst), $@"{arrayIndexFirst}"));
                //pbsProgramArgs.Add((nameof(programArgs.WholeArrayIndexLast), $@"{arrayIndexLast}"));
                //pbsProgramArgs.Add((nameof(programArgs.WholeArrayStepSize), $@"{arrayStepSize}"));

                var arrayRange = Routines.Range(arrayIndexFirst, arrayIndexLast, arrayStepSize);
                Logging.WriteLine($@"{nameof(arrayRange)}: {string.Join(@", ", arrayRange)}", ModuleName);
            }

            foreach (var programArg in programArgs.Args)
            {
                var keyExists = pbsProgramArgs.All(pbsProgramArg => !string.Equals(pbsProgramArg.key, programArg.key, StringComparison.OrdinalIgnoreCase));

                if (!keyExists) pbsProgramArgs.Add((programArg.key, $@"{programArg.asStr}"));
            }

            if (!string.IsNullOrEmpty(programStdoutFilename)) pbsProgramArgs.Add((@"1>", programStdoutFilename));
            if (!string.IsNullOrEmpty(programStderrFilename)) pbsProgramArgs.Add((@"2>", programStderrFilename));

            var runLine = $@"{programRuntime} {string.Join(" ", pbsProgramArgs.Select(a => string.Join(@" ", new[] { $@"-{a.key}", a.value }.Where(c => !string.IsNullOrWhiteSpace(c)).ToArray())).Where(b => !string.IsNullOrWhiteSpace(b)).ToArray())}";

            if (!string.IsNullOrWhiteSpace(pbsExecutionDirectory)) pbsScriptLines.Add($@"cd {pbsExecutionDirectory}");
            pbsScriptLines.Add($@"module load GCCcore");
            pbsScriptLines.Add(runLine);

            // after program has exited, write the exit code to a text file
            if (!string.IsNullOrEmpty(exitNotifyFn))
            {
                if (isJobArray && !Path.GetFileNameWithoutExtension(exitNotifyFn).Contains(envJobArrayIndex))
                {
                    var d = Path.GetDirectoryName(exitNotifyFn);
                    var f = Path.GetFileNameWithoutExtension(exitNotifyFn) + $"_{envJobArrayIndex}";
                    var e = Path.GetExtension(exitNotifyFn);

                    exitNotifyFn = !string.IsNullOrEmpty(d) ? Path.Combine(d, f + e) : f + e;
                }

                // print program exit code to exitNotifyFn (i.e. filename: "_array_index")
                pbsScriptLines.Add($@"echo $? > {exitNotifyFn}");
            }


            var pbsFn = Path.Combine(pbsExecutionDirectory, $@"{pbsJobname}.pbs");

            await IoProxy.WriteAllLinesAsync(true, ct, pbsFn, pbsScriptLines).ConfigureAwait(false);
            Logging.WriteLine($@"{programArgs.ExperimentName}: Saved PBS script. File: {pbsFn}.");

            Logging.LogExit(ModuleName);

            return ct.IsCancellationRequested ? default : (pbsFn, pbsScriptLines, runLine);
        }

    }
}
