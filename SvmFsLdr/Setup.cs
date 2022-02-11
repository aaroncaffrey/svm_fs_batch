using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Reflection;
using SvmFsLib;

namespace SvmFsLdr
{
    public class Setup
    {
        public const string ModuleName = nameof(Setup);



        //public static async Task SetupPbsJobAsync(ProgramArgs programArgs, CancellationToken ct)
        //{
        //    Logging.LogCall(ModuleName);

        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

        //    const string MethodName = nameof(Setup);


        //    programArgs.Setup = false;
        //    //program_args.args.RemoveAll(a => string.Equals(a.key, nameof(program_args.setup), StringComparison.OrdinalIgnoreCase));
        //    //
        //    //programArgs.ExperimentName += $@"_{DateTime.UtcNow:yyyy_MM_dd_HH_mm_ss_fff}";
        //    programArgs.ExperimentName += $@"_{DateTime.UtcNow:yyyyMMddHHmmssfffffff}";

        //    programArgs.SetupTotalVcpus = programArgs.SetupTotalVcpus <= 0 ? 1504 : programArgs.SetupTotalVcpus;
        //    programArgs.SetupInstanceVcpus = programArgs.SetupInstanceVcpus <= 0 ? 64 : programArgs.SetupInstanceVcpus;
        //    var setupTotalInstances = (int)Math.Floor(programArgs.SetupTotalVcpus / (double)programArgs.SetupInstanceVcpus);

        //    var setupArrayStart = 0;
        //    var setupArrayEnd = setupTotalInstances - 1;
        //    var setupArrayStep = 1;

        //    Logging.WriteLine($@"{programArgs.ExperimentName}: {nameof(programArgs.SetupTotalVcpus)} = {programArgs.SetupTotalVcpus}", ModuleName, MethodName);
        //    Logging.WriteLine($@"{programArgs.ExperimentName}: {nameof(programArgs.SetupInstanceVcpus)} = {programArgs.SetupInstanceVcpus}", ModuleName, MethodName);
        //    Logging.WriteLine($@"{programArgs.ExperimentName}: {nameof(setupTotalInstances)} = {setupTotalInstances}", ModuleName, MethodName);
        //    Logging.WriteLine();

        //    Logging.WriteLine($@"{programArgs.ExperimentName}: {nameof(setupArrayStart)} = {setupArrayStart}", ModuleName, MethodName);
        //    Logging.WriteLine($@"{programArgs.ExperimentName}: {nameof(setupArrayEnd)} = {setupArrayEnd}", ModuleName, MethodName);
        //    Logging.WriteLine($@"{programArgs.ExperimentName}: {nameof(setupArrayStep)} = {setupArrayStep}", ModuleName, MethodName);
        //    Logging.WriteLine();

        //    // todo: check if the parameters are all given, given in correct order, and everything makes sense
        //    var arrayConcurrentLimit = 0;
        //    var pbsScript = await MakePbsScriptAsync(programArgs, programArgs.SetupInstanceVcpus, true, setupArrayStart, setupArrayEnd, setupArrayStep, arrayConcurrentLimit, ct: ct).ConfigureAwait(false);

        //    for (var index = 0; index < pbsScript.pbs_script_lines.Count; index++) Logging.WriteLine($"{index,3}: {pbsScript.pbs_script_lines[index]}", ModuleName, MethodName);

        //    Logging.WriteLine();

        //    Logging.LogExit(ModuleName);
        //}

       
    }
}