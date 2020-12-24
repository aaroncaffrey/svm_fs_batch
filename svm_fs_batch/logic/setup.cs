using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal class setup
    {
        public const string module_name = nameof(setup);

        internal static void setup_pbs_job(CancellationTokenSource cts, program_args program_args)
        {
            const string method_name = nameof(setup);

            if (cts.IsCancellationRequested) return;

            program_args.setup = false;
            //program_args.args.RemoveAll(a => string.Equals(a.key, nameof(program_args.setup), StringComparison.OrdinalIgnoreCase));

            program_args.experiment_name += $@"_{DateTime.Now:yyyy_MM_dd_HH_mm_ss_fff}";

            program_args.setup_total_vcpus = program_args.setup_total_vcpus <= 0 ? 1504 : program_args.setup_total_vcpus;
            program_args.setup_instance_vcpus = program_args.setup_instance_vcpus <= 0 ? 64 : program_args.setup_instance_vcpus;
            var setup_total_instances = (int)Math.Floor((double)program_args.setup_total_vcpus / (double)program_args.setup_instance_vcpus);

            var setup_array_start = 0;
            var setup_array_end = setup_total_instances - 1;
            var setup_array_step = 1;

            io_proxy.WriteLine($@"{program_args.experiment_name}: {nameof(program_args.setup_total_vcpus)} = {program_args.setup_total_vcpus}", module_name, method_name);
            io_proxy.WriteLine($@"{program_args.experiment_name}: {nameof(program_args.setup_instance_vcpus)} = {program_args.setup_instance_vcpus}", module_name, method_name);
            io_proxy.WriteLine($@"{program_args.experiment_name}: {nameof(setup_total_instances)} = {setup_total_instances}", module_name, method_name);
            io_proxy.WriteLine("");

            io_proxy.WriteLine($@"{program_args.experiment_name}: {nameof(setup_array_start)} = {setup_array_start}", module_name, method_name);
            io_proxy.WriteLine($@"{program_args.experiment_name}: {nameof(setup_array_end)} = {setup_array_end}", module_name, method_name);
            io_proxy.WriteLine($@"{program_args.experiment_name}: {nameof(setup_array_step)} = {setup_array_step}", module_name, method_name);
            io_proxy.WriteLine("");


            var pbs_script = make_pbs_script(cts, program_args, program_args.setup_instance_vcpus, true, setup_array_start, setup_array_end, setup_array_step, true);

            for (var index = 0; index < pbs_script.pbs_script_lines.Count; index++)
            {
                io_proxy.WriteLine($"{((index).ToString().PadLeft(3))}: {pbs_script.pbs_script_lines[index]}", module_name, method_name);
            }

            io_proxy.WriteLine("");
        }

        internal static (List<string> pbs_script_lines, string run_line) make_pbs_script(CancellationTokenSource cts, program_args program_args, int pbs_ppn = 1, bool is_job_array = false, int array_index_first = 0, int array_index_last = 0, int array_step_size = 1, bool rerunnable = true)
        {
            const string method_name = nameof(make_pbs_script);

            if (is_job_array && array_step_size == 0) throw new ArgumentOutOfRangeException(nameof(array_step_size));
            //-experiment_name test2 -job_id _ -job_name _ -instance_array_index_start 0 -array_instances 1 -array_start 0 -array_end 6929 -array_step 6930 -inner_folds 5 -outer_cv_folds 5 -outer_cv_folds_to_run 1 -repetitions 5



            var pbs_script_lines = new List<string>();

            var program_runtime = Process.GetCurrentProcess().MainModule?.FileName;

            if (string.IsNullOrWhiteSpace(program_runtime)) throw new Exception();

            var is_win = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

            //is_win = false;

            var env_pbs_array = !is_win ? @"%J_%I" : @"_";

            var env_jobid = !is_win ? @"${PBS_JOBID}${MOAB_JOBID}" : (program_args.args_key_exists(nameof(program_args.job_id)) ? $"{program_args.job_id}" : "_");
            var env_jobname = !is_win ? @"${PBS_JOBNAME}${MOAB_JOBNAME}" : (program_args.args_key_exists(nameof(program_args.job_name)) ? $"{program_args.job_name}" : "_");


            var env_job_array_index = !is_win ? @"${PBS_ARRAYID}${MOAB_JOBARRAYINDEX}" : (program_args.args_key_exists(nameof(program_args.partition_array_index_first)) ? $"{program_args.partition_array_index_first}" : "0");
            var env_job_array_length = !is_win ? @"${MOAB_JOBARRAYRANGE}" : (program_args.args_key_exists(nameof(program_args.whole_array_length)) ? $"{program_args.whole_array_length}" : "1");


            //var pbs_args = new program_args();



            TimeSpan? pbs_walltime = TimeSpan.FromHours(240);

            var pbs_execution_directory = string.Join(Path.DirectorySeparatorChar, new string[] { settings.svm_fs_batch_home, "pbs", program_args.experiment_name });

            var pbs_jobname = $@"{program_args.experiment_name}_{nameof(svm_fs_batch)}";
            var pbs_mail_addr = "";
            var pbs_mail_opt = "n";
            var pbs_mem = "";

            // for each job, request 1 node with 64 vcpu, by default, if ppn not specified
            int pbs_nodes = 1;
            if (pbs_ppn <= 0) pbs_ppn = 64;

            var pbs_stdout_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), (is_job_array ? env_pbs_array : "") }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.pbs.stdout";
            var pbs_stderr_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), (is_job_array ? env_pbs_array : "") }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.pbs.stderr";

            var program_stdout_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), env_jobid, env_jobname, env_job_array_index }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.program.stdout";
            var program_stderr_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), env_jobid, env_jobname, env_job_array_index }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.program.stderr";




            // 1. pbs directives
            if (is_job_array) pbs_script_lines.Add($@"#PBS -t {array_index_first}-{array_index_last}:{array_step_size}");

            if (pbs_walltime != null && pbs_walltime.Value.TotalSeconds > 0) pbs_script_lines.Add($@"#PBS -l walltime={Math.Floor(pbs_walltime.Value.TotalHours):00}:{pbs_walltime.Value.Minutes:00}:{pbs_walltime.Value.Seconds:00}");
            if (pbs_nodes > 0) pbs_script_lines.Add($@"#PBS -l nodes={pbs_nodes}{(pbs_ppn > 0 ? $@":ppn={pbs_ppn}" : $@"")}");
            if (!string.IsNullOrWhiteSpace(pbs_mem)) pbs_script_lines.Add($@"#PBS -l mem={pbs_mem}");
            pbs_script_lines.Add($@"#PBS -r {(rerunnable ? "y" : "n")}");
            if (!string.IsNullOrWhiteSpace(pbs_jobname)) pbs_script_lines.Add($@"#PBS -N {pbs_jobname}");
            if (!string.IsNullOrWhiteSpace(pbs_mail_opt)) pbs_script_lines.Add($@"#PBS -m {pbs_mail_opt}");
            if (!string.IsNullOrWhiteSpace(pbs_mail_addr)) pbs_script_lines.Add($@"#PBS -M {pbs_mail_addr}");
            if (!string.IsNullOrWhiteSpace(pbs_stdout_filename)) pbs_script_lines.Add($@"#PBS -o {pbs_stdout_filename}");
            if (!string.IsNullOrWhiteSpace(pbs_stderr_filename)) pbs_script_lines.Add($@"#PBS -e {pbs_stderr_filename}");
            if (!string.IsNullOrWhiteSpace(pbs_execution_directory)) pbs_script_lines.Add($@"#PBS -d {pbs_execution_directory}");

            // 2. program directives
            var pbs_program_args = new List<(string key, string value)>();

            // experiment name
            pbs_program_args.Add((nameof(program_args.experiment_name), program_args.experiment_name));

            // job id, job name
            pbs_program_args.Add((nameof(program_args.job_id), env_jobid));
            pbs_program_args.Add((nameof(program_args.job_name), env_jobname));

            if (is_job_array)
            {
                // first index for the hpc instance job to start from
                pbs_program_args.Add((nameof(program_args.partition_array_index_first), env_job_array_index));

                // total number of jobs in the array
                pbs_program_args.Add((nameof(program_args.whole_array_length), env_job_array_length));

                // pbs -t array start, end, and step (requested to the scheduler with #pbs -t start-end:step)
                pbs_program_args.Add((nameof(program_args.whole_array_index_first), $@"{array_index_first}"));
                pbs_program_args.Add((nameof(program_args.whole_array_index_last), $@"{array_index_last}"));
                pbs_program_args.Add((nameof(program_args.whole_array_step_size), $@"{array_step_size}"));

                var array_range = routines.range(array_index_first, array_index_last, array_step_size);
                io_proxy.WriteLine($@"{nameof(array_range)}: {string.Join($@", ", array_range)}", module_name, method_name);

            }

            foreach (var program_arg in program_args.args)
            {
                var key_exists = pbs_program_args.All(pbs_program_arg => !string.Equals(pbs_program_arg.key, program_arg.key, StringComparison.OrdinalIgnoreCase));

                if (!key_exists)
                {
                    pbs_program_args.Add((program_arg.key, $@"{program_arg.as_str}"));
                }
            }

            if (!string.IsNullOrEmpty(program_stdout_filename)) pbs_program_args.Add(($@"1>", program_stdout_filename));
            if (!string.IsNullOrEmpty(program_stderr_filename)) pbs_program_args.Add(($@"2>", program_stderr_filename));

            var run_line = $@"{program_runtime} {string.Join(" ", pbs_program_args.Select(a => string.Join($@" ", new[] { $@"-{a.key}", a.value }.Where(c => !string.IsNullOrWhiteSpace(c)).ToArray())).Where(b => !string.IsNullOrWhiteSpace(b)).ToArray())}";

            if (!string.IsNullOrWhiteSpace(pbs_execution_directory)) pbs_script_lines.Add($@"cd {pbs_execution_directory}");
            pbs_script_lines.Add($@"module load GCCcore");
            pbs_script_lines.Add(run_line);

            var pbs_fn = Path.Combine(pbs_execution_directory, $@"{pbs_jobname}.pbs");

            io_proxy.WriteAllLines(cts, pbs_fn, pbs_script_lines);
            io_proxy.WriteLine($@"{program_args.experiment_name}: Saved PBS script. File: {pbs_fn}.");

            return (pbs_script_lines, run_line);
        }
    }
}
