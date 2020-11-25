using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal class program
    {
        internal static
            List<(
                (string file_tag, string alphabet, string dimension, string category, string source, string @group, string member, string perspective) grouped_by_key,
                (int internal_column_index, int external_column_index, string file_tag, string alphabet, string dimension, string category, string source, string @group, string member, string perspective)[] grouped_list,
                int[] grouped_list_internal_column_indexes
                )>
            get_groups_to_use(dataset_loader dataset)
        {
            var groups = dataset.get_groups
            (
                file_tag: true, 
                alphabet: true,
                dimension: true, 
                category: true, 
                source: true, 
                @group: true, 
                member: false, 
                perspective: false
            );

            groups = groups.Take(3).ToList();

            return groups;
        }

            internal static void Main(string[] args)
        {
            //-experiment_name _20201028084510741 -job_id _ -job_name _ -instance_array_index_start 0 -array_instances 1 -array_start 0 -array_end 6929 -array_step 6930 -inner_folds 1 -outer_cv_folds 5 -outer_cv_folds_to_run 1 -repetitions 1

            //var x=confusion_matrix.load($@"C:\mmfs1\data\scratch\k1040015\svm_fs_batch\results\test\it_5\x_it-5_gr-5_sv-1_kr-3_sc-2_rn-1_oc-10_ic-10_ix-1-5.cm.csv");

            // debug cmd line parameters: -experiment_name test2 -array_start 0 -array_end 4 -array_index 0 -array_step 5 -array_instances 1 -array_last_index -1


            io_proxy.WriteLine($@"cmd line: {Environment.CommandLine}", nameof(program), nameof(Main));
            io_proxy.WriteLine($@"processor count: {Environment.ProcessorCount}", nameof(program), nameof(Main));

            var main_cts = new CancellationTokenSource();
            init.close_notifications(main_cts);
            init.check_x64();
            init.set_gc_mode();
            init.set_thread_counts();

            var program_args = new program_args(args);

            //-experiment_name test_20201025014739579 -job_id _ -job_name _ -array_index _ -array_instances _ -array_start 0 -array_end 6929 -array_step 385

            if (program_args.run_local)
            {
                program_args.experiment_name += $@"_{DateTime.Now:yyyyMMddHHmmssfff}";


                program_args.run_local = false;
                program_args.args.RemoveAll(a => a.key == nameof(program_args.run_local));

                // set other vars

                if (program_args.args.All(a => a.key != nameof(program_args.inner_folds)))
                {
                    program_args.inner_folds = 1;
                    program_args.args.Add((nameof(program_args.inner_folds), program_args.inner_folds, $"{program_args.inner_folds}"));
                }

                if (program_args.args.All(a => a.key != nameof(program_args.outer_cv_folds)))
                {
                    program_args.outer_cv_folds = 5;
                    program_args.args.Add((nameof(program_args.outer_cv_folds), program_args.outer_cv_folds, $"{program_args.outer_cv_folds}"));
                }

                if (program_args.args.All(a => a.key != nameof(program_args.outer_cv_folds_to_run)))
                {
                    program_args.outer_cv_folds_to_run = 1;
                    program_args.args.Add((nameof(program_args.outer_cv_folds_to_run), program_args.outer_cv_folds_to_run, $"{program_args.outer_cv_folds_to_run}"));
                }

                if (program_args.args.All(a => a.key != nameof(program_args.repetitions)))
                {
                    program_args.repetitions = 1;
                    program_args.args.Add((nameof(program_args.repetitions), program_args.repetitions, $"{program_args.repetitions}"));
                }

                //if (program_args.args.All(a => a.key != nameof(program_args.instance_array_index_start)))
                //{
                //    program_args.instance_array_index_start = 0;
                //    program_args.args.Add((nameof(program_args.instance_array_index_start), program_args.instance_array_index_start, $"{program_args.instance_array_index_start}"));
                //}

                //if (program_args.args.All(a => a.key != nameof(program_args.array_instances)))
                //{
                //    program_args.array_instances = 1;
                //    program_args.args.Add((nameof(program_args.array_instances), program_args.array_instances, $"{program_args.array_instances}"));
                //}



                // set default vcpu amounts if not specified
                program_args.setup_total_vcpus = Environment.ProcessorCount;
                program_args.setup_instance_vcpus = Environment.ProcessorCount;

                // calculate array size (get total number of groups)
                var dataset = new dataset_loader();

                var groups = get_groups_to_use(dataset);

                // number of jobs
                var setup_array_instances = groups.Count;

                // calculate total number of instanced and array step
                var setup_total_instances = (int)Math.Floor((double)program_args.setup_total_vcpus / (double)program_args.setup_instance_vcpus);
                var setup_array_step = (int)Math.Ceiling((double)setup_array_instances / (double)setup_total_instances);

                var ps = make_pbs_script(program_args, program_args.setup_instance_vcpus, true, 0, setup_array_instances - 1, setup_array_step, true);

                Console.WriteLine(ps.run_line);

                return;
            }

            if (program_args.setup)
            {
                program_args.experiment_name += $@"_{DateTime.Now:yyyy_MM_dd_HH_mm_ss_fff}";

                // set default vcpu amounts if not specified
                if (program_args.setup_total_vcpus <= 0) program_args.setup_total_vcpus = 1504 - 320;
                if (program_args.setup_instance_vcpus <= 0) program_args.setup_instance_vcpus = 64;

                // calculate array size (get total number of groups)
                var dataset = new dataset_loader();
                var groups = get_groups_to_use(dataset);

                // todo: group features ...
                var setup_array_instances = groups.Count;

                // calculate total number of instanced and array step
                var setup_total_instances = (int)Math.Floor((double)program_args.setup_total_vcpus / (double)program_args.setup_instance_vcpus);
                var setup_array_step = (int)Math.Ceiling((double)setup_array_instances / (double)setup_total_instances);

                // calculate list of expected indexes
                //var group_index_pairs = new List<(int instance_id, int group_index_first, int group_index_last)>();
                //var instance_id = -1;
                //for (var a = 0; a <= setup_array_instances - 1; a += setup_array_step)
                //{
                //    instance_id++;
                //    group_index_pairs.Add((instance_id, a, a + setup_array_step - 1));
                //}

                io_proxy.WriteLine($@"{program_args.experiment_name}: {nameof(setup_array_instances)}: {setup_array_instances}");
                io_proxy.WriteLine($@"{program_args.experiment_name}: {nameof(program_args.setup_total_vcpus)}: {program_args.setup_total_vcpus}");
                io_proxy.WriteLine($@"{program_args.experiment_name}: {nameof(program_args.setup_instance_vcpus)}: {program_args.setup_instance_vcpus}");
                io_proxy.WriteLine($@"{program_args.experiment_name}: {nameof(setup_total_instances)}: {setup_total_instances}");
                io_proxy.WriteLine($@"{program_args.experiment_name}: {nameof(setup_array_step)}: {setup_array_step}");
                io_proxy.WriteLine("");

                var ps = make_pbs_script(program_args, program_args.setup_instance_vcpus, true, 0, setup_array_instances - 1, setup_array_step, true);

                for (var index = 0; index < ps.pbs_script_lines.Count; index++)
                {
                    io_proxy.WriteLine((index).ToString().PadLeft(3) + ": " + ps.pbs_script_lines[index]);
                }

                io_proxy.WriteLine("");
                return;
            }



            if (string.IsNullOrWhiteSpace(program_args.experiment_name)) { throw new ArgumentOutOfRangeException(nameof(args), $"{nameof(program_args.experiment_name)}: must specify experiment name"); }

            if (program_args.array_start <= -1) { throw new ArgumentOutOfRangeException(nameof(args), nameof(program_args.array_start)); }
            if (program_args.array_end <= -1) { throw new ArgumentOutOfRangeException(nameof(args), nameof(program_args.array_end)); }
            if (program_args.instance_array_index_start <= -1) { throw new ArgumentOutOfRangeException(nameof(args), nameof(program_args.instance_array_index_start)); }
            if (program_args.array_step <= 0) { throw new ArgumentOutOfRangeException(nameof(args), nameof(program_args.array_step)); }
            if (program_args.array_instances <= 0) { throw new ArgumentOutOfRangeException(nameof(args), nameof(program_args.array_instances)); }
            if (program_args.instance_array_index_end <= -1) { program_args.instance_array_index_end = program_args.instance_array_index_start + (program_args.array_step - 1); }

            var instance_id = -1;
            for (var i = program_args.array_start; i <= program_args.instance_array_index_start; i += program_args.array_step) { instance_id++; } // is this correct?

            worker(program_args.experiment_name, instance_id, program_args.array_instances, program_args.instance_array_index_start, program_args.array_step, program_args.instance_array_index_end, program_args.repetitions, program_args.outer_cv_folds, program_args.outer_cv_folds_to_run, program_args.inner_folds);
        }

        internal static (List<string> pbs_script_lines, string run_line) make_pbs_script(program_args program_args, int pbs_ppn = 1, bool is_job_array = false, int job_array_start = 0, int job_array_end = 0, int job_array_step = 1, bool rerunnable = true)
        {

            //const string module_name = nameof(program);
            //const string method_name = nameof(make_pbs_script); 

            var pbs_script_lines = new List<string>();

            var program_runtime = Process.GetCurrentProcess().MainModule?.FileName;

            if (string.IsNullOrWhiteSpace(program_runtime)) throw new Exception();

            var is_win = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);



            var env_pbs_array = is_win ? @"_" : @"%J_%I";
            var env_jobid = is_win ? (program_args.args.Any(a => a.key == nameof(program_args.job_id)) ? $"{program_args.job_id}" : "_") : @"${PBS_JOBID}${MOAB_JOBID}";
            var env_jobname = is_win ? (program_args.args.Any(a => a.key == nameof(program_args.job_name)) ? $"{program_args.job_name}" : "_") : @"${PBS_JOBNAME}${MOAB_JOBNAME}";
            var env_arrayindex = is_win ? (program_args.args.Any(a => a.key == nameof(program_args.instance_array_index_start)) ? $"{program_args.instance_array_index_start}" : "0") : @"${PBS_ARRAYID}${MOAB_JOBARRAYINDEX}";
            var env_arraycount = is_win ? (program_args.args.Any(a => a.key == nameof(program_args.array_instances)) ? $"{program_args.array_instances}" : "1") : @"${MOAB_JOBARRAYRANGE}";


            var pbs_args = new program_args();



            TimeSpan? pbs_walltime = TimeSpan.FromHours(240);
            string pbs_execution_directory = $@"{settings.svm_fs_batch_home}{Path.DirectorySeparatorChar}pbs{Path.DirectorySeparatorChar}{program_args.experiment_name}{Path.DirectorySeparatorChar}";
            string pbs_jobname = $@"{program_args.experiment_name}_{nameof(svm_fs_batch)}";
            string pbs_mail_addr = "";
            string pbs_mail_opt = "n";
            string pbs_mem = null;

            // for each job, request 1 node with 64 vcpu, by default, if ppn not specified
            int pbs_nodes = 1;
            if (pbs_ppn <= 0) pbs_ppn = 64;

            string pbs_stdout_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), (is_job_array ? env_pbs_array : "") }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.pbs.stdout";
            string pbs_stderr_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), (is_job_array ? env_pbs_array : "") }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.pbs.stderr";

            string program_stdout_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), env_jobid, env_jobname, env_arrayindex }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.program.stdout";
            string program_stderr_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), env_jobid, env_jobname, env_arrayindex }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.program.stderr";




            // pbs directives
            if (is_job_array) pbs_script_lines.Add($@"#PBS -t {job_array_start}-{job_array_end}:{job_array_step}");
            if (pbs_walltime != null && pbs_walltime.Value.TotalSeconds > 0) pbs_script_lines.Add($@"#PBS -l walltime={Math.Floor(pbs_walltime.Value.TotalHours):00}:{pbs_walltime.Value.Minutes:00}:{pbs_walltime.Value.Seconds:00}");
            if (pbs_nodes > 0) pbs_script_lines.Add($@"#PBS -l nodes={pbs_nodes}{(pbs_ppn > 0 ? $@":ppn={pbs_ppn}" : "")}");
            if (!string.IsNullOrWhiteSpace(pbs_mem)) pbs_script_lines.Add($@"#PBS -l mem={pbs_mem}");
            pbs_script_lines.Add($@"#PBS -r {(rerunnable ? "y" : "n")}");
            if (!string.IsNullOrWhiteSpace(pbs_jobname)) pbs_script_lines.Add($@"#PBS -N {pbs_jobname}");
            if (!string.IsNullOrWhiteSpace(pbs_mail_opt)) pbs_script_lines.Add($@"#PBS -m {pbs_mail_opt}");
            if (!string.IsNullOrWhiteSpace(pbs_mail_addr)) pbs_script_lines.Add($@"#PBS -M {pbs_mail_addr}");
            if (!string.IsNullOrWhiteSpace(pbs_stdout_filename)) pbs_script_lines.Add($@"#PBS -o {pbs_stdout_filename}");
            if (!string.IsNullOrWhiteSpace(pbs_stderr_filename)) pbs_script_lines.Add($@"#PBS -e {pbs_stderr_filename}");
            if (!string.IsNullOrWhiteSpace(pbs_execution_directory)) pbs_script_lines.Add($@"#PBS -d {pbs_execution_directory}");

            // program directives
            var p = new List<(string key, string value)>();

            if (!string.IsNullOrWhiteSpace(program_args.experiment_name)) p.Add(($@"-{nameof(program_args.experiment_name)}", program_args.experiment_name));

            //program_args.job_id = env_jobid;
            //program_args.job_name = env_jobname;
            ////program_args.instance_array_index_start = env_arrayindex;
            ////program_args.array_instances = env_arraycount;
            //program_args.array_start = job_array_start;
            //program_args.array_end = job_array_end;
            //program_args.array_step = job_array_step;


            p.Add(($@"-{nameof(program_args.job_id)}", env_jobid));
            p.Add(($@"-{nameof(program_args.job_name)}", env_jobname));

            if (is_job_array && job_array_step != 0) p.Add(($@"-{nameof(program_args.instance_array_index_start)}", env_arrayindex));
            if (is_job_array && job_array_step != 0) p.Add(($@"-{nameof(program_args.array_instances)}", env_arraycount));

            if (is_job_array && job_array_step != 0) p.Add(($@"-{nameof(program_args.array_start)}", job_array_start.ToString()));
            if (is_job_array && job_array_step != 0) p.Add(($@"-{nameof(program_args.array_end)}", job_array_end.ToString()));
            if (is_job_array && job_array_step != 0) p.Add(($@"-{nameof(program_args.array_step)}", job_array_step.ToString()));

            foreach (var arg in program_args.args)
            {
                p.Add(($@"-{arg.key}", $@"{arg.str_value}"));
            }

            if (!string.IsNullOrEmpty(program_stdout_filename)) p.Add(($@"1>", program_stdout_filename));
            if (!string.IsNullOrEmpty(program_stderr_filename)) p.Add(($@"2>", program_stderr_filename));

            var run_line = $@"{program_runtime} {string.Join(" ", p.Select(a => string.Join(" ", new[] { a.key, a.value }.Where(c => !string.IsNullOrWhiteSpace(c)).ToList())).Where(b => !string.IsNullOrWhiteSpace(b)).ToList())}";

            if (!string.IsNullOrWhiteSpace(pbs_execution_directory)) pbs_script_lines.Add($@"cd {pbs_execution_directory}");
            pbs_script_lines.Add($@"module load GCCcore");
            pbs_script_lines.Add(run_line);

            var pbs_fn = Path.Combine(pbs_execution_directory, $@"{pbs_jobname}.pbs");

            io_proxy.WriteAllLines(pbs_fn, pbs_script_lines);
            io_proxy.WriteLine($@"{program_args.experiment_name}: Saved PBS script. File: {pbs_fn}.");

            return (pbs_script_lines, run_line);
        }


        internal static string get_iteration_folder(string results_root_folder, string experiment_name, int iteration_index, string iteration_name = null, int? group_index = null)
        {
            var hr = false;

            var it = $@"it_{(iteration_index + (hr ? 1 : 0))}" + (!string.IsNullOrWhiteSpace(iteration_name) ? iteration_name : "");

            if (group_index == null) return Path.Combine(results_root_folder, experiment_name, it);

            var gr = $@"gr_{(group_index + (hr ? 1 : 0))}";
            return Path.Combine(results_root_folder, experiment_name, it, gr);
        }


        public static List<(int from, int to, int step)> get_ranges(List<int> ids)
        {
            if (ids == null || ids.Count == 0) return new List<(int @from, int to, int step)>();

            ids = ids.OrderBy(a => a).Distinct().ToList();

            if (ids.Count == 1) return new List<(int @from, int to, int step)>() { (ids[0], ids[0], 0) };

            var ranges = new List<(int @from, int to, int step)>();

            var step = ids[1] - ids[0];
            var step_start_index = 0;

            for (var i = 1; i < ids.Count; i++)
            {
                // if step has changed, save last
                if (ids[i] - ids[i - 1] != step)
                {
                    ranges.Add((ids[step_start_index], ids[step_start_index] + (step * (i - step_start_index - 1)), step));

                    step = ids[i] - ids[i - 1];
                    step_start_index = i - 1;
                    i--;
                    continue;
                }

                if (i == ids.Count - 1)
                {
                    step = step_start_index == i ? 1 : step;
                    ranges.Add((ids[step_start_index], ids[step_start_index] + (step * (i - step_start_index)), step));
                }
            }

            return ranges;
        }

        public static string get_ranges_str(List<int> ids)
        {
            var ranges = get_ranges(ids);

            return string.Join("_", ranges.Select(range => $@"{range.@from}" + (range.from != range.to ? $@"-{range.to}" + (range.step != -1 && range.step != 0 && range.step != +1 ? $@":{range.step}" : $@"") : $@"")).ToList());
        }


        internal static void worker(
            //dataset_loader dataset,
            string experiment_name,
            int instance_index,
            int total_instances,
            int array_index_start,
            int array_step,
            int array_index_last,
            int repetitions,
            int outer_cv_folds,
            int outer_cv_folds_to_run,
            int inner_folds,
            bool order_by_ppf = false,
            int limit_iteration_not_better_than_all = 10,
            int limit_iteration_not_better_than_last = 5,
            bool make_outer_cv_confusion_matrices = false,
            bool test_final_best_bias = true
        )
        {
            var dataset = new dataset_loader();

            var groups = program.get_groups_to_use(dataset);

            var winner = worker2(
                dataset,
                groups,
                experiment_name,
                instance_index,
                total_instances,
                array_index_start,
                array_step,
                array_index_last,
                repetitions,
                outer_cv_folds,
                outer_cv_folds_to_run,
                inner_folds,
                order_by_ppf,
                limit_iteration_not_better_than_all,
                limit_iteration_not_better_than_last,
                make_outer_cv_confusion_matrices,
                test_final_best_bias);
        }

        internal static (List<int> best_groups, List<int> best_columns, int best_iteration, double best_score) worker2(
            dataset_loader dataset,
            List<(
                (string file_tag, string alphabet, string dimension, string category, string source, string @group, string member, string perspective) grouped_by_key,
                (int internal_column_index, int external_column_index, string file_tag, string alphabet, string dimension, string category, string source, string @group, string member, string perspective)[] grouped_list,
                int[] grouped_list_internal_column_indexes
            )> groups,
            string experiment_name,
            int instance_index,
            int total_instances,
            int array_index_start,
            int array_step,
            int array_index_last,
            int repetitions,
            int outer_cv_folds,
            int outer_cv_folds_to_run,
            int inner_folds,
            bool order_by_ppf = false,
            int limit_iteration_not_better_than_all = 10,
            int limit_iteration_not_better_than_last = 5,
            bool make_outer_cv_confusion_matrices = false,
            bool test_final_best_bias = true
        )
        {
            // todo: add parameter 'groups' subset of 'header_list'

            // todo: return winning features, and winning performance score
            // todo: allow selection by PPF score

            const string module_name = nameof(program);
            const string method_name = nameof(worker);


            var all_time_highest_score = 0d;
            var all_time_highest_score_iteration_index = 0;
            var all_time_highest_score_iteration_name = "";
            var all_time_highest_score_selected_columns = new List<int>();
            var all_time_highest_score_selected_groups = new List<int>();
            var cache_files_loaded = new List<string>();
            var deep_search_index = -1;
            var feature_selection_finished = false;
            var iteration_index = 0;
            var iteration_name = "";
            var iterations_not_better_than_all = 0;
            var iterations_not_better_than_last = 0;
            var previous_selected_columns = new List<int>();
            var previous_selected_groups = new List<int>();
            var previous_winner_group_array_index = -1;
            var previous_winner_score = 0d;
            var selected_columns = new List<int>();
            var selected_groups = new List<int>();
            var total_groups = groups.Count;
            var all_iterations_winners_cm = new List<confusion_matrix>();
            var feature_selection_status_list = new List<feature_selection_status>();

            io_proxy.WriteLine($@"{experiment_name}: Groups: {(array_index_start)} to {(array_index_last)}. Total groups: {total_groups}. (This instance #{(instance_index)}/#{total_instances} indexes: [{array_index_start}..{array_index_last}]. All indexes: [0..{(total_groups - 1)}].)", module_name, method_name);


            while (!feature_selection_finished || test_final_best_bias)
            {
                // get list of work to do this iteration
                (List<index_data> indexes_whole, List<index_data> indexes_partition) unrolled_indexes = default;

                if (!feature_selection_finished)
                {
                    unrolled_indexes = cache_load.get_unrolled_indexes_basic(dataset, iteration_index, iteration_name, total_groups, instance_index, total_instances, repetitions, outer_cv_folds, outer_cv_folds_to_run, inner_folds);
                    //(string experiment_name, int instance_index, int total_instances, int array_index_start, int array_step, int array_index_last, int repetitions, int outer_cv_folds, int outer_cv_folds_to_run, int inner_folds)
                }
                else if (test_final_best_bias)
                {
                    // todo: fix problem: 5 groups still being processed, rather than 1...

                    selected_groups = all_time_highest_score_selected_groups.ToList();
                    selected_columns = all_time_highest_score_selected_columns.ToList();

                    deep_search_index++;
                    iteration_name = $"check_bias_{deep_search_index}";

                    io_proxy.WriteLine($"{experiment_name}: iteration: {iteration_index}: feature selection finished; statistical tests on best set of features, bias test index: {deep_search_index}.");

                    unrolled_indexes = cache_load.get_unrolled_indexes_check_bias(deep_search_index, dataset, iteration_index, iteration_name, total_groups, instance_index, total_instances);

                    if ((unrolled_indexes.indexes_whole == null || unrolled_indexes.indexes_whole.Count == 0) || (unrolled_indexes.indexes_partition == null || unrolled_indexes.indexes_partition.Count == 0))
                    {
                        test_final_best_bias = false;
                        break;
                    }
                }
                else
                {
                    throw new Exception();
                }

                var total_whole_indexes = unrolled_indexes.indexes_whole.Count;
                var total_partition_indexes = unrolled_indexes.indexes_partition.Count;
                io_proxy.WriteLine($"{experiment_name}: iteration: {iteration_index}, instance_index = {instance_index}/{total_instances}, total_whole_indexes = {total_whole_indexes}, total_partition_indexes = {total_partition_indexes}.");

                // get folder and file names for this iteration (iteration_folder & iteration_whole_cm_filename are the same for all partitions; iteration_partition_cm_filename is specific to the partition)
                var iteration_folder = get_iteration_folder(settings.results_root_folder, experiment_name, iteration_index, iteration_name);
                var iteration_whole_cm_filename = Path.Combine(iteration_folder, $@"z_{get_iteration_filename(unrolled_indexes.indexes_whole)}.cm.csv");
                var iteration_partition_cm_filename = Path.Combine(iteration_folder, $@"x_{get_iteration_filename(unrolled_indexes.indexes_partition)}.cm.csv");

                // iteration_all_cm is a list of all merged results (i.e. the individual outer-cross-validation partitions merged)
                var iteration_all_cm = new List<confusion_matrix>();

                // load cache (first try whole iteration, then try partition, then try individual work items)
                var unrolled_indexes_state = cache_load.load_cache(instance_index, iteration_index, iteration_name, experiment_name, false, cache_files_loaded, iteration_all_cm, unrolled_indexes.indexes_whole, unrolled_indexes.indexes_partition);

                if (iteration_index == 11)
                {
                    Console.WriteLine();
                }

                // check if all partitions are loaded....
                if (unrolled_indexes_state.indexes_missing_whole.Any())
                {
                    // check if this partition is loaded....
                    if (unrolled_indexes_state.indexes_missing_partition.Any())
                    {
                        //var p_iteration_index = iteration_index;
                        var p_selected_groups = selected_groups.ToList();
                        var p_selected_columns = selected_columns.ToList();
                        var p_previous_winner_group_index = previous_winner_group_array_index;
                        var p_previous_selected_columns = previous_selected_columns.ToList();
                        var p_previous_selected_groups = previous_selected_groups.ToList();
                        var p_all_time_highest_score_selected_columns = all_time_highest_score_selected_columns.ToList();
                        var p_all_time_highest_score_selected_groups = all_time_highest_score_selected_groups.ToList();

                        // after loading the whole iteration cache, all partitions cache, and partition individual merged items cache, and if there are still partition items missing... 
                        // use parallel to split the missing items into cpu bound partitions
                        var merge_cache2 = unrolled_indexes_state
                            .indexes_missing_partition
                            .AsParallel()
                            .AsOrdered()
                            .Select(unrolled_index_data =>
                            {
                                io_proxy.WriteLine($@"{experiment_name}: Start parallel index: {unrolled_index_data.id_index_str()} {unrolled_index_data.id_fold_str()} {unrolled_index_data.id_ml_str()} (groups {(array_index_start)} to {(array_index_last)}).");

                                var group_key = unrolled_index_data.group_array_index > -1 ? groups[unrolled_index_data.group_array_index].grouped_by_key : default;

                                io_proxy.WriteLine($@"{experiment_name}: Group cache: Unavailable for iteration {(unrolled_index_data.iteration_index)} group {(unrolled_index_data.group_array_index)}/{total_groups} (groups {(array_index_start)} to {(array_index_last)}).");// File: {group_merged_cm_fn}.");

                                var test_selected_groups = p_selected_groups.ToList();
                                var test_selected_columns = p_selected_columns.ToList();

                                var is_group_selected = test_selected_groups.Contains(unrolled_index_data.group_array_index);
                                var is_only_selection = is_group_selected && test_selected_groups.Count == 1;
                                var is_last_winner = unrolled_index_data.group_array_index == p_previous_winner_group_index;

                                // if selected, remove
                                // if not selected, add
                                // if only group, no action
                                // if just added, no action

                                var selection_direction = direction.neutral;

                                if (unrolled_index_data.group_array_index > -1)
                                {
                                    if (is_group_selected)
                                    {
                                        if (!is_only_selection && !is_last_winner)
                                        {
                                            selection_direction = direction.backwards;
                                            test_selected_groups.Remove(unrolled_index_data.group_array_index);
                                            test_selected_columns = test_selected_columns.Except(groups[unrolled_index_data.group_array_index].grouped_list_internal_column_indexes).ToList();
                                        }
                                    }
                                    else
                                    {
                                        selection_direction = direction.forwards;
                                        test_selected_groups.Add(unrolled_index_data.group_array_index);
                                        test_selected_columns = test_selected_columns.Union(groups[unrolled_index_data.group_array_index].grouped_list_internal_column_indexes).OrderBy(a => a).ToList();
                                    }

                                    if (selection_direction != direction.neutral)
                                    {
                                        // ensure lists are consistent between instances
                                        test_selected_groups = test_selected_groups.OrderBy(a => a).Distinct().ToList();
                                        test_selected_columns = test_selected_columns.OrderBy(a => a).Distinct().ToList();
                                    }
                                }

                                remove_duplicate_columns(dataset, test_selected_columns);

                                var num_columns_added_from_last_iteration = test_selected_columns.Count - p_previous_selected_columns.Count;
                                var num_groups_added_from_last_iteration = test_selected_groups.Count - p_previous_selected_groups.Count;
                                var num_columns_added_from_highest_score_iteration = test_selected_columns.Count - p_all_time_highest_score_selected_columns.Count;
                                var num_groups_added_from_highest_score_iteration = test_selected_groups.Count - p_all_time_highest_score_selected_groups.Count;

                                var selection_test_info = new selection_test_info()
                                {
                                    y_is_group_selected = is_group_selected,
                                    y_is_only_selection = is_only_selection,
                                    y_is_last_winner = is_last_winner,
                                    y_num_columns_added_from_last_iteration = num_columns_added_from_last_iteration,
                                    y_num_groups_added_from_last_iteration = num_groups_added_from_last_iteration,
                                    y_num_columns_added_from_highest_score_iteration = num_columns_added_from_highest_score_iteration,
                                    y_num_groups_added_from_highest_score_iteration = num_groups_added_from_highest_score_iteration,
                                    y_selection_direction = selection_direction,
                                    y_previous_selected_groups = p_previous_selected_groups,
                                    y_previous_selected_columns = p_previous_selected_columns,
                                    y_selected_groups = p_selected_groups,
                                    y_selected_columns = p_selected_columns,
                                    y_test_selected_groups = test_selected_groups,
                                    y_test_selected_columns = test_selected_columns,
                                };

                                var group_folder = get_iteration_folder(settings.results_root_folder, experiment_name, unrolled_index_data.iteration_index, unrolled_index_data.iteration_name, unrolled_index_data.group_array_index);

                                var ocv_result = outer_cross_validation(dataset, experiment_name, selection_test_info, test_selected_columns, unrolled_index_data, group_folder, make_outer_cv_confusion_matrices, group_key, p_previous_selected_columns, p_previous_selected_groups, test_selected_groups);

                                io_proxy.WriteLine($@"{experiment_name}: Finished parallel index: {unrolled_index_data.id_index_str()} {unrolled_index_data.id_fold_str()} {unrolled_index_data.id_ml_str()} (groups {(array_index_start)} to {(array_index_last)}).");

                                return ocv_result.mcv_cm;
                            }

                            )
                            .ToList();


                        iteration_all_cm.AddRange(merge_cache2.Where(a => a != default).SelectMany((a, i) => a).ToList());


                        // all partition should be loaded by this point, check
                        unrolled_indexes_state = cache_load.get_missing(instance_index, iteration_all_cm, unrolled_indexes.indexes_whole);
                        if (unrolled_indexes_state.indexes_missing_partition.Any()) throw new Exception();


                        // 4. save CM for all groups of this instance (from index start to index end) merged outer-cv results
                        //
                        var iteration_instance_all_groups_cm_list = iteration_all_cm
                            .Where((cm, i) => cm.x_iteration_index == iteration_index && cm.x_group_array_index >= array_index_start && cm.x_group_array_index <= array_index_last)
                            .ToList();

                        var iteration_instance_all_groups_cm_lines = iteration_instance_all_groups_cm_list
                            .Select(a => a.csv_values())
                            .ToList();

                        io_proxy.WriteAllLines(iteration_partition_cm_filename, iteration_instance_all_groups_cm_lines);
                        io_proxy.WriteLine($"{experiment_name}: Part cache: Saved for iteration {(iteration_index)} group {(array_index_start)} to {(array_index_last)}. File: {iteration_partition_cm_filename}.");
                    }

                    // 5. load results from other instances
                    //
                    unrolled_indexes_state = cache_load.load_cache(instance_index, iteration_index, iteration_name, experiment_name, true, cache_files_loaded, iteration_all_cm, unrolled_indexes.indexes_whole, unrolled_indexes.indexes_partition);

                    if (unrolled_indexes_state.indexes_missing_whole.Any()) throw new Exception();

                    if (instance_index == 0)
                    {
                        // save CM with results from all instances
                        var iteration_all_cm_lines = iteration_all_cm.Select(a => a.csv_values()).ToList();

                        io_proxy.WriteAllLines(iteration_whole_cm_filename, iteration_all_cm_lines);
                        io_proxy.WriteLine($"{experiment_name}: Full cache: Saved for iteration {(iteration_index)}. File: {iteration_whole_cm_filename}.");
                    }
                }



                // 5. find winner
                // ensure ordering will be consistent between instances

                var iteration_scoring_cm_list = iteration_all_cm.Where(cm =>
                    cm.x_class_id == scoring_args.scoring_class_id &&
                    cm.x_prediction_threshold == null &&
                    cm.x_repetitions_index == -1 &&
                    cm.x_outer_cv_index == -1 &&
                    cm.x_iteration_index == iteration_index &&
                    cm.x_group_array_index != null).ToList();

                // ensure consistent reordering for items with equal scores
                iteration_scoring_cm_list = iteration_scoring_cm_list.OrderBy(cm => cm.x_group_array_index).ThenBy(a => a.x_class_id).ToList();

                // reorder by score

                if (order_by_ppf)
                {
                    //var num_features = iteration_scoring_cm_list.Select(cm => cm.x_new_feature_count).ToList();
                    //var scores = iteration_scoring_cm_list.Select(cm => scoring_args.scoring_metrics.Select(metric_name => cm.metrics.get_value_by_name(metric_name)).Average()).ToList();
                    //var scores_ppf = iteration_scoring_cm_list.Select(cm => (scoring_args.scoring_metrics.Select(metric => (cm.x_new_feature_count - cm.x_old_feature_count == 0) ? 0d : cm.metrics.get_value_by_name(metric)).Average() - previous_winner_score) / (cm.x_new_feature_count - cm.x_old_feature_count)).ToList();

                    iteration_scoring_cm_list = iteration_scoring_cm_list
                        .OrderByDescending(cm => (scoring_args.scoring_metrics.Select(metric => (cm.x_new_feature_count - cm.x_old_feature_count == 0) ? 0d : cm.metrics.get_value_by_name(metric)).Average() - previous_winner_score) / (cm.x_new_feature_count - cm.x_old_feature_count))
                        .ThenByDescending(cm => scoring_args.scoring_metrics.Select(metric_name => cm.metrics.get_value_by_name(metric_name)).Average())
                        .ThenBy(cm => cm.x_new_feature_count)
                        .ToList();
                }
                else
                {
                    iteration_scoring_cm_list = iteration_scoring_cm_list
                        .OrderByDescending(cm => scoring_args.scoring_metrics.Select(metric_name => cm.metrics.get_value_by_name(metric_name)).Average())
                        .ThenByDescending(cm => (scoring_args.scoring_metrics.Select(metric => (cm.x_new_feature_count - cm.x_old_feature_count == 0) ? 0d : cm.metrics.get_value_by_name(metric)).Average() - previous_winner_score) / (cm.x_new_feature_count - cm.x_old_feature_count))
                        .ThenBy(cm => cm.x_new_feature_count)
                        .ToList();
                }

                //var scoring_cm_higher = scoring_cm.Where(a => scoring_args.scoring_metrics.Select(metric => a.cm.get_value_by_name(metric)).Average() >= previous_winner_score).ToList();
                //var scoring_cm_lower = scoring_cm.Except(scoring_cm_higher).ToList();


                //var scoring_cm_query_tree = new[] { scoring_cm.OrderByDescending(a => a.cm.get_value_by_name(scoring_args.scoring_metrics[0])) }.ToList();
                //
                //for (var si = 1; si < scoring_args.scoring_metrics.Length; si++)
                //{
                //    var six = si;
                //    scoring_cm_query_tree.Add(scoring_cm_query_tree.Last().ThenByDescending(a => a.cm.get_value_by_name(scoring_args.scoring_metrics[six])));
                //}
                //
                ////scoring_cm = scoring_cm.OrderByDescending(a => a.cm.get_value_by_name(scoring_args.score1_metric)).ThenByDescending(a => a.cm.get_value_by_name(scoring_args.score2_metric)).ThenByDescending(a => a.cm.get_value_by_name(scoring_args.score3_metric)).ToList();
                //scoring_cm = scoring_cm_query_tree.Last().ToList();


                var scores_winner_index = 0;
                if (iteration_scoring_cm_list[scores_winner_index].x_group_array_index == previous_winner_group_array_index && scores_winner_index < total_groups - 1)
                {
                    //scores_winner_index++;

                    var ix0 = iteration_scoring_cm_list[scores_winner_index];
                    var ix1 = iteration_scoring_cm_list[scores_winner_index + 1];

                    iteration_scoring_cm_list[scores_winner_index] = ix1;
                    iteration_scoring_cm_list[scores_winner_index + 1] = ix0;
                }

                var iteration_winner_cm = iteration_scoring_cm_list[scores_winner_index];
                var iteration_winner_group_array_index = (int)iteration_winner_cm.x_group_array_index;
                var iteration_winner_cm_list = iteration_scoring_cm_list.Where(a => a.x_group_array_index == iteration_winner_group_array_index).ToList();
                var iteration_winner_group = iteration_winner_group_array_index > -1 ? groups[iteration_winner_group_array_index] : default;
                var iteration_winner_group_key = iteration_winner_group.grouped_by_key;
                var iteration_winner_score = scoring_args.scoring_metrics.Select(metric_name => iteration_winner_cm.metrics.get_value_by_name(metric_name)).Average();
                var iteration_winner_direction = iteration_winner_cm.selection_test_info.y_selection_direction;

                all_iterations_winners_cm.AddRange(iteration_winner_cm_list);

                var score_increase_from_last = iteration_winner_score - previous_winner_score;
                var score_increase_from_all = iteration_winner_score - all_time_highest_score;

                var score_increase_from_last_pct = previous_winner_score != 0 ? score_increase_from_last / previous_winner_score : 0;
                var score_increase_from_all_pct = all_time_highest_score != 0 ? score_increase_from_all / all_time_highest_score : 0;

                var score_better_than_last = score_increase_from_last > 0d;
                var score_better_than_all = score_increase_from_all > 0d;


                if (!feature_selection_finished)
                {


                    iterations_not_better_than_last = score_better_than_last ? 0 : iterations_not_better_than_last + 1;
                    iterations_not_better_than_all = score_better_than_all ? 0 : iterations_not_better_than_all + 1;

                    if (iteration_winner_group_array_index > -1)
                    {
                        if (iteration_winner_direction == direction.forwards)
                        {
                            selected_groups.Add(iteration_winner_group_array_index);
                            selected_columns.AddRange(iteration_winner_group.grouped_list_internal_column_indexes);
                        }
                        else if (iteration_winner_direction == direction.backwards)
                        {
                            selected_groups.Remove(iteration_winner_group_array_index);
                            selected_columns = selected_columns.Except(iteration_winner_group.grouped_list_internal_column_indexes).ToList();
                        }

                        if (iteration_winner_direction != direction.neutral)
                        {
                            selected_groups = selected_groups.OrderBy(a => a).Distinct().ToList();
                            selected_columns = selected_columns.OrderBy(a => a).Distinct().ToList();
                        }
                    }

                    if (score_better_than_all)
                    {
                        all_time_highest_score = iteration_winner_score;
                        all_time_highest_score_iteration_index = iteration_index;
                        all_time_highest_score_iteration_name = iteration_name;
                        all_time_highest_score_selected_groups = selected_groups.ToList();
                        all_time_highest_score_selected_columns = selected_columns.ToList();
                    }


                    feature_selection_finished = !((score_better_than_last || (iterations_not_better_than_last < limit_iteration_not_better_than_last && iterations_not_better_than_all < limit_iteration_not_better_than_all)) && (selected_groups.Count < total_groups));


                }

                // if main program instance, then save iteration winner to file
                if (instance_index == 0)
                {

                    {
                        var fss = new feature_selection_status()
                        {
                            all_time_highest_score = all_time_highest_score,
                            all_time_highest_score_iteration_index = all_time_highest_score_iteration_index,
                            all_time_highest_score_iteration_name = all_time_highest_score_iteration_name,
                            all_time_highest_score_selected_columns = all_time_highest_score_selected_columns.ToList(),
                            all_time_highest_score_selected_groups = all_time_highest_score_selected_groups.ToList(),
                            deep_search_index = deep_search_index,
                            feature_selection_finished = feature_selection_finished,
                            iteration_index = iteration_index,
                            iteration_name = iteration_name,
                            iterations_not_better_than_all = iterations_not_better_than_all,
                            iterations_not_better_than_last = iterations_not_better_than_last,
                            previous_selected_columns = previous_selected_columns.ToList(),
                            previous_selected_groups = previous_selected_groups.ToList(),
                            previous_winner_group_array_index = previous_winner_group_array_index,
                            previous_winner_score = previous_winner_score,
                            selected_columns = selected_columns.ToList(),
                            selected_groups = selected_groups.ToList(),
                            total_groups = total_groups,
                            iteration_winner_group_array_index = iteration_winner_group_array_index,
                            iteration_winner_group = iteration_winner_group,
                            iteration_winner_group_key = iteration_winner_group_key,
                            iteration_winner_score = iteration_winner_score,
                            iteration_winner_direction = iteration_winner_direction,
                            score_increase_from_last = score_increase_from_last,
                            score_increase_from_all = score_increase_from_all,
                            score_increase_from_last_pct = score_increase_from_last_pct,
                            score_increase_from_all_pct = score_increase_from_all_pct,
                            score_better_than_last = score_better_than_last,
                            score_better_than_all = score_better_than_all,
                        };

                        feature_selection_status_list.Add(fss);

                        {
                            var iteration_status_fn = Path.Combine(iteration_folder, $@"iteration_status.csv");
                            feature_selection_status.save(iteration_status_fn, new[] {fss});
                        }
                        {
                            var all_iterations_status_fn = Path.Combine(iteration_folder, $@"all_iterations_status.csv");
                            feature_selection_status.save(all_iterations_status_fn, feature_selection_status_list);
                        }
                    }

                    {
                        var iteration_winner_fn = Path.Combine(iteration_folder, $@"iteration_winner.csv");
                        if (io_proxy.is_file_available(iteration_winner_fn)) { io_proxy.WriteLine($@"{experiment_name}: Iteration winner cache: Already saved for iteration {(iteration_index)}. File: {iteration_winner_fn}."); }
                        else
                        {
                            io_proxy.WriteLine($@"{experiment_name}: Iteration winner cache: Unavailable for iteration {(iteration_index)}. File: {iteration_winner_fn}.");
                            confusion_matrix.save(iteration_winner_fn, iteration_winner_cm_list);
                            io_proxy.WriteLine($@"{experiment_name}: Iteration winner cache: Saved for iteration {(iteration_index)}. File: {iteration_winner_fn}.");
                        }
                    }

                    {
                        var all_iterations_winner_fn = Path.Combine(iteration_folder, $@"all_iterations_winners.csv");
                        if (io_proxy.is_file_available(all_iterations_winner_fn)) { io_proxy.WriteLine($@"{experiment_name}: All iterations winners cache: Already saved for iteration {(iteration_index)}. File: {all_iterations_winner_fn}."); }
                        else
                        {
                            io_proxy.WriteLine($@"{experiment_name}: All iterations winners cache: Unavailable for iteration {(iteration_index)}. File: {all_iterations_winner_fn}.");
                            confusion_matrix.save(all_iterations_winner_fn, all_iterations_winners_cm);
                            io_proxy.WriteLine($@"{experiment_name}: All iterations winners cache: Saved for iteration {(iteration_index)}. File: {all_iterations_winner_fn}.");
                        }
                    }
                }


                io_proxy.WriteLine($"{experiment_name}: Finished: iteration {(iteration_index)} for {(array_index_start)} to {(array_index_last)}.  {(feature_selection_finished ? "Finished" : "Not finished")}.");

                previous_winner_score = iteration_winner_score;
                previous_winner_group_array_index = iteration_winner_group_array_index;
                previous_selected_groups = selected_groups.ToList();
                previous_selected_columns = selected_columns.ToList();

                iteration_index++;
            }

            //test_group_kernel_scaling_perf(instance_id, total_instances, experiment_name, unrolled_index.total_groups, selected_groups.ToList(), selected_columns.ToList(), ida, idr, oca);

            io_proxy.WriteLine($"{experiment_name}: Finished: all iterations for groups {(array_index_start)} to {(array_index_last)}.");

            return (all_time_highest_score_selected_groups, all_time_highest_score_selected_columns, all_time_highest_score_iteration_index, all_time_highest_score);
        }

        private static (List<confusion_matrix> ocv_cm, List<confusion_matrix> mcv_cm) outer_cross_validation
        (
            dataset_loader dataset,
            string experiment_name,
            selection_test_info selection_test_info,
            List<int> test_selected_columns,
            index_data unrolled_index_data,
            string group_folder,
            bool make_outer_cv_confusion_matrices,
            (string file_tag, string alphabet, string dimension, string category, string source, string @group, string member, string perspective) group_key,
            List<int> p_previous_selected_columns,
            List<int> p_previous_selected_groups,
            List<int> test_selected_groups
        )
        {

            //var ocv_cm = new List<confusion_matrix>();
            
            //var mcv_cm = new List<confusion_matrix_data>();


            // 1. make outer-cv files
            var outer_cv_inputs = make_outer_cv_inputs(dataset, test_selected_columns, group_folder, unrolled_index_data);

            // 1a. the ocvi index -1 is merged data
            var merged_cv_input = outer_cv_inputs.First(a => a.outer_cv_index == -1);

            // 2. run libsvm
            //var prediction_data_list = new List<((long grid_dur, long train_dur, long predict_dur) dur, grid_point grid_point, string[] predict_text)>();

            var pdl = outer_cv_inputs
                .Where(a=>a.outer_cv_index != -1 && a.repetitions_index != -1)
                .AsParallel()
                .AsOrdered()
                .Select(outer_cv_input =>
                    //foreach (var outer_cv_input in outer_cv_inputs)
                {
                    var ocv_cm = new List<confusion_matrix>();

                    //if (outer_cv_input.outer_cv_index == -1 || outer_cv_input.repetitions_index == -1) continue; // -1 is the index for the merged text

                    // call libsvm... returns raw prediction file data from doing: parameter search -> train -> predict
                    var prediction_data = inner_cross_validation(unrolled_index_data, outer_cv_input);

                    // add results from libsvm to list
                    //prediction_data_list.Add(prediction_data);

                    // optional: make_outer_cv_confusion_matrices: this will output the individual outer-cross-validation confusion matrices (i.e. if outer-cv-folds = 5, then 5 respective confusion-matrices will be created, as well as the merged data confusion-matrix).
                    if (make_outer_cv_confusion_matrices)
                    {
                        // convert text results to confusion matrix and performance metrics
                        var ocv_prediction_file_data = perf.load_prediction_file(outer_cv_input.test_text, null, prediction_data.predict_text, unrolled_index_data.calc_11p_thresholds);

                        // add any missing meta details to the confusion-matrix
                        //update_ocv_cm(dataset, experiment_name, ocv_prediction_file_data, unrolled_index_data, group_key,                    p_previous_selected_columns, test_selected_columns,                   p_previous_selected_groups, test_selected_groups, outer_cv_input, prediction_data);
                        update_merged_cm(dataset, experiment_name, selection_test_info, ocv_prediction_file_data, unrolled_index_data, group_key, p_previous_selected_columns, test_selected_columns, p_previous_selected_groups, test_selected_groups, outer_cv_input, new[] {prediction_data});

                        ocv_cm.AddRange(ocv_prediction_file_data.cm_list);

                        // save outer-cross-validation confusion-matrix CM for group
                        confusion_matrix.save(outer_cv_input.cm_fn, ocv_prediction_file_data.cm_list);
                        io_proxy.WriteLine($@"{experiment_name}: Group OCV cache: Saved: [R({outer_cv_input.repetitions_index}/{unrolled_index_data.repetitions}) O({outer_cv_input.outer_cv_index}/{unrolled_index_data.outer_cv_folds})] {unrolled_index_data.id_index_str()} {unrolled_index_data.id_fold_str()} {unrolled_index_data.id_ml_str()}. File: {outer_cv_input.cm_fn}.");
                    }

                    // delete temporary files
                    io_proxy.Delete(outer_cv_input.train_fn);
                    io_proxy.Delete(outer_cv_input.grid_fn);
                    io_proxy.Delete(outer_cv_input.model_fn);
                    io_proxy.Delete(outer_cv_input.test_fn);
                    io_proxy.Delete(outer_cv_input.predict_fn);
                    // do not delete the confusion-matrix: io_proxy.Delete(outer_cv_input.cm_fn);

                    return (prediction_data, ocv_cm);
                })
                .ToList();

            var ocv_cm = pdl.SelectMany(a => a.ocv_cm).ToList();
            var prediction_data_list = pdl.Select(a => a.prediction_data).ToList();

            // 3. make confusion matrix from the merged prediction results
            // note: repeated 'labels' lines will be ignored
            var merged_prediction_text = prediction_data_list.SelectMany(a => a.predict_text).ToList();

            var test_class_sample_id_list = merged_cv_input.test_fold_indexes.SelectMany(a => a.test_indexes).ToList();

            var prediction_file_data = perf.load_prediction_file(merged_cv_input.test_text, null, merged_prediction_text, unrolled_index_data.calc_11p_thresholds, test_class_sample_id_list);
            var mcv_cm = prediction_file_data.cm_list;

            // add any missing details to the confusion-matrix
            update_merged_cm(dataset, experiment_name, selection_test_info, prediction_file_data, unrolled_index_data, group_key, p_previous_selected_columns, test_selected_columns, p_previous_selected_groups, test_selected_groups, merged_cv_input, prediction_data_list);

            // save CM for group
            confusion_matrix.save(merged_cv_input.cm_fn, mcv_cm);
            io_proxy.WriteLine($@"{experiment_name}: Group MCV cache: Saved: {unrolled_index_data.id_index_str()} {unrolled_index_data.id_fold_str()} {unrolled_index_data.id_ml_str()}. File: {merged_cv_input.cm_fn}.");

            return (ocv_cm, mcv_cm);
        }

        private static void update_merged_cm
        (
            dataset_loader dataset,
            string experiment_name,
            selection_test_info selection_test_info,
            (List<prediction> prediction_list, List<confusion_matrix> cm_list) prediction_file_data,
            index_data unrolled_index,
            (string file_tag, string alphabet, string dimension, string category, string source, string @group, string member, string perspective) group_key,
            List<int> previous_selected_columns,
            List<int> test_selected_columns,
            List<int> previous_selected_groups,
            List<int> test_selected_groups,
            (int repetitions_index, int outer_cv_index, string train_fn, string grid_fn, string model_fn, string test_fn, string predict_fn, string cm_fn, List<string> train_text, List<string> test_text, List<(int class_id, int training_size)> train_sizes, List<(int class_id, int testing_size)> test_sizes, List<(int class_id, List<int> train_indexes)> train_fold_indexes, List<(int class_id, List<int> test_indexes)> test_fold_indexes) merged_cv_input,
            IList<((long grid_dur, long train_dur, long predict_dur) dur, grid_point grid_point, string[] predict_text)> prediction_data_list
        )
        {
            Parallel.ForEach(prediction_file_data.cm_list,
                cm =>
                    //foreach (var cm in prediction_file_data.cm_list)
                {
                    var update_ppf_ppg = cm.x_new_feature_count != test_selected_columns.Count || cm.x_new_group_count != test_selected_groups.Count;

                    cm.selection_test_info = selection_test_info;

                    cm.x_id = null;

                    cm.grid_point = new grid_point() {cost = prediction_data_list.Where(a => a.grid_point?.cost != null).Select(a => a.grid_point.cost).DefaultIfEmpty(default).Average(), gamma = prediction_data_list.Where(a => a.grid_point?.gamma != null).Select(a => a.grid_point.gamma).DefaultIfEmpty(default).Average(), epsilon = prediction_data_list.Where(a => a.grid_point?.epsilon != null).Select(a => a.grid_point.epsilon).DefaultIfEmpty(default).Average(), coef0 = prediction_data_list.Where(a => a.grid_point?.coef0 != null).Select(a => a.grid_point.coef0).DefaultIfEmpty(default).Average(), degree = prediction_data_list.Where(a => a.grid_point?.degree != null).Select(a => a.grid_point.degree).DefaultIfEmpty(default).Average(),};

                    cm.x_libsvm_cv = prediction_data_list.Where(a => a.grid_point?.cv_rate != null).Select(a => (double) a.grid_point.cv_rate).DefaultIfEmpty(default).Average();

                    cm.x_class_name = settings.class_names?.FirstOrDefault(b => cm.x_class_id == b.class_id).class_name;
                    cm.x_class_size = dataset.class_sizes?.First(b => b.class_id == cm.x_class_id).class_size ?? -1;
                    cm.x_class_testing_size = merged_cv_input.test_sizes?.First(b => b.class_id == cm.x_class_id).testing_size ?? -1;
                    cm.x_class_training_size = merged_cv_input.train_sizes?.First(b => b.class_id == cm.x_class_id).training_size ?? -1;
                    cm.x_class_weight = unrolled_index.class_weights?.FirstOrDefault(b => cm.x_class_id == b.class_id).class_weight;
                    cm.x_duration_grid_search = prediction_data_list.Select(a => a.dur.grid_dur).Sum().ToString(CultureInfo.InvariantCulture);
                    cm.x_duration_testing = prediction_data_list.Select(a => a.dur.predict_dur).Sum().ToString(CultureInfo.InvariantCulture);
                    cm.x_duration_training = prediction_data_list.Select(a => a.dur.train_dur).Sum().ToString(CultureInfo.InvariantCulture);
                    cm.x_experiment_name = experiment_name;
                    cm.x_group_array_index = unrolled_index.group_array_index;
                    cm.x_inner_cv_folds = unrolled_index.inner_cv_folds;
                    cm.x_iteration_index = unrolled_index.iteration_index;
                    cm.x_iteration_name = unrolled_index.iteration_name;
                    cm.x_key_file_tag = group_key.file_tag;
                    cm.x_key_alphabet = group_key.alphabet;
                    cm.x_key_category = group_key.category;
                    cm.x_key_dimension = group_key.dimension;
                    cm.x_key_group = group_key.@group;
                    cm.x_key_member = group_key.member;
                    cm.x_key_perspective = group_key.perspective;
                    cm.x_key_source = group_key.source;

                    cm.x_new_feature_count = test_selected_columns.Count;
                    cm.x_new_group_count = test_selected_groups.Count;
                    cm.x_old_feature_count = previous_selected_columns.Count;
                    cm.x_old_group_count = previous_selected_groups.Count;
                    cm.x_outer_cv_folds = unrolled_index.outer_cv_folds;
                    cm.x_outer_cv_folds_to_run = unrolled_index.outer_cv_folds_to_run;
                    cm.x_outer_cv_index = merged_cv_input.outer_cv_index;
                    cm.x_calc_11p_thresholds = unrolled_index.calc_11p_thresholds;
                    cm.x_repetitions_index = merged_cv_input.repetitions_index;
                    cm.x_repetitions_total = unrolled_index.repetitions;
                    cm.x_scale_function = unrolled_index.scale_function;
                    cm.x_svm_kernel = unrolled_index.svm_kernel;
                    cm.x_svm_type = unrolled_index.svm_type;
                    cm.x_total_groups = unrolled_index.total_groups;
                    cm.x_features_included = string.Join(";", test_selected_columns ?? new List<int>());
                    cm.x_groups_included = string.Join(";", test_selected_groups ?? new List<int>());

                    if (update_ppf_ppg)
                    {
                        cm.calculate_ppf_ppg();
                    }
                });
        }

        internal static void remove_duplicate_columns(dataset_loader dataset, List<int> query_cols)
        {
            // remove duplicate columns (may exist in separate groups)
            //var query_col_dupe_check = idr.dataset_instance_list_grouped.SelectMany(a => a.examples).SelectMany(a => query_cols.Select(b => (query_col: b, fv: a.feature_data[b].fv)).ToList()).GroupBy(b => b.query_col).Select(b => (query_col: b.Key, values: b.Select(c => c.fv).ToList())).ToList();

            var query_col_dupe_check = query_cols.Select(col_index => dataset
                    .value_list
                    .SelectMany(class_values => class_values.val_list.Select(row => row.row_columns[col_index].row_column_val).ToList()).ToList()).ToList();

            var dupe_clusters = new List<List<int>>();
            for (var i = 0; i < query_col_dupe_check.Count; i++)
            {
                for (var j = 0; j < query_col_dupe_check.Count; j++)
                {
                    if (i <= j) continue;

                    if (query_col_dupe_check[i].SequenceEqual(query_col_dupe_check[j]))
                    {
                        var cluster = new List<int>() { query_cols[i], query_cols[j] };
                        var x = dupe_clusters.Where(a => a.Any(b => cluster.Any(c => b == c))).ToList();
                        x.ForEach(a =>
                        {
                            cluster.AddRange(a);
                            dupe_clusters.Remove(a);
                        });
                        cluster = cluster.OrderBy(a => a).Distinct().ToList();
                        dupe_clusters.Add(cluster);
                    }
                }
            }

            foreach (var dc in dupe_clusters)
            {
                if (dc == null || dc.Count == 0) continue;
                var remove = dc.Skip(1 /* leave 1 copy of the item */).ToList();
                if (remove.Count > 0) { query_cols.RemoveAll(a => remove.Any(b => a == b)); }
            }
        }

        internal enum direction { forwards, neutral, backwards }

        internal static List<(
            int repetitions_index,
            int outer_cv_index,
            string train_fn,
            string grid_fn,
            string model_fn,
            string test_fn,
            string predict_fn,
            string cm_fn,
            List<string> train_text,
            List<string> test_text,
            List<(int class_id, int training_size)> train_sizes,
            List<(int class_id, int testing_size)> test_sizes,
            List<(int class_id, List<int> train_indexes)> train_fold_indexes,
            List<(int class_id, List<int> test_indexes)> test_fold_indexes
            )>

            make_outer_cv_inputs(
                dataset_loader dataset,
                List<int> column_indexes,
                string group_folder,
                index_data unrolled_index
                )
        {
            var preserve_fid = false; // whether to keep the original FID in the libsvm training/testing files (note: if not, bear in mind that features with zero values are removed, so this must not distort the ordering...).

            if (unrolled_index.repetitions <= 0) throw new ArgumentOutOfRangeException(nameof(unrolled_index), nameof(unrolled_index.repetitions));
            if (unrolled_index.outer_cv_folds <= 0) throw new ArgumentOutOfRangeException(nameof(unrolled_index), nameof(unrolled_index.repetitions));
            if (unrolled_index.outer_cv_folds_to_run > unrolled_index.outer_cv_folds) throw new ArgumentOutOfRangeException(nameof(unrolled_index), nameof(unrolled_index.outer_cv_folds_to_run));


            // ensure columns in correct order, and has class id
            column_indexes = column_indexes.OrderBy(a => a).ToList();
            if (column_indexes[0] != 0) column_indexes.Insert(0, 0);


            var r_o_indexes = new List<(int repetitions_index, int outer_cv_index)>();
            for (var _repetitions_cv_index = 0; _repetitions_cv_index < (unrolled_index.repetitions == 0 ? 1 : unrolled_index.repetitions); _repetitions_cv_index++)
            {
                for (var _outer_cv_index = 0; _outer_cv_index < (unrolled_index.outer_cv_folds_to_run == 0 ? unrolled_index.outer_cv_folds : unrolled_index.outer_cv_folds_to_run); _outer_cv_index++)
                {
                    r_o_indexes.Add((_repetitions_cv_index, _outer_cv_index));
                }
            }


            var ocv_data = r_o_indexes
                .AsParallel()
                .AsOrdered()
                .Select(r_o_index =>
                {
                    var repetitions_index = r_o_index.repetitions_index;
                    var outer_cv_index = r_o_index.outer_cv_index;

                    var filename = Path.Combine(group_folder, $@"o_{get_item_filename(unrolled_index, repetitions_index, outer_cv_index)}");
                    var train_fn = $@"{filename}.train.libsvm";
                    var grid_fn = $@"{filename}.grid.libsvm";
                    var model_fn = $@"{filename}.model.libsvm";
                    var test_fn = $@"{filename}.test.libsvm";
                    var predict_fn = $@"{filename}.predict.libsvm";
                    var cm_fn = $@"{filename}.cm.csv";

                    var train_fold_indexes = unrolled_index.down_sampled_training_class_folds/* down sample for training */.AsParallel().AsOrdered().Select(a => (a.class_id, train_indexes: a.folds.Where(b => b.repetitions_index == repetitions_index && b.outer_cv_index != outer_cv_index/* do not select test fold */).SelectMany(b => b.indexes).OrderBy(b => b).ToList())).ToList();
                    var train_sizes = train_fold_indexes.Select(a => (class_id: a.class_id, train_size: a.train_indexes.Count)).ToList();
                    var train_row_values = dataset.get_row_features(train_fold_indexes, column_indexes);
                    var train_scaling = dataset_loader.get_scaling_params(train_row_values, column_indexes);
                    var train_row_scaled_values = dataset_loader.get_scaled_rows(train_row_values, /*column_indexes,*/ train_scaling, unrolled_index.scale_function);
                    var train_text = train_row_scaled_values.AsParallel().AsOrdered().Select(row => $@"{(int)row[0]} {string.Join(" ", row.Skip(1 /* skip class id column */).Select((col_val, x_index) => col_val != 0 ? $@"{(preserve_fid ? column_indexes[x_index] : x_index)}:{col_val:G17}" : $@"").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray())}").Where(c => !string.IsNullOrWhiteSpace(c)).ToList();

                    // todo: add ID to predictions for misclassification analysis
                    //var v = train_fold_indexes.Select(a => a.indexes.Select(ix => dataset.value_list.First(b => b.class_id == a.class_id).val_list[ix].row_comment).ToArray()).ToArray();


                    var test_fold_indexes = unrolled_index.class_folds/* natural distribution for testing */.AsParallel().AsOrdered().Select(a => (a.class_id, test_indexes: a.folds.Where(b => b.repetitions_index == repetitions_index && b.outer_cv_index == outer_cv_index/* select only test fold */).SelectMany(b => b.indexes).OrderBy(b => b).ToList())).ToList();
                    var test_sizes = test_fold_indexes.Select(a => (class_id: a.class_id, test_size: a.test_indexes.Count)).ToList();
                    var test_row_values = dataset.get_row_features(test_fold_indexes, column_indexes);
                    var test_scaling = train_scaling; /* scale test data with training data */
                    var test_row_scaled_values = dataset_loader.get_scaled_rows(test_row_values, /*column_indexes,*/ test_scaling, unrolled_index.scale_function);
                    var test_text = test_row_scaled_values.AsParallel().AsOrdered().Select(row => $@"{(int)row[0]} {string.Join(" ", row.Skip(1 /* skip class id column */).Select((col_val, x_index) => col_val != 0 ? $@"{(preserve_fid ? column_indexes[x_index] : x_index)}:{col_val:G17}" : $@"").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray())}").Where(c => !string.IsNullOrWhiteSpace(c)).ToList();

                    return (repetitions_index, outer_cv_index, train_fn, grid_fn, model_fn, test_fn, predict_fn, cm_fn, train_text, test_text, train_sizes, test_sizes, train_fold_indexes, test_fold_indexes);
                })
                .ToList();

            Parallel.ForEach(ocv_data,
                item =>
                {
                    io_proxy.WriteAllLines(item.train_fn, item.train_text);
                    io_proxy.WriteAllLines(item.test_fn, item.test_text);
                });

            var merged_train_text = ocv_data.SelectMany(a => a.train_text).ToList();
            var merged_test_text = ocv_data.SelectMany(a => a.test_text).ToList();
            var merged_train_sizes = ocv_data.SelectMany(a => a.train_sizes).GroupBy(a => a.class_id).Select(b => (class_id: b.Key, training_size: b.Select(c => c.train_size).Sum())).ToList();
            var merged_test_sizes = ocv_data.SelectMany(a => a.test_sizes).GroupBy(a => a.class_id).Select(b => (class_id: b.Key, testing_size: b.Select(c => c.test_size).Sum())).ToList();

            var merged_train_fold_indexes = ocv_data.SelectMany(a => a.train_fold_indexes).GroupBy(a => a.class_id).Select(a => (class_id: a.Key, train_indexes: a.SelectMany(b => b.train_indexes).ToList())).ToList();
            var merged_test_fold_indexes = ocv_data.SelectMany(a => a.test_fold_indexes).GroupBy(a => a.class_id).Select(a => (class_id: a.Key, test_indexes: a.SelectMany(b => b.test_indexes).ToList())).ToList();

            // todo : are these above lines missing groupby statements?

            // filenames for merging all repetition indexes and outer cv indexes... as if it were a single test.
            var merged_filename_prefix = Path.Combine(group_folder, $@"m_{get_iteration_filename(new[] { unrolled_index })}");
            var merged_train_fn = $@"{merged_filename_prefix}.train.libsvm";
            var merged_grid_fn = $@"{merged_filename_prefix}.grid.libsvm";
            var merged_model_fn = $@"{merged_filename_prefix}.model.libsvm";
            var merged_test_fn = $@"{merged_filename_prefix}.test.libsvm";
            var merged_predict_fn = $@"{merged_filename_prefix}.predict.libsvm";
            var merged_cm_fn = $@"{merged_filename_prefix}.cm.csv";

            var merged_data = (repetitions_index: -1, outer_cv_index: -1, merged_train_fn, merged_grid_fn, merged_model_fn, merged_test_fn, merged_predict_fn, merged_cm_fn, merged_train_text, merged_test_text, merged_train_sizes, merged_test_sizes, merged_train_fold_indexes, merged_test_fold_indexes);
            ocv_data.Insert(0, merged_data);

            var save_merged_files = false;
            if (save_merged_files)
            {
                io_proxy.WriteAllLines(merged_train_fn, merged_train_text);
                io_proxy.WriteAllLines(merged_test_fn, merged_test_text);
            }

            return ocv_data;
        }

        internal static string get_item_filename(index_data unrolled_index, int repetition_index, int outer_cv_index)
        {
            return $@"{get_iteration_filename(new[] { unrolled_index })}_ri[{repetition_index}]_oi[{outer_cv_index}]";
        }

        internal static string get_iteration_filename(
            IList<index_data> indexes)
        {
            static string get_initials(string name)
            {
                var initials = string.Join("", name.Replace("_", " ", StringComparison.InvariantCulture).Split().Where(a => a.Length > 0).Select(a => a.First()).ToList());
                return initials.Length > 2 ?/* initials.Substring(0, 2) */ $@"{initials.First()}{initials.Last()}" : initials;
            }

            var iteration_index = get_ranges_str(indexes.Select(a => a.iteration_index).ToList());
            var group_index = get_ranges_str(indexes.Select(a => a.group_array_index).ToList());
            var total_groups = get_ranges_str(indexes.Select(a => a.total_groups).ToList());
            var calc_11p_thresholds = get_ranges_str(indexes.Select(a => a.calc_11p_thresholds ? 1 : 0).ToList());
            var repetitions = get_ranges_str(indexes.Select(a => a.repetitions).ToList());
            var outer_cv_folds = get_ranges_str(indexes.Select(a => a.outer_cv_folds).ToList());
            var outer_cv_folds_to_run = get_ranges_str(indexes.Select(a => a.outer_cv_folds_to_run).ToList());
            var class_weights = string.Join("_",
                indexes
                    .Where(a => a.class_weights != null)
                    .SelectMany(a => a.class_weights)
                    .GroupBy(a => a.class_id)
                    .Select(a => $"{a.Key}_{get_ranges_str(a.Select(b => (int)(b.class_weight * 100)).ToList())}")
                    .ToList());
            var svm_type = get_ranges_str(indexes.Select(a => (int)a.svm_type).ToList());
            var svm_kernel = get_ranges_str(indexes.Select(a => (int)a.svm_kernel).ToList());
            var scale_function = get_ranges_str(indexes.Select(a => (int)a.scale_function).ToList());
            var inner_cv_folds = get_ranges_str(indexes.Select(a => a.inner_cv_folds).ToList());

            var p = new List<(string name, string value)>()
            {
                (get_initials(nameof(iteration_index)), iteration_index),//it
                (get_initials(nameof(group_index)), group_index),//gi
                (get_initials(nameof(total_groups)), total_groups),//tg
                (get_initials(nameof(calc_11p_thresholds)), calc_11p_thresholds),//ot
                (get_initials(nameof(repetitions)), repetitions),//r
                (get_initials(nameof(outer_cv_folds)), outer_cv_folds),//oc
                (get_initials(nameof(outer_cv_folds_to_run)), outer_cv_folds_to_run),//oc
                (get_initials(nameof(class_weights)), class_weights),//cw
                (get_initials(nameof(svm_type)), svm_type),//st
                (get_initials(nameof(svm_kernel)), svm_kernel),//sk
                (get_initials(nameof(scale_function)), scale_function),//sf
                (get_initials(nameof(inner_cv_folds)), inner_cv_folds),//ic
            };

            var iter_fn = string.Join("_", p.Select(a => $@"{a.name}[{a.value ?? ""}]").ToList());

            return iter_fn;
        }


        internal static ((long grid_dur, long train_dur, long predict_dur) dur, grid_point grid_point, string[] predict_text)
            inner_cross_validation(
                index_data unrolled_index,
                (int repetitions_index, int outer_cv_index, string train_fn, string grid_fn, string model_fn, string test_fn, string predict_fn, string cm_fn, List<string> train_text, List<string> test_text, List<(int class_id, int training_size)> train_sizes, List<(int class_id, int testing_size)> test_sizes, List<(int class_id, List<int> train_indexes)> train_fold_indexes, List<(int class_id, List<int> test_indexes)> test_fold_indexes) input,
            bool libsvm_train_probability_estimates = true,
            bool log = false)

        {
            const string module_name = nameof(program);
            const string method_name = nameof(inner_cross_validation);

            var train_stdout_filename = "";
            var train_stderr_filename = "";

            var predict_stdout_filename = "";
            var predict_stderr_filename = "";

            var train_grid_search_result = new grid_point();

            var sw_grid = new Stopwatch();

            // perform inner-cv
            if (unrolled_index.inner_cv_folds >= 2)
            {
                sw_grid.Start();
                if (!string.IsNullOrWhiteSpace(input.grid_fn))
                {
                    var train_grid_stdout_file = "";
                    var train_grid_stderr_file = "";

                    train_grid_search_result = grid.grid_parameter_search(
                        settings.libsvm_train_runtime,
                        input.grid_fn,
                        input.train_fn,
                        train_grid_stdout_file,
                        train_grid_stderr_file,
                        unrolled_index.class_weights,
                        unrolled_index.svm_type,
                        unrolled_index.svm_kernel,
                        unrolled_index.repetitions,
                        input.repetitions_index,
                        unrolled_index.outer_cv_folds,
                        input.outer_cv_index,
                        unrolled_index.inner_cv_folds,
                        libsvm_train_probability_estimates);
                }

                sw_grid.Stop();

            }

            var sw_grid_dur = sw_grid.ElapsedMilliseconds;


            // train
            var sw_train = new Stopwatch();
            sw_train.Start();
            var train_result = libsvm.train(settings.libsvm_train_runtime, input.train_fn, input.model_fn, train_stdout_filename, train_stderr_filename, train_grid_search_result.cost, train_grid_search_result.gamma, train_grid_search_result.epsilon, train_grid_search_result.coef0, train_grid_search_result.degree, null, unrolled_index.svm_type, unrolled_index.svm_kernel, null, probability_estimates: libsvm_train_probability_estimates);
            sw_train.Stop();
            var sw_train_dur = sw_train.ElapsedMilliseconds;

            if (log && !string.IsNullOrWhiteSpace(train_result.cmd_line)) io_proxy.WriteLine(train_result.cmd_line, module_name, method_name);
            if (log && !string.IsNullOrWhiteSpace(train_result.stdout)) train_result.stdout.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(line => io_proxy.WriteLine($@"{nameof(train_result)}.{nameof(train_result.stdout)}: {line}", module_name, method_name));
            if (log && !string.IsNullOrWhiteSpace(train_result.stderr)) train_result.stderr.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(line => io_proxy.WriteLine($@"{nameof(train_result)}.{nameof(train_result.stderr)}: {line}", module_name, method_name));


            // predict
            var sw_predict = new Stopwatch();
            sw_predict.Start();
            var predict_result = libsvm.predict(settings.libsvm_predict_runtime, input.test_fn, input.model_fn, input.predict_fn, libsvm_train_probability_estimates, predict_stdout_filename, predict_stderr_filename);

            sw_predict.Stop();
            var sw_predict_dur = sw_train.ElapsedMilliseconds;

            if (log && !string.IsNullOrWhiteSpace(predict_result.cmd_line)) io_proxy.WriteLine(predict_result.cmd_line, module_name, method_name);
            if (log && !string.IsNullOrWhiteSpace(predict_result.stdout)) predict_result.stdout.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(line => io_proxy.WriteLine($@"{nameof(predict_result)}.{nameof(predict_result.stdout)}: {line}", module_name, method_name));
            if (log && !string.IsNullOrWhiteSpace(predict_result.stderr)) predict_result.stderr.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(line => io_proxy.WriteLine($@"{nameof(predict_result)}.{nameof(predict_result.stderr)}: {line}", module_name, method_name));

            var predict_text = io_proxy.ReadAllLines(input.predict_fn, module_name, method_name);
            //io_proxy.WriteLine($@"Loaded {input.predict_fn}");

            return ((sw_grid_dur, sw_train_dur, sw_predict_dur), train_grid_search_result, predict_text);
        }
    }
}
