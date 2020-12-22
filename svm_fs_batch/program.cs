using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace svm_fs_batch
{
    internal class program
    {
        public const string module_name = nameof(program);

        //internal static
        //    (
        //        dataset_group_key group_key,
        //        (int internal_column_index, int external_column_index, string file_tag, string alphabet, string stats, string dimension, string category, string source, string @group, string member, string perspective)[] group_members,
        //        int[] group_members_column_indexes
        //        )[]
        //    get_groups_to_use(dataset_loader dataset)
        //{
        //    var groups = dataset_group_methods.get_groups
        //    (
        //        dataset: dataset,
        //        file_tag: true,
        //        alphabet: true,
        //        stats: true,
        //        dimension: true,
        //        category: true,
        //        source: true,
        //        @group: true,
        //        member: false,
        //        perspective: false
        //    );
        //
        //    groups = groups.Take(10).ToArray();
        //
        //    //var g2 = dataset_group_methods.get_subgroups(groups, true, true, true, true, true, true, true, true, true);
        //
        //    return groups;
        //}

        internal static void Main(string[] args)
        {
            const string method_name = nameof(Main);
            //-experiment_name _20201028084510741 -job_id _ -job_name _ -instance_array_index_start 0 -array_instances 1 -array_start 0 -array_end 6929 -array_step 6930 -inner_folds 1 -outer_cv_folds 5 -outer_cv_folds_to_run 1 -repetitions 1
            //-experiment_name test_20201025014739579 -job_id _ -job_name _ -array_index _ -array_instances _ -array_start 0 -array_end 6929 -array_step 385
            //var x=confusion_matrix.load($@"C:\mmfs1\data\scratch\k1040015\svm_fs_batch\results\test\it_5\x_it-5_gr-5_sv-1_kr-3_sc-2_rn-1_oc-10_ic-10_ix-1-5.cm.csv");
            // debug cmd line parameters: -experiment_name test2 -array_start 0 -array_end 4 -array_index 0 -array_step 5 -array_instances 1 -array_last_index -1


            io_proxy.WriteLine($@"cmd line: {Environment.CommandLine}", module_name, method_name);
            io_proxy.WriteLine($@"processor count: {Environment.ProcessorCount}", module_name, method_name);

            var main_cts = new CancellationTokenSource();
            init.close_notifications(main_cts);
            init.check_x64();
            init.set_gc_mode();
            init.set_thread_counts();

            var program_args = new program_args(args);

            if (program_args.setup)
            {
                setup(main_cts, program_args);
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
            for (var i = program_args.array_start; i <= program_args.instance_array_index_start; i += program_args.array_step) { instance_id++; }
            if (instance_id < 0) throw new ArgumentException(nameof(args), nameof(instance_id));

            worker
            (
                main_cts,
                program_args.experiment_name,
                instance_id,
                program_args.array_instances,
                //program_args.instance_array_index_start,
                //program_args.array_step,
                //program_args.instance_array_index_end,
                program_args.repetitions, 
                program_args.outer_cv_folds,
                program_args.outer_cv_folds_to_run, 
                program_args.inner_folds
            );
        }

        internal static void setup(CancellationTokenSource cts, program_args program_args)
        {
            const string method_name = nameof(setup);

            if (cts.IsCancellationRequested) return;

            program_args.setup = false;
            program_args.args.RemoveAll(a => string.Equals(a.key, nameof(program_args.setup), StringComparison.OrdinalIgnoreCase));

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

        internal static (List<string> pbs_script_lines, string run_line) make_pbs_script(CancellationTokenSource cts, program_args program_args, int pbs_ppn = 1, bool is_job_array = false, int job_array_start = 0, int job_array_end = 0, int job_array_step = 1, bool rerunnable = true)
        {
            //-experiment_name test2 -job_id _ -job_name _ -instance_array_index_start 0 -array_instances 1 -array_start 0 -array_end 6929 -array_step 6930 -inner_folds 5 -outer_cv_folds 5 -outer_cv_folds_to_run 1 -repetitions 5

            //const string method_name = nameof(make_pbs_script); 

            var pbs_script_lines = new List<string>();

            var program_runtime = Process.GetCurrentProcess().MainModule?.FileName;

            if (string.IsNullOrWhiteSpace(program_runtime)) throw new Exception();

            var is_win = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

            //is_win = false;

            var env_pbs_array =  !is_win ? @"%J_%I"                               : @"_";
            var env_jobid =      !is_win ? @"${PBS_JOBID}${MOAB_JOBID}"           : (program_args.args.Any(a => string.Equals(a.key, nameof(program_args.job_id), StringComparison.OrdinalIgnoreCase))                     ? $"{program_args.job_id}"                     : "_");
            var env_jobname =    !is_win ? @"${PBS_JOBNAME}${MOAB_JOBNAME}"       : (program_args.args.Any(a => string.Equals(a.key, nameof(program_args.job_name), StringComparison.OrdinalIgnoreCase))                   ? $"{program_args.job_name}"                   : "_");
            var env_arrayindex = !is_win ? @"${PBS_ARRAYID}${MOAB_JOBARRAYINDEX}" : (program_args.args.Any(a => string.Equals(a.key, nameof(program_args.instance_array_index_start), StringComparison.OrdinalIgnoreCase)) ? $"{program_args.instance_array_index_start}" : "0");
            var env_arraycount = !is_win ? @"${MOAB_JOBARRAYRANGE}"               : (program_args.args.Any(a => string.Equals(a.key, nameof(program_args.array_instances), StringComparison.OrdinalIgnoreCase))            ? $"{program_args.array_instances}"            : "1");


            //var pbs_args = new program_args();



            TimeSpan? pbs_walltime = TimeSpan.FromHours(240);
            
            var pbs_execution_directory = string.Join(Path.DirectorySeparatorChar, new string[] {settings.svm_fs_batch_home, "pbs", program_args.experiment_name});
            
            var pbs_jobname = $@"{program_args.experiment_name}_{nameof(svm_fs_batch)}";
            var pbs_mail_addr = "";
            var pbs_mail_opt = "n";
            var pbs_mem = "";

            // for each job, request 1 node with 64 vcpu, by default, if ppn not specified
            int pbs_nodes = 1;
            if (pbs_ppn <= 0) pbs_ppn = 64;

            var pbs_stdout_filename =     $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), (is_job_array ? env_pbs_array : "") }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.pbs.stdout";
            var pbs_stderr_filename =     $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), (is_job_array ? env_pbs_array : "") }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.pbs.stderr";

            var program_stdout_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), env_jobid, env_jobname, env_arrayindex }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.program.stdout";
            var program_stderr_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), env_jobid, env_jobname, env_arrayindex }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.program.stderr";




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
            var pbs_program_args = new List<(string key, string value)>();

            if (!string.IsNullOrWhiteSpace(program_args.experiment_name)) pbs_program_args.Add(($@"-{nameof(program_args.experiment_name)}", program_args.experiment_name));

            //program_args.job_id = env_jobid;
            //program_args.job_name = env_jobname;
            ////program_args.instance_array_index_start = env_arrayindex;
            ////program_args.array_instances = env_arraycount;
            //program_args.array_start = job_array_start;
            //program_args.array_end = job_array_end;
            //program_args.array_step = job_array_step;


            pbs_program_args.Add(($@"-{nameof(program_args.job_id)}", env_jobid));
            pbs_program_args.Add(($@"-{nameof(program_args.job_name)}", env_jobname));

            if (is_job_array && job_array_step != 0) pbs_program_args.Add(($@"-{nameof(program_args.instance_array_index_start)}", env_arrayindex));
            if (is_job_array && job_array_step != 0) pbs_program_args.Add(($@"-{nameof(program_args.array_instances)}", env_arraycount));

            if (is_job_array && job_array_step != 0) pbs_program_args.Add(($@"-{nameof(program_args.array_start)}", job_array_start.ToString()));
            if (is_job_array && job_array_step != 0) pbs_program_args.Add(($@"-{nameof(program_args.array_end)}", job_array_end.ToString()));
            if (is_job_array && job_array_step != 0) pbs_program_args.Add(($@"-{nameof(program_args.array_step)}", job_array_step.ToString()));

            foreach (var program_arg in program_args.args)
            {
                if (pbs_program_args.All(pbs_program_arg => !string.Equals(pbs_program_arg.key, $@"-{program_arg.key}", StringComparison.OrdinalIgnoreCase)))
                {
                    pbs_program_args.Add(($@"-{program_arg.key}", $@"{program_arg.as_str}"));
                }
            }

            if (!string.IsNullOrEmpty(program_stdout_filename)) pbs_program_args.Add(($@"1>", program_stdout_filename));
            if (!string.IsNullOrEmpty(program_stderr_filename)) pbs_program_args.Add(($@"2>", program_stderr_filename));

            var run_line = $@"{program_runtime} {string.Join(" ", pbs_program_args.Select(a => string.Join(" ", new[] { a.key, a.value }.Where(c => !string.IsNullOrWhiteSpace(c)).ToList())).Where(b => !string.IsNullOrWhiteSpace(b)).ToList())}";

            if (!string.IsNullOrWhiteSpace(pbs_execution_directory)) pbs_script_lines.Add($@"cd {pbs_execution_directory}");
            pbs_script_lines.Add($@"module load GCCcore");
            pbs_script_lines.Add(run_line);

            var pbs_fn = Path.Combine(pbs_execution_directory, $@"{pbs_jobname}.pbs");

            io_proxy.WriteAllLines(cts, pbs_fn, pbs_script_lines);
            io_proxy.WriteLine($@"{program_args.experiment_name}: Saved PBS script. File: {pbs_fn}.");

            return (pbs_script_lines, run_line);
        }


        internal static string get_iteration_folder(string results_root_folder, string experiment_name, int iteration_index, /*string iteration_name = null,*/ int? group_index = null)
        {
            var hr = false;

            var it = $@"it_{(iteration_index + (hr ? 1 : 0))}" /*+ (!string.IsNullOrWhiteSpace(iteration_name) ? iteration_name : "")*/;

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

            return string.Join("_", ranges.Select(range => $@"{range.@from}" + (range.from != range.to ? $@"-{range.to}" + (range.step != -1 && range.step != 0 && range.step != +1 ? $@";{range.step}" : $@"") : $@"")).ToList());
        }


        internal static void worker(
            CancellationTokenSource cts,
            //dataset_loader dataset,
            string experiment_name,
            int instance_index,
            int total_instances,
            //int array_index_start,
            //int array_step,
            //int array_index_last,
            int repetitions,
            int outer_cv_folds,
            int outer_cv_folds_to_run,
            int inner_folds,
            //bool order_by_ppf = false,
            int limit_iteration_not_higher_than_all = 14,
            int limit_iteration_not_higher_than_last = 7,
            bool make_outer_cv_confusion_matrices = false,
            bool test_final_best_bias = false
        )
        {
            const string method_name = nameof(worker);
            if (cts.IsCancellationRequested) return;

            var find_best_group_features_first = false;
            var check_individual_last = true;


            // Load dataset
            var dataset_names = "[1i.aaindex]";
            var dataset = new dataset_loader(cts, dataset_names);

            // Get the feature groups within the dataset
            var groups1 = dataset_group_methods.get_main_groups(cts, dataset, file_tag: true, alphabet: true, stats: true, dimension: true, category: true, source: true, @group: true, member: false, perspective: false);

            // Limit for testing
            groups1 = groups1.Take(2).ToArray();

            // Feature select within each group first, to reduce number of columns
            if (find_best_group_features_first)
            {
                io_proxy.WriteLine($@"Finding best of {groups1.Sum(a => a.columns.Length)} individual columns within the {groups1.Length} groups", module_name, method_name);

                // get best features in each group
                var groups1_reduce_input = dataset_group_methods.get_sub_groups(cts, groups1, file_tag: true, alphabet: true, stats: true, dimension: true, category: true, source: true, @group: true, member: true, perspective: true);

                // There is 1 performance test per instance (i.e. each nested cross validation performance test [1-repetition, 5-fold outer, 5-fold inner])
                // This means that if number of groups is less than number of instances, some instances could be idle... problem, but rare enough to ignore.

                var groups1_reduce_output = groups1_reduce_input
                    .AsParallel()
                    .AsOrdered()
                    .WithCancellation(cts.Token)
                    .Select
                    (
                        (group, group_index) =>
                        feature_selection_worker
                        (
                            cts: cts,
                            dataset: dataset,
                            groups: group,
                            preselect: true,
                            save_status: true,
                            cache_full: false,
                            cache_summary: true,
                            base_group_indexes: null,
                            experiment_name: $"{experiment_name}_stage1_{group_index}",
                            instance_index: instance_index,
                            total_instances: total_instances,
                            //array_index_start: array_index_start,
                            //array_step: array_step,
                            //array_index_last: array_index_last,
                            repetitions: repetitions,
                            outer_cv_folds: outer_cv_folds,
                            outer_cv_folds_to_run: outer_cv_folds_to_run,
                            inner_folds: inner_folds,
                            min_score_increase: 0.01,
                            max_iterations: 10,
                            limit_iteration_not_higher_than_all: 6,
                            limit_iteration_not_higher_than_last: 3,
                            make_outer_cv_confusion_matrices: false
                        )
                    ).ToArray();

                // ungroup (member & perspective)
                var groups1_reduce_output_ungrouped = dataset_group_methods.ungroup(cts, groups1_reduce_output.Select(a => a.best_winner_groups).ToArray());

                // regroup (without member & perspective)
                var groups1_reduce_output_regrouped = dataset_group_methods.get_main_groups(cts, groups1_reduce_output_ungrouped, file_tag: true, alphabet: true, stats: true, dimension: true, category: true, source: true, @group: true, member: false, perspective: false);

                groups1 = groups1_reduce_output_regrouped;
            }


            // Feature select between the dataset groups
            io_proxy.WriteLine($@"Finding best of {groups1.Length} groups (made of {groups1.Sum(a => a.columns.Length)} columns)", module_name, method_name);

            var winner = feature_selection_worker(
                cts: cts,
                dataset: dataset,
                groups: groups1,
                preselect: false,
                save_status: true,
                cache_full: true,
                cache_summary: true,
                base_group_indexes: null,
                experiment_name: $"{experiment_name}_stage2",
                instance_index: instance_index,
                total_instances: total_instances,
                //array_index_start: array_index_start,
                //array_step: array_step,
                //array_index_last: array_index_last,
                repetitions: repetitions,
                outer_cv_folds: outer_cv_folds,
                outer_cv_folds_to_run: outer_cv_folds_to_run,
                inner_folds: inner_folds,
                //order_by_ppf: order_by_ppf,
                min_score_increase: 0.005,
                max_iterations: 100,
                limit_iteration_not_higher_than_all: limit_iteration_not_higher_than_all,
                limit_iteration_not_higher_than_last: limit_iteration_not_higher_than_last,
                make_outer_cv_confusion_matrices: make_outer_cv_confusion_matrices
                );


            // Column based feature select from the winners
            if (check_individual_last)
            {
                // preselect all winner group columns, then test if feature selection goes backwards.

                var best_winner_columns = dataset_group_methods.ungroup(cts, winner.best_winner_groups);
                var best_winner_columns_input = dataset_group_methods.get_main_groups(cts, best_winner_columns, file_tag: true, alphabet: true, stats: true, dimension: true, category: true, source: true, @group: true, member: true, perspective: true);
                var best_winner_columns_output = feature_selection_worker
                (
                    cts: cts,
                    dataset: dataset,
                    groups: best_winner_columns_input,
                    preselect: true,
                    save_status: true,
                    cache_full: false,
                    cache_summary: true,
                    base_group_indexes: null,
                    experiment_name: $"{experiment_name}_stage3",
                    instance_index: instance_index,
                    total_instances: total_instances,
                    //array_index_start: array_index_start,
                    //array_step: array_step,
                    //array_index_last: array_index_last,
                    repetitions: repetitions,
                    outer_cv_folds: outer_cv_folds,
                    outer_cv_folds_to_run: outer_cv_folds_to_run,
                    inner_folds: inner_folds,
                    min_score_increase: 0.01,
                    max_iterations: 10,
                    limit_iteration_not_higher_than_all: 6,
                    limit_iteration_not_higher_than_last: 3,
                    make_outer_cv_confusion_matrices: false
                );

            }

            // Check if result is approximately the same with other parameters values (i.e. variance number of repetitions, outer folds, inner folds, etc.)
            if (test_final_best_bias)
            {
                // stage4 ...

                // 1. test variance of kernel & scale
                //feature_selection_worker(dataset, winner.groups);

                // 2. test variance of repetitions, outer-cv, inner-cv

                // 3. test variance of class weight

            }
        }

        internal static
        (
            (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] best_winner_groups,
            (index_data id, confusion_matrix cm, score_data sd, rank_data rd) best_winner_data,
            List<(index_data id, confusion_matrix cm, score_data sd, rank_data rd)> winners
        )
        feature_selection_worker
        (
            CancellationTokenSource cts,
            dataset_loader dataset,
            (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] groups,
            bool preselect,// preselect all groups
            bool save_status,
            bool cache_full,
            bool cache_summary,
            int[] base_group_indexes,//always include these groups
            string experiment_name,
            int instance_index,
            int total_instances,
            //int array_index_start,
            //int array_step,
            //int array_index_last,
            int repetitions,
            int outer_cv_folds,
            int outer_cv_folds_to_run,
            int inner_folds,
            //bool order_by_ppf = false,
            double min_score_increase = 0.005,
            int max_iterations = 100,
            int limit_iteration_not_higher_than_all = 14,
            int limit_iteration_not_higher_than_last = 7,
            bool make_outer_cv_confusion_matrices = false
        )
        {
            const string method_name = nameof(feature_selection_worker);

            const bool overwrite_cache = false;

            var total_groups = groups.Length;

            base_group_indexes = base_group_indexes?.OrderBy(a => a).Distinct().ToArray();
            var base_column_indexes = base_group_indexes?.SelectMany(base_group_index => groups[base_group_index].columns).OrderBy(a => a).Distinct().ToArray();

            (index_data id, confusion_matrix cm, score_data sd, rank_data rd) best_winner_cm_sd_rd = default;
            (index_data id, confusion_matrix cm, score_data sd, rank_data rd) last_winner_cm_sd_rd = default;

            var all_winners_cm_sd_rd_list = new List<(index_data id, confusion_matrix cm, score_data sd, rank_data rd)>();
            var last_iteration_cm_sd_rd_list = Array.Empty<(index_data id, confusion_matrix cm, score_data sd, rank_data rd)>();

            var cache_files_loaded = new List<string>();
            //var deep_search_index = -1;
            var feature_selection_finished = false;
            var iteration_index = 0;
            //var iteration_name = "";
            var iterations_not_higher_than_best = 0;
            var iterations_not_higher_than_last = 0;
            var last_iteration_folder = "";

            //var all_iterations_winners_cm = new List<confusion_matrix>();
            var feature_selection_status_list = new List<feature_selection_status>();

            var selection_excluded_groups = new List<int>();

            //io_proxy.WriteLine($@"{experiment_name}: Groups: (array indexes: [{(array_index_start)}..{(array_index_last)}]). Total groups: {total_groups}. (This instance #{(instance_index)}/#{total_instances}. array indexes: [{(array_index_start)}..{(array_index_last)}]). All indexes: [0..{(total_groups - 1)}].)", module_name, method_name);
            io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: Total groups: {total_groups}.", module_name, method_name);
            var calibrate = false;


            while (!feature_selection_finished)// || test_final_best_bias)
            {
                

                // get list of work to do this iteration
                (index_data[] indexes_whole, index_data[] indexes_partition) unrolled_indexes = default;



                //if (!feature_selection_finished)
                //{

                var selected_groups = last_winner_cm_sd_rd.sd?.selected_groups?.ToArray() ?? Array.Empty<int>();
                var selected_columns = last_winner_cm_sd_rd.sd?.selected_columns?.ToArray() ?? Array.Empty<int>();

                var selection_excluded_groups2 = last_winner_cm_sd_rd.sd != null ? selection_excluded_groups.Concat(new List<int>() { last_winner_cm_sd_rd.sd.group_array_index }).ToArray() : selection_excluded_groups.ToArray();

                if (preselect)
                {
                    selected_groups = selected_groups.Union(Enumerable.Range(0, groups.Length).ToArray()).ToArray();
                    selected_columns = selected_groups.SelectMany(group_index => groups[group_index].columns).OrderBy(internal_column_index => internal_column_index).Distinct().ToArray();
                    preselect = false;
                    calibrate = true;
                }

                unrolled_indexes = cache_load.get_unrolled_indexes_basic(cts, dataset, experiment_name, iteration_index, /*iteration_name,*/ calibrate ? -1 : total_groups, instance_index, total_instances, repetitions, outer_cv_folds, outer_cv_folds_to_run, inner_folds, selection_excluded_groups2);


                //(string experiment_name, int instance_index, int total_instances, int array_index_start, int array_step, int array_index_last, int repetitions, int outer_cv_folds, int outer_cv_folds_to_run, int inner_folds)
                //}
                //else if (test_final_best_bias)
                //{
                //    // todo: fix problem: 5 groups still being processed, rather than 1...
                //
                //    selected_groups = best_winner_cm_sd_rd.sd?.selected_groups?.ToArray() ?? Array.Empty<int>();
                //    selected_columns = best_winner_cm_sd_rd.sd?.selected_columns?.ToArray() ?? Array.Empty<int>();
                //
                //    deep_search_index++;
                //    iteration_name = $"check_bias_{deep_search_index}";
                //
                //    io_proxy.WriteLine($"{experiment_name}: iteration: {iteration_index}: feature selection finished; statistical tests on best set of features, bias test index: {deep_search_index}.");
                //
                //    unrolled_indexes = cache_load.get_unrolled_indexes_check_bias(deep_search_index, dataset, iteration_index, iteration_name, total_groups, instance_index, total_instances);
                //
                //    if ((unrolled_indexes.indexes_whole == null || unrolled_indexes.indexes_whole.Count == 0) || (unrolled_indexes.indexes_partition == null || unrolled_indexes.indexes_partition.Count == 0))
                //    {
                //        test_final_best_bias = false;
                //        break;
                //    }
                //}
                //else
                //{
                //    throw new Exception();
                //}


                if (base_group_indexes != null && base_group_indexes.Length > 0)
                {
                    selected_groups = selected_groups.Union(base_group_indexes).OrderBy(a => a).ToArray();
                    selected_columns = selected_groups.SelectMany(group_index => groups[group_index].columns).OrderBy(internal_column_index => internal_column_index).Distinct().ToArray();
                }

                if (base_column_indexes != null && base_column_indexes.Length > 0)
                {
                    selected_columns = selected_columns.Union(base_column_indexes).OrderBy(a => a).ToArray();
                }

                if (calibrate && (selected_columns == null || selected_columns.Length == 0))
                {
                    throw new Exception();
                }

                var total_whole_indexes = unrolled_indexes.indexes_whole.Length;
                var total_partition_indexes = unrolled_indexes.indexes_partition.Length;
                io_proxy.WriteLine($"[{instance_index}/{total_instances}] {experiment_name}: iteration: {iteration_index}, total_whole_indexes = {total_whole_indexes}, total_partition_indexes = {total_partition_indexes}.");

                // iteration_all_cm is a list of all merged results (i.e. the individual outer-cross-validation partitions merged)
                var iteration_whole_results = new List<(index_data id, confusion_matrix cm, score_data sd)>();
                



                // get folder and file names for this iteration (iteration_folder & iteration_whole_cm_filename are the same for all partitions; iteration_partition_cm_filename is specific to the partition)
                //var last_iteration_folder = get_iteration_folder(settings.results_root_folder, experiment_name, iteration_index/*, iteration_name*/);
                var iteration_folder = get_iteration_folder(settings.results_root_folder, experiment_name, iteration_index /*, iteration_name*/);
                var iteration_whole_cm_filename1 = cache_full ? Path.Combine(iteration_folder, $@"z_{get_iteration_filename(unrolled_indexes.indexes_whole)}_full.cm.csv") : null;
                var iteration_whole_cm_filename2 = cache_summary ? Path.Combine(iteration_folder, $@"z_{get_iteration_filename(unrolled_indexes.indexes_whole)}_summary.cm.csv") : null;
                var iteration_partition_cm_filename1 = cache_full ? Path.Combine(iteration_folder, $@"x_{get_iteration_filename(unrolled_indexes.indexes_partition)}_full.cm.csv") : null;
                var iteration_partition_cm_filename2 = cache_summary ? Path.Combine(iteration_folder, $@"x_{get_iteration_filename(unrolled_indexes.indexes_partition)}_summary.cm.csv") : null;



                // load cache (first try whole iteration, then try partition, then try individual work items)
                var unrolled_indexes_state = cache_load.load_cache(cts, instance_index, iteration_index, experiment_name, false, cache_files_loaded, iteration_whole_results, unrolled_indexes.indexes_whole, unrolled_indexes.indexes_partition, last_iteration_cm_sd_rd_list, last_winner_cm_sd_rd, best_winner_cm_sd_rd);


                // check if this partition is loaded....
                if (unrolled_indexes_state.indexes_missing_partition.Any())
                {
                    //var p_iteration_index = iteration_index;
                    //var p_selected_groups = selected_groups.ToList();
                    //var p_selected_columns = selected_columns.ToList();

                    var previous_winner_group_index = last_winner_cm_sd_rd.sd?.group_array_index;

                    // after loading the whole iteration cache, all partitions cache, and partition individual merged items cache, and if there are still partition items missing... 
                    // use parallel to split the missing items into cpu bound partitions
                    var indexes_missing_partition_results = unrolled_indexes_state.indexes_missing_partition.AsParallel().AsOrdered().WithCancellation(cts.Token).Select(unrolled_index_data => parallel_index_run(cts: cts, dataset: dataset, groups: groups, base_group_indexes: base_group_indexes, experiment_name: experiment_name, /*array_index_start: array_index_start,*/ /*array_index_last: array_index_last,*/ unrolled_index_data: unrolled_index_data, total_groups: total_groups, selected_groups: selected_groups, selected_columns: selected_columns, previous_winner_group_index: previous_winner_group_index, last_iteration_cm_sd_rd_list: last_iteration_cm_sd_rd_list, last_winner_cm_sd_rd: last_winner_cm_sd_rd, best_winner_cm_sd_rd: best_winner_cm_sd_rd, make_outer_cv_confusion_matrices: make_outer_cv_confusion_matrices)).ToList();

                    iteration_whole_results.AddRange(indexes_missing_partition_results.Where(a => a != default).SelectMany(a => a).ToList());

                    // all partition should be loaded by this point, check
                    unrolled_indexes_state = cache_load.get_missing(instance_index, iteration_whole_results, unrolled_indexes.indexes_whole, unrolled_indexes.indexes_partition);
                    if (unrolled_indexes_state.indexes_missing_partition.Any()) throw new Exception();
                }

                // save partition cache
                {
                    // 4. save CM for all groups of this hpc instance (from index start to index end) merged outer-cv results
                    //
                    
                    //var iteration_partition_results = iteration_whole_results.Where((cm_sd, i) => cm_sd.cm.x_iteration_index == iteration_index && cm_sd.cm.x_group_array_index >= array_index_start && cm_sd.cm.x_group_array_index <= array_index_last).ToList();
                    var iteration_partition_results = iteration_whole_results.Where((cm_sd, i) => cm_sd.id.unrolled_instance_index == instance_index).ToList();

                    confusion_matrix.save(cts, cache_full ? iteration_partition_cm_filename1 : null, cache_summary ? iteration_partition_cm_filename2 : null, overwrite_cache, iteration_partition_results);
                    io_proxy.WriteLine($"[{instance_index}/{total_instances}] {experiment_name}: Partition cache: Saved for iteration {(iteration_index)} group. Files: {iteration_partition_cm_filename1}, {iteration_partition_cm_filename2}.");
                }


                // check if all partitions are loaded....
                if (unrolled_indexes_state.indexes_missing_whole.Any())
                {
                    // 5. load results from other instances (into iteration_whole_results)
                    
                    unrolled_indexes_state = cache_load.load_cache(cts, instance_index, iteration_index, experiment_name, true, cache_files_loaded, iteration_whole_results, unrolled_indexes.indexes_whole, unrolled_indexes.indexes_partition, last_iteration_cm_sd_rd_list, last_winner_cm_sd_rd, best_winner_cm_sd_rd);
                    if (unrolled_indexes_state.indexes_missing_whole.Any()) { throw new Exception($@"{module_name}.{method_name}: {nameof(unrolled_indexes_state)}.{nameof(unrolled_indexes_state.indexes_loaded_whole)}.Any() was true."); }
                }

                if (instance_index == 0)
                {
                    // make sure z saved, in case stopped after completed work, before saving z cache file
                    // save CM with results from all instances
                    confusion_matrix.save(cts, cache_full ? iteration_whole_cm_filename1 : null, cache_summary ? iteration_whole_cm_filename2 : null, overwrite_cache, iteration_whole_results);
                    io_proxy.WriteLine($"[{instance_index}/{total_instances}] {experiment_name}: Full cache: Saved for iteration {(iteration_index)}. Files: {iteration_whole_cm_filename1}, {iteration_whole_cm_filename2}.");
                }
                

                // 6. find winner (highest performance of any group of any class [within scoring_metrics' and 'scoring_class_ids'])
                //      ensure ordering will be consistent between instances
                //      ensure score_data instances are created, may not have been if from cache.

                var iteration_whole_results_fixed = iteration_whole_results
                    .Where
                    (cm_sd =>
                        cm_sd.cm != null &&
                        cm_sd.cm.x_class_id != null &&
                        scoring_args.scoring_class_id == cm_sd.cm.x_class_id.Value &&
                        cm_sd.cm.x_prediction_threshold == null &&
                        cm_sd.cm.x_repetitions_index == -1 &&
                        cm_sd.cm.x_outer_cv_index == -1 &&
                        cm_sd.cm.x_iteration_index == iteration_index &&
                        cm_sd.cm.x_group_array_index != null
                    )
                    .Select
                    (cm_sd =>
                        {

                            var sd = cm_sd.sd;

                            if (sd == null) // could be null if loaded from cache ( only cm loaded, not sd/rd) .
                            {
                                var same_group = last_iteration_cm_sd_rd_list
                                    .FirstOrDefault
                                    (a =>
                                        a.sd.group_array_index == cm_sd.cm.x_group_array_index.Value &&
                                        a.sd.iteration_index + 1 == cm_sd.cm.x_iteration_index.Value &&
                                        a.sd.class_id == cm_sd.cm.x_class_id.Value
                                    );

                                sd = new score_data(cm_sd.cm,
                                    same_group: same_group.sd,
                                    last_winner: last_winner_cm_sd_rd.sd,
                                    best_winner: best_winner_cm_sd_rd.sd
                                    );
                            }

                            return (cm_sd.id, cm_sd.cm, sd);
                        }
                    )
                    .ToList();

                var iteration_whole_results_fixed_with_ranks = rank_data.set_ranks(cts, ref iteration_whole_results_fixed, last_winner_cm_sd_rd.sd, last_iteration_cm_sd_rd_list);

                // todo: check this is correct - ban bad groups from future selection
                {
                    var zero_score = iteration_whole_results_fixed_with_ranks.Where(a => a.sd.same_group_score.value == 0).Select(a => a.sd.group_array_index).ToArray();
                    if (zero_score.Length > 0)
                    {
                        selection_excluded_groups.AddRange(zero_score);
                        io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}:  Excluding groups with zero scores: {string.Join(", ", zero_score)}.");
                    }
                }

                {
                    var always_poor = iteration_whole_results_fixed_with_ranks.Where(a => a.rd.rank_score.value_history.Length >= 5 && a.rd.rank_score.value_history.TakeLast(5).All(b => b.value <= 0.1 /* bottom 10% of ranks for last 5 iterations */)).Select(a => a.sd.group_array_index).ToArray();
                    if (always_poor.Length > 0)
                    {
                        selection_excluded_groups.AddRange(always_poor);
                        io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}:  Excluding groups with always poor scores: {string.Join(", ", always_poor)}.");
                    }
                }

                var this_iteration_winner_cm_sd_rd = iteration_whole_results_fixed_with_ranks[0];
                all_winners_cm_sd_rd_list.Add(this_iteration_winner_cm_sd_rd);

                
                var iteration_winner_group = this_iteration_winner_cm_sd_rd.sd.group_array_index > -1 ? groups[this_iteration_winner_cm_sd_rd.sd.group_array_index] : default;
                var iteration_winner_group_key = iteration_winner_group != default ? iteration_winner_group.group_key : default;


                if (!feature_selection_finished)
                {
                    iterations_not_higher_than_last = this_iteration_winner_cm_sd_rd.sd.is_score_higher_than_last_winner ? 0 : iterations_not_higher_than_last + 1;
                    iterations_not_higher_than_best = this_iteration_winner_cm_sd_rd.sd.is_score_higher_than_best_winner ? 0 : iterations_not_higher_than_best + 1;

                    if (this_iteration_winner_cm_sd_rd.sd.is_score_higher_than_best_winner)
                    {
                        best_winner_cm_sd_rd = this_iteration_winner_cm_sd_rd;
                    }

                    var available_groups = Enumerable.Range(0, groups.Length).ToArray();
                    available_groups = available_groups.Except(this_iteration_winner_cm_sd_rd.sd.selected_groups).ToArray();
                    if (base_group_indexes != null && base_group_indexes.Length > 0) available_groups = available_groups.Except(base_group_indexes).ToArray();
                    if (selection_excluded_groups != null && selection_excluded_groups.Count > 0) available_groups = available_groups.Except(selection_excluded_groups).ToArray();


                    var groups_not_available = available_groups.Length == 0;
                    var not_higher_than_last_limit_reached = iterations_not_higher_than_last >= limit_iteration_not_higher_than_last;
                    var not_higher_than_best_limit_reached = iterations_not_higher_than_best >= limit_iteration_not_higher_than_all;
                    var max_iterations_reached = max_iterations > 0 && iteration_index + 1 >= max_iterations;
                    var score_increase_not_reached = min_score_increase > 0 && (this_iteration_winner_cm_sd_rd.sd.same_group_score.value - (last_winner_cm_sd_rd.sd?.same_group_score?.value ?? 0)) < min_score_increase;

                    //if (feature_selection_finished) io_proxy.WriteLine($@"{nameof(feature_selection_finished)}: {nameof(feature_selection_finished)} = {feature_selection_finished}");
                    if (not_higher_than_last_limit_reached) io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(not_higher_than_last_limit_reached)} = {not_higher_than_last_limit_reached}");
                    if (not_higher_than_best_limit_reached) io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(not_higher_than_best_limit_reached)} = {not_higher_than_best_limit_reached}");
                    if (groups_not_available) io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(groups_not_available)} = {groups_not_available}");
                    if (max_iterations_reached) io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(max_iterations_reached)} = {max_iterations_reached}");
                    if (score_increase_not_reached) io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(score_increase_not_reached)} = {score_increase_not_reached}");

                    feature_selection_finished = /*feature_selection_finished ||*/ not_higher_than_last_limit_reached || not_higher_than_best_limit_reached || groups_not_available;
                }

                // if main program instance, then save iteration winner to file
                if (save_status && instance_index == 0)
                {
                    {
                        var feature_selection_status = new feature_selection_status()
                        {
                            scoring_metrics = scoring_args.scoring_metrics,
                            scoring_class_ids = new[] { scoring_args.scoring_class_id },

                            this_winner_score_data = this_iteration_winner_cm_sd_rd.sd ?? new score_data(),
                            last_winner_score_data = last_winner_cm_sd_rd.sd ?? new score_data(),
                            best_winner_score_data = best_winner_cm_sd_rd.sd ?? new score_data(),

                            winner_key = iteration_winner_group_key,
                            //deep_search_index = deep_search_index,
                            feature_selection_finished = feature_selection_finished,
                            iterations_not_higher_than_all = iterations_not_higher_than_best,
                            iterations_not_higher_than_last = iterations_not_higher_than_last,
                            total_groups = total_groups,
                        };

                        feature_selection_status_list.Add(feature_selection_status);

                        {
                            // Save the SD of winners from all iterations

                            var all_iterations_status_fn = Path.Combine(iteration_folder, $@"winners_sd_compare_{iteration_index}.csv");

                            if (io_proxy.is_file_available(cts, all_iterations_status_fn))
                            {
                                io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: Already saved for iteration {(iteration_index)}. File: {all_iterations_status_fn}.");
                            }
                            else
                            {
                                io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: Unavailable for iteration {(iteration_index)}. File: {all_iterations_status_fn}.");
                                feature_selection_status.save(cts, all_iterations_status_fn, feature_selection_status_list);
                                io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: Saved for iteration {(iteration_index)}. File: {all_iterations_status_fn}.");
                            }
                        }
                    }

                    {
                        // Save the CM ranked for the current iteration (winner rank #0)
                        var iteration_cm_ranks_fn1 = Path.Combine(iteration_folder, $@"iteration_ranks_cm_{iteration_index}.csv");
                        var iteration_cm_ranks_fn2 = Path.Combine(iteration_folder, $@"iteration_ranks_cm_{iteration_index}_summary.csv");
                        if (io_proxy.is_file_available(cts, iteration_cm_ranks_fn1) && io_proxy.is_file_available(cts, iteration_cm_ranks_fn2))
                        {
                            io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: Already saved for iteration {(iteration_index)}. Files: {iteration_cm_ranks_fn1}, {iteration_cm_ranks_fn2}.");
                        }
                        else
                        {
                            io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: Unavailable for iteration {(iteration_index)}. Files: {iteration_cm_ranks_fn1}, {iteration_cm_ranks_fn2}.");
                            confusion_matrix.save(cts, cache_full ? iteration_cm_ranks_fn1 : null, cache_summary ? iteration_cm_ranks_fn2 : null, overwrite_cache, iteration_whole_results_fixed_with_ranks);
                            io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: Saved for iteration {(iteration_index)}. Files: {iteration_cm_ranks_fn1}, {iteration_cm_ranks_fn2}.");
                        }
                    }

                    {
                        // Save the CM of winners from all iterations
                        var winners_cm_fn1 = Path.Combine(iteration_folder, $@"winners_cm_{iteration_index}.csv");
                        var winners_cm_fn2 = Path.Combine(iteration_folder, $@"winners_cm_{iteration_index}_summary.csv");
                        if (io_proxy.is_file_available(cts, winners_cm_fn1) && io_proxy.is_file_available(cts, winners_cm_fn2))
                        {
                            io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: Already saved for iteration {(iteration_index)}. Files: {winners_cm_fn1}, {winners_cm_fn2}.");
                        }
                        else
                        {
                            io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: Unavailable for iteration {(iteration_index)}. Files: {winners_cm_fn1}, {winners_cm_fn2}.");
                            confusion_matrix.save(cts, cache_full ? winners_cm_fn1 : null, cache_summary ? winners_cm_fn2 : null, overwrite_cache, all_winners_cm_sd_rd_list);
                            io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: Saved for iteration {(iteration_index)}. Files: {winners_cm_fn1}, {winners_cm_fn2}.");
                        }
                    }

                    {
                        // Save the prediction list for misclassification analysis
                        var prediction_list_filename = Path.Combine(iteration_folder, $@"iteration_prediction_list_{iteration_index}.csv");
                        if (io_proxy.is_file_available(cts, prediction_list_filename))
                        {
                            io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: Already saved for iteration {(iteration_index)}. File: {prediction_list_filename}.");
                        }
                        else
                        {
                            io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: Unavailable for iteration {(iteration_index)}. File: {prediction_list_filename}.");
                            prediction.save(cts, prediction_list_filename, iteration_whole_results_fixed_with_ranks.Select(a => a.cm).ToArray());
                            io_proxy.WriteLine($@"[{instance_index}/{total_instances}] {experiment_name}: Saved for iteration {(iteration_index)}. File: {prediction_list_filename}.");
                        }

                    }
                }

                io_proxy.WriteLine($"[{instance_index}/{total_instances}] {experiment_name}: Finished: iteration {(iteration_index)}.  {(feature_selection_finished ? "Finished" : "Not finished")}.");

                last_winner_cm_sd_rd = this_iteration_winner_cm_sd_rd;
                last_iteration_cm_sd_rd_list = iteration_whole_results_fixed_with_ranks;
                last_iteration_folder = iteration_folder;
                iteration_index++;
                calibrate = false;
            }

            io_proxy.WriteLine($"[{instance_index}/{total_instances}] {experiment_name}: Finished: all iterations of feature selection for {total_groups} groups.");
            io_proxy.WriteLine($"[{instance_index}/{total_instances}] {experiment_name}: Finished: winning score = {best_winner_cm_sd_rd.sd.same_group_score.value}, total columns = {best_winner_cm_sd_rd.sd.num_columns}.");


            //var best_grouped_keys = best_winner_score_data.sd.selected_groups.Select(group_index =>
            //{
            //    var g = groups[group_index];
            //    return g.group_key;
            //}).ToArray();
            //
            //var best_column_keys = best_winner_score_data.sd.selected_columns.Select(column_index =>
            //{
            //    var h = dataset.header_list[column_index];
            //    if (h.internal_column_index != column_index) throw new Exception();
            //    return h;
            //}).ToArray();
            //
            //var best_column_internal_column_indexes = best_column_keys.Select(a => a.internal_column_index).ToArray();
            //
            //var winner_groups_ret = (best_grouped_keys, best_column_keys, best_column_internal_column_indexes);

            var best_winner_groups = best_winner_cm_sd_rd.sd.selected_groups.Select(group_index => groups[group_index]).ToArray();

            return (best_winner_groups, best_winner_cm_sd_rd, all_winners_cm_sd_rd_list);
        }

        private static (index_data id, confusion_matrix cm, score_data sd)[] parallel_index_run(
            CancellationTokenSource cts,
            dataset_loader dataset,
            (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] groups,
            int[] base_group_indexes,
            string experiment_name,
            //int array_index_start,
            //int array_index_last,
            index_data unrolled_index_data,
            int total_groups,
            int[] selected_groups,
            int[] selected_columns,
            int? previous_winner_group_index,
            (index_data id, confusion_matrix cm, score_data sd, rank_data rd)[] last_iteration_cm_sd_rd_list,
            (index_data id, confusion_matrix cm, score_data sd, rank_data rd) last_winner_cm_sd_rd,
            (index_data id, confusion_matrix cm, score_data sd, rank_data rd) best_winner_cm_sd_rd,
            bool make_outer_cv_confusion_matrices = false,
            bool overwrite_cache = false,
            bool save_group_cache = false,
            bool save_full = false,
            bool save_summary = false)
        {
            if (cts.IsCancellationRequested) return default;

            if (dataset == null) throw new ArgumentOutOfRangeException(nameof(dataset));
            if (string.IsNullOrWhiteSpace(experiment_name)) throw new ArgumentOutOfRangeException(nameof(experiment_name));
            if (unrolled_index_data == null) throw new ArgumentOutOfRangeException(nameof(unrolled_index_data));


            io_proxy.WriteLine($@"{experiment_name}: Start parallel index: {unrolled_index_data?.id_index_str()} {unrolled_index_data?.id_fold_str()} {unrolled_index_data?.id_ml_str()}");// (array indexes: [{(array_index_start)}..{(array_index_last)}]).");

            var group_array_index = unrolled_index_data?.group_array_index ?? -1;

            var group_key = group_array_index > -1 && groups != null && groups.Length - 1 >= group_array_index ? groups[group_array_index].group_key : default;
            var group_folder = get_iteration_folder(settings.results_root_folder, experiment_name, unrolled_index_data.iteration_index, /*unrolled_index_data.iteration_name,*/ group_array_index);
            
            io_proxy.WriteLine($@"{experiment_name}: Group cache: Unavailable for iteration {(unrolled_index_data.iteration_index)} group {(group_array_index)}/{total_groups}");// (array indexes: [{(array_index_start)}..{(array_index_last)}]).");

            var test_selected_groups = selected_groups.OrderBy(group_index => group_index).ToArray();
            var test_selected_columns = selected_columns.OrderBy(col_index => col_index).ToArray();

            var is_group_selected = test_selected_groups.Contains(group_array_index);
            var is_only_selection = is_group_selected && test_selected_groups.Length == 1;
            var is_last_winner = group_array_index == previous_winner_group_index;

            // if selected, remove.  if not selected, add.  if only group, no action.  if just added, no action.
            var selection_direction = !is_group_selected ? direction.forwards : (!is_only_selection && !is_last_winner ? direction.backwards : direction.neutral);
            
            // don't add/remove group indexes that don't exist (i.e. for calibration)
            if (group_array_index < 0) selection_direction = direction.neutral;

            // don't add/remove base features
            if (base_group_indexes != null && base_group_indexes.Contains(group_array_index)) selection_direction = direction.neutral;

            if (group_array_index > -1 && selection_direction != direction.neutral)
            {
                if (selection_direction == direction.forwards)
                {
                    test_selected_groups = test_selected_groups.Union(new[] { group_array_index }).OrderBy(group_index => group_index).ToArray();
                } 
                else if (selection_direction == direction.backwards)
                {
                    test_selected_groups = test_selected_groups.Except(new[] { group_array_index }).OrderBy(group_index => group_index).ToArray();
                }

                test_selected_columns = test_selected_groups.SelectMany(group_index => groups[group_index].columns).OrderBy(col_index => col_index).Distinct().ToArray();
            }

            test_selected_columns = remove_duplicate_columns(dataset, test_selected_columns);


            var selection_test_info = new selection_test_info() { y_is_group_selected = is_group_selected, y_is_only_selection = is_only_selection, y_is_last_winner = is_last_winner, y_selection_direction = selection_direction, y_test_groups_count = test_selected_groups?.Length ?? 0, y_test_columns_count = test_selected_columns?.Length ?? 0, y_test_groups = test_selected_groups?.ToArray() ?? Array.Empty<int>(), y_test_columns = test_selected_columns?.ToArray() ?? Array.Empty<int>(), };

            var ocv_result = outer_cross_validation(cts: cts, dataset: dataset, experiment_name: experiment_name, selection_test_info: selection_test_info, test_selected_columns: test_selected_columns, unrolled_index_data: unrolled_index_data, group_folder: group_folder, 
                make_outer_cv_confusion_matrices: make_outer_cv_confusion_matrices, group_key: group_key, overwrite_cache: overwrite_cache, save_group_cache: save_group_cache, save_full: save_full, save_summary: save_summary
                );

            io_proxy.WriteLine($@"{experiment_name}: Finished parallel index: {unrolled_index_data?.id_index_str()} {unrolled_index_data?.id_fold_str()} {unrolled_index_data?.id_ml_str()}");// (array indexes: [{(array_index_start)}..{(array_index_last)}]).");

            //return ocv_result.mcv_cm;

            var group_cm_sd_list = ocv_result.mcv_cm.Select(cm =>
                {
                    var same_group = last_iteration_cm_sd_rd_list.FirstOrDefault(a => a.sd.group_array_index == cm.x_group_array_index.Value && a.sd.iteration_index + 1 == cm.x_iteration_index.Value && a.sd.class_id == cm.x_class_id.Value);

                    var sd = new score_data(cm, same_group: same_group.sd, last_winner: last_winner_cm_sd_rd.sd, best_winner: best_winner_cm_sd_rd.sd);

                    return (ocv_result.id, cm, sd);
                })
                .ToArray();

            return group_cm_sd_list;
        }

        private static (index_data id, confusion_matrix[] ocv_cm, confusion_matrix[] mcv_cm) outer_cross_validation
        (
            CancellationTokenSource cts,
            dataset_loader dataset,
            string experiment_name,
            selection_test_info selection_test_info,
            int[] test_selected_columns,
            index_data unrolled_index_data,
            string group_folder,
            bool make_outer_cv_confusion_matrices,
            dataset_group_key group_key,

            bool overwrite_cache = false,
            bool save_group_cache = false,
            bool save_full = false,
            bool save_summary = false
        )
        {
            if (cts.IsCancellationRequested) return default;

            if (dataset==null) throw new ArgumentOutOfRangeException(nameof(dataset));
            if (string.IsNullOrWhiteSpace(experiment_name)) throw new ArgumentOutOfRangeException(nameof(experiment_name));
            if (unrolled_index_data == null) throw new ArgumentOutOfRangeException(nameof(unrolled_index_data));
            if (test_selected_columns == null || test_selected_columns.Length == 0) throw new ArgumentOutOfRangeException(nameof(test_selected_columns));
            //if (selection_test_info == null) throw new ArgumentOutOfRangeException(nameof(selection_test_info));
            //if (group_key == null) throw new ArgumentOutOfRangeException(nameof(group_key));

            // 1. make outer-cv files
            var outer_cv_inputs = make_outer_cv_inputs(
                cts: cts,
                dataset: dataset,
                column_indexes: test_selected_columns,
                group_folder: group_folder,
                unrolled_index: unrolled_index_data
                );


            // 2. run libsvm
            //foreach (var outer_cv_input in outer_cv_inputs)
            var pdl = outer_cv_inputs
                .Where(a => a.outer_cv_index != -1 && a.repetitions_index != -1)
                .AsParallel()
                .AsOrdered()
                .WithCancellation(cts.Token)
                .Select(outer_cv_input => outer_cross_validation_single(cts, dataset, experiment_name, selection_test_info, unrolled_index_data, group_folder, group_key, outer_cv_input,
                    make_outer_cv_confusion_matrices, overwrite_cache, save_group_cache, save_full, save_summary
                    ))
                .ToArray();

            // 1a. the ocvi index -1 is merged data
            var merged_cv_input = outer_cv_inputs.First(a => a.outer_cv_index == -1);

            var ocv_cm = make_outer_cv_confusion_matrices ? pdl.Where(a => a.ocv_cm != null).SelectMany(a => a.ocv_cm).ToArray() : null;
            var prediction_data_list = pdl.Select(a => a.prediction_data).ToArray();

            // 3. make confusion matrix from the merged prediction results
            // note: repeated 'labels' lines will be ignored
            var merged_prediction_text = prediction_data_list.SelectMany(a => a.predict_text).ToList();

            var merged_test_class_sample_id_list = merged_cv_input.test_fold_indexes.SelectMany(a => a.test_indexes).ToList();

            var prediction_file_data = perf.load_prediction_file(cts, merged_cv_input.test_text, null, merged_prediction_text, unrolled_index_data.calc_11p_thresholds, merged_test_class_sample_id_list);
            var mcv_cm = prediction_file_data.cm_list;

            // add any missing details to the confusion-matrix
            update_merged_cm(
                cts: cts,
                dataset: dataset,
                experiment_name: experiment_name,
                selection_test_info: selection_test_info,
                prediction_file_data: prediction_file_data,
                unrolled_index: unrolled_index_data,
                group_key: group_key,
                merged_cv_input: merged_cv_input,
                prediction_data_list: prediction_data_list
                );

            // save CM for group
            if (save_group_cache && (save_full || save_summary))
            {
                confusion_matrix.save(cts, save_full ? merged_cv_input.cm_fn1 : null, save_summary ? merged_cv_input.cm_fn2 : null, overwrite_cache, mcv_cm);
                io_proxy.WriteLine($@"{experiment_name}: Group MCV cache: Saved: {unrolled_index_data?.id_index_str()} {unrolled_index_data?.id_fold_str()} {unrolled_index_data?.id_ml_str()}. Files: {merged_cv_input.cm_fn1}, {merged_cv_input.cm_fn2}.");
            }
            else
            {
                io_proxy.WriteLine($@"{experiment_name}: Group MCV cache: Save disabled: {unrolled_index_data?.id_index_str()} {unrolled_index_data?.id_fold_str()} {unrolled_index_data?.id_ml_str()}. Files: {merged_cv_input.cm_fn1}, {merged_cv_input.cm_fn2}.");

                if (!save_group_cache) io_proxy.delete_directory(group_folder);
            }


            return (unrolled_index_data, ocv_cm, mcv_cm);
        }

        private static (((long grid_dur, long train_dur, long predict_dur) dur, grid_point grid_point, string[] predict_text) prediction_data, confusion_matrix[] ocv_cm)
            outer_cross_validation_single(
                CancellationTokenSource cts,
                dataset_loader dataset,
                string experiment_name, 
                selection_test_info selection_test_info,
                index_data unrolled_index_data,
                string group_folder,
                dataset_group_key group_key, 
                (int repetitions_index, int outer_cv_index, string train_fn, string grid_fn, string model_fn, string test_fn, string predict_fn, string cm_fn1, string cm_fn2, string[] train_text, string[] test_text, (int class_id, int training_size)[] train_sizes, (int class_id, int testing_size)[] test_sizes, (int class_id, int[] train_indexes)[] train_fold_indexes, (int class_id, int[] test_indexes)[] test_fold_indexes) outer_cv_input,
                bool make_outer_cv_confusion_matrices = false,
                bool overwrite_cache = false,
                bool save_group_cache = false,
                bool save_full = false,
                bool save_summary = false

            )
        {
            confusion_matrix[] ocv_cm = null;

            //if (outer_cv_input.outer_cv_index == -1 || outer_cv_input.repetitions_index == -1) continue; // -1 is the index for the merged text

            // call libsvm... returns raw prediction file data from doing: parameter search -> train (with best parameters) -> predict
            var prediction_data = inner_cross_validation(cts, unrolled_index_data, outer_cv_input);

            // optional: make_outer_cv_confusion_matrices: this will output the individual outer-cross-validation confusion matrices (i.e. if outer-cv-folds = 5, then 5 respective confusion-matrices will be created, as well as the merged data confusion-matrix).
            if (make_outer_cv_confusion_matrices)
            {
                var ocv_test_class_sample_id_list = outer_cv_input.test_fold_indexes.SelectMany(a => a.test_indexes).ToList();

                // convert text results to confusion matrix and performance metrics
                var ocv_prediction_file_data = perf.load_prediction_file(cts, outer_cv_input.test_text, null, prediction_data.predict_text, unrolled_index_data.calc_11p_thresholds, ocv_test_class_sample_id_list);

                // add any missing meta details to the confusion-matrix
                update_merged_cm(cts: cts, dataset: dataset, experiment_name: experiment_name, selection_test_info: selection_test_info, prediction_file_data: ocv_prediction_file_data, unrolled_index: unrolled_index_data, group_key: group_key, merged_cv_input: outer_cv_input, prediction_data_list: new[] { prediction_data });

                //ocv_cm.AddRange(ocv_prediction_file_data.cm_list);
                ocv_cm = ocv_prediction_file_data.cm_list;

                if (save_group_cache && (save_full || save_summary))
                {
                    // save outer-cross-validation confusion-matrix CM for group
                    confusion_matrix.save(cts, save_full ? outer_cv_input.cm_fn1 : null, save_summary ? outer_cv_input.cm_fn2 : null, overwrite_cache, ocv_prediction_file_data.cm_list);
                    io_proxy.WriteLine($@"{experiment_name}: Group OCV cache: Saved: [R({outer_cv_input.repetitions_index}/{unrolled_index_data.repetitions}) O({outer_cv_input.outer_cv_index}/{unrolled_index_data.outer_cv_folds})] {unrolled_index_data.id_index_str()} {unrolled_index_data.id_fold_str()} {unrolled_index_data.id_ml_str()}. Files: {outer_cv_input.cm_fn1}, {outer_cv_input.cm_fn2}.");
                }
                else
                {
                    io_proxy.WriteLine($@"{experiment_name}: Group OCV cache: Save disabled: [R({outer_cv_input.repetitions_index}/{unrolled_index_data.repetitions}) O({outer_cv_input.outer_cv_index}/{unrolled_index_data.outer_cv_folds})] {unrolled_index_data.id_index_str()} {unrolled_index_data.id_fold_str()} {unrolled_index_data.id_ml_str()}. Files: {outer_cv_input.cm_fn1}, {outer_cv_input.cm_fn2}.");
                }
            }

            // delete temporary files
            io_proxy.delete_file(outer_cv_input.train_fn);
            io_proxy.delete_file(outer_cv_input.grid_fn);
            io_proxy.delete_file(outer_cv_input.model_fn);
            io_proxy.delete_file(outer_cv_input.test_fn);
            io_proxy.delete_file(outer_cv_input.predict_fn);
            // do not delete the confusion-matrix: io_proxy.Delete(outer_cv_input.cm_fn);

            return (prediction_data, ocv_cm);
        }

        private static void update_merged_cm
        (
            CancellationTokenSource cts,
            dataset_loader dataset,
            string experiment_name,
            selection_test_info selection_test_info,
            (prediction[] prediction_list, confusion_matrix[] cm_list) prediction_file_data,
            index_data unrolled_index,
            dataset_group_key group_key,

            //List<int> test_selected_groups,
            //List<int> test_selected_columns,


            (int repetitions_index, int outer_cv_index, string train_fn, string grid_fn, string model_fn, string test_fn, string predict_fn, string cm_fn1, string cm_fn2, string[] train_text, string[] test_text, (int class_id, int training_size)[] train_sizes, (int class_id, int testing_size)[] test_sizes, (int class_id, int[] train_indexes)[] train_fold_indexes, (int class_id, int[] test_indexes)[] test_fold_indexes) merged_cv_input,
            ((long grid_dur, long train_dur, long predict_dur) dur, grid_point grid_point, string[] predict_text)[] prediction_data_list
        )
        {
            if (cts.IsCancellationRequested) return;

            if (prediction_file_data.cm_list == null) throw new ArgumentOutOfRangeException(nameof(prediction_file_data.cm_list));

            Parallel.ForEach(prediction_file_data.cm_list,
                cm =>
                //foreach (var cm in prediction_file_data.cm_list)
                {
                    //var update_ppf_ppg = true;//cm.x_new_column_count != test_selected_columns.Count || cm.x_new_group_count != test_selected_groups.Count;

                    cm.selection_test_info = selection_test_info;

                    if (cm.grid_point == null)
                    {
                        cm.grid_point = new grid_point(prediction_data_list?.Select(a => a.grid_point).ToArray());
                    }

                    //cm.x_libsvm_cv = prediction_data_list?.Where(a => a.grid_point?.cv_rate != null).Select(a => (double)a.grid_point.cv_rate).DefaultIfEmpty(default).Average() ?? default;

                    cm.x_class_name = settings.class_names?.FirstOrDefault(b => cm.x_class_id == b.class_id).class_name;
                    cm.x_class_size = dataset.class_sizes?.First(b => b.class_id == cm.x_class_id).class_size ?? -1;
                    cm.x_class_testing_size = merged_cv_input.test_sizes?.First(b => b.class_id == cm.x_class_id).testing_size ?? -1;
                    cm.x_class_training_size = merged_cv_input.train_sizes?.First(b => b.class_id == cm.x_class_id).training_size ?? -1;
                    cm.x_class_weight = unrolled_index.class_weights?.FirstOrDefault(b => cm.x_class_id == b.class_id).class_weight;
                    cm.x_duration_grid_search = prediction_data_list?.Select(a => a.dur.grid_dur).DefaultIfEmpty(0).Sum().ToString(CultureInfo.InvariantCulture);
                    cm.x_duration_testing = prediction_data_list?.Select(a => a.dur.predict_dur).DefaultIfEmpty(0).Sum().ToString(CultureInfo.InvariantCulture);
                    cm.x_duration_training = prediction_data_list?.Select(a => a.dur.train_dur).DefaultIfEmpty(0).Sum().ToString(CultureInfo.InvariantCulture);
                    cm.x_experiment_name = experiment_name;
                    cm.x_group_array_index = unrolled_index.group_array_index;
                    cm.x_inner_cv_folds = unrolled_index.inner_cv_folds;
                    cm.x_iteration_index = unrolled_index.iteration_index;
                    //cm.x_iteration_name = unrolled_index.iteration_name;

                    cm.x_key_file_tag = group_key?.value.file_tag ?? "";
                    cm.x_key_alphabet = group_key?.value.alphabet ?? "";
                    cm.x_key_stats = group_key?.value.stats ?? "";
                    cm.x_key_category = group_key?.value.category ?? "";
                    cm.x_key_dimension = group_key?.value.dimension ?? "";
                    cm.x_key_group = group_key?.value.@group ?? "";
                    cm.x_key_member = group_key?.value.member ?? "";
                    cm.x_key_perspective = group_key?.value.perspective ?? "";
                    cm.x_key_source = group_key?.value.source ?? "";

                    //cm.x_new_column_count = test_selected_columns.Count;
                    //cm.x_new_group_count = test_selected_groups.Count;
                    //cm.x_old_column_count = previous_selected_columns.Count;
                    //cm.x_old_group_count = previous_selected_groups.Count;
                    cm.x_outer_cv_folds = unrolled_index?.outer_cv_folds ?? default;
                    cm.x_outer_cv_folds_to_run = unrolled_index?.outer_cv_folds_to_run ?? default;
                    cm.x_outer_cv_index = merged_cv_input.outer_cv_index;
                    cm.x_calc_11p_thresholds = unrolled_index?.calc_11p_thresholds;
                    cm.x_repetitions_index = merged_cv_input.repetitions_index;
                    cm.x_repetitions_total = unrolled_index?.repetitions ?? default;
                    cm.x_scale_function = unrolled_index?.scale_function ?? default;
                    cm.x_svm_kernel = unrolled_index?.svm_kernel ?? default;
                    cm.x_svm_type = unrolled_index?.svm_type ?? default;
                    cm.x_total_groups = unrolled_index?.total_groups;
                    //cm.x_columns_included = test_selected_columns.ToArray();
                    //cm.x_groups_included = test_selected_groups.ToArray();

                    //if (update_ppf_ppg)
                    //{
                    //    cm.calculate_ppf_ppg();
                    //}
                });
        }

        internal static int[] remove_duplicate_columns(dataset_loader dataset, int[] query_cols)
        {
            const string method_name = nameof(remove_duplicate_columns);
            // remove duplicate columns (may exist in separate groups)
            //var query_col_dupe_check = idr.dataset_instance_list_grouped.SelectMany(a => a.examples).SelectMany(a => query_cols.Select(b => (query_col: b, fv: a.feature_data[b].fv)).ToList()).GroupBy(b => b.query_col).Select(b => (query_col: b.Key, values: b.Select(c => c.fv).ToList())).ToList();

            if (query_cols == null) throw new ArgumentOutOfRangeException(nameof(query_cols));
            if (query_cols.Length <= 1) return query_cols;

            var query_col_dupe_check = query_cols.Select(col_index => dataset
                    .value_list
                    .SelectMany(class_values => class_values.val_list.Select((row, row_index) =>
                    {
                        var rc = row.row_columns[col_index];

                        if (rc.col_index != col_index || rc.row_index != row_index) throw new Exception();

                        return rc.row_column_val;
                    }).ToArray()).ToArray()).ToArray();

            var dupe_clusters = new List<List<int>>();
            for (var i = 0; i < query_col_dupe_check.Length; i++)
            {
                for (var j = 0; j < query_col_dupe_check.Length; j++)
                {
                    if (i <= j) continue;

                    if (query_col_dupe_check[i].SequenceEqual(query_col_dupe_check[j]))
                    {
                        var cluster = new List<int>() { query_cols[i], query_cols[j] };
                        var x = dupe_clusters.Where(a => a.Any(b => cluster.Any(c => b == c))).ToArray();
                        
                        for (var k = 0; k < x.Length; k++)
                        {
                            cluster.AddRange(x[k]);
                            dupe_clusters.Remove(x[k]);
                        }

                        cluster = cluster.OrderBy(a => a).Distinct().ToList();
                        dupe_clusters.Add(cluster);
                    }
                }
            }

            var indexes_to_remove = dupe_clusters.Where(dc => dc != null && dc.Count > 1).SelectMany(dc => dc.Skip(1).ToArray()).ToArray();

            if (indexes_to_remove.Length > 0)
            {
                var ret = query_cols.Except(indexes_to_remove).ToArray();
                io_proxy.WriteLine($"Removed duplicate columns: {string.Join(", ", indexes_to_remove)}.  Preserved columns: {string.Join(", ", ret)}", module_name, method_name);
                return ret;
            }

            return query_cols;
        }

        internal enum direction { forwards, neutral, backwards }

        internal static (
            int repetitions_index,
            int outer_cv_index,
            string train_fn,
            string grid_fn,
            string model_fn,
            string test_fn,
            string predict_fn,
            string cm_fn1,
            string cm_fn2,
            string[] train_text,
            string[] test_text,
            (int class_id, int training_size)[] train_sizes,
            (int class_id, int testing_size)[] test_sizes,
            (int class_id, int[] train_indexes)[] train_fold_indexes,
            (int class_id, int[] test_indexes)[] test_fold_indexes
            )[]

            make_outer_cv_inputs(
                CancellationTokenSource cts,
                dataset_loader dataset,
                int[] column_indexes,
                string group_folder,
                index_data unrolled_index
                )
        {
            if (cts.IsCancellationRequested) return default;

            const bool preserve_fid = false; // whether to keep the original FID in the libsvm training/testing files (note: if not, bear in mind that features with zero values are removed, so this must not distort the ordering...).

            if (column_indexes == null || column_indexes.Length == 0) throw new ArgumentOutOfRangeException(nameof(column_indexes));
            if (unrolled_index.repetitions <= 0) throw new ArgumentOutOfRangeException(nameof(unrolled_index), nameof(unrolled_index.repetitions));
            if (unrolled_index.outer_cv_folds <= 0) throw new ArgumentOutOfRangeException(nameof(unrolled_index), nameof(unrolled_index.repetitions));
            if (unrolled_index.outer_cv_folds_to_run > unrolled_index.outer_cv_folds) throw new ArgumentOutOfRangeException(nameof(unrolled_index), nameof(unrolled_index.outer_cv_folds_to_run));


            // ensure columns in correct order, and has class id
            column_indexes = column_indexes.OrderBy(a => a).ToArray();
            if (column_indexes[0] != 0)
            {
                column_indexes = (new int[]{0}).Concat(column_indexes).ToArray();
                //.Insert(0, 0);
            }


            //var r_o_indexes = new List<(int repetitions_index, int outer_cv_index)>();
            var total_repetitions = (unrolled_index.repetitions == 0 ? 1 : unrolled_index.repetitions);
            var total_outer_folds_to_run = (unrolled_index.outer_cv_folds_to_run == 0 ? unrolled_index.outer_cv_folds : unrolled_index.outer_cv_folds_to_run);
            var r_o_indexes = new (int repetitions_index, int outer_cv_index)[total_repetitions * total_outer_folds_to_run];
            var r_o_indexes_index = 0;
            for (var _repetitions_cv_index = 0; _repetitions_cv_index < total_repetitions; _repetitions_cv_index++)
            {
                for (var _outer_cv_index = 0; _outer_cv_index < total_outer_folds_to_run; _outer_cv_index++)
                {
                    //r_o_indexes.Add((_repetitions_cv_index, _outer_cv_index));
                    r_o_indexes[r_o_indexes_index++] = (repetitions_index: _repetitions_cv_index, outer_cv_index: _outer_cv_index);
                }
            }
            if (r_o_indexes_index < r_o_indexes.Length) throw new Exception();

            var ocv_data = r_o_indexes
                .AsParallel()
                .AsOrdered()
                .WithCancellation(cts.Token)
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
                    var cm_fn1 = $@"{filename}_full.cm.csv";
                    var cm_fn2 = $@"{filename}_summary.cm.csv";

                    var train_fold_indexes = unrolled_index.down_sampled_training_class_folds/* down sample for training */
                        .AsParallel()
                        .AsOrdered()
                        .WithCancellation(cts.Token)
                        .Select(a =>
                            (
                                a.class_id,
                                train_indexes: a.folds
                                    .Where(b => b.repetitions_index == repetitions_index && b.outer_cv_index != outer_cv_index/* do not select test fold */)
                                    .SelectMany(b => b.indexes)
                                    .OrderBy(b => b)
                                    .ToArray()
                            )
                        ).ToArray();

                    var train_sizes = train_fold_indexes.Select(a => (class_id: a.class_id, train_size: a.train_indexes?.Length??0)).ToArray();
                    var train_row_values = dataset.get_row_features(cts, train_fold_indexes, column_indexes);
                    var train_scaling = dataset_loader.get_scaling_params(train_row_values, column_indexes);
                    var train_row_scaled_values = dataset_loader.get_scaled_rows(train_row_values, /*column_indexes,*/ train_scaling, unrolled_index.scale_function);
                    var train_text = train_row_scaled_values.AsParallel().AsOrdered().WithCancellation(cts.Token).Select(row => $@"{(int)row[0]} {string.Join(" ", row.Skip(1 /* skip class id column */).Select((col_val, x_index) => col_val != 0 ? $@"{(preserve_fid ? column_indexes[x_index] : (x_index + 1))}:{col_val:G17}" : $@"").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray())}").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray();

                    //var v = train_fold_indexes.Select(a => a.indexes.Select(ix => dataset.value_list.First(b => b.class_id == a.class_id).val_list[ix].row_comment).ToArray()).ToArray();


                    var test_fold_indexes = unrolled_index
                        .class_folds/* natural distribution for testing */
                        .AsParallel()
                        .AsOrdered()
                        .WithCancellation(cts.Token)
                        .Select(a =>
                            (
                                a.class_id,
                                test_indexes: a.folds
                                    .Where(b => b.repetitions_index == repetitions_index && b.outer_cv_index == outer_cv_index/* select only test fold */)
                                    .SelectMany(b => b.indexes)
                                    .OrderBy(b => b)
                                    .ToArray()
                            )
                        ).ToArray();

                    var test_sizes = test_fold_indexes.Select(a => (class_id: a.class_id, test_size: a.test_indexes?.Length??0)).ToArray();
                    var test_row_values = dataset.get_row_features(cts, test_fold_indexes, column_indexes);
                    var test_scaling = train_scaling; /* scale test data with training data */
                    var test_row_scaled_values = dataset_loader.get_scaled_rows(test_row_values, /*column_indexes,*/ test_scaling, unrolled_index.scale_function);
                    var test_text = test_row_scaled_values.AsParallel().AsOrdered().WithCancellation(cts.Token).Select(row => $@"{(int)row[0]} {string.Join(" ", row.Skip(1 /* skip class id column */).Select((col_val, x_index) => col_val != 0 ? $@"{(preserve_fid ? column_indexes[x_index] : (x_index + 1))}:{col_val:G17}" : $@"").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray())}").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray();

                    return (repetitions_index, outer_cv_index, train_fn, grid_fn, model_fn, test_fn, predict_fn, cm_fn1, cm_fn2, train_text, test_text, train_sizes, test_sizes, train_fold_indexes, test_fold_indexes);
                })
                .ToArray();

            Parallel.ForEach(ocv_data,
                item =>
                {
                    io_proxy.WriteAllLines(cts, item.train_fn, item.train_text);
                    io_proxy.WriteAllLines(cts, item.test_fn, item.test_text);
                });

            var merged_train_text = ocv_data.SelectMany(a => a.train_text).ToArray();
            var merged_test_text = ocv_data.SelectMany(a => a.test_text).ToArray();
            var merged_train_sizes = ocv_data.SelectMany(a => a.train_sizes).GroupBy(a => a.class_id).Select(b => (class_id: b.Key, training_size: b.Select(c => c.train_size).Sum())).ToArray();
            var merged_test_sizes = ocv_data.SelectMany(a => a.test_sizes).GroupBy(a => a.class_id).Select(b => (class_id: b.Key, testing_size: b.Select(c => c.test_size).Sum())).ToArray();

            var merged_train_fold_indexes = ocv_data.SelectMany(a => a.train_fold_indexes).GroupBy(a => a.class_id).Select(a => (class_id: a.Key, train_indexes: a.SelectMany(b => b.train_indexes).ToArray())).ToArray();
            var merged_test_fold_indexes = ocv_data.SelectMany(a => a.test_fold_indexes).GroupBy(a => a.class_id).Select(a => (class_id: a.Key, test_indexes: a.SelectMany(b => b.test_indexes).ToArray())).ToArray();


            // filenames for merging all repetition indexes and outer cv indexes... as if it were a single test.
            var merged_filename_prefix = Path.Combine(group_folder, $@"m_{get_iteration_filename(new[] { unrolled_index })}");
            var merged_train_fn = $@"{merged_filename_prefix}.train.libsvm";
            var merged_grid_fn = $@"{merged_filename_prefix}.grid.libsvm";
            var merged_model_fn = $@"{merged_filename_prefix}.model.libsvm";
            var merged_test_fn = $@"{merged_filename_prefix}.test.libsvm";
            var merged_predict_fn = $@"{merged_filename_prefix}.predict.libsvm";
            var merged_cm_fn1 = $@"{merged_filename_prefix}_full.cm.csv";
            var merged_cm_fn2 = $@"{merged_filename_prefix}_summary.cm.csv";

            var merged_cv_input =
            (
                repetitions_index: -1,
                outer_cv_index: -1,
                train_fn: merged_train_fn,
                grid_fn: merged_grid_fn,
                model_fn: merged_model_fn,
                test_fn: merged_test_fn,
                predict_fn: merged_predict_fn,
                cm_fn1: merged_cm_fn1,
                cm_fn2: merged_cm_fn2,
                train_text: merged_train_text,
                test_text: merged_test_text,
                train_sizes: merged_train_sizes,
                test_sizes: merged_test_sizes,
                train_fold_indexes: merged_train_fold_indexes,
                test_fold_indexes: merged_test_fold_indexes
            );
            ocv_data = (new[] {merged_cv_input}).Concat(ocv_data).ToArray();


            //var test_class_sample_id_list = merged_cv_input.test_fold_indexes.SelectMany(a => a.test_indexes).ToList();


            var save_merged_files = false;
            if (save_merged_files)
            {
                io_proxy.WriteAllLines(cts, merged_train_fn, merged_train_text);
                io_proxy.WriteAllLines(cts, merged_test_fn, merged_test_text);
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
                var initials = string.Join("", name.Replace("_", " ", StringComparison.Ordinal).Split().Where(a => a.Length > 0).Select(a => a.First()).ToList());
                return initials.Length > 2 ?/* initials.Substring(0, 2) */ $@"{initials.First()}{initials.Last()}" : initials;
            }

            static string reduce(string text, int max = 30)
            {
                return text != null && text.Length > max ? $"{text.Substring(0, max / 3)}_{text.Substring((text.Length / 2) - (((max / 3) - 2) / 2), (max / 3) - 2)}_{text.Substring(text.Length - (max / 3), max / 3)}" : text;
            }

            var experiment_group_name = reduce(string.Join($@"_", indexes.Select(a => a.experiment_group_name).Distinct().ToArray()));
            var iteration_index = reduce(get_ranges_str(indexes.Select(a => a.iteration_index).ToList()));
            var group_index = reduce(get_ranges_str(indexes.Select(a => a.group_array_index).ToList()));
            var total_groups = reduce(get_ranges_str(indexes.Select(a => a.total_groups).ToList()));
            var calc_11p_thresholds = reduce(get_ranges_str(indexes.Select(a => a.calc_11p_thresholds ? 1 : 0).ToList()));
            var repetitions = reduce(get_ranges_str(indexes.Select(a => a.repetitions).ToList()));
            var outer_cv_folds = reduce(get_ranges_str(indexes.Select(a => a.outer_cv_folds).ToList()));
            var outer_cv_folds_to_run = reduce(get_ranges_str(indexes.Select(a => a.outer_cv_folds_to_run).ToList()));
            var class_weights = string.Join("_",
                indexes
                    .Where(a => a.class_weights != null)
                    .SelectMany(a => a.class_weights)
                    .GroupBy(a => a.class_id)
                    .Select(a => $@"{a.Key}_{reduce(get_ranges_str(a.Select(b => (int)(b.class_weight * 100)).ToList()))}")
                    .ToList());
            var svm_type = reduce(get_ranges_str(indexes.Select(a => (int)a.svm_type).ToList()));
            var svm_kernel = reduce(get_ranges_str(indexes.Select(a => (int)a.svm_kernel).ToList()));
            var scale_function = reduce(get_ranges_str(indexes.Select(a => (int)a.scale_function).ToList()));
            var inner_cv_folds = reduce(get_ranges_str(indexes.Select(a => a.inner_cv_folds).ToList()));

            var p = new List<(string name, string value)>()
            {
                (get_initials(nameof(experiment_group_name)), experiment_group_name),//it
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

            var iter_fn = string.Join($@"_", p.Select(a => $@"{a.name}[{a.value ?? ""}]").ToList());

            const string fn_chars = @"0123456789[]{}()_+-.;qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";

            if (!iter_fn.All(a => fn_chars.Contains(a)))
            {
                throw new Exception();
            }

            return iter_fn;
        }


        internal static ((long grid_dur, long train_dur, long predict_dur) dur, grid_point grid_point, string[] predict_text)
            inner_cross_validation(
                CancellationTokenSource cts,
                index_data unrolled_index,
                (int repetitions_index, int outer_cv_index, string train_fn, string grid_fn, string model_fn, string test_fn, string predict_fn, string cm_fn1, string cm_fn2, string[] train_text, string[] test_text, (int class_id, int training_size)[] train_sizes, (int class_id, int testing_size)[] test_sizes, (int class_id, int[] train_indexes)[] train_fold_indexes, (int class_id, int[] test_indexes)[] test_fold_indexes) input,
                bool libsvm_train_probability_estimates = true,
                bool log = false
            )
        {

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
                        cts,
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
            var train_result = libsvm.train(cts, settings.libsvm_train_runtime, input.train_fn, input.model_fn, train_stdout_filename, train_stderr_filename, train_grid_search_result.cost, train_grid_search_result.gamma, train_grid_search_result.epsilon, train_grid_search_result.coef0, train_grid_search_result.degree, null, unrolled_index.svm_type, unrolled_index.svm_kernel, null, probability_estimates: libsvm_train_probability_estimates);
            sw_train.Stop();
            var sw_train_dur = sw_train.ElapsedMilliseconds;

            if (log && !string.IsNullOrWhiteSpace(train_result.cmd_line)) io_proxy.WriteLine(train_result.cmd_line, module_name, method_name);
            if (log && !string.IsNullOrWhiteSpace(train_result.stdout)) train_result.stdout.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(line => io_proxy.WriteLine($@"{nameof(train_result)}.{nameof(train_result.stdout)}: {line}", module_name, method_name));
            if (log && !string.IsNullOrWhiteSpace(train_result.stderr)) train_result.stderr.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(line => io_proxy.WriteLine($@"{nameof(train_result)}.{nameof(train_result.stderr)}: {line}", module_name, method_name));


            // predict
            var sw_predict = new Stopwatch();
            sw_predict.Start();
            var predict_result = libsvm.predict(cts, settings.libsvm_predict_runtime, input.test_fn, input.model_fn, input.predict_fn, libsvm_train_probability_estimates, predict_stdout_filename, predict_stderr_filename);

            sw_predict.Stop();
            var sw_predict_dur = sw_train.ElapsedMilliseconds;

            if (log && !string.IsNullOrWhiteSpace(predict_result.cmd_line)) io_proxy.WriteLine(predict_result.cmd_line, module_name, method_name);
            if (log && !string.IsNullOrWhiteSpace(predict_result.stdout)) predict_result.stdout.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(line => io_proxy.WriteLine($@"{nameof(predict_result)}.{nameof(predict_result.stdout)}: {line}", module_name, method_name));
            if (log && !string.IsNullOrWhiteSpace(predict_result.stderr)) predict_result.stderr.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(line => io_proxy.WriteLine($@"{nameof(predict_result)}.{nameof(predict_result.stderr)}: {line}", module_name, method_name));

            var predict_text = io_proxy.ReadAllLines(cts, input.predict_fn, module_name, method_name);
            //io_proxy.WriteLine($@"Loaded {input.predict_fn}");

            return ((sw_grid_dur, sw_train_dur, sw_predict_dur), train_grid_search_result, predict_text);
        }
    }
}
