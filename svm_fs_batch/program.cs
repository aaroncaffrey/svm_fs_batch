using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Runtime.InteropServices;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal class program
    {
        internal static void Main(string[] args)
        {
            //var x=perf.confusion_matrix.load($@"C:\mmfs1\data\scratch\k1040015\svm_fs_batch\results\test\it_5\x_it-5_gr-5_sv-1_kr-3_sc-2_rn-1_oc-10_ic-10_ix-1-5.cm.csv");

            io_proxy.WriteLine($@"cmd line: {Environment.CommandLine}");
            io_proxy.WriteLine($@"processor count: {Environment.ProcessorCount}");

            var cts = new CancellationTokenSource();
            close_notifications(cts);
            check_x64();
            set_gc_mode();
            set_thread_counts();



            var setup = false;
            var experiment_name = "";
            var job_id = "";
            var job_name = "";
            var array_index = -1;
            var array_count = 0;
            var array_step = 0;
            var array_start = -1;
            var array_end = -1;
            var array_index_last = -1;

            var setup_total_vcpus = -1;
            var setup_instance_vcpus = -1;

            var arg_index_setup = args.ToList().FindIndex(a => string.Equals(a, $"-{nameof(setup)}", StringComparison.InvariantCultureIgnoreCase));
            var arg_index_setup_total_vcpus = args.ToList().FindIndex(a => string.Equals(a, $"-{nameof(setup_total_vcpus)}", StringComparison.InvariantCultureIgnoreCase));
            var arg_index_setup_instance_vcpus = args.ToList().FindIndex(a => string.Equals(a, $"-{nameof(setup_instance_vcpus)}", StringComparison.InvariantCultureIgnoreCase));


            var arg_index_experiment_name = args.ToList().FindIndex(a => string.Equals(a, $"-{nameof(experiment_name)}", StringComparison.InvariantCultureIgnoreCase));
            var arg_index_job_id = args.ToList().FindIndex(a => string.Equals(a, $"-{nameof(job_id)}", StringComparison.InvariantCultureIgnoreCase));
            var arg_index_job_name = args.ToList().FindIndex(a => string.Equals(a, $"-{nameof(job_name)}", StringComparison.InvariantCultureIgnoreCase));
            var arg_index_array_index = args.ToList().FindIndex(a => string.Equals(a, $"-{nameof(array_index)}", StringComparison.InvariantCultureIgnoreCase));
            var arg_index_array_index_last = args.ToList().FindIndex(a => string.Equals(a, $"-{nameof(array_index_last)}", StringComparison.InvariantCultureIgnoreCase));
            var arg_index_array_count = args.ToList().FindIndex(a => string.Equals(a, $"-{nameof(array_count)}", StringComparison.InvariantCultureIgnoreCase));
            var arg_index_array_step = args.ToList().FindIndex(a => string.Equals(a, $"-{nameof(array_step)}", StringComparison.InvariantCultureIgnoreCase));
            var arg_index_array_start = args.ToList().FindIndex(a => string.Equals(a, $"-{nameof(array_start)}", StringComparison.InvariantCultureIgnoreCase));
            var arg_index_array_end = args.ToList().FindIndex(a => string.Equals(a, $"-{nameof(array_end)}", StringComparison.InvariantCultureIgnoreCase));

            var arg_indexes = new int[]
            {
                arg_index_setup,
                arg_index_setup_total_vcpus,
                arg_index_setup_instance_vcpus,

                arg_index_experiment_name    ,
                arg_index_job_id             ,
                arg_index_job_name           ,
                arg_index_array_index        ,
                arg_index_array_index_last   ,
                arg_index_array_count        ,
                arg_index_array_step         ,
                arg_index_array_start        ,
                arg_index_array_end          ,
            };

            if (arg_index_setup > -1) setup = true;
            if (arg_index_setup_total_vcpus > -1 && args.Length - 1 >= arg_index_setup_total_vcpus + 1 && !arg_indexes.Contains(arg_index_setup_total_vcpus + 1)) setup_total_vcpus = int.TryParse(args[arg_index_setup_total_vcpus], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var setup_total_vcpus2) ? setup_total_vcpus2 : -1;
            if (arg_index_setup_instance_vcpus > -1 && args.Length - 1 >= arg_index_setup_instance_vcpus + 1 && !arg_indexes.Contains(arg_index_setup_instance_vcpus + 1)) setup_instance_vcpus = int.TryParse(args[arg_index_setup_instance_vcpus], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var setup_instance_vcpus2) ? setup_instance_vcpus2 : -1;

            if (arg_index_experiment_name > -1 && args.Length - 1 >= arg_index_experiment_name + 1 && !arg_indexes.Contains(arg_index_experiment_name + 1)) experiment_name = args[arg_index_experiment_name];
            if (arg_index_job_id > -1 && args.Length - 1 >= arg_index_job_id + 1 && !arg_indexes.Contains(arg_index_job_id + 1)) job_id = args[arg_index_job_id];
            if (arg_index_job_name > -1 && args.Length - 1 >= arg_index_job_name + 1 && !arg_indexes.Contains(arg_index_job_name + 1)) job_name = args[arg_index_job_name];
            if (arg_index_array_index > -1 && args.Length - 1 >= arg_index_array_index + 1 && !arg_indexes.Contains(arg_index_array_index + 1)) array_index = int.TryParse(args[arg_index_array_index], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var array_index2) ? array_index2 : -1;
            if (arg_index_array_index_last > -1 && args.Length - 1 >= arg_index_array_index_last + 1 && !arg_indexes.Contains(arg_index_array_index_last + 1)) array_index_last = int.TryParse(args[arg_index_array_index_last], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var array_index_last2) ? array_index_last2 : -1;
            if (arg_index_array_count > -1 && args.Length - 1 >= arg_index_array_count + 1 && !arg_indexes.Contains(arg_index_array_count + 1)) array_count = int.TryParse(args[arg_index_array_count], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var array_count2) ? array_count2 : -1;
            if (arg_index_array_step > -1 && args.Length - 1 >= arg_index_array_step + 1 && !arg_indexes.Contains(arg_index_array_step + 1)) array_step = int.TryParse(args[arg_index_array_step], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var array_step2) ? array_step2 : -1;
            if (arg_index_array_start > -1 && args.Length - 1 >= arg_index_array_start + 1 && !arg_indexes.Contains(arg_index_array_start + 1)) array_start = int.TryParse(args[arg_index_array_start], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var array_start2) ? array_start2 : -1;
            if (arg_index_array_end > -1 && args.Length - 1 >= arg_index_array_end + 1 && !arg_indexes.Contains(arg_index_array_end + 1)) array_end = int.TryParse(args[arg_index_array_end], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var array_end2) ? array_end2 : -1;




            if (setup)
            {
                experiment_name += $@"_{DateTime.Now:yyyyMMddHHmmssfff}";

                // set default vcpu amounts if not specified
                if (setup_total_vcpus <= 0) setup_total_vcpus = 1504 - 320;
                if (setup_instance_vcpus <= 0) setup_instance_vcpus = 64;

                // calculate array size (get total number of groups)
                var ida = new init_dataset_args();
                var idr = init_dataset(ida);
                var setup_array_size = idr.groups.Count;

                // calculate total number of instanced and array step
                var setup_total_instances = (int)Math.Floor((double)setup_total_vcpus / (double)setup_instance_vcpus);
                var setup_array_step = (int)Math.Ceiling((double)setup_array_size / (double)setup_total_instances);

                // calculate list of expected indexes
                //var group_index_pairs = new List<(int instance_id, int group_index_first, int group_index_last)>();
                //var instance_id = -1;
                //for (var a = 0; a <= setup_array_size - 1; a += setup_array_step)
                //{
                //    instance_id++;
                //    group_index_pairs.Add((instance_id, a, a + setup_array_step - 1));
                //}

                io_proxy.WriteLine($@"{experiment_name}: {nameof(setup_array_size)}: {setup_array_size}");
                io_proxy.WriteLine($@"{experiment_name}: {nameof(setup_total_vcpus)}: {setup_total_vcpus}");
                io_proxy.WriteLine($@"{experiment_name}: {nameof(setup_instance_vcpus)}: {setup_instance_vcpus}");
                io_proxy.WriteLine($@"{experiment_name}: {nameof(setup_total_instances)}: {setup_total_instances}");
                io_proxy.WriteLine($@"{experiment_name}: {nameof(setup_array_step)}: {setup_array_step}");
                io_proxy.WriteLine("");

                var ps = make_pbs_script(experiment_name, setup_instance_vcpus, true, 0, setup_array_size - 1, setup_array_step, true);

                ps.ForEach(a => io_proxy.WriteLine(a));
                io_proxy.WriteLine("");
                return;
            }


            if (string.IsNullOrWhiteSpace(experiment_name)) { throw new ArgumentOutOfRangeException(nameof(experiment_name), "must specify experiment name"); }

            if (array_start <= -1) { throw new ArgumentOutOfRangeException(nameof(array_start)); }
            if (array_end <= -1) { throw new ArgumentOutOfRangeException(nameof(array_end)); }
            if (array_index <= -1) { throw new ArgumentOutOfRangeException(nameof(array_index)); }
            if (array_step <= 0) { throw new ArgumentOutOfRangeException(nameof(array_step)); }
            if (array_count <= 0) { throw new ArgumentOutOfRangeException(nameof(array_count)); }
            if (array_index_last <= -1) { array_index_last = array_index + (array_step - 1); }

            var instance_id = -1;
            for (var i = array_start; i <= array_index; i += array_step) instance_id++;

            worker(experiment_name, instance_id, array_count, array_index, array_step, array_index_last);
        }

        internal static List<string> make_pbs_script(string experiment_name, int pbs_ppn = 1, bool array = false, int array_start = 0, int array_end = 0, int array_step = 1, bool rerunnable = true)
        {

            //var module_name = nameof(program);
            //var method_name = nameof(make_pbs_script);

            var pbs_script_lines = new List<string>();

            var program_runtime = Process.GetCurrentProcess().MainModule.FileName;

            var env_pbs_array = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"_" : @"%J_%I";
            var env_jobid = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"_" : @"${PBS_JOBID}${MOAB_JOBID}";
            var env_jobname = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"_" : @"${PBS_JOBNAME}${MOAB_JOBNAME}";
            var env_arrayindex = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"_" : @"${PBS_ARRAYID}${MOAB_JOBARRAYINDEX}";
            var env_arraycount = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"_" : @"${MOAB_JOBARRAYRANGE}";

            TimeSpan pbs_walltime = TimeSpan.FromHours(240);
            string pbs_execution_directory = $@"{outer_cv_args.svm_fs_home}{Path.DirectorySeparatorChar}pbs{Path.DirectorySeparatorChar}{experiment_name}{Path.DirectorySeparatorChar}";
            string pbs_jobname = $@"{experiment_name}_{nameof(svm_fs_batch)}";
            string pbs_mail_addr = "";
            string pbs_mail_opt = "n";
            string pbs_mem = null;
            string pbs_stdout_filename = $@"{experiment_name}_{nameof(svm_fs_batch)}_{(array ? env_pbs_array : "")}.pbs.stdout";
            string pbs_stderr_filename = $@"{experiment_name}_{nameof(svm_fs_batch)}_{(array ? env_pbs_array : "")}.pbs.stderr";
            int pbs_nodes = 1;
            if (pbs_ppn <= 0) pbs_ppn = 64;
            string program_stdout_filename = $@"{experiment_name}_{nameof(svm_fs_batch)}_{env_jobid}_{env_jobname}_{env_arrayindex}.program.stdout";
            string program_stderr_filename = $@"{experiment_name}_{nameof(svm_fs_batch)}_{env_jobid}_{env_jobname}_{env_arrayindex}.program.stderr";




            // pbs directives
            if (array) pbs_script_lines.Add($@"#PBS -t {array_start}-{array_end}:{array_step}");
            if (pbs_walltime != null && pbs_walltime.TotalSeconds > 0) pbs_script_lines.Add($@"#PBS -l walltime={Math.Floor(pbs_walltime.TotalHours):00}:{pbs_walltime.Minutes:00}:{pbs_walltime.Seconds:00}");
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

            if (!string.IsNullOrWhiteSpace(experiment_name)) p.Add(("-experiment_name", experiment_name));

            p.Add(("-job_id", env_jobid));
            p.Add(("-job_name", env_jobname));

            if (array && array_step != 0) p.Add(("-array_index", env_arrayindex));
            if (array && array_step != 0) p.Add(("-array_count", env_arraycount));

            if (array && array_step != 0) p.Add(("-array_step", array_step.ToString()));
            if (array && array_step != 0) p.Add(("-array_start", array_start.ToString()));
            if (array && array_step != 0) p.Add(("-array_end", array_end.ToString()));

            if (!string.IsNullOrEmpty(program_stdout_filename)) p.Add(("1>", program_stdout_filename));
            if (!string.IsNullOrEmpty(program_stderr_filename)) p.Add(("2>", program_stderr_filename));

            var run_line = $@"{program_runtime} {string.Join(" ", p.Where(a => !string.IsNullOrWhiteSpace(a.key) || !string.IsNullOrWhiteSpace(a.value)).Select(a => $@"{a.key}{(!string.IsNullOrWhiteSpace(a.key) ? " " : "")}{a.value}").ToList())}";

            if (!string.IsNullOrWhiteSpace(pbs_execution_directory)) pbs_script_lines.Add($@"cd {pbs_execution_directory}");
            pbs_script_lines.Add($@"module load GCCcore");
            pbs_script_lines.Add(run_line);

            var pbs_fn = Path.Combine(pbs_execution_directory, $"{pbs_jobname}.pbs");

            io_proxy.WriteAllLines(pbs_fn, pbs_script_lines);
            io_proxy.WriteLine($@"{experiment_name}: Saved PBS script. File: {pbs_fn}.");

            return pbs_script_lines;
        }

        internal static void set_thread_counts()
        {
            ThreadPool.SetMinThreads(Environment.ProcessorCount * 10, Environment.ProcessorCount * 10);
            ThreadPool.SetMaxThreads(Environment.ProcessorCount * 100, Environment.ProcessorCount * 100);
        }

        internal static void close_notifications(CancellationTokenSource cts)
        {
            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                io_proxy.WriteLine($@"Console.CancelKeyPress", nameof(program), nameof(close_notifications));
                cts.Cancel();
            };
            AssemblyLoadContext.Default.Unloading += context =>
            {
                io_proxy.WriteLine($@"AssemblyLoadContext.Default.Unloading", nameof(program), nameof(close_notifications));
                cts.Cancel();
            };
            AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) =>
            {
                io_proxy.WriteLine($@"AppDomain.CurrentDomain.ProcessExit", nameof(program), nameof(close_notifications));
                cts.Cancel();
            };
        }

        public static void check_x64()
        {
            bool is64Bit = IntPtr.Size == 8;
            if (!is64Bit) { throw new Exception("Must run in 64bit mode"); }
        }

        public static void set_gc_mode()
        {
            GCSettings.LatencyMode = GCLatencyMode.Batch;
            //GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
        }



        internal class init_dataset_ret
        {
            internal bool required_default;
            internal List<(bool required, string alphabet, string dimension, string category, string source, string @group, string member, string perspective)> required_matches;
            internal dataset_loader.dataset dataset;
            internal List<int> class_ids;
            internal List<(int class_id, int class_size)> class_sizes;
            internal List<(int class_id, int size, List<(int randomisation_cv_index, int outer_cv_index, List<int> indexes)> folds)> class_folds;
            internal List<(int class_id, int size, List<(int randomisation_cv_index, int outer_cv_index, List<int> indexes)> folds)> downsampled_training_class_folds;
            internal List<(int class_id, List<(int class_id, int example_id, int class_example_id, List<(string comment_header, string comment_value)> comment_columns, List<(int fid, double fv)> feature_data)> examples)> dataset_instance_list_grouped;
            internal List<(int index, (string alphabet, string dimension, string category, string source, string @group, string member, string perspective) key, List<(int fid, string alphabet, string dimension, string category, string source, string @group, string member, string perspective, int alphabet_id, int dimension_id, int category_id, int source_id, int group_id, int member_id, int perspective_id)> list, List<int> columns)> groups;
            internal List<(string alphabet, string dimension, string category, string source, string @group, string member, string perspective)> group_keys;
            internal List<(string alphabet, string dimension, string category, string source, string @group, string member, string perspective)> group_keys_distinct;
        }

        internal class init_dataset_args
        {
            internal static string user_home = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\home\k1040015" : @"/home/k1040015";
            internal string dataset_dir = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"E:\caddy\input\" : $@"{user_home}/dataset/";

            internal int negative_class_id = -1;
            internal int positive_class_id = +1;
            internal List<(int class_id, string class_name)> class_names = new List<(int class_id, string class_name)>() { (-1, "standard_coil"), (+1, "dimorphic_coil") };

            internal int outer_cv_folds = 10;
            internal int randomisation_cv_folds = 1;
            internal bool group_related_columns = true;
        }


        internal class outer_cv_args
        {
            //internal readonly string experiment_name;
            internal bool output_threshold_adjustment_performance = false;

            internal int scoring_class_id = +1;
            internal int inner_cv_folds = 10;

            internal string score1_metric = nameof(perf.confusion_matrix.F1S);
            internal string score2_metric = nameof(perf.confusion_matrix.MCC);
            internal string score3_metric = nameof(perf.confusion_matrix.API_All);

            internal int total_groups = -1;
            //internal int iteration_index = -1;
            //internal int group_index = -1;

            internal routines.libsvm_svm_type svm_type = routines.libsvm_svm_type.c_svc;
            internal routines.libsvm_kernel_type svm_kernel = routines.libsvm_kernel_type.rbf;
            internal routines.scale_function scale_function = routines.scale_function.rescale;
            internal List<(int class_id, double class_weight)> class_weights = null;

            //internal readonly string pbs_folder = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\mmfs1\data\scratch\k1040015\svm_fs\pbs\" : @"/mmfs1/data/scratch/k1040015/svm_fs/pbs";
            internal static readonly string user_home = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\home\k1040015" : @"/home/k1040015";
            internal static readonly string svm_fs_home = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? $@"C:\mmfs1\data\scratch\k1040015\{nameof(svm_fs_batch)}" : $@"/mmfs1/data/scratch/k1040015/{nameof(svm_fs_batch)}";
            internal static readonly string results_root_folder = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? $@"{svm_fs_home}\results\" : $@"{svm_fs_home}/results/";
            internal static readonly string libsvm_predict_runtime = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? $@"C:\libsvm\windows\svm-predict.exe" : $@"{user_home}/libsvm/svm-predict";
            internal static readonly string libsvm_train_runtime = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? $@"C:\libsvm\windows\svm-train.exe" : $@"{user_home}/libsvm/svm-train";
        }

        internal static init_dataset_ret init_dataset(init_dataset_args ida)
        {
            //var module_name = nameof(Program);
            //var method_name = nameof(init_dataset);

            var required_default = false;
            var required_matches = new List<(bool required, string alphabet, string dimension, string category, string source, string group, string member, string perspective)>();
            //required_matches.Add((required: true, alphabet: null, dimension: null, category: null, source: null, group: null, member: null, perspective: null));

            // file tags: 2i, 2n, 2p, 3i, 3n, 3p (2d, 3d, interface, neighbourhood, protein)

            var dataset = dataset_loader.read_binary_dataset(ida.dataset_dir,
                "2i",
                ida.negative_class_id,
                ida.positive_class_id,
                ida.class_names,
                use_parallel: true,
                perform_integrity_checks: false,
                //fix_double: false,
                required_default,
                required_matches);

            var class_ids = dataset.dataset_instance_list.Select(a => a.class_id).Distinct().ToList();

            var class_sizes = class_ids.Select(a => (class_id: a, class_size: dataset.dataset_instance_list.Count(b => b.class_id == a))).ToList();


            var class_folds = class_sizes.Select(a => (class_id: a.class_id, size: a.class_size, folds: routines.folds(a.class_size, ida.outer_cv_folds, ida.randomisation_cv_folds))).ToList();

            var downsampled_training_class_folds = class_folds.Select(a => (class_id: a.class_id, size: a.size, folds: a.folds.Select(b =>
                    {
                        var min_num_items_in_fold = class_folds.Min(c => c.folds.Where(e => e.randomisation_cv_index == b.randomisation_cv_index && e.outer_cv_index == b.outer_cv_index).Min(e => e.indexes.Count));

                        return (randomisation_cv_index: b.randomisation_cv_index, outer_cv_index: b.outer_cv_index, indexes: b.indexes.Take(min_num_items_in_fold).ToList());
                    })
                    .ToList()))
                .ToList();

            var dataset_instance_list_grouped = dataset.dataset_instance_list.GroupBy(a => a.class_id).Select(a => (class_id: a.Key, examples: a.ToList())).ToList();

            var groups = ida.group_related_columns ? dataset.dataset_headers.GroupBy(a => (a.alphabet, a.dimension, a.category, a.source, a.@group, member: "", perspective: "")).Skip(1).Select((a, i) => (index: i, key: a.Key, list: a.ToList(), columns: a.Select(b => b.fid).ToList())).ToList() : dataset.dataset_headers.GroupBy(a => a.fid).Skip(1).Select((a, i) => (index: i, key: (a.First().alphabet, a.First().dimension, a.First().category, a.First().source, a.First().@group, a.First().member, a.First().perspective), list: a.ToList(), columns: a.Select(b => b.fid).ToList())).ToList();
            var group_keys = groups.Select(a => a.key).ToList();
            var group_keys_distinct = group_keys.Distinct().ToList();

            if (group_keys.Count != group_keys_distinct.Count) throw new Exception();

            return new init_dataset_ret() { required_default = required_default, required_matches = required_matches, dataset = dataset, class_ids = class_ids, class_sizes = class_sizes, class_folds = class_folds, downsampled_training_class_folds = downsampled_training_class_folds, dataset_instance_list_grouped = dataset_instance_list_grouped, groups = groups, group_keys = group_keys, group_keys_distinct = group_keys_distinct, };
        }






        internal static string get_iteration_folder(string results_root_folder, string experiment_name, int iteration_index, int? group_index = null)
        {
            var hr = true;

            if (group_index == null) return Path.Combine(results_root_folder, experiment_name, $@"it_{(iteration_index + (hr ? 1 : 0))}");

            return Path.Combine(results_root_folder, experiment_name, $@"it_{(iteration_index + (hr ? 1 : 0))}", $@"gr_{(group_index + (hr ? 1 : 0))}");
        }


        //internal static (string cm_header_line, string[] lines, List<(List<(string key, string value)> key_value_list, string cm_line)> cm_list) load_cm(string cm_fn)
        //{
        //    var r = new List<(List<(string key, string value)> key_value_list, string cm_line)>();
        //
        //    var lines = io_proxy.ReadAllLines(cm_fn);
        //    ///io_proxy.WriteLine($@"Loaded {cm_fn}");
        //
        //    var line_header_split = lines.First().Split(',');
        //
        //    foreach (var line in lines.Skip(1).Where(a => !string.IsNullOrWhiteSpace(a)))
        //    {
        //        var key_value_list = line.Split(',').Select((v, i) => (key: line_header_split[i], value: v)).ToList();
        //
        //        r.Add((key_value_list, line));
        //    }
        //
        //    return (lines.First(), lines, r);
        //}

        //internal static List<(int iteration_index, int group_index, int class_id, direction dir, double score1, double score2, double score3)> scores load_cm_scores(string cm_fn, outer_cv_args oca)
        //{
        //    var scores = new List<(int iteration_index, int group_index, int class_id, direction dir, double score1, double score2, double score3)>();

        //    var lcr = perf.confusion_matrix.load(cm_fn);

        //    foreach (var cm in lcr)
        //    {
        //        if (cm.cm.class_id == oca.scoring_class_id)
        //        {

        //            var cm_iteration_index = int.Parse(cm.key_value_list.FirstOrDefault(a => a.key == "iteration_index").value, NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
        //            var cm_group_index = int.Parse(cm.key_value_list.FirstOrDefault(a => a.key == "group_index").value, NumberStyles.Integer, NumberFormatInfo.InvariantInfo);


        //            var cm_score1 = double.Parse(cm.key_value_list.FirstOrDefault(a => a.key == oca.score1_metric).value, NumberStyles.Float, NumberFormatInfo.InvariantInfo);
        //            var cm_score2 = double.Parse(cm.key_value_list.FirstOrDefault(a => a.key == oca.score2_metric).value, NumberStyles.Float, NumberFormatInfo.InvariantInfo);
        //            var cm_score3 = double.Parse(cm.key_value_list.FirstOrDefault(a => a.key == oca.score3_metric).value, NumberStyles.Float, NumberFormatInfo.InvariantInfo);

        //            var cm_dir = (direction)Enum.Parse(typeof(direction), cm.key_value_list.FirstOrDefault(a => a.key == "dir").value, true);

        //            scores.Add((cm_iteration_index, cm_group_index, cm.cm.class_id, cm_dir, cm_score1, cm_score2, cm_score3));
        //        }

        //    }

        //    return scores;
        //}

        public static void worker(string experiment_name, int instance_id, int total_instances, int array_index_start, int array_step, int array_index_last)
        {



            //var module_name = nameof(Program);
            //var method_name = nameof(worker);

            //var is_primary_instance = array_index_start == 0;

            int limit_iteration_not_better_than_all = 10;
            int limit_iteration_not_better_than_last = 5;

            var make_outer_cv_confusion_matrices = false;

            var ida = new init_dataset_args();
            var idr = init_dataset(ida);

            idr.groups = idr.groups.Take(5).ToList();

            var oca = new outer_cv_args() { total_groups = idr.groups.Count, };

            //var instance_id = array_index_start / array_step;
            //var total_instances = Math.Ceiling((double)idr.groups.Count / (double)array_step);

            io_proxy.WriteLine($@"{experiment_name}: Groups: {(array_index_start + 1)} to {(array_index_last + 1)}. Total groups: {oca.total_groups}. (This instance #{(instance_id + 1)}/#{total_instances} indexes: [{array_index_start}..{array_index_last}]. All indexes: [0..{(oca.total_groups - 1)}].)");


            var iteration_index = 0;
            var finished = false;

            var selected_groups = new List<int>();
            var selected_columns = new List<int>();
            var previous_selected_groups = new List<int>();
            var previous_selected_columns = new List<int>();

            //var previous_winner_group;
            //var previous_winner_group_key;
            var previous_winner_group_index = -1;
            var previous_winner_score = 0d;

            var all_time_highest_score_iteration = 0;
            var all_time_highest_score = 0d;
            var all_time_highest_score_selected_groups = new List<int>();
            var all_time_highest_score_selected_columns = new List<int>();


            var iterations_not_better_than_last = 0;
            var iterations_not_better_than_all = 0;


            //var all_iteration_winner_csv = new List<string>();


            var winners_cm = new List<(string line, perf.confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)>();


            while (!finished)
            {
                var iteration_folder = get_iteration_folder(outer_cv_args.results_root_folder, experiment_name, iteration_index);

                var iteration_all_cm = new List<(string line, perf.confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)>();
                //var iteration_instance_cm = new List<(string line, perf.confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)>();

                
                var iteration_all_group_cm_filename = Path.Combine(iteration_folder, $@"x_{get_filename(oca.class_weights, iteration_index, null, oca.total_groups, (int)oca.svm_type, (int)oca.svm_kernel, (int)oca.scale_function, null, ida.randomisation_cv_folds, null, ida.outer_cv_folds, oca.inner_cv_folds, null, null)}.cm.csv");
                var iteration_instance_group_cm_filename = Path.Combine(iteration_folder, $@"x_{get_filename(oca.class_weights, iteration_index, null, oca.total_groups, (int)oca.svm_type, (int)oca.svm_kernel, (int)oca.scale_function, null, ida.randomisation_cv_folds, null, ida.outer_cv_folds, oca.inner_cv_folds, array_index_start, array_index_last)}.cm.csv");

                var instance_groups_loaded = false;
                var load_other_instances = true;

                //var _iteration_all_group_cm_list = new List<string>();
                //var _iteration_instance_all_groups_cm_lines = new List<string>();

                //var _ocv_iteration_all_group_cm_list = new List<string>();
                //var _ocv_iteration_instance_all_groups_cm_lines = new List<string>();

                if (!instance_groups_loaded)
                {
                    // A. Try to load whole iteration file
                    if (io_proxy.is_file_available(iteration_all_group_cm_filename))
                    {
                        instance_groups_loaded = true;
                        load_other_instances = false;

                        var lcs = perf.confusion_matrix.load(iteration_all_group_cm_filename);
                        lcs = lcs.Where((a, i) => i == 0 || a.unknown_key_value_list.First(b => b.key == nameof(iteration_index)).value_int == iteration_index).ToList();

                        if (lcs != null && lcs.Count > 1)
                        {
                            //all_cm.AddRange(lcs.Skip(all_cm.Count == 0 ? 0 : 1));
                            iteration_all_cm.AddRange(lcs.Skip(iteration_all_cm.Count == 0 ? 0 : 1));
                            //iteration_instance_cm.AddRange(lcs.Skip(iteration_instance_cm.Count == 0 ? 0 : 1));

                            io_proxy.WriteLine($@"{experiment_name}: Full cache: Loaded for iteration {(iteration_index + 1)}. File: {iteration_all_group_cm_filename}.");
                        }
                    }
                    else { io_proxy.WriteLine($@"{experiment_name}: Full cache: Unavailable for iteration {(iteration_index + 1)}. File: {iteration_all_group_cm_filename}."); }
                }

                if (!instance_groups_loaded)
                {
                    // B. Try to load whole instance file (i.e. array_index_first to array_index_last)

                    if (io_proxy.is_file_available(iteration_instance_group_cm_filename))
                    {
                        instance_groups_loaded = true;

                        var lcs = perf.confusion_matrix.load(iteration_instance_group_cm_filename);
                        lcs = lcs.Where((a, i) => i == 0 || a.unknown_key_value_list.First(b => b.key == nameof(iteration_index)).value_int == iteration_index).ToList();

                        if (lcs != null && lcs.Count > 1)
                        {
                            var lcs_lines = lcs.Select(a => a.line).ToList();

                            //all_cm.AddRange(lcs.Skip(all_cm.Count == 0 ? 0 : 1));
                            iteration_all_cm.AddRange(lcs.Skip(iteration_all_cm.Count == 0 ? 0 : 1));
                            //iteration_instance_cm.AddRange(lcs.Skip(iteration_instance_cm.Count == 0 ? 0 : 1));

                            io_proxy.WriteLine($@"{experiment_name}: Part cache: Loaded for iteration {(iteration_index + 1)} groups {(array_index_start + 1)} to {(array_index_last + 1)}. File: {iteration_instance_group_cm_filename}.");
                        }
                    }
                    else { io_proxy.WriteLine($@"{experiment_name}: Part cache: Unavailable for iteration {(iteration_index + 1)} groups {(array_index_start + 1)} to {(array_index_last + 1)}. File: {iteration_instance_group_cm_filename}."); }
                }

                var instance_files_loaded = new List<string>() { iteration_instance_group_cm_filename, iteration_all_group_cm_filename };

                if (!instance_groups_loaded)
                {
                    // C. In case of differing array_step_size etc, try to load other whole instance files.

                    io_proxy.WriteLine($"{experiment_name}: Trying to load other indexes for iteration {(iteration_index + 1)} groups {(array_index_start + 1)} to {(array_index_last + 1)}.");

                    var dir_files = Directory.GetFiles(iteration_folder, "x_*.cm.csv", SearchOption.TopDirectoryOnly);

                    foreach (var file in dir_files)
                    {
                        if (instance_files_loaded.Contains(file)) continue;
                        instance_files_loaded.Add(file);

                        var lcs = perf.confusion_matrix.load(file);
                        lcs = lcs.Where((a, i) => i == 0 || a.unknown_key_value_list.First(b => b.key == nameof(iteration_index)).value_int == iteration_index).ToList();

                        if (lcs != null && lcs.Count > 1)
                        {
                            var group_indexes_loaded = lcs.Skip(1).Select(a => a.cm.x_group_index).ToList();

                            //all_cm.AddRange(lcs.Skip(iteration_cm.Count == 0 ? 0 : 1));
                            iteration_all_cm.AddRange(lcs.Skip(iteration_all_cm.Count == 0 ? 0 : 1));
                            //iteration_instance_cm.AddRange(lcs.Skip(iteration_instance_cm.Count == 0 ? 0 : 1).Where(a => a.unknown_key_value_list.First(b => b.key == "group_index").value_int >= array_index_start && a.unknown_key_value_list.First(b => b.key == "group_index").value_int <= array_index_last));


                            // todo: check loaded_indexes are consecutive

                            io_proxy.WriteLine($"{experiment_name}: Part cache (instances): Loaded for iteration {(iteration_index + 1)} groups {(group_indexes_loaded.Min() + 1)} to {(group_indexes_loaded.Max() + 1)}. File: {file}.");
                        }
                    }

                    if (dir_files.Length > 0)
                    {
                        var instance_indexes = Enumerable.Range(array_index_start, array_step);
                        
                        var iteration_all_groups_indexes_loaded = iteration_all_cm.Skip(1).Select(a => a.cm.x_group_index).Distinct().ToList();
                        //var iteration_instance_groups_indexes_loaded = iteration_instance_cm.Skip(1).Select(a => a.unknown_key_value_list.First(b => b.key == "group_index").value_int).Distinct().ToList();

                        if (instance_indexes.All(a=> iteration_all_groups_indexes_loaded.Contains(a)))
                        {
                            instance_groups_loaded = true;
                        }
                    }
                }

                if (!instance_groups_loaded)
                {
                    instance_groups_loaded = true;

                    var tasks = new List<Task<(int iteration_index, int group_index,
                        List<(string line, perf.confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)> iteration_all_cm
                        )>>();

                    for (var _group_index = array_index_start; _group_index <= array_index_last && _group_index < oca.total_groups; _group_index++)
                    {
                        var group_index = _group_index;

                        
                        var iteration_all_groups_indexes_loaded = iteration_all_cm.Skip(1).Select(a => a.cm.x_group_index).Distinct().ToList();
                        //var iteration_instance_groups_indexes_loaded = iteration_instance_cm.Skip(1).Select(a => a.unknown_key_value_list.First(b => b.key == "group_index").value_int).Distinct().ToList();


                        if (iteration_all_groups_indexes_loaded.Any(a => /*a.iteration_index == iteration_index && a.*/group_index == a))
                        {
                            // skip if already loaded from mismatching index (different start/end index depending on number of instances)
                            continue;
                        }

                        var task = Task.Run(() =>
                        {
                            io_proxy.WriteLine($@"{experiment_name}: Starting: iteration {(iteration_index + 1)} group {(group_index + 1)} (groups {(array_index_start + 1)} to {(array_index_last + 1)}).");


                            var group_folder = get_iteration_folder(outer_cv_args.results_root_folder, experiment_name, iteration_index, group_index);
                            var group_key = idr.groups[group_index].key;

                            //var group_cm_filename =     Path.Combine(group_folder, $@"x_{get_filename(oca.class_weights, oca.iteration_index, oca.group_index, oca.total_groups, (int)oca.svm_type, (int)oca.svm_kernel, (int)oca.scale_function, null, ida.randomisation_cv_folds, null, ida.outer_cv_folds, oca.inner_cv_folds, start, end)}");

                            var group_merged_filename = Path.Combine(group_folder, $@"m_{get_filename(oca.class_weights, iteration_index, group_index, oca.total_groups, (int)oca.svm_type, (int)oca.svm_kernel, (int)oca.scale_function, null, ida.randomisation_cv_folds, null, ida.outer_cv_folds, oca.inner_cv_folds, null, null)}");
                            //var group_merged_train_fn = $@"{group_merged_filename}.train.libsvm";
                            //var group_merged_grid_fn = $@"{group_merged_filename}.grid.libsvm";
                            //var group_merged_testing_fn = $@"{group_merged_filename}.test.libsvm";
                            //var group_merged_predict_fn = $@"{group_merged_filename}.predict.libsvm";
                            var group_merged_cm_fn = $@"{group_merged_filename}.cm.csv";

                            //var iteration_all_group_cm_list = new List<string>();
                            //var iteration_instance_all_groups_cm_lines = new List<string>();

                            //var ocv_iteration_all_group_cm_list = new List<string>();
                            //var ocv_iteration_instance_all_groups_cm_lines = new List<string>();

                            var iteration_all_cm = new List<(string line, perf.confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)>();


                            if (io_proxy.is_file_available(group_merged_cm_fn))
                            {
                                var lcs = perf.confusion_matrix.load(group_merged_cm_fn);
                                lcs = lcs.Where((a, i) => i == 0 || a.unknown_key_value_list.First(b => b.key == nameof(iteration_index)).value_int == iteration_index).ToList();
                                if (lcs != null && lcs.Count > 1)
                                {
                                    iteration_all_cm.AddRange(lcs.Skip(iteration_all_cm.Count == 0 ? 0 : 1));
                                }

                                //var lcs = load_cm_scores(group_merged_cm_fn, oca);
                                //scores.AddRange(lcs.scores);

                                //iteration_instance_all_groups_cm_lines.AddRange(iteration_instance_all_groups_cm_lines.Count == 0 ? lcs.lines : lcs.lines.Skip(1));
                                //iteration_all_group_cm_list.AddRange(iteration_all_group_cm_list.Count == 0 ? lcs.lines : lcs.lines.Skip(1));

                                io_proxy.WriteLine($@"{experiment_name}: Group cache: Loaded for {(iteration_index + 1)} group {(group_index + 1)} (groups {(array_index_start + 1)} to {(array_index_last + 1)}). File: {group_merged_cm_fn}.");

                            }
                            else
                            {
                                io_proxy.WriteLine($@"{experiment_name}: Group cache: Unavailable for {(iteration_index + 1)} group {(group_index + 1)} (groups {(array_index_start + 1)} to {(array_index_last + 1)}). File: {group_merged_cm_fn}.");



                                var test_selected_groups = selected_groups.ToList();
                                var test_selected_columns = selected_columns.ToList();

                                var is_group_selected = test_selected_groups.Contains(group_index);
                                var is_only_selection = is_group_selected && test_selected_groups.Count == 1;
                                var is_last_winner = group_index == previous_winner_group_index;



                                // if selected, remove
                                // if not selected, add
                                // if only group, no action
                                // if just added, no action

                                var dir = direction.neutral;

                                if (is_group_selected)
                                {
                                    if (!is_only_selection && !is_last_winner)
                                    {
                                        dir = direction.backwards;
                                        test_selected_groups.Remove(group_index);
                                        test_selected_columns = test_selected_columns.Except(idr.groups[group_index].columns).ToList();
                                    }
                                }
                                else
                                {
                                    dir = direction.forwards;
                                    test_selected_groups.Add(group_index);
                                    test_selected_columns = test_selected_columns.Union(idr.groups[group_index].columns).OrderBy(a => a).ToList();
                                }

                                if (dir != direction.neutral)
                                {
                                    // ensure lists are consistent between instances
                                    test_selected_groups = test_selected_groups.OrderBy(a => a).Distinct().ToList();
                                    test_selected_columns = test_selected_columns.OrderBy(a => a).Distinct().ToList();
                                }

                                remove_duplicate_columns(idr, test_selected_columns);

                                var num_columns_added_from_last_iteration = test_selected_columns.Count - previous_selected_columns.Count;
                                var num_groups_added_from_last_iteration = test_selected_groups.Count - previous_selected_groups.Count;
                                var num_columns_added_from_highest_score_iteration = test_selected_columns.Count - all_time_highest_score_selected_columns.Count;
                                var num_groups_added_from_highest_score_iteration = test_selected_groups.Count - all_time_highest_score_selected_groups.Count;

                                var exp_data = new List<(string key, string value)>()
                                {
                                    ($@"{nameof(is_group_selected)}", $@"{is_group_selected}"),
                                    ($@"{nameof(is_only_selection)}", $@"{is_only_selection}"),
                                    ($@"{nameof(is_last_winner)}", $@"{is_last_winner}"),
                                    ($@"{nameof(num_columns_added_from_last_iteration)}", $@"{num_columns_added_from_last_iteration}"),
                                    ($@"{nameof(num_groups_added_from_last_iteration)}", $@"{num_groups_added_from_last_iteration}"),
                                    ($@"{nameof(num_columns_added_from_highest_score_iteration)}", $@"{num_columns_added_from_highest_score_iteration}"),
                                    ($@"{nameof(num_groups_added_from_highest_score_iteration)}", $@"{num_groups_added_from_highest_score_iteration}"),
                                    ($@"{nameof(dir)}", $@"{dir}"),
                                    ($@"{nameof(previous_selected_groups)}", $@"{string.Join(";", previous_selected_groups)}"),
                                    ($@"{nameof(previous_selected_columns)}", $@"{string.Join(";", previous_selected_columns)}"),
                                    ($@"{nameof(selected_groups)}", $@"{string.Join(";", selected_groups)}"),
                                    ($@"{nameof(selected_columns)}", $@"{string.Join(";", selected_columns)}"),
                                    ($@"{nameof(test_selected_groups)}", $@"{string.Join(";", test_selected_groups)}"),
                                    ($@"{nameof(test_selected_columns)}", $@"{string.Join(";", test_selected_columns)}"),
                                };

                                var cm_header = $"{string.Join(",", exp_data.Select(a => a.key).ToList())},{string.Join(",", perf.confusion_matrix.csv_header)}";

                                //if (iteration_instance_all_groups_cm_lines.Count == 0) { iteration_instance_all_groups_cm_lines.Add(cm_header); }
                                //if (iteration_all_group_cm_list.Count == 0) iteration_all_group_cm_list.Add(cm_header);

                                // 1. make outer-cv files
                                var outer_cv_inputs = make_outer_cv_inputs(test_selected_columns, ida, idr, oca, group_folder, iteration_index, group_index);
                                var merged_cv_input = outer_cv_inputs.First(a => a.ocvi == -1);

                                // 2. run libsvm
                                var prediction_data_list = new List<((long grid_dur, long train_dur, long predict_dur) dur, ((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double? cv_rate) grid, string[] predict_text)>();



                                foreach (var outer_cv_input in outer_cv_inputs)
                                {
                                    if (outer_cv_input.ocvi == -1 || outer_cv_input.rcvi == -1) continue; // -1 is the index for the merged text

                                    var prediction_data = inner_cross_validation(ida, idr, oca, outer_cv_input);

                                    prediction_data_list.Add(prediction_data);

                                    if (make_outer_cv_confusion_matrices)
                                    {
                                        var ocv_prediction_file_data = perf.load_prediction_file(outer_cv_input.testing_text, null, prediction_data.predict_text, oca.output_threshold_adjustment_performance);

                                        foreach (var ocv_cm in ocv_prediction_file_data.cm_list)
                                        {
                                            ocv_cm.x_iteration_index = iteration_index;
                                            ocv_cm.x_group_index = group_index;
                                            ocv_cm.x_total_groups = oca.total_groups;

                                            ocv_cm.x_key_alphabet = group_key.alphabet;
                                            ocv_cm.x_key_dimension = group_key.dimension;
                                            ocv_cm.x_key_category = group_key.category;
                                            ocv_cm.x_key_source = group_key.source;
                                            ocv_cm.x_key_group = group_key.group;
                                            ocv_cm.x_key_member = group_key.member;
                                            ocv_cm.x_key_perspective = group_key.perspective;

                                            ocv_cm.x_experiment_name = experiment_name;
                                            ocv_cm.x_scaling_function = oca.scale_function.ToString();
                                            ocv_cm.x_old_feature_count = previous_selected_columns.Count;
                                            ocv_cm.x_new_feature_count = test_selected_columns.Count;
                                            ocv_cm.x_old_group_count = previous_selected_groups.Count;
                                            ocv_cm.x_new_group_count = test_selected_groups.Count;
                                            ocv_cm.x_inner_cv_folds = oca.inner_cv_folds;
                                            ocv_cm.x_randomisation_cv_index = outer_cv_input.rcvi;
                                            ocv_cm.x_randomisation_cv_folds = ida.randomisation_cv_folds;
                                            ocv_cm.x_outer_cv_index = outer_cv_input.ocvi;
                                            ocv_cm.x_outer_cv_folds = ida.outer_cv_folds;
                                            ocv_cm.x_svm_type = oca.svm_type.ToString();
                                            ocv_cm.x_svm_kernel = oca.svm_kernel.ToString();
                                            ocv_cm.x_class_weight = oca.class_weights?.FirstOrDefault(b => ocv_cm.class_id == b.class_id).class_weight;
                                            ocv_cm.x_class_name = ida.class_names?.FirstOrDefault(b => ocv_cm.class_id == b.class_id).class_name;
                                            ocv_cm.x_class_size = idr.class_sizes?.First(b => b.class_id == ocv_cm.class_id).class_size ?? -1;
                                            ocv_cm.x_class_training_size = outer_cv_input.training_sizes?.First(b => b.class_id == ocv_cm.class_id).training_size ?? -1;
                                            ocv_cm.x_class_testing_size = outer_cv_input.testing_sizes?.First(b => b.class_id == ocv_cm.class_id).testing_size ?? -1;
                                            ocv_cm.x_cost = prediction_data.grid.point.cost;
                                            ocv_cm.x_gamma = prediction_data.grid.point.gamma;
                                            ocv_cm.x_coef0 = prediction_data.grid.point.coef0;
                                            ocv_cm.x_epsilon = prediction_data.grid.point.epsilon;
                                            ocv_cm.x_degree = prediction_data.grid.point.degree;
                                            ocv_cm.x_libsvm_cv = prediction_data.grid.cv_rate.GetValueOrDefault();
                                            ocv_cm.x_duration_grid_search = prediction_data.dur.grid_dur.ToString(CultureInfo.InvariantCulture);
                                            ocv_cm.x_duration_training = prediction_data.dur.train_dur.ToString(CultureInfo.InvariantCulture);
                                            ocv_cm.x_duration_testing = prediction_data.dur.predict_dur.ToString(CultureInfo.InvariantCulture);
                                            ocv_cm.calculate_ppf();
                                        }

                                        
                                        var ocv_cm_lines = new List<string>() { cm_header };
                                        ocv_cm_lines.AddRange(ocv_prediction_file_data.cm_list.Select(a => $"{string.Join(",", exp_data.Select(c => c.value).ToList())},{a}").ToList());
                                        var ocv_lcs = perf.confusion_matrix.load(ocv_cm_lines);

                                        if (ocv_lcs != null && ocv_lcs.Count > 1)
                                        {
                                            iteration_all_cm.AddRange(ocv_lcs.Skip(iteration_all_cm.Count == 0 ? 0 : 1));
                                        }

                                        //foreach (var cm in ocv_prediction_file_data.cm_list)
                                        //{
                                        //    //var cm_score_increase_from_last = cm_winner_score - previous_winner_score;
                                        //    //var cm_score_increase_from_all = cm_winner_score - all_time_highest_score;

                                        //    //var cm_score_increase_from_last_pct = previous_winner_score != 0 ? cm_score_increase_from_last / previous_winner_score : 0;
                                        //    //var cm_score_increase_from_all_pct = all_time_highest_score != 0 ? cm_score_increase_from_all / all_time_highest_score : 0;

                                        //    //var cm_score_better_than_last = cm_score_increase_from_last > 0d;
                                        //    //var cm_score_better_than_all = cm_score_increase_from_all > 0d;

                                        //    var ocv_cm_line = ;

                                        //    ocv_cm_lines.Add(ocv_cm_line);

                                        //    ocv_iteration_instance_all_groups_cm_lines.Add(ocv_cm_line);

                                        //    ocv_iteration_all_group_cm_list.Add(ocv_cm_line);
                                        //}

                                        // save OCV CM for group
                                        io_proxy.WriteAllLines(outer_cv_input.cm_fn, ocv_cm_lines);
                                        io_proxy.WriteLine($@"{experiment_name}: Group OCV cache: Saved for iteration {(iteration_index + 1)} group {(group_index + 1)} (groups {(array_index_start + 1)} to {(array_index_last + 1)}) OCV R({outer_cv_input.rcvi}/{ida.randomisation_cv_folds}) O({outer_cv_input.ocvi}/{ida.outer_cv_folds}). File: {outer_cv_input.cm_fn}.");

                                    }

                                    io_proxy.Delete(outer_cv_input.training_fn);
                                    io_proxy.Delete(outer_cv_input.grid_fn);
                                    io_proxy.Delete(outer_cv_input.model_fn);
                                    io_proxy.Delete(outer_cv_input.testing_fn);
                                    io_proxy.Delete(outer_cv_input.predict_fn);
                                    //io_proxy.Delete(outer_cv_input.cm_fn);
                                }

                                // 3. make confusion matrix from the merged prediction results
                                var merged_prediction_text = prediction_data_list.SelectMany(a => a.predict_text).ToList();

                                var prediction_file_data = perf.load_prediction_file(merged_cv_input.testing_text, null, merged_prediction_text, oca.output_threshold_adjustment_performance);

                                foreach (var cm in prediction_file_data.cm_list)
                                {
                                    cm.x_experiment_name = experiment_name;

                                    cm.x_iteration_index = iteration_index;
                                    cm.x_group_index = group_index;
                                    cm.x_total_groups = oca.total_groups;

                                    cm.x_key_alphabet = group_key.alphabet;
                                    cm.x_key_dimension = group_key.dimension;
                                    cm.x_key_category = group_key.category;
                                    cm.x_key_source = group_key.source;
                                    cm.x_key_group = group_key.group;
                                    cm.x_key_member = group_key.member;
                                    cm.x_key_perspective = group_key.perspective;

                                    cm.x_scaling_function = oca.scale_function.ToString();
                                    cm.x_old_feature_count = previous_selected_columns.Count;
                                    cm.x_new_feature_count = test_selected_columns.Count;
                                    cm.x_old_group_count = previous_selected_groups.Count;
                                    cm.x_new_group_count = test_selected_groups.Count;
                                    cm.x_inner_cv_folds = oca.inner_cv_folds;
                                    cm.x_randomisation_cv_index = -1; //input.rcvi;
                                    cm.x_randomisation_cv_folds = ida.randomisation_cv_folds;
                                    cm.x_outer_cv_index = -1; //input.ocvi;
                                    cm.x_outer_cv_folds = ida.outer_cv_folds;
                                    cm.x_svm_type = oca.svm_type.ToString();
                                    cm.x_svm_kernel = oca.svm_kernel.ToString();
                                    cm.x_class_weight = oca.class_weights?.FirstOrDefault(b => cm.class_id == b.class_id).class_weight;
                                    cm.x_class_name = ida.class_names?.FirstOrDefault(b => cm.class_id == b.class_id).class_name;
                                    cm.x_class_size = idr.class_sizes?.First(b => b.class_id == cm.class_id).class_size ?? -1;
                                    cm.x_class_training_size = merged_cv_input.training_sizes?.First(b => b.class_id == cm.class_id).training_size ?? -1;
                                    cm.x_class_testing_size = merged_cv_input.testing_sizes?.First(b => b.class_id == cm.class_id).testing_size ?? -1;

                                    cm.x_cost = prediction_data_list.Where(a => a.grid.point.cost != null).Select(a => a.grid.point.cost).DefaultIfEmpty(0).Average();
                                    cm.x_gamma = prediction_data_list.Where(a => a.grid.point.gamma != null).Select(a => a.grid.point.gamma).DefaultIfEmpty(0).Average();
                                    cm.x_coef0 = prediction_data_list.Where(a => a.grid.point.coef0 != null).Select(a => a.grid.point.coef0).DefaultIfEmpty(0).Average();
                                    cm.x_epsilon = prediction_data_list.Where(a => a.grid.point.epsilon != null).Select(a => a.grid.point.epsilon).DefaultIfEmpty(0).Average();
                                    cm.x_degree = prediction_data_list.Where(a => a.grid.point.degree != null).Select(a => a.grid.point.degree).DefaultIfEmpty(0).Average();
                                    cm.x_libsvm_cv = prediction_data_list.Where(a => a.grid.cv_rate != null).Select(a => (double)a.grid.cv_rate).DefaultIfEmpty(0).Average();

                                    cm.x_duration_grid_search = prediction_data_list.Select(a => a.dur.grid_dur).Sum().ToString(CultureInfo.InvariantCulture);
                                    cm.x_duration_training = prediction_data_list.Select(a => a.dur.train_dur).Sum().ToString(CultureInfo.InvariantCulture);
                                    cm.x_duration_testing = prediction_data_list.Select(a => a.dur.predict_dur).Sum().ToString(CultureInfo.InvariantCulture);

                                    cm.calculate_ppf();
                                }



                                var merge_cm_lines = new List<string>() { cm_header };
                                merge_cm_lines.AddRange(prediction_file_data.cm_list.Select(a => $"{string.Join(",", exp_data.Select(c => c.value).ToList())},{a}").ToList());
                                var lcs = perf.confusion_matrix.load(merge_cm_lines);

                                if (lcs != null && lcs.Count > 1)
                                {
                                    iteration_all_cm.AddRange(lcs.Skip(iteration_all_cm.Count == 0 ? 0 : 1));
                                }

                                //var cm_score = prediction_file_data.cm_list.FirstOrDefault(a=>a.class_id==oca.scoring_class_id).get_value_by_name(oca.score1_metric);
                                //var cm_score_increase_from_last = cm_score - previous_winner_score;
                                //var cm_score_increase_from_all = cm_score - all_time_highest_score;
                                //var cm_score_increase_from_last_pct = previous_winner_score != 0 ? cm_score_increase_from_last / previous_winner_score : 0;
                                //var cm_score_increase_from_all_pct = all_time_highest_score != 0 ? cm_score_increase_from_all / all_time_highest_score : 0;
                                //var cm_score_better_than_last = cm_score_increase_from_last > 0d;
                                //var cm_score_better_than_all = cm_score_increase_from_all > 0d;

                                //// todo: ppf value of new features.... score increase / num features added?

                                //var cm_score_ppf = num_columns_added_from_last_iteration == 0 ? 0 : cm_score_increase_from_all / num_columns_added_from_last_iteration;

                                //var ppf_header = $@"{nameof(cm_score_increase_from_last)},{nameof(cm_score_increase_from_all)},{nameof(cm_score_increase_from_last_pct)},{nameof(cm_score_increase_from_all_pct)},{nameof(cm_score_better_than_last)},{nameof(cm_score_better_than_all)}";
                                //var ppf_data = $@"{cm_score_increase_from_last},{cm_score_increase_from_all},{cm_score_increase_from_last_pct},{cm_score_increase_from_all_pct},{cm_score_better_than_last},{cm_score_better_than_all}";


                                //foreach (var cm in prediction_file_data.cm_list)
                                //{
                                //    var cm_line = $"{string.Join(",", exp_data.Select(c => c.value).ToList())},{cm}";

                                //    merge_cm_lines.Add(cm_line);

                                //    //iteration_instance_all_groups_cm_lines.Add(cm_line);

                                //    //iteration_all_group_cm_list.Add(cm_line);

                                //    //var lcs = perf.confusion_matrix.load(group_merged_cm_fn);
                                //    //lcs = lcs.Where((a, i) => i == 0 || a.unknown_key_value_list.First(b => b.key == nameof(iteration_index)).value_int == iteration_index).ToList();
                                    
                                //    if (lcs != null && lcs.Count > 1)
                                //    {
                                //        iteration_all_cm.AddRange(lcs.Skip(iteration_all_cm.Count == 0 ? 0 : 1));
                                //    }

                                //    //if (cm.class_id == oca.scoring_class_id) { scores.Add((iteration_index, group_index, (int)cm.class_id, dir, cm.get_value_by_name(oca.score1_metric), cm.get_value_by_name(oca.score2_metric), cm.get_value_by_name(oca.score3_metric))); }
                                //}


                                // save CM for group
                                io_proxy.WriteAllLines( /*merged_cv_input.cm_fn*/group_merged_cm_fn, merge_cm_lines);
                                io_proxy.WriteLine($@"{experiment_name}: Group cache: Saved for iteration {(iteration_index + 1)} group {(group_index + 1)} ({(array_index_start + 1)} to {(array_index_last + 1)}). File: {group_merged_cm_fn}.");
                            }

                            io_proxy.WriteLine($@"{experiment_name}: Finished: iteration {(iteration_index + 1)} group {(group_index + 1)} ({(array_index_start + 1)} to {(array_index_last + 1)}).");

                            return (iteration_index, group_index, iteration_all_cm); //iteration_all_group_cm_list, iteration_instance_all_groups_cm_lines, ocv_iteration_all_group_cm_list, ocv_iteration_instance_all_groups_cm_lines);
                        });

                        tasks.Add(task);
                    }

                    Task.WaitAll(tasks.ToArray<Task>());

                    iteration_all_cm.AddRange(tasks.SelectMany((a,i)=> a.Result.iteration_all_cm.Skip(i == 0 && iteration_all_cm.Count == 0 ? 0 : 1)));
                    var iteration_instance_all_groups_cm_lines = iteration_all_cm.Where((a,i) => i == 0 || (a.cm.x_group_index >= array_index_start && a.cm.x_group_index <= array_index_last));
                        
                    //_iteration_all_group_cm_list.AddRange(tasks.SelectMany((a, i) => a.Result.iteration_all_group_cm_list.Skip(i == 0 && _iteration_all_group_cm_list.Count == 0 ? 0 : 1).ToList()));
                    //_iteration_instance_all_groups_cm_lines.AddRange(tasks.SelectMany((a, i) => a.Result.iteration_instance_all_groups_cm_lines.Skip(i == 0 && _iteration_instance_all_groups_cm_lines.Count == 0 ? 0 : 1).ToList()));
                    //_ocv_iteration_all_group_cm_list.AddRange(tasks.SelectMany((a, i) => a.Result.ocv_iteration_all_group_cm_list.Skip(i == 0 && _ocv_iteration_all_group_cm_list.Count == 0 ? 0 : 1).ToList()));
                    //_ocv_iteration_instance_all_groups_cm_lines.AddRange(tasks.SelectMany((a, i) => a.Result.ocv_iteration_instance_all_groups_cm_lines.Skip(i == 0 && _ocv_iteration_instance_all_groups_cm_lines.Count == 0 ? 0 : 1).ToList()));

                    // 4. save CM for all groups of this instance (from index start to index end) merged outer-cv results
                    //

                    io_proxy.WriteAllLines(iteration_instance_group_cm_filename, iteration_instance_all_groups_cm_lines.Select(a=>a.line));
                    io_proxy.WriteLine($"{experiment_name}: Part cache: Saved for iteration {(iteration_index + 1)} group {(array_index_start + 1)} to {(array_index_last + 1)}. File: {iteration_instance_group_cm_filename}.");
                }

                // 5. load results from other instances
                //

                if (load_other_instances)// && loaded_indexes.Count < oca.total_groups)
                {
                    io_proxy.WriteLine($"{experiment_name}: Waiting for other instances to complete iteration {(iteration_index + 1)}. (Completed groups {(array_index_start + 1)} to {(array_index_last + 1)}). File: {iteration_instance_group_cm_filename}.");

                    //var instances_done = new List<string>() { iteration_instance_group_cm_filename, iteration_all_group_cm_filename };

                    var loaded_indexes = iteration_all_cm.Skip(1).Select(a => a.cm.x_group_index).Distinct().ToList();

                    while (loaded_indexes.Count < oca.total_groups)
                    {
                        var dir_files = Directory.GetFiles(iteration_folder, "x_*.cm.csv", SearchOption.TopDirectoryOnly);
                        var tasks = new List<Task<List<(string line, perf.confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)>>>();

                        for (var dir_file_index = 0; dir_file_index < dir_files.Length; dir_file_index++)
                        {
                            var file = dir_files[dir_file_index];

                            if (instance_files_loaded.Contains(file)) continue;
                            instance_files_loaded.Add(file);

                            var task = Task.Run(() =>
                            {
                                var lcs = perf.confusion_matrix.load(file);

                                io_proxy.WriteLine($"{experiment_name}: Part cache (instances): Loaded for iteration {(iteration_index + 1)} groups {(lcs.Min(a => a.cm.x_group_index) + 1)} to {(lcs.Max(a => a.cm.x_group_index) + 1)}. File: {file}.");

                                return lcs;
                            });

                            tasks.Add(task);
                        }

                        Task.WaitAll(tasks.ToArray<Task>());

                        iteration_all_cm.AddRange(tasks.SelectMany((a,i)=>a.Result.Skip(i == 0 && iteration_all_cm.Count == 0 ? 0: 1)));

                        loaded_indexes = iteration_all_cm.Skip(1).Select(a => a.cm.x_group_index).Distinct().ToList();

                        if (loaded_indexes.Count < oca.total_groups) { Task.Delay(new TimeSpan(0, 0, 15)).Wait(); }
                    }

                    if (instance_id == 0)
                    {
                        // save CM with results from all instances
                        io_proxy.WriteAllLines(iteration_all_group_cm_filename, iteration_all_cm.Select(a=>a.line));
                        io_proxy.WriteLine($"{experiment_name}: Full cache: Saved for iteration {(iteration_index + 1)}. File: {iteration_all_group_cm_filename}.");
                    }

                    // todo: get indexes for all instances
                    // todo: change which class/metrics are used.  e.g. highest_score_this_iteration = winner.cms.cm_list.Where(b => p.feature_selection_classes == null || p.feature_selection_classes.Count == 0 || p.feature_selection_classes.Contains(b.class_id.Value)).Average(b => b.get_perf_value_strings().Where(c => p.feature_selection_metrics.Any(d => string.Equals(c.name, d, StringComparison.InvariantCultureIgnoreCase))).Average(c => c.value));

                }



                // 5. find winner
                // ensure ordering will be consistent between instances

                var scoring_cm = iteration_all_cm.Skip(1).Where(a => a.cm.class_id == oca.scoring_class_id && a.cm.x_prediction_threshold == null && a.cm.x_randomisation_cv_index == -1 && a.cm.x_outer_cv_index == -1).ToList();
                //scoring_cm = scoring_cm.OrderBy(a => a.unknown_key_value_list.First(b => b.key == "iteration_index").value_int).ToList();
                scoring_cm = scoring_cm.OrderBy(a => a.cm.x_group_index).ToList();
                scoring_cm = scoring_cm.OrderByDescending(a => a.cm.get_value_by_name(oca.score1_metric)).ThenByDescending(a => a.cm.get_value_by_name(oca.score2_metric)).ThenByDescending(a => a.cm.get_value_by_name(oca.score3_metric)).ToList();

                
                var scores_winner_index = 0;
                while (scoring_cm[scores_winner_index].cm.x_group_index == previous_winner_group_index && scores_winner_index < oca.total_groups - 1) { scores_winner_index++; }

                var winner_group_index = (int)scoring_cm[scores_winner_index].cm.x_group_index;
                var winner_group = idr.groups[winner_group_index];
                var winner_group_key = winner_group.key;
                var winner_score = scoring_cm[scores_winner_index].cm.get_value_by_name(oca.score1_metric);
                var winner_direction = (direction) Enum.Parse(typeof(direction), scoring_cm[scores_winner_index].unknown_key_value_list.FirstOrDefault(a => a.key == "dir").value_str, true);

                


                var score_increase_from_last = winner_score - previous_winner_score;
                var score_increase_from_all = winner_score - all_time_highest_score;

                var score_increase_from_last_pct = previous_winner_score != 0 ? score_increase_from_last / previous_winner_score : 0;
                var score_increase_from_all_pct = all_time_highest_score != 0 ? score_increase_from_all / all_time_highest_score : 0;

                var score_better_than_last = score_increase_from_last > 0d;
                var score_better_than_all = score_increase_from_all > 0d;

                iterations_not_better_than_last = score_better_than_last ? 0 : iterations_not_better_than_last + 1;
                iterations_not_better_than_all = score_better_than_all ? 0 : iterations_not_better_than_all + 1;


                if (winner_direction == direction.forwards)
                {
                    selected_groups.Add(winner_group_index);
                    selected_columns.AddRange(idr.groups[winner_group_index].columns);
                }
                else if (winner_direction == direction.backwards)
                {
                    selected_groups.Remove(winner_group_index);
                    selected_columns = selected_columns.Except(idr.groups[winner_group_index].columns).ToList();
                }

                if (winner_direction != direction.neutral)
                {
                    selected_groups = selected_groups.OrderBy(a => a).Distinct().ToList();
                    selected_columns = selected_columns.OrderBy(a => a).Distinct().ToList();
                }


                if (score_better_than_all)
                {
                    all_time_highest_score = winner_score;
                    all_time_highest_score_iteration = iteration_index;
                    all_time_highest_score_selected_groups = selected_groups.ToList();
                    all_time_highest_score_selected_columns = selected_columns.ToList();
                }

                if (instance_id == 0)
                {
                    // todo: check winners csv file has the group winners from original winning iterations (otherwise metrics will be overwritten)

                    var winner_fn = Path.Combine(iteration_folder, $@"iteration_winner.csv");

                    if (io_proxy.is_file_available(winner_fn))
                    {
                        io_proxy.WriteLine($@"{experiment_name}: Winner cache: Already saved for iteration {(iteration_index + 1)}. File: {winner_fn}.");
                    }
                    else {

                        io_proxy.WriteLine($@"{experiment_name}: Winner cache: Unavailable for iteration {(iteration_index + 1)}. File: {winner_fn}.");

                        
                        // todo: process some other indexes (random) whilst waiting...? with option to cancel if other instance finishes.

                        

                        var cm_winner = iteration_all_cm.Where((a, i) => i == 0 || a.cm.x_iteration_index == iteration_index && a.cm.x_group_index == winner_group_index).Select((a,i) =>
                        {
                            if (i==0) return $"{nameof(all_time_highest_score)},{nameof(score_better_than_all)},{nameof(all_time_highest_score_iteration)},{nameof(all_time_highest_score_selected_groups)},{nameof(all_time_highest_score_selected_columns)},{a.line}";
                            return $"{all_time_highest_score:G17},{score_better_than_all},{all_time_highest_score_iteration},{string.Join(";", all_time_highest_score_selected_groups)},{string.Join(";", all_time_highest_score_selected_columns)},{a.line}";
                        }).ToList();

                        winners_cm.AddRange(perf.confusion_matrix.load(cm_winner).Skip(winners_cm.Count == 0 ? 0 : 1));
                        

                        io_proxy.WriteAllLines(winner_fn, winners_cm.Select(a=>a.line));
                        io_proxy.WriteLine($@"{experiment_name}: Winner cache: Saved for iteration {(iteration_index + 1)}. File: {winner_fn}.");

                    }
                }

                finished = !((score_better_than_last || (iterations_not_better_than_last < limit_iteration_not_better_than_last && iterations_not_better_than_all < limit_iteration_not_better_than_all)) && (selected_groups.Count < oca.total_groups));


                io_proxy.WriteLine($"{experiment_name}: Finished: iteration {(iteration_index + 1)} for {(array_index_start + 1)} to {(array_index_last + 1)}.  {(finished ? "finished" : "not finished")}.");

                previous_winner_score = winner_score;
                previous_winner_group_index = winner_group_index;
                previous_selected_groups = selected_groups.ToList();
                previous_selected_columns = selected_columns.ToList();
                iteration_index++;

            }

            test_group_kernel_scaling_perf();

            io_proxy.WriteLine($"{experiment_name}: Finished: all iterations for groups {(array_index_start + 1)} to {(array_index_last + 1)}.");
        }



        internal static void remove_duplicate_columns(init_dataset_ret idr, List<int> query_cols)
        {
            // remove duplicate columns (may exist in separate groups)
            var query_col_dupe_check = idr.dataset_instance_list_grouped.SelectMany(a => a.examples).SelectMany(a => query_cols.Select(b => (query_col: b, fv: a.feature_data[b].fv)).ToList()).GroupBy(b => b.query_col).Select(b => (query_col: b.Key, values: b.Select(c => c.fv).ToList())).ToList();
            var dupe_clusters = new List<List<int>>();
            for (var i = 0; i < query_col_dupe_check.Count; i++)
            {
                for (var j = 0; j < query_col_dupe_check.Count; j++)
                {
                    if (i <= j) continue;

                    if (query_col_dupe_check[i].values.SequenceEqual(query_col_dupe_check[j].values))
                    {
                        var cluster = new List<int>() { query_col_dupe_check[i].query_col, query_col_dupe_check[j].query_col };
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
                //var keep = dc.First();
                var remove = dc.Skip(1).ToList();
                query_cols.RemoveAll(a => remove.Any(b => a == b));
            }

            ///
        }

        internal enum direction { forwards, neutral, backwards }



        internal static List<(int rcvi, int ocvi, string training_fn, string grid_fn, string model_fn, string testing_fn, string predict_fn, string cm_fn, List<string> training_text, List<string> testing_text, List<(int class_id, int training_size)> training_sizes, List<(int class_id, int testing_size)> testing_sizes)> make_outer_cv_inputs(List<int> column_indexes, init_dataset_args ida, init_dataset_ret idr, outer_cv_args oca, string group_folder, int iteration_index, int group_index)
        {
            //var module_name = nameof(Program);
            //var method_name = nameof(outer_cv);

            column_indexes = column_indexes.ToList();



            var tasks = new List<Task<(int rcvi, int ocvi, string training_fn, string grid_fn, string model_fn, string testing_fn, string predict_fn, string cm_fn, List<string> training_text, List<string> testing_text, List<(int class_id, int training_size)> training_sizes, List<(int class_id, int testing_size)> testing_sizes)>>();

            for (var _randomisation_cv_index = 0; _randomisation_cv_index < ida.randomisation_cv_folds; _randomisation_cv_index++)
            {
                for (var _outer_cv_index = 0; _outer_cv_index < ida.outer_cv_folds; _outer_cv_index++)
                {
                    var randomisation_cv_index = _randomisation_cv_index;
                    var outer_cv_index = _outer_cv_index;

                    var task = Task.Run(() =>
                    {
                        var filename = Path.Combine(group_folder, $@"o_{get_filename(oca.class_weights, iteration_index, group_index, oca.total_groups, (int)oca.svm_type, (int)oca.svm_kernel, (int)oca.scale_function, randomisation_cv_index, ida.randomisation_cv_folds, outer_cv_index, ida.outer_cv_folds, oca.inner_cv_folds, null, null)}");
                        var train_fn = $@"{filename}.train.libsvm";
                        var grid_fn = $@"{filename}.grid.libsvm";
                        var model_fn = $@"{filename}.model.libsvm";
                        var testing_fn = $@"{filename}.test.libsvm";
                        var predict_fn = $@"{filename}.predict.libsvm";
                        var cm_fn = $@"{filename}.cm.csv";


                        var training_fold_indexes = idr.downsampled_training_class_folds.Select(a => (a.class_id, outer_cv_index: outer_cv_index, indexes: a.folds.Where(b => b.randomisation_cv_index == randomisation_cv_index && b.outer_cv_index != outer_cv_index).SelectMany(b => b.indexes).OrderBy(b => b).ToList())).ToList();
                        var training_examples = training_fold_indexes.Select(a => (a.class_id, examples: a.indexes.Select(row_index => idr.dataset_instance_list_grouped.First(b => a.class_id == b.class_id).examples[row_index]).ToList())).ToList();
                        var training_example_columns = training_examples.AsParallel().AsOrdered().Select(a => (a.class_id, examples: a.examples.Select(b => (example: b, columns: column_indexes.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList() /*, columns_hash: hash.calc_hash(string.Join(" ", query_cols.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList()))*/)).ToList())).ToList();
                        if (training_example_columns.Any(a => a.examples == null || a.examples.Count == 0)) { throw new Exception(); }

                        var training_scaling_params = training_example_columns.AsParallel()
                            .AsOrdered()
                            .SelectMany(a => a.examples.SelectMany(b => b.columns).ToList())
                            .GroupBy(a => a.fid)
                            .Select(a =>
                            {
                                var x = a.Select(b => b.fv).ToList();

                                return (fid: a.Key, list: x, non_zero: x.Count(y => y != 0), abs_sum: x.Sum(y => Math.Abs(y)), srsos: routines.sqrt_sumofsqrs(x), average: x.Average(), stdev: routines.standard_deviation_sample(x), min: x.Min(), max: x.Max());
                            })
                            .ToList();
                        var training_example_columns_scaled = training_example_columns.AsParallel()
                            .AsOrdered()
                            .Select(a => (a.class_id, examples: a.examples.Select(b => (b.example, columns: b.columns.Select(c =>
                                    {
                                        var x = training_scaling_params.First(y => y.fid == c.fid);

                                        return (fid: c.fid, fv: routines.scale(c.fv, x.non_zero, x.abs_sum, x.srsos, x.min, x.max, x.average, x.stdev, oca.scale_function));
                                    })
                                    .ToList()))
                                .ToList()))
                            .ToList();
                        var training_sizes = training_example_columns_scaled.Select(a => (class_id: a.class_id, training_size: a.examples.Count)).ToList();
                        var training_text = training_example_columns_scaled.AsParallel().AsOrdered().SelectMany(a => a.examples.Select(b => $@"{a.class_id} {String.Join(" ", b.columns.Where(c => c.fv != 0).OrderBy(c => c.fid).Select(c => $"{c.fid}:{c.fv:G17}").ToList())}").ToList()).ToList();
                        //merged_training_text.AddRange(training_text);
                        io_proxy.WriteAllLines(train_fn, training_text);


                        var testing_fold_indexes = idr.class_folds.Select(a => (a.class_id, outer_cv_index: outer_cv_index, indexes: a.folds.Where(b => b.randomisation_cv_index == randomisation_cv_index && b.outer_cv_index == outer_cv_index).SelectMany(b => b.indexes).OrderBy(b => b).ToList())).ToList();
                        var testing_examples = testing_fold_indexes.Select(a => (a.class_id, examples: a.indexes.Select(row_index => idr.dataset_instance_list_grouped.First(b => a.class_id == b.class_id).examples[row_index]).ToList())).ToList();
                        var testing_example_columns = testing_examples.AsParallel().AsOrdered().Select(a => (a.class_id, examples: a.examples.Select(b => (example: b, columns: column_indexes.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList() /*, columns_hash: hash.calc_hash(string.Join(" ", query_cols.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList()))*/)).ToList())).ToList();
                        if (testing_example_columns.Any(a => a.examples == null || a.examples.Count == 0)) { throw new Exception(); }

                        var testing_example_columns_scaled = testing_example_columns.AsParallel()
                            .AsOrdered()
                            .Select(a => (a.class_id, examples: a.examples.Select(b => (b.example, columns: b.columns.Select(c =>
                                    {
                                        var x = training_scaling_params.First(y => y.fid == c.fid);

                                        return (fid: c.fid, fv: routines.scale(c.fv, x.non_zero, x.abs_sum, x.srsos, x.min, x.max, x.average, x.stdev, oca.scale_function));
                                    })
                                    .ToList()))
                                .ToList()))
                            .ToList();
                        var testing_text = testing_example_columns_scaled.AsParallel().AsOrdered().SelectMany(a => a.examples.Select(b => $@"{a.class_id} {String.Join(" ", b.columns.Where(c => c.fv != 0).OrderBy(c => c.fid).Select(c => $"{c.fid}:{c.fv:G17}").ToList())}").ToList()).ToList();
                        var testing_sizes = testing_example_columns_scaled.Select(a => (class_id: a.class_id, testing_size: a.examples.Count)).ToList();
                        //merged_testing_text.AddRange(testing_text);
                        io_proxy.WriteAllLines(testing_fn, testing_text);

                        return (randomisation_cv_index, outer_cv_index, train_fn, grid_fn, model_fn, testing_fn, predict_fn, cm_fn, training_text, testing_text, training_sizes, testing_sizes);
                    });

                    tasks.Add(task);

                    //ocv_data.Add((randomisation_cv_index, outer_cv_index, train_fn, grid_fn, model_fn, testing_fn, predict_fn, cm_fn, training_text, testing_text, training_sizes, testing_sizes));
                }
            }

            //var ocv_data = new List<(int rcvi, int ocvi, string training_fn, string grid_fn, string model_fn, string testing_fn, string predict_fn, string cm_fn, List<string> training_text, List<string> testing_text, List<(int class_id, int training_size)> training_sizes, List<(int class_id, int testing_size)> testing_sizes)>();

            Task.WaitAll(tasks.ToArray<Task>());

            var ocv_data = tasks.Select(a => a.Result).ToList();

            var merged_training_text = ocv_data.SelectMany(a => a.training_text).ToList();
            var merged_testing_text = ocv_data.SelectMany(a => a.testing_text).ToList();


            var merged_filename = Path.Combine(group_folder, $@"m_{get_filename(oca.class_weights, iteration_index, group_index, oca.total_groups, (int)oca.svm_type, (int)oca.svm_kernel, (int)oca.scale_function, null, ida.randomisation_cv_folds, null, ida.outer_cv_folds, oca.inner_cv_folds, null, null)}");
            var merged_train_fn = $@"{merged_filename}.train.libsvm";
            var merged_grid_fn = $@"{merged_filename}.grid.libsvm";
            var merged_model_fn = $@"{merged_filename}.model.libsvm";
            var merged_testing_fn = $@"{merged_filename}.test.libsvm";
            var merged_predict_fn = $@"{merged_filename}.predict.libsvm";
            var merged_cm_fn = $@"{merged_filename}.cm.csv";

            var merged_training_sizes = ocv_data.SelectMany(a => a.training_sizes).GroupBy(a => a.class_id).Select(b => (class_id: b.Key, training_size: b.Select(c => c.training_size).Sum())).ToList();
            var merged_testing_sizes = ocv_data.SelectMany(a => a.testing_sizes).GroupBy(a => a.class_id).Select(b => (class_id: b.Key, testing_size: b.Select(c => c.testing_size).Sum())).ToList();

            var merged_data = (-1, -1, merged_train_fn, merged_grid_fn, merged_model_fn, merged_testing_fn, merged_predict_fn, merged_cm_fn, merged_training_text, merged_testing_text, merged_training_sizes, merged_testing_sizes);


            ocv_data.Insert(0, merged_data);

            var save_merged_files = false;
            if (save_merged_files)
            {
                io_proxy.WriteAllLines(merged_train_fn, merged_training_text);
                io_proxy.WriteAllLines(merged_testing_fn, merged_testing_text);
            }


            return ocv_data;
        }

        internal static string get_filename(List<(int class_id, double weight)> weights, int? iteration_index, int? group_index, int? total_groups, int? svm_type, int? svm_kernel, int? scale_function, int? randomisation_cv_index, int? randomisation_cv_folds, int? outer_cv_index, int? outer_cv_folds, int? inner_cv_folds, int? index_first, int? index_last)
        {
            var hr = true;

            var w = weights != null && weights.Count > 0 ? weights.Select(a => (key: "wi" + a.class_id, current: (int?)(a.weight * 100), goal: (int?)null)).Where(a => a.current != null || a.goal != null).ToList() : null;

            var filename_parts = new List<(string key, int? current, int? goal)>() { ("it", iteration_index != null ? iteration_index + (hr ? 1 : 0) : null, null), ("gr", group_index != null ? group_index + (hr ? 1 : 0) : null, total_groups), ("sv", svm_type != null ? svm_type + (hr ? 1 : 0) : null, null), ("kr", svm_kernel != null ? svm_kernel + (hr ? 1 : 0) : null, null), ("sc", scale_function != null ? scale_function + (hr ? 1 : 0) : null, null), ("rn", randomisation_cv_index != null ? randomisation_cv_index + (hr ? 1 : 0) : null, randomisation_cv_folds), ("oc", outer_cv_index != null ? outer_cv_index + (hr ? 1 : 0) : null, outer_cv_folds), ("ic", null, inner_cv_folds), ("ix", index_first + (hr ? 1 : 0), index_last + (hr ? 1 : 0)), }.Where(a => a.current != null || a.goal != null).ToList();

            if (w != null && w.Count > 0) { filename_parts.AddRange(w); }

            return $@"{string.Join("_", filename_parts.Select(a => $@"{a.key}-{(a.current != null ? $"{a.current}" : "")}{(a.current != null && a.goal != null ? "-" : "")}{(a.goal != null ? $"{a.goal}" : "")}").ToList())}";
        }

        internal static ((long grid_dur, long train_dur, long predict_dur) dur, ((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double? cv_rate) grid, string[] predict_text) inner_cross_validation(init_dataset_args ida, init_dataset_ret idr, outer_cv_args oca, (int rcvi, int ocvi, string training_fn, string grid_fn, string model_fn, string testing_fn, string predict_fn, string cm_fn, List<string> training_text, List<string> testing_text, List<(int class_id, int training_size)> training_sizes, List<(int class_id, int testing_size)> testing_sizes) input)
        {
            var module_name = nameof(program);
            var method_name = nameof(inner_cross_validation);

            var libsvm_train_probability_estimates = true;

            var log = false;

            var train_stdout_filename = "";
            var train_stderr_filename = "";

            var predict_stdout_filename = "";
            var predict_stderr_filename = "";

            ((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double? cv_rate) train_grid_search_result = ((null, null, null, null, null), null);

            var sw_grid = new Stopwatch();

            // perform inner-cv
            if (oca.inner_cv_folds > 1)
            {
                sw_grid.Start();
                if (!string.IsNullOrWhiteSpace(input.grid_fn))
                {
                    var train_grid_stdout_file = "";
                    var train_grid_stderr_file = "";

                    train_grid_search_result = grid.grid_parameter_search(outer_cv_args.libsvm_train_runtime, input.grid_fn, input.training_fn, train_grid_stdout_file, train_grid_stderr_file, oca.class_weights, oca.svm_type, oca.svm_kernel, ida.randomisation_cv_folds, input.rcvi, ida.outer_cv_folds, input.ocvi, oca.inner_cv_folds, libsvm_train_probability_estimates);
                }

                sw_grid.Stop();

            }

            var sw_grid_dur = sw_grid.ElapsedMilliseconds;


            // train
            var sw_train = new Stopwatch();
            sw_train.Start();
            var train_result = libsvm.train(outer_cv_args.libsvm_train_runtime, input.training_fn, input.model_fn, train_stdout_filename, train_stderr_filename, train_grid_search_result.point.cost, train_grid_search_result.point.gamma, train_grid_search_result.point.epsilon, train_grid_search_result.point.coef0, train_grid_search_result.point.degree, null, oca.svm_type, oca.svm_kernel, null, probability_estimates: libsvm_train_probability_estimates);
            sw_train.Stop();
            var sw_train_dur = sw_train.ElapsedMilliseconds;

            if (log && !string.IsNullOrWhiteSpace(train_result.cmd_line)) io_proxy.WriteLine(train_result.cmd_line, module_name, method_name);
            if (log && !string.IsNullOrWhiteSpace(train_result.stdout)) train_result.stdout.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(train_result)}.{nameof(train_result.stdout)}: {a}", module_name, method_name));
            if (log && !string.IsNullOrWhiteSpace(train_result.stderr)) train_result.stderr.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(train_result)}.{nameof(train_result.stderr)}: {a}", module_name, method_name));


            // predict
            var sw_predict = new Stopwatch();
            sw_predict.Start();
            var predict_result = libsvm.predict(outer_cv_args.libsvm_predict_runtime, input.testing_fn, input.model_fn, input.predict_fn, libsvm_train_probability_estimates, predict_stdout_filename, predict_stderr_filename);

            sw_predict.Stop();
            var sw_predict_dur = sw_train.ElapsedMilliseconds;

            if (log && !string.IsNullOrWhiteSpace(predict_result.cmd_line)) io_proxy.WriteLine(predict_result.cmd_line, module_name, method_name);
            if (log && !string.IsNullOrWhiteSpace(predict_result.stdout)) predict_result.stdout.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(predict_result)}.{nameof(predict_result.stdout)}: {a}", module_name, method_name));
            if (log && !string.IsNullOrWhiteSpace(predict_result.stderr)) predict_result.stderr.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(predict_result)}.{nameof(predict_result.stderr)}: {a}", module_name, method_name));

            var predict_text = io_proxy.ReadAllLines(input.predict_fn);
            //io_proxy.WriteLine($@"Loaded {input.predict_fn}");

            return ((sw_grid_dur, sw_train_dur, sw_predict_dur), train_grid_search_result, predict_text);
        }



        private static void test_group_kernel_scaling_perf(int instance_id, int total_instances, string experiment_name, int total_groups, List<int> selected_groups, List<int> selected_columns, init_dataset_args ida, init_dataset_ret idr)
        {
            var module_name = nameof(program);
            var method_name = nameof(test_group_kernel_scaling_perf);

            var iteration_index = -1;
            var group_index = -1;
            var make_outer_cv_confusion_matrices = false;


            var iteration_folder = get_iteration_folder(outer_cv_args.results_root_folder, experiment_name, iteration_index);
            //var scores = new List<(int iteration_index, int group_index, int class_id, direction dir, double score1, double score2, double score3)>();

            var iteration_all_group_cm_filename = Path.Combine(iteration_folder, $@"x_{get_filename(null, iteration_index, null, total_groups, null, null, null, null, ida.randomisation_cv_folds, null, ida.outer_cv_folds, null, null, null)}.cm.csv");
            var iteration_instance_group_cm_filename = Path.Combine(iteration_folder, $@"x_{get_filename(null, iteration_index, null, total_groups, null, null, null, null, ida.randomisation_cv_folds, null, ida.outer_cv_folds, null, instance_id, instance_id)}.cm.csv");

            //var loaded = false;
            //var load_other_instances = true;

            var iteration_all_group_cm_list = new List<string>();
            var iteration_instance_all_groups_cm_lines = new List<string>();

            var ocv_iteration_all_group_cm_list = new List<string>();
            var ocv_iteration_instance_all_groups_cm_lines = new List<string>();

            var _iteration_all_group_cm_list = new List<string>();
            var _iteration_instance_all_groups_cm_lines = new List<string>();

            var _ocv_iteration_all_group_cm_list = new List<string>();
            var _ocv_iteration_instance_all_groups_cm_lines = new List<string>();




            var kernel_types = ((routines.libsvm_kernel_type[])Enum.GetValues(typeof(routines.libsvm_kernel_type))).Where(a => a != routines.libsvm_kernel_type.precomputed).ToList();
            var scale_functions = ((routines.scale_function[])Enum.GetValues(typeof(routines.scale_function))).ToList();

            io_proxy.WriteLine($@"{experiment_name}: Starting kernel and scale ranking...", module_name, method_name);


            // alter 5 variables: (a) kernel, (b) scale function, (c) num folds, (d) class weights, (e) class prediction thresholds

            var tasks = new List<Task<(int iteration_index, int group_index, List<string> iteration_all_group_cm_list, List<string> iteration_instance_all_groups_cm_lines, List<string> ocv_iteration_all_group_cm_list, List<string> ocv_iteration_instance_all_groups_cm_lines)>>();

            var responsible_instance_id = -1;


            for (var _randomisation_cv_folds = 1; _randomisation_cv_folds <= 20; _randomisation_cv_folds++)
            {
                for (var _outer_cv_folds = 1; _outer_cv_folds <= 20; _outer_cv_folds++)
                {
                    var class_folds = idr.class_sizes.Select(a => (class_id: a.class_id, size: a.class_size, folds: routines.folds(a.class_size, _outer_cv_folds, _randomisation_cv_folds))).ToList();

                    var downsampled_training_class_folds = class_folds.Select(a => (class_id: a.class_id, size: a.size, folds: a.folds.Select(b =>
                            {
                                var min_num_items_in_fold = class_folds.Min(c => c.folds.Where(e => e.randomisation_cv_index == b.randomisation_cv_index && e.outer_cv_index == b.outer_cv_index).Min(e => e.indexes.Count));

                                return (randomisation_cv_index: b.randomisation_cv_index, outer_cv_index: b.outer_cv_index, indexes: b.indexes.Take(min_num_items_in_fold).ToList());
                            })
                            .ToList()))
                        .ToList();

                    for (var _kernel_index = 0; _kernel_index < kernel_types.Count; _kernel_index++)
                    {
                        for (var _scale_function_index = 0; _scale_function_index < scale_functions.Count; _scale_function_index++)
                        {
                            for (var _inner_cv_folds = 1; _inner_cv_folds <= 20; _inner_cv_folds++)
                            {
                                for (var _weight_p = 0.0m; _weight_p <= 2.0m; _weight_p = _weight_p += 0.2m)
                                {
                                    for (var _weight_n = 0.0m; _weight_n <= 2.0m; _weight_n = _weight_n += 0.2m)
                                    {
                                        var randomisation_cv_folds = _randomisation_cv_folds;
                                        var outer_cv_folds = _outer_cv_folds;

                                        responsible_instance_id++;
                                        if (responsible_instance_id > total_instances - 1) responsible_instance_id = 0;
                                        if (responsible_instance_id != instance_id) continue;

                                        var oca = new outer_cv_args();
                                        oca.total_groups = -1;
                                        oca.scale_function = scale_functions[_scale_function_index];
                                        oca.svm_kernel = kernel_types[_kernel_index];
                                        oca.output_threshold_adjustment_performance = true;
                                        oca.inner_cv_folds = _inner_cv_folds;
                                        oca.class_weights = new List<(int class_id, double class_weight)>() { (+1, (double)_weight_p), (-1, (double)_weight_n) };

                                        var task = Task.Run(() =>
                                        {
                                            var note = $@"iteration {(iteration_index + 1)} group {(group_index + 1)} K({(int)oca.svm_kernel}: {oca.svm_kernel}) S({(int)oca.scale_function}: {oca.scale_function}) R({randomisation_cv_folds}) O({outer_cv_folds}) I({oca.inner_cv_folds}) W({string.Join(", ", oca.class_weights)})";

                                            io_proxy.WriteLine($@"{experiment_name}: Starting: {note}");


                                            var group_folder = get_iteration_folder(outer_cv_args.results_root_folder, experiment_name, iteration_index, group_index);
                                            var group_key = idr.groups[group_index].key;

                                            //var group_cm_filename =     Path.Combine(group_folder, $@"x_{get_filename(oca.class_weights, oca.iteration_index, oca.group_index, oca.total_groups, (int)oca.svm_type, (int)oca.svm_kernel, (int)oca.scale_function, null, ida.randomisation_cv_folds, null, ida.outer_cv_folds, oca.inner_cv_folds, start, end)}");

                                            var group_merged_filename = Path.Combine(group_folder, $@"m_{get_filename(oca.class_weights, iteration_index, group_index, oca.total_groups, (int)oca.svm_type, (int)oca.svm_kernel, (int)oca.scale_function, null, randomisation_cv_folds, null, outer_cv_folds, oca.inner_cv_folds, null, null)}");
                                            //var group_merged_train_fn = $@"{group_merged_filename}.train.libsvm";
                                            //var group_merged_grid_fn = $@"{group_merged_filename}.grid.libsvm";
                                            //var group_merged_testing_fn = $@"{group_merged_filename}.test.libsvm";
                                            //var group_merged_predict_fn = $@"{group_merged_filename}.predict.libsvm";
                                            var group_merged_cm_fn = $@"{group_merged_filename}.cm.csv";

                                            var iteration_all_group_cm_list = new List<string>();
                                            var iteration_instance_all_groups_cm_lines = new List<string>();

                                            var ocv_iteration_all_group_cm_list = new List<string>();
                                            var ocv_iteration_instance_all_groups_cm_lines = new List<string>();

                                            if (io_proxy.is_file_available(group_merged_cm_fn))
                                            {
                                                var lcs = load_cm_scores(group_merged_cm_fn, oca);
                                                //scores.AddRange(lcs.scores);

                                                iteration_instance_all_groups_cm_lines.AddRange(iteration_instance_all_groups_cm_lines.Count == 0 ? lcs.lines : lcs.lines.Skip(1));
                                                iteration_all_group_cm_list.AddRange(iteration_all_group_cm_list.Count == 0 ? lcs.lines : lcs.lines.Skip(1));

                                                io_proxy.WriteLine($@"{experiment_name}: Index cache: Loaded for {note}. File: {group_merged_cm_fn}.");

                                            }
                                            else
                                            {
                                                io_proxy.WriteLine($@"{experiment_name}: Index cache: Unavailable for {note}. File: {group_merged_cm_fn}.");


                                                var exp_data = new List<(string key, string value)>()
                                                {
                                                        ($@"{nameof(iteration_index)}", $@"{iteration_index}"),
                                                        ($@"{nameof(group_index)}", $@"{group_index}"),
                                                        ($@"{nameof(group_key.alphabet)}", group_key.alphabet),
                                                        ($@"{nameof(group_key.dimension)}", group_key.dimension),
                                                        ($@"{nameof(group_key.category)}", group_key.category),
                                                        ($@"{nameof(group_key.source)}", group_key.source),
                                                        ($@"{nameof(group_key.group)}", group_key.group),
                                                        ($@"{nameof(group_key.member)}", group_key.member),
                                                        ($@"{nameof(group_key.perspective)}", group_key.perspective),
                                                        ($@"{nameof(selected_groups)}", $@"{string.Join(";", selected_groups)}"),
                                                        ($@"{nameof(selected_columns)}", $@"{string.Join(";", selected_columns)}"),
                                                };

                                                var cm_header = $"{string.Join(",", exp_data.Select(a => a.key).ToList())},{string.Join(",", perf.confusion_matrix.csv_header)}";
                                                if (iteration_instance_all_groups_cm_lines.Count == 0) { iteration_instance_all_groups_cm_lines.Add(cm_header); }

                                                if (iteration_all_group_cm_list.Count == 0) iteration_all_group_cm_list.Add(cm_header);

                                                // 1. make outer-cv files
                                                var outer_cv_inputs = make_outer_cv_inputs(selected_columns, ida, idr, oca, group_folder, iteration_index, group_index);
                                                var merged_cv_input = outer_cv_inputs.First(a => a.ocvi == -1);

                                                // 2. run libsvm
                                                var prediction_data_list = new List<((long grid_dur, long train_dur, long predict_dur) dur, ((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double? cv_rate) grid, string[] predict_text)>();



                                                foreach (var outer_cv_input in outer_cv_inputs)
                                                {
                                                    if (outer_cv_input.ocvi == -1 || outer_cv_input.rcvi == -1) continue; // -1 is the index for the merged text

                                                    var prediction_data = inner_cross_validation(ida, idr, oca, outer_cv_input);

                                                    prediction_data_list.Add(prediction_data);

                                                    if (make_outer_cv_confusion_matrices)
                                                    {
                                                        var ocv_prediction_file_data = perf.load_prediction_file(outer_cv_input.testing_text, null, prediction_data.predict_text, oca.output_threshold_adjustment_performance);

                                                        foreach (var ocv_cm in ocv_prediction_file_data.cm_list)
                                                        {
                                                            ocv_cm.x_experiment_name = experiment_name;
                                                            ocv_cm.x_scaling_function = oca.scale_function.ToString();
                                                            ocv_cm.x_old_feature_count = selected_columns.Count;
                                                            ocv_cm.x_new_feature_count = selected_columns.Count;
                                                            ocv_cm.x_old_group_count = selected_groups.Count;
                                                            ocv_cm.x_new_group_count = selected_groups.Count;
                                                            ocv_cm.x_inner_cv_folds = oca.inner_cv_folds;
                                                            ocv_cm.x_randomisation_cv_index = outer_cv_input.rcvi;
                                                            ocv_cm.x_randomisation_cv_folds = randomisation_cv_folds;
                                                            ocv_cm.x_outer_cv_index = outer_cv_input.ocvi;
                                                            ocv_cm.x_outer_cv_folds = outer_cv_folds;
                                                            ocv_cm.x_svm_type = oca.svm_type.ToString();
                                                            ocv_cm.x_svm_kernel = oca.svm_kernel.ToString();
                                                            ocv_cm.x_class_weight = oca.class_weights?.FirstOrDefault(b => ocv_cm.class_id == b.class_id).class_weight;
                                                            ocv_cm.x_class_name = ida.class_names?.FirstOrDefault(b => ocv_cm.class_id == b.class_id).class_name;
                                                            ocv_cm.x_class_size = idr.class_sizes?.First(b => b.class_id == ocv_cm.class_id).class_size ?? -1;
                                                            ocv_cm.x_class_training_size = outer_cv_input.training_sizes?.First(b => b.class_id == ocv_cm.class_id).training_size ?? -1;
                                                            ocv_cm.x_class_testing_size = outer_cv_input.testing_sizes?.First(b => b.class_id == ocv_cm.class_id).testing_size ?? -1;
                                                            ocv_cm.x_cost = prediction_data.grid.point.cost;
                                                            ocv_cm.x_gamma = prediction_data.grid.point.gamma;
                                                            ocv_cm.x_coef0 = prediction_data.grid.point.coef0;
                                                            ocv_cm.x_epsilon = prediction_data.grid.point.epsilon;
                                                            ocv_cm.x_degree = prediction_data.grid.point.degree;
                                                            ocv_cm.x_libsvm_cv = prediction_data.grid.cv_rate.GetValueOrDefault();
                                                            ocv_cm.x_duration_grid_search = prediction_data.dur.grid_dur.ToString(CultureInfo.InvariantCulture);
                                                            ocv_cm.x_duration_training = prediction_data.dur.train_dur.ToString(CultureInfo.InvariantCulture);
                                                            ocv_cm.x_duration_testing = prediction_data.dur.predict_dur.ToString(CultureInfo.InvariantCulture);
                                                            ocv_cm.calculate_ppf();
                                                        }

                                                        var ocv_cm_lines = new List<string>() { cm_header };

                                                        foreach (var cm in ocv_prediction_file_data.cm_list)
                                                        {
                                                            //var cm_score_increase_from_last = cm_winner_score - previous_winner_score;
                                                            //var cm_score_increase_from_all = cm_winner_score - all_time_highest_score;

                                                            //var cm_score_increase_from_last_pct = previous_winner_score != 0 ? cm_score_increase_from_last / previous_winner_score : 0;
                                                            //var cm_score_increase_from_all_pct = all_time_highest_score != 0 ? cm_score_increase_from_all / all_time_highest_score : 0;

                                                            //var cm_score_better_than_last = cm_score_increase_from_last > 0d;
                                                            //var cm_score_better_than_all = cm_score_increase_from_all > 0d;

                                                            var ocv_cm_line = $"{string.Join(",", exp_data.Select(c => c.value).ToList())},{cm}";

                                                            ocv_cm_lines.Add(ocv_cm_line);

                                                            ocv_iteration_instance_all_groups_cm_lines.Add(ocv_cm_line);

                                                            ocv_iteration_all_group_cm_list.Add(ocv_cm_line);
                                                        }

                                                        // save OCV CM for group
                                                        io_proxy.WriteAllLines(outer_cv_input.cm_fn, ocv_cm_lines);
                                                        io_proxy.WriteLine($@"{experiment_name}: Group OCV cache: Saved for {note}. File: {outer_cv_input.cm_fn}.");

                                                    }

                                                    io_proxy.Delete(outer_cv_input.training_fn);
                                                    io_proxy.Delete(outer_cv_input.grid_fn);
                                                    io_proxy.Delete(outer_cv_input.model_fn);
                                                    io_proxy.Delete(outer_cv_input.testing_fn);
                                                    io_proxy.Delete(outer_cv_input.predict_fn);
                                                    //io_proxy.Delete(outer_cv_input.cm_fn);
                                                }

                                                // 3. make confusion matrix from the merged prediction results
                                                var merged_prediction_text = prediction_data_list.SelectMany(a => a.predict_text).ToList();

                                                var prediction_file_data = perf.load_prediction_file(merged_cv_input.testing_text, null, merged_prediction_text, oca.output_threshold_adjustment_performance);

                                                foreach (var cm in prediction_file_data.cm_list)
                                                {
                                                    cm.x_experiment_name = experiment_name;
                                                    cm.x_scaling_function = oca.scale_function.ToString();
                                                    cm.x_old_feature_count = selected_columns.Count;
                                                    cm.x_new_feature_count = selected_columns.Count;
                                                    cm.x_old_group_count = selected_groups.Count;
                                                    cm.x_new_group_count = selected_groups.Count;
                                                    cm.x_inner_cv_folds = oca.inner_cv_folds;
                                                    cm.x_randomisation_cv_index = -1; //input.rcvi;
                                                    cm.x_randomisation_cv_folds = ida.randomisation_cv_folds;
                                                    cm.x_outer_cv_index = -1; //input.ocvi;
                                                    cm.x_outer_cv_folds = ida.outer_cv_folds;
                                                    cm.x_svm_type = oca.svm_type.ToString();
                                                    cm.x_svm_kernel = oca.svm_kernel.ToString();
                                                    cm.x_class_weight = oca.class_weights?.FirstOrDefault(b => cm.class_id == b.class_id).class_weight;
                                                    cm.x_class_name = ida.class_names?.FirstOrDefault(b => cm.class_id == b.class_id).class_name;
                                                    cm.x_class_size = idr.class_sizes?.First(b => b.class_id == cm.class_id).class_size ?? -1;
                                                    cm.x_class_training_size = merged_cv_input.training_sizes?.First(b => b.class_id == cm.class_id).training_size ?? -1;
                                                    cm.x_class_testing_size = merged_cv_input.testing_sizes?.First(b => b.class_id == cm.class_id).testing_size ?? -1;

                                                    cm.x_cost = prediction_data_list.Where(a => a.grid.point.cost != null).Select(a => a.grid.point.cost).DefaultIfEmpty(0).Average();
                                                    cm.x_gamma = prediction_data_list.Where(a => a.grid.point.gamma != null).Select(a => a.grid.point.gamma).DefaultIfEmpty(0).Average();
                                                    cm.x_coef0 = prediction_data_list.Where(a => a.grid.point.coef0 != null).Select(a => a.grid.point.coef0).DefaultIfEmpty(0).Average();
                                                    cm.x_epsilon = prediction_data_list.Where(a => a.grid.point.epsilon != null).Select(a => a.grid.point.epsilon).DefaultIfEmpty(0).Average();
                                                    cm.x_degree = prediction_data_list.Where(a => a.grid.point.degree != null).Select(a => a.grid.point.degree).DefaultIfEmpty(0).Average();
                                                    cm.x_libsvm_cv = prediction_data_list.Where(a => a.grid.cv_rate != null).Select(a => (double)a.grid.cv_rate).DefaultIfEmpty(0).Average();

                                                    cm.x_duration_grid_search = prediction_data_list.Select(a => a.dur.grid_dur).Sum().ToString(CultureInfo.InvariantCulture);
                                                    cm.x_duration_training = prediction_data_list.Select(a => a.dur.train_dur).Sum().ToString(CultureInfo.InvariantCulture);
                                                    cm.x_duration_testing = prediction_data_list.Select(a => a.dur.predict_dur).Sum().ToString(CultureInfo.InvariantCulture);

                                                    cm.calculate_ppf();
                                                }



                                                var merge_cm_lines = new List<string>() { cm_header };

                                                //var cm_score = prediction_file_data.cm_list.FirstOrDefault(a=>a.class_id==oca.scoring_class_id).get_value_by_name(oca.score1_metric);
                                                //var cm_score_increase_from_last = cm_score - previous_winner_score;
                                                //var cm_score_increase_from_all = cm_score - all_time_highest_score;
                                                //var cm_score_increase_from_last_pct = previous_winner_score != 0 ? cm_score_increase_from_last / previous_winner_score : 0;
                                                //var cm_score_increase_from_all_pct = all_time_highest_score != 0 ? cm_score_increase_from_all / all_time_highest_score : 0;
                                                //var cm_score_better_than_last = cm_score_increase_from_last > 0d;
                                                //var cm_score_better_than_all = cm_score_increase_from_all > 0d;

                                                //// todo: ppf value of new features.... score increase / num features added?

                                                //var cm_score_ppf = num_columns_added_from_last_iteration == 0 ? 0 : cm_score_increase_from_all / num_columns_added_from_last_iteration;

                                                //var ppf_header = $@"{nameof(cm_score_increase_from_last)},{nameof(cm_score_increase_from_all)},{nameof(cm_score_increase_from_last_pct)},{nameof(cm_score_increase_from_all_pct)},{nameof(cm_score_better_than_last)},{nameof(cm_score_better_than_all)}";
                                                //var ppf_data = $@"{cm_score_increase_from_last},{cm_score_increase_from_all},{cm_score_increase_from_last_pct},{cm_score_increase_from_all_pct},{cm_score_better_than_last},{cm_score_better_than_all}";

                                                foreach (var cm in prediction_file_data.cm_list)
                                                {
                                                    var cm_line = $"{string.Join(",", exp_data.Select(c => c.value).ToList())},{cm}";

                                                    merge_cm_lines.Add(cm_line);

                                                    iteration_instance_all_groups_cm_lines.Add(cm_line);

                                                    iteration_all_group_cm_list.Add(cm_line);

                                                    //if (cm.class_id == oca.scoring_class_id)
                                                    //{
                                                    //    scores.Add((iteration_index, group_index, (int)cm.class_id, dir, cm.get_value_by_name(oca.score1_metric), cm.get_value_by_name(oca.score2_metric), cm.get_value_by_name(oca.score3_metric)));
                                                    //}
                                                }

                                                // save CM for group
                                                io_proxy.WriteAllLines( /*merged_cv_input.cm_fn*/group_merged_cm_fn, merge_cm_lines);
                                                io_proxy.WriteLine($@"{experiment_name}: Group cache: Saved for {note}. File: {group_merged_cm_fn}.");
                                            }

                                            io_proxy.WriteLine($@"{experiment_name}: Finished: {note}.");

                                            return (iteration_index, group_index, iteration_all_group_cm_list, iteration_instance_all_groups_cm_lines, ocv_iteration_all_group_cm_list, ocv_iteration_instance_all_groups_cm_lines);
                                        });

                                        tasks.Add(task);
                                    }
                                }
                            }
                        }
                    }
                }

            }
            Task.WaitAll(tasks.ToArray<Task>());


            // 5. load results from other instances
            //
            if (instance_id == 0)
            {
                io_proxy.WriteLine($"{experiment_name}: Waiting for other instances to complete iteration {(iteration_index + 1)}. (Completed groups {(array_index_start + 1)} to {(array_index_last + 1)}). File: {iteration_instance_group_cm_filename}.");

                var instances_done = new List<string>() { iteration_instance_group_cm_filename, iteration_all_group_cm_filename };

                while (scores.Count < oca.total_groups)
                {
                    var dir_files = Directory.GetFiles(iteration_folder, "x_*.cm.csv", SearchOption.TopDirectoryOnly);

                    foreach (var file in dir_files)
                    {
                        if (instances_done.Contains(file)) continue;
                        instances_done.Add(file);

                        var lcs = load_cm_scores(file, oca);
                        scores.AddRange(lcs.scores);
                        _iteration_all_group_cm_list.AddRange(_iteration_all_group_cm_list.Count == 0 ? lcs.lines : lcs.lines.Skip(1));

                        io_proxy.WriteLine($"{experiment_name}: Part cache (instances): Loaded for iteration {(iteration_index + 1)} groups {(lcs.scores.Min(a => a.group_index) + 1)} to {(lcs.scores.Max(a => a.group_index) + 1)}. File: {file}.");
                    }

                    if (scores.Count < oca.total_groups) { Task.Delay(new TimeSpan(0, 0, 15)).Wait(); }
                }

                if (instance_id == 0)
                {
                    // save CM with results from all instances
                    io_proxy.WriteAllLines(iteration_all_group_cm_filename, _iteration_all_group_cm_list);
                    io_proxy.WriteLine($"{experiment_name}: Full cache: Saved for iteration {(iteration_index + 1)}. File: {iteration_all_group_cm_filename}.");

                }

                // todo: get indexes for all instances
                // todo: change which class/metrics are used.  e.g. highest_score_this_iteration = winner.cms.cm_list.Where(b => p.feature_selection_classes == null || p.feature_selection_classes.Count == 0 || p.feature_selection_classes.Contains(b.class_id.Value)).Average(b => b.get_perf_value_strings().Where(c => p.feature_selection_metrics.Any(d => string.Equals(c.name, d, StringComparison.InvariantCultureIgnoreCase))).Average(c => c.value));

            }



            // 5. find winner
            // ensure ordering will be consistent between instances
            scores = scores.OrderBy(a => a.group_index).ToList();
            scores = scores.OrderByDescending(a => a.score1).ThenByDescending(a => a.score2).ThenByDescending(a => a.score3).ToList();

            var scores_winner_index = 0;
            while (scores[scores_winner_index].group_index == previous_winner_group_index && scores_winner_index < oca.total_groups - 1) { scores_winner_index++; }

            var winner_group_index = scores[scores_winner_index].group_index;
            var winner_group = idr.groups[winner_group_index];
            var winner_group_key = winner_group.key;
            var winner_score = scores[scores_winner_index].score1;
            var winner_direction = scores[scores_winner_index].dir;



            var score_increase_from_last = winner_score - previous_winner_score;
            var score_increase_from_all = winner_score - all_time_highest_score;

            var score_increase_from_last_pct = previous_winner_score != 0 ? score_increase_from_last / previous_winner_score : 0;
            var score_increase_from_all_pct = all_time_highest_score != 0 ? score_increase_from_all / all_time_highest_score : 0;

            var score_better_than_last = score_increase_from_last > 0d;
            var score_better_than_all = score_increase_from_all > 0d;

            iterations_not_better_than_last = score_better_than_last ? 0 : iterations_not_better_than_last + 1;
            iterations_not_better_than_all = score_better_than_all ? 0 : iterations_not_better_than_all + 1;


            if (winner_direction == direction.forwards)
            {
                selected_groups.Add(winner_group_index);
                selected_columns.AddRange(idr.groups[winner_group_index].columns);
            }
            else if (winner_direction == direction.backwards)
            {
                selected_groups.Remove(winner_group_index);
                selected_columns = selected_columns.Except(idr.groups[winner_group_index].columns).ToList();
            }

            if (winner_direction != direction.neutral)
            {
                selected_groups = selected_groups.OrderBy(a => a).Distinct().ToList();
                selected_columns = selected_columns.OrderBy(a => a).Distinct().ToList();
            }


            if (score_better_than_all)
            {
                all_time_highest_score = winner_score;
                all_time_highest_score_iteration = iteration_index;
                all_time_highest_score_selected_groups = selected_groups.ToList();
                all_time_highest_score_selected_columns = selected_columns.ToList();
            }

            if (is_primary_instance)
            {
                // todo: check winners csv file has the group winners from original winning iterations (otherwise metrics will be overwritten)

                var winner_fn = Path.Combine(iteration_folder, $@"iteration_winner.csv");

                if (!io_proxy.is_file_available(winner_fn))
                {
                    io_proxy.WriteLine($@"{experiment_name}: Winner cache: Unavailable for iteration {(iteration_index + 1)}. File: {winner_fn}.");

                    //var winner_csv = new List<string>();


                    // todo: process some other indexes (random) whilst waiting...? with option to cancel if other instance finishes.

                    var header = _iteration_all_group_cm_list.First();
                    var header_split = header.Split(',').ToList();

                    var cm_iteration_index = header_split.FindIndex(a => a == nameof(iteration_index));
                    var cm_group_index = header_split.FindIndex(a => a == "group_index");

                    var cm_winner = _iteration_all_group_cm_list.Skip(1)
                        .Where(a =>
                        {

                            var s = a.Split(',');
                            return int.Parse(s[cm_group_index], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) == winner_group_index && int.Parse(s[cm_iteration_index], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) == iteration_index;
                        })
                        .ToList();

                    header = $"{nameof(all_time_highest_score)},{nameof(score_better_than_all)},{nameof(all_time_highest_score_iteration)},{nameof(all_time_highest_score_selected_groups)},{nameof(all_time_highest_score_selected_columns)},{header}";

                    cm_winner = cm_winner.Select(a => $"{all_time_highest_score:G17},{score_better_than_all},{all_time_highest_score_iteration},{string.Join(";", all_time_highest_score_selected_groups)},{string.Join(";", all_time_highest_score_selected_columns)},{a}").ToList();

                    if (all_iteration_winner_csv.Count == 0) { all_iteration_winner_csv.Add(header); }

                    all_iteration_winner_csv.AddRange(cm_winner);

                    io_proxy.WriteAllLines(winner_fn, all_iteration_winner_csv);
                    io_proxy.WriteLine($@"{experiment_name}: Winner cache: Saved for iteration {(iteration_index + 1)}. File: {winner_fn}.");

                }
            }
        }
    }
}
