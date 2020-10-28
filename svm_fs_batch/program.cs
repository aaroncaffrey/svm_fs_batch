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
        internal class program_args
        {
            internal int? folds = (int?)null;
            internal int repetitions = 1;
            internal int outer_cv_folds = 5;
            internal int outer_cv_folds_to_run = 0;
            internal int inner_folds = 5;
            internal bool setup = false;
            internal bool run_local = false;
            internal string experiment_name = "";
            internal string job_id = "";
            internal string job_name = "";

            internal int instance_array_index_start = -1; // start index for current instance
            internal int instance_array_index_end = -1; // end index for current instance 

            internal int array_instances = 0;
            internal int array_step = 0;
            internal int array_start = -1; // start index of whole array
            internal int array_end = -1; // end index of whole array

            internal int setup_total_vcpus = -1;
            internal int setup_instance_vcpus = -1;

            internal List<(string key, int int_value, string str_value)> args = new List<(string key, int int_value, string str_value)>();


            
            public program_args()
            {
                
            }

            public program_args(string[] args)
            {
                var arg_list = new string[]
                {
                    nameof(folds), nameof(repetitions), nameof(outer_cv_folds), nameof(outer_cv_folds_to_run), nameof(inner_folds), nameof(setup), nameof(run_local), nameof(experiment_name), nameof(job_id), nameof(job_name), 
                    nameof(instance_array_index_start), nameof(instance_array_index_end), nameof(array_instances), nameof(array_step), nameof(array_start), nameof(array_end), nameof(setup_total_vcpus), nameof(setup_instance_vcpus),
                };

                var args_given = args.Where(a => a.StartsWith('-')).Select(a => a.Substring(1)).ToList();
                
                if (args_given.Any(a=>!arg_list.Contains(a))) throw new Exception();

                if (args_given.Any(a=> args_given.Count(b=> a==b) > 1)) throw new Exception();

                //var arg_list_indexes = arg_list.Select(name => args.ToList().FindIndex(arg => string.Equals(arg, $"-{name}", StringComparison.InvariantCultureIgnoreCase))).ToList();
                var arg_list_indexes = arg_list.Select(name => (arg_name: name, arg_index: args.ToList().FindIndex(arg => string.Equals(arg, $"-{name}", StringComparison.InvariantCultureIgnoreCase)))).Where(a=> a.arg_index != -1 ).OrderBy(a=>a.arg_index).ToList();


                for (var i = 0; i < arg_list_indexes.Count; i++)
                {
                    var arg = arg_list_indexes[i];
                    
                    
                    var arg_given = arg.arg_index > -1;

                    var next_arg_index = arg_list_indexes.FindIndex(a => a.arg_index > arg.arg_index);
                    var value_str = arg_given ? string.Join(" ", args.Where((a, k) => k > arg.arg_index && (next_arg_index == -1 || k < arg_list_indexes[next_arg_index].arg_index)).ToList()) : "";
                    var value_given = !string.IsNullOrWhiteSpace(value_str);

                    if ((arg_given && !value_given) || (!arg_given && value_given)) throw new Exception();

                    if (arg_given && value_given)
                    {
                        var value_int = int.TryParse(value_str, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_value_int) ? out_value_int : -1;
                        //var value_double = double.TryParse(value_str, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var out_value_double) ? out_value_double : -1;
                        //var value_bool = bool.TryParse(value_str, out var out_value_bool) ? out_value_bool : false;

                        if (this.args.Any(a=>a.key== arg.arg_name)) throw new Exception();

                        this.args.Add((arg.arg_name, value_int, value_str));

                        switch (arg.arg_name)
                        {
                            case nameof(experiment_name):
                                experiment_name = value_str;
                                break;
                            case nameof(job_id):
                                job_id = value_str;
                                break;
                            case nameof(job_name):
                                job_name = value_str;
                                break;
                            case nameof(run_local):
                                run_local = value_int == 1;
                                break;
                            case nameof(setup):
                                setup = value_int == 1;
                                break;
                            case nameof(folds):
                                folds = value_int;
                                inner_folds = value_int;
                                outer_cv_folds = value_int;
                                outer_cv_folds_to_run = value_int;
                                repetitions = value_int;
                                break;
                            case nameof(repetitions):
                                repetitions = value_int;
                                break;
                            case nameof(outer_cv_folds):
                                outer_cv_folds = value_int;
                                break;
                            case nameof(outer_cv_folds_to_run):
                                outer_cv_folds_to_run = value_int;
                                break;
                            case nameof(inner_folds):
                                inner_folds = value_int;
                                break;
                            case nameof(instance_array_index_start):
                                instance_array_index_start = value_int;
                                break;
                            case nameof(instance_array_index_end):
                                instance_array_index_end = value_int;
                                break;
                            case nameof(array_instances):
                                array_instances = value_int;
                                break;
                            case nameof(array_step):
                                array_step = value_int;
                                break;
                            case nameof(array_start):
                                array_start = value_int;
                                break;
                            case nameof(array_end):
                                array_end = value_int;
                                break;
                            case nameof(setup_total_vcpus):
                                setup_total_vcpus = value_int;
                                break;
                            case nameof(setup_instance_vcpus):
                                setup_instance_vcpus = value_int;
                                break;
                            default: break;
                        }
                    }
                }
            }
        }

        internal static void Main(string[] args)
        {
            //var x=perf.confusion_matrix.load($@"C:\mmfs1\data\scratch\k1040015\svm_fs_batch\results\test\it_5\x_it-5_gr-5_sv-1_kr-3_sc-2_rn-1_oc-10_ic-10_ix-1-5.cm.csv");
            
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
                var idr = init_dataset_ret.init_dataset(group_related_columns: true);
                var setup_array_instances = idr.groups.Count;

                // calculate total number of instanced and array step
                var setup_total_instances = (int)Math.Floor((double)program_args.setup_total_vcpus / (double)program_args.setup_instance_vcpus);
                var setup_array_step = (int)Math.Ceiling((double)setup_array_instances / (double)setup_total_instances);

                var ps = make_pbs_script(program_args, program_args.setup_instance_vcpus, true, 0, setup_array_instances - 1, setup_array_step, true);

                Console.WriteLine(ps.run_line);

                return;
            }

            if (program_args.setup)
            {
                program_args.experiment_name += $@"_{DateTime.Now:yyyyMMddHHmmssfff}";

                // set default vcpu amounts if not specified
                if (program_args.setup_total_vcpus <= 0) program_args.setup_total_vcpus = 1504 - 320;
                if (program_args.setup_instance_vcpus <= 0) program_args.setup_instance_vcpus = 64;

                // calculate array size (get total number of groups)
                var idr = init_dataset_ret.init_dataset(group_related_columns: true);
                var setup_array_instances = idr.groups.Count;

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
                    io_proxy.WriteLine((index ).ToString().PadLeft(3) + ": " + ps.pbs_script_lines[index]);
                }

                io_proxy.WriteLine("");
                return;
            }



            if (string.IsNullOrWhiteSpace(program_args.experiment_name)) { throw new ArgumentOutOfRangeException(nameof(program_args.experiment_name), "must specify experiment name"); }

            if (program_args.array_start <= -1) { throw new ArgumentOutOfRangeException(nameof(program_args.array_start)); }
            if (program_args.array_end <= -1) { throw new ArgumentOutOfRangeException(nameof(program_args.array_end)); }
            if (program_args.instance_array_index_start <= -1) { throw new ArgumentOutOfRangeException(nameof(program_args.instance_array_index_start)); }
            if (program_args.array_step <= 0) { throw new ArgumentOutOfRangeException(nameof(program_args.array_step)); }
            if (program_args.array_instances <= 0) { throw new ArgumentOutOfRangeException(nameof(program_args.array_instances)); }
            if (program_args.instance_array_index_end <= -1) { program_args.instance_array_index_end = program_args.instance_array_index_start + (program_args.array_step - 1); }

            var instance_id = -1;
            for (var i = program_args.array_start; i <= program_args.instance_array_index_start; i += program_args.array_step) { instance_id++; } // is this correct?

            worker(program_args.experiment_name, instance_id, program_args.array_instances, program_args.instance_array_index_start, program_args.array_step, program_args.instance_array_index_end, program_args.repetitions, program_args.outer_cv_folds, program_args.outer_cv_folds_to_run, program_args.inner_folds);
        }

        internal static (List<string> pbs_script_lines, string run_line) make_pbs_script(program_args program_args, int pbs_ppn = 1, bool is_job_array = false, int job_array_start = 0, int job_array_end = 0, int job_array_step = 1, bool rerunnable = true)
        {

            //var module_name = nameof(program);
            //var method_name = nameof(make_pbs_script); 

            var pbs_script_lines = new List<string>();

            var program_runtime = Process.GetCurrentProcess().MainModule?.FileName;

            if (string.IsNullOrWhiteSpace(program_runtime)) throw new Exception();

            var is_win = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

            

            var env_pbs_array =  is_win ? @"_" : @"%J_%I";
            var env_jobid =      is_win ? (program_args.args.Any(a => a.key == nameof(program_args.job_id)) ? $"{program_args.job_id}" : "_") : @"${PBS_JOBID}${MOAB_JOBID}";
            var env_jobname =    is_win ? (program_args.args.Any(a => a.key == nameof(program_args.job_name)) ? $"{program_args.job_name}" : "_") : @"${PBS_JOBNAME}${MOAB_JOBNAME}";
            var env_arrayindex = is_win ? (program_args.args.Any(a => a.key == nameof(program_args.instance_array_index_start)) ? $"{program_args.instance_array_index_start}" : "0") : @"${PBS_ARRAYID}${MOAB_JOBARRAYINDEX}";
            var env_arraycount = is_win ? (program_args.args.Any(a => a.key == nameof(program_args.array_instances)) ? $"{program_args.array_instances}" : "1") : @"${MOAB_JOBARRAYRANGE}";
            

            var pbs_args = new program_args();
            


            TimeSpan pbs_walltime = TimeSpan.FromHours(240);
            string pbs_execution_directory = $@"{init_dataset_ret.svm_fs_batch_home}{Path.DirectorySeparatorChar}pbs{Path.DirectorySeparatorChar}{program_args.experiment_name}{Path.DirectorySeparatorChar}";
            string pbs_jobname = $@"{program_args.experiment_name}_{nameof(svm_fs_batch)}";
            string pbs_mail_addr = "";
            string pbs_mail_opt = "n";
            string pbs_mem = null;
            
            // for each job, request 1 node with 64 vcpu, by default, if ppn not specified
            int pbs_nodes = 1;
            if (pbs_ppn <= 0) pbs_ppn = 64;
            
            string pbs_stdout_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), (is_job_array ? env_pbs_array : "") }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.pbs.stdout";
            string pbs_stderr_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), (is_job_array ? env_pbs_array : "") }.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.pbs.stderr";
            
            string program_stdout_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), env_jobid, env_jobname, env_arrayindex}.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.program.stdout";
            string program_stderr_filename = $@"{string.Join("_", new string[] { program_args.experiment_name, nameof(svm_fs_batch), env_jobid, env_jobname, env_arrayindex}.Where(a => !string.IsNullOrWhiteSpace(a)).ToList())}.program.stderr";




            // pbs directives
            if (is_job_array) pbs_script_lines.Add($@"#PBS -t {job_array_start}-{job_array_end}:{job_array_step}");
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

            var run_line = $@"{program_runtime} {string.Join(" ", p.Select(a => string.Join(" ", new[] {a.key, a.value}.Where(c => !string.IsNullOrWhiteSpace(c)).ToList())).Where(b=>!string.IsNullOrWhiteSpace(b)).ToList())}";

            if (!string.IsNullOrWhiteSpace(pbs_execution_directory)) pbs_script_lines.Add($@"cd {pbs_execution_directory}");
            pbs_script_lines.Add($@"module load GCCcore");
            pbs_script_lines.Add(run_line);

            var pbs_fn = Path.Combine(pbs_execution_directory, $@"{pbs_jobname}.pbs");

            io_proxy.WriteAllLines(pbs_fn, pbs_script_lines);
            io_proxy.WriteLine($@"{program_args.experiment_name}: Saved PBS script. File: {pbs_fn}.");

            return (pbs_script_lines, run_line);
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
        //        if (cm.cm.class_id == unrolled_index.scoring_class_id)
        //        {

        //            var cm_iteration_index = int.Parse(cm.key_value_list.FirstOrDefault(a => a.key == "iteration_index").value, NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
        //            var cm_group_index = int.Parse(cm.key_value_list.FirstOrDefault(a => a.key == "group_index").value, NumberStyles.Integer, NumberFormatInfo.InvariantInfo);


        //            var cm_score1 = double.Parse(cm.key_value_list.FirstOrDefault(a => a.key == unrolled_index.score1_metric).value, NumberStyles.Float, NumberFormatInfo.InvariantInfo);
        //            var cm_score2 = double.Parse(cm.key_value_list.FirstOrDefault(a => a.key == unrolled_index.score2_metric).value, NumberStyles.Float, NumberFormatInfo.InvariantInfo);
        //            var cm_score3 = double.Parse(cm.key_value_list.FirstOrDefault(a => a.key == unrolled_index.score3_metric).value, NumberStyles.Float, NumberFormatInfo.InvariantInfo);

        //            var cm_dir = (direction)Enum.Parse(typeof(direction), cm.key_value_list.FirstOrDefault(a => a.key == "dir").value, true);

        //            scores.Add((cm_iteration_index, cm_group_index, cm.cm.class_id, cm_dir, cm_score1, cm_score2, cm_score3));
        //        }

        //    }

        //    return scores;
        //}

        //internal static List<(int iteration_index, int group_index, int id)> get_indexes(int iteration, int total_groups, int total_ids)
        //{
        //    // get list of indexes which are required to be processed

        //    var range_groups = Enumerable.Range(0, total_groups).ToList();
        //    var range_ids = Enumerable.Range(0, total_ids).ToList();

        //    var indexes = range_groups.SelectMany(group_index => range_ids.Select(id => (iteration, group_index, id)).ToList()).ToList();

        //    return indexes;
        //}


        //internal static (
        //    List<(int iteration_index, int group_index, int id)> indexes,
        //    List<(int iteration_index, int group_index, int id)> indexes_loaded,
        //    List<(int iteration_index, int group_index, int id)> indexes_missing_whole,
        //    List<(int iteration_index, int group_index, int id)> indexes_missing_partition
        //    )
        //    update_missing
        //    (
        //        int iteration,
        //        List<(int iteration_index, int group_index, int id)> indexes,
        //        List<(string line, perf.confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)> iteration_cm_all,
        //        int group_index_start,
        //        int group_index_last
        //    )
        //{
        //    // update list of which indexes are not yet done

        //    var indexes_loaded =
        //        iteration_cm_all.Where(a => a != default && a.cm != default && a.cm.x_iteration_index == iteration)
        //            .Select(a => (iteration_index: (int)a.cm.x_iteration_index, group_index: (int)a.cm.x_group_index, id: (int)a.cm.x_id)).Distinct().ToList();

        //    var indexes_missing_whole = indexes.Except(indexes_loaded).ToList();

        //    var indexes_missing_partition = indexes_missing_whole.Where(a =>
        //        (a.group_index >= group_index_start && a.group_index <= group_index_last)
        //    //|| (a.id >= id_index_start && a.id <= id_index_last)
        //        ).ToList();


        //    return (indexes, indexes_loaded, indexes_missing_whole, indexes_missing_partition);
        //}

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


        internal static void worker(string experiment_name, int instance_index, int total_instances, int array_index_start, int array_step, int array_index_last, int repetitions, int outer_cv_folds, int outer_cv_folds_to_run, int inner_folds)
        {

            var module_name = nameof(program);
            var method_name = nameof(worker);

            //var is_primary_instance = array_index_start == 0;

            var limit_iteration_not_better_than_all = 10;
            var limit_iteration_not_better_than_last = 5;

            var make_outer_cv_confusion_matrices = false;


            var caller_idr = init_dataset_ret.init_dataset();

            //caller_idr.groups = caller_idr.groups.Take(5).ToList();


            var total_groups = caller_idr.groups.Count;

            io_proxy.WriteLine($@"{experiment_name}: Groups: {(array_index_start )} to {(array_index_last )}. Total groups: {total_groups}. (This instance #{(instance_index )}/#{total_instances} indexes: [{array_index_start}..{array_index_last}]. All indexes: [0..{(total_groups - 1)}].)", module_name, method_name);



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


            var winners_cm = new List<perf.confusion_matrix_data>();

            var cache_files_loaded = new List<string>();

            var test_final_best_bias = true;
            var deep_search_index = -1;

            while (!finished || test_final_best_bias)
            {
                // get list of work to do this iteration
                (List<index_data> indexes_whole, List<index_data> indexes_partition) unrolled_indexes = default;

                if (!finished)
                {
                    unrolled_indexes = cache_load.get_unrolled_indexes_basic(caller_idr, iteration_index, total_groups, instance_index, total_instances, repetitions, outer_cv_folds, outer_cv_folds_to_run, inner_folds);
                    //(string experiment_name, int instance_index, int total_instances, int array_index_start, int array_step, int array_index_last, int repetitions, int outer_cv_folds, int outer_cv_folds_to_run, int inner_folds)
                }
                else if (test_final_best_bias)
                {
                    // todo: fix problem: 5 groups still being processed, rather than 1...

                    selected_groups = all_time_highest_score_selected_groups.ToList();
                    selected_columns = all_time_highest_score_selected_columns.ToList();

                    deep_search_index++;

                    io_proxy.WriteLine($"{experiment_name}: iteration: {iteration_index}: feature selection finished; statistical tests on best set of features, bias test index: {deep_search_index}.");

                    unrolled_indexes = cache_load.get_unrolled_indexes_check_bias(deep_search_index, caller_idr, iteration_index, total_groups, instance_index, total_instances);

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
                var iteration_folder = get_iteration_folder(init_dataset_ret.results_root_folder, experiment_name, iteration_index);
                var iteration_whole_cm_filename = Path.Combine(iteration_folder, $@"z_{get_iteration_filename(unrolled_indexes.indexes_whole)}.cm.csv");
                var iteration_partition_cm_filename = Path.Combine(iteration_folder, $@"x_{get_iteration_filename(unrolled_indexes.indexes_partition)}.cm.csv");

                // iteration_all_cm is a list of all merged results (i.e. the individual outer-cross-validation partitions merged)
                var iteration_all_cm = new List<perf.confusion_matrix_data>();

                // load cache (first try whole iteration, then try partition, then try individual work items)
                var unrolled_indexes_state = cache_load.load_cache(instance_index, iteration_index, experiment_name, false, cache_files_loaded, iteration_all_cm, unrolled_indexes.indexes_whole, unrolled_indexes.indexes_partition);

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
                        var p_selected_groups = selected_groups.ToList();
                        var p_selected_columns = selected_columns.ToList();
                        var p_previous_winner_group_index = previous_winner_group_index;
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
                                io_proxy.WriteLine($@"{experiment_name}: Start parallel index: {unrolled_index_data.id_index_str()} {unrolled_index_data.id_fold_str()} {unrolled_index_data.id_ml_str()} (groups {(array_index_start )} to {(array_index_last )}).");

                                //{nameof(unrolled_index.idr)}:{unrolled_index.idr}

                                var group_key = unrolled_index_data.group_index > -1 ? unrolled_index_data.idr.groups[unrolled_index_data.group_index].key : default;


                                // hide outside scope iteration_all_cm
                                //var p_iteration_all_cm = new List<perf.confusion_matrix_data>();

                                io_proxy.WriteLine($@"{experiment_name}: Group cache: Unavailable for iteration {(unrolled_index_data.iteration_index )} group {(unrolled_index_data.group_index )}/{total_groups} (groups {(array_index_start )} to {(array_index_last )}).");// File: {group_merged_cm_fn}.");

                                var test_selected_groups = p_selected_groups.ToList();
                                var test_selected_columns = p_selected_columns.ToList();

                                var is_group_selected = test_selected_groups.Contains(unrolled_index_data.group_index);
                                var is_only_selection = is_group_selected && test_selected_groups.Count == 1;
                                var is_last_winner = unrolled_index_data.group_index == p_previous_winner_group_index;

                                // if selected, remove
                                // if not selected, add
                                // if only group, no action
                                // if just added, no action

                                var dir = direction.neutral;

                                if (unrolled_index_data.group_index > -1)
                                {
                                    if (is_group_selected)
                                    {
                                        if (!is_only_selection && !is_last_winner)
                                        {
                                            dir = direction.backwards;
                                            test_selected_groups.Remove(unrolled_index_data.group_index);
                                            test_selected_columns = test_selected_columns.Except(unrolled_index_data.idr.groups[unrolled_index_data.group_index].columns).ToList();
                                        }
                                    }
                                    else
                                    {
                                        dir = direction.forwards;
                                        test_selected_groups.Add(unrolled_index_data.group_index);
                                        test_selected_columns = test_selected_columns.Union(unrolled_index_data.idr.groups[unrolled_index_data.group_index].columns).OrderBy(a => a).ToList();
                                    }

                                    if (dir != direction.neutral)
                                    {
                                        // ensure lists are consistent between instances
                                        test_selected_groups = test_selected_groups.OrderBy(a => a).Distinct().ToList();
                                        test_selected_columns = test_selected_columns.OrderBy(a => a).Distinct().ToList();
                                    }
                                }

                                remove_duplicate_columns(unrolled_index_data.idr, test_selected_columns);

                                var num_columns_added_from_last_iteration = test_selected_columns.Count - p_previous_selected_columns.Count;
                                var num_groups_added_from_last_iteration = test_selected_groups.Count - p_previous_selected_groups.Count;
                                var num_columns_added_from_highest_score_iteration = test_selected_columns.Count - p_all_time_highest_score_selected_columns.Count;
                                var num_groups_added_from_highest_score_iteration = test_selected_groups.Count - p_all_time_highest_score_selected_groups.Count;

                                var exp_data = new List<(string key, string value)>() { ($@"{nameof(is_group_selected)}", $@"{is_group_selected}"), ($@"{nameof(is_only_selection)}", $@"{is_only_selection}"), ($@"{nameof(is_last_winner)}", $@"{is_last_winner}"), ($@"{nameof(num_columns_added_from_last_iteration)}", $@"{num_columns_added_from_last_iteration}"), ($@"{nameof(num_groups_added_from_last_iteration)}", $@"{num_groups_added_from_last_iteration}"), ($@"{nameof(num_columns_added_from_highest_score_iteration)}", $@"{num_columns_added_from_highest_score_iteration}"), ($@"{nameof(num_groups_added_from_highest_score_iteration)}", $@"{num_groups_added_from_highest_score_iteration}"), ($@"{nameof(dir)}", $@"{dir}"), ($@"{nameof(previous_selected_groups)}", $@"{string.Join(";", previous_selected_groups)}"), ($@"{nameof(previous_selected_columns)}", $@"{string.Join(";", previous_selected_columns)}"), ($@"{nameof(selected_groups)}", $@"{string.Join(";", selected_groups)}"), ($@"{nameof(selected_columns)}", $@"{string.Join(";", selected_columns)}"), ($@"{nameof(test_selected_groups)}", $@"{string.Join(";", test_selected_groups)}"), ($@"{nameof(test_selected_columns)}", $@"{string.Join(";", test_selected_columns)}"), };

                                var group_folder = get_iteration_folder(init_dataset_ret.results_root_folder, experiment_name, unrolled_index_data.iteration_index, unrolled_index_data.group_index);




                                var ocv_result = outer_cross_validation(experiment_name, exp_data, test_selected_columns, unrolled_index_data, group_folder, make_outer_cv_confusion_matrices, group_key, p_previous_selected_columns, p_previous_selected_groups, test_selected_groups);


                                //if (lcs != null && lcs.Count > 1) { p_iteration_all_cm.AddRange(lcs.Skip(p_iteration_all_cm.Count == 0 ? 0 : 1)); }

                                //total_whole_indexes total_partition_indexes total_instances
                                io_proxy.WriteLine($@"{experiment_name}: Finished parallel index: {unrolled_index_data.id_index_str()} {unrolled_index_data.id_fold_str()} {unrolled_index_data.id_ml_str()} (groups {(array_index_start )} to {(array_index_last )}).");

                                return ocv_result.mcv_cm;
                                //return p_iteration_all_cm;
                            }

                            )
                            .ToList();


                        //iteration_all_cm.AddRange(tasks.SelectMany((a, i) => a.Result.iteration_all_cm.Skip(i == 0 && iteration_all_cm.Count == 0 ? 0 : 1)));
                        //var iteration_instance_all_groups_cm_lines = iteration_all_cm.Where((a, i) => i == 0 || (a.cm.x_group_index >= array_index_start && a.cm.x_group_index <= array_index_last));
                        //var merge_cache_indexes_loaded2 = merge_cache2.Where(a => a != default).SelectMany(a => a.Where(b => b != default && b.cm != default && b.cm.x_iteration_index != null && b.cm.x_group_index != null).Select(b => ((int)b.cm.x_iteration_index, (int)b.cm.x_group_index)).ToList()).ToList();

                        iteration_all_cm.AddRange(merge_cache2.Where(a => a != default).SelectMany((a, i) => a.Skip(i == 0 && iteration_all_cm.Count == 0 ? 0 : 1)));


                        // all partition should be loaded by this point, check
                        unrolled_indexes_state = cache_load.get_missing(instance_index, iteration_all_cm, unrolled_indexes.indexes_whole);
                        if (unrolled_indexes_state.indexes_missing_partition.Any()) throw new Exception();


                        // 4. save CM for all groups of this instance (from index start to index end) merged outer-cv results
                        //
                        var iteration_instance_all_groups_cm_lines = iteration_all_cm.Where((a, i) => i == 0 || (a.cm.x_iteration_index == iteration_index && a.cm.x_group_index >= array_index_start && a.cm.x_group_index <= array_index_last));

                        io_proxy.WriteAllLines(iteration_partition_cm_filename, iteration_instance_all_groups_cm_lines.Select(a => a.line));
                        io_proxy.WriteLine($"{experiment_name}: Part cache: Saved for iteration {(iteration_index )} group {(array_index_start )} to {(array_index_last )}. File: {iteration_partition_cm_filename}.");
                    }

                    // 5. load results from other instances
                    //
                    unrolled_indexes_state = cache_load.load_cache(instance_index, iteration_index, experiment_name, true, cache_files_loaded, iteration_all_cm, unrolled_indexes.indexes_whole, unrolled_indexes.indexes_partition);

                    if (unrolled_indexes_state.indexes_missing_whole.Any()) throw new Exception();

                    if (instance_index == 0)
                    {
                        // save CM with results from all instances
                        io_proxy.WriteAllLines(iteration_whole_cm_filename, iteration_all_cm.Select(a => a.line));
                        io_proxy.WriteLine($"{experiment_name}: Full cache: Saved for iteration {(iteration_index )}. File: {iteration_whole_cm_filename}.");
                    }

                    // todo: change which class/metrics are used.  e.g. highest_score_this_iteration = winner.cms.cm_list.Where(b => p.feature_selection_classes == null || p.feature_selection_classes.Count == 0 || p.feature_selection_classes.Contains(b.class_id.Value)).Average(b => b.get_perf_value_strings().Where(c => p.feature_selection_metrics.Any(d => string.Equals(c.name, d, StringComparison.InvariantCultureIgnoreCase))).Average(c => c.value));
                }



                // 5. find winner
                // ensure ordering will be consistent between instances

                var scoring_cm = iteration_all_cm.Skip(1).Where(a =>
                    a.cm.class_id == scoring_args.scoring_class_id &&
                    a.cm.x_prediction_threshold == null &&
                    a.cm.x_repetitions_index == -1 &&
                    a.cm.x_outer_cv_index == -1 &&
                    a.cm.x_iteration_index == iteration_index &&
                    a.cm.x_group_index != null).ToList();

                //scoring_cm = scoring_cm.OrderBy(a => a.unknown_key_value_list.First(b => b.key == "iteration_index").value_int).ToList();
                scoring_cm = scoring_cm.OrderBy(a => a.cm.x_group_index).ToList();

                var scoring_cm_query_tree = new[] { scoring_cm.OrderByDescending(a => a.cm.get_value_by_name(scoring_args.scoring_metrics[0])) }.ToList();

                for (var si = 1; si < scoring_args.scoring_metrics.Length; si++)
                {
                    var six = si;
                    scoring_cm_query_tree.Add(scoring_cm_query_tree.Last().ThenByDescending(a => a.cm.get_value_by_name(scoring_args.scoring_metrics[six])));
                }

                //scoring_cm = scoring_cm.OrderByDescending(a => a.cm.get_value_by_name(scoring_args.score1_metric)).ThenByDescending(a => a.cm.get_value_by_name(scoring_args.score2_metric)).ThenByDescending(a => a.cm.get_value_by_name(scoring_args.score3_metric)).ToList();
                scoring_cm = scoring_cm_query_tree.Last().ToList();


                var scores_winner_index = 0;
                while (scoring_cm[scores_winner_index].cm.x_group_index == previous_winner_group_index && scores_winner_index < total_groups - 1) { scores_winner_index++; }

                var winner_group_index = (int)(scoring_cm[scores_winner_index].cm.x_group_index ?? -1);
                var winner_group = winner_group_index > -1 ? caller_idr.groups[winner_group_index] : default;
                var winner_group_key = winner_group.key;
                var winner_score = scoring_cm[scores_winner_index].cm.get_value_by_name(scoring_args.scoring_metrics[0]);
                var winner_direction = (direction)Enum.Parse(typeof(direction), scoring_cm[scores_winner_index].unknown_key_value_list.FirstOrDefault(a => a.key == "dir").value_str, true);


                var score_increase_from_last = winner_score - previous_winner_score;
                var score_increase_from_all = winner_score - all_time_highest_score;

                var score_increase_from_last_pct = previous_winner_score != 0 ? score_increase_from_last / previous_winner_score : 0;
                var score_increase_from_all_pct = all_time_highest_score != 0 ? score_increase_from_all / all_time_highest_score : 0;

                var score_better_than_last = score_increase_from_last > 0d;
                var score_better_than_all = score_increase_from_all > 0d;


                if (!finished)
                {


                    iterations_not_better_than_last = score_better_than_last ? 0 : iterations_not_better_than_last + 1;
                    iterations_not_better_than_all = score_better_than_all ? 0 : iterations_not_better_than_all + 1;

                    if (winner_group_index > -1)
                    {
                        if (winner_direction == direction.forwards)
                        {
                            selected_groups.Add(winner_group_index);
                            selected_columns.AddRange(caller_idr.groups[winner_group_index].columns);
                        }
                        else if (winner_direction == direction.backwards)
                        {
                            selected_groups.Remove(winner_group_index);
                            selected_columns = selected_columns.Except(caller_idr.groups[winner_group_index].columns).ToList();
                        }

                        if (winner_direction != direction.neutral)
                        {
                            selected_groups = selected_groups.OrderBy(a => a).Distinct().ToList();
                            selected_columns = selected_columns.OrderBy(a => a).Distinct().ToList();
                        }
                    }

                    if (score_better_than_all)
                    {
                        all_time_highest_score = winner_score;
                        all_time_highest_score_iteration = iteration_index;
                        all_time_highest_score_selected_groups = selected_groups.ToList();
                        all_time_highest_score_selected_columns = selected_columns.ToList();
                    }


                    finished = !((score_better_than_last || (iterations_not_better_than_last < limit_iteration_not_better_than_last && iterations_not_better_than_all < limit_iteration_not_better_than_all)) && (selected_groups.Count < total_groups));

                    previous_winner_score = winner_score;
                    previous_winner_group_index = winner_group_index;
                    previous_selected_groups = selected_groups.ToList();
                    previous_selected_columns = selected_columns.ToList();
                }

                if (instance_index == 0)
                {


                    var winner_fn = Path.Combine(iteration_folder, $@"iteration_winner.csv");

                    if (io_proxy.is_file_available(winner_fn))
                    {
                        io_proxy.WriteLine($@"{experiment_name}: Winner cache: Already saved for iteration {(iteration_index )}. File: {winner_fn}.");
                    }
                    else
                    {

                        io_proxy.WriteLine($@"{experiment_name}: Winner cache: Unavailable for iteration {(iteration_index )}. File: {winner_fn}.");


                        var cm_winner = iteration_all_cm.Where((a, i) => i == 0 || a.cm.x_iteration_index == iteration_index && a.cm.x_group_index == winner_group_index).Select((a, i) =>
                        {
                            if (i == 0) return $"{nameof(all_time_highest_score)},{nameof(score_better_than_all)},{nameof(all_time_highest_score_iteration)},{nameof(all_time_highest_score_selected_groups)},{nameof(all_time_highest_score_selected_columns)},{a.line}";
                            return $"{all_time_highest_score:G17},{score_better_than_all},{all_time_highest_score_iteration},{string.Join(";", all_time_highest_score_selected_groups)},{string.Join(";", all_time_highest_score_selected_columns)},{a.line}";
                        }).ToList();

                        winners_cm.AddRange(perf.confusion_matrix.load(cm_winner).Skip(winners_cm.Count == 0 ? 0 : 1));


                        io_proxy.WriteAllLines(winner_fn, winners_cm.Select(a => a.line));
                        io_proxy.WriteLine($@"{experiment_name}: Winner cache: Saved for iteration {(iteration_index )}. File: {winner_fn}.");

                    }
                }


                io_proxy.WriteLine($"{experiment_name}: Finished: iteration {(iteration_index )} for {(array_index_start )} to {(array_index_last )}.  {(finished ? "Finished" : "Not finished")}.");

                iteration_index++;
            }

            //test_group_kernel_scaling_perf(instance_id, total_instances, experiment_name, unrolled_index.total_groups, selected_groups.ToList(), selected_columns.ToList(), ida, idr, oca);

            io_proxy.WriteLine($"{experiment_name}: Finished: all iterations for groups {(array_index_start )} to {(array_index_last )}.");
        }

        private static (List<String> ocv_cm_lines, List<perf.confusion_matrix_data> ocv_cm, List<String> mcv_cm_lines, List<perf.confusion_matrix_data> mcv_cm) outer_cross_validation
        (
            string experiment_name,
            List<(string key, string value)> exp_data, 
            List<int> test_selected_columns,
            index_data unrolled_index_data,
            string group_folder,
            bool make_outer_cv_confusion_matrices,
            (string alphabet, string dimension, string category, string source, string @group, string member, string perspective) group_key,
            List<int> p_previous_selected_columns,
            List<int> p_previous_selected_groups,
            List<int> test_selected_groups
        )
        {

            var ocv_cm = new List<perf.confusion_matrix_data>();
            var ocv_cm_lines = new List<string>();
            //var mcv_cm = new List<perf.confusion_matrix_data>();


            var cm_header = $"{string.Join(",", exp_data.Select(a => a.key).ToList())},{string.Join(",", perf.confusion_matrix.csv_header)}";


            // 1. make outer-cv files
            var outer_cv_inputs = make_outer_cv_inputs(test_selected_columns, unrolled_index_data.idr, group_folder, unrolled_index_data);

            // 1a. the ocvi index -1 is merged data
            var merged_cv_input = outer_cv_inputs.First(a => a.ocvi == -1);

            // 2. run libsvm
            var prediction_data_list = new List<((long grid_dur, long train_dur, long predict_dur) dur, ((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double? cv_rate) grid, string[] predict_text)>();

            foreach (var outer_cv_input in outer_cv_inputs)
            {
                if (outer_cv_input.ocvi == -1 || outer_cv_input.rcvi == -1) continue; // -1 is the index for the merged text

                // call libsvm... returns predictions from doing: parameter search -> train -> predict
                var prediction_data = inner_cross_validation(unrolled_index_data, outer_cv_input);

                // add results from libsvm to list
                prediction_data_list.Add(prediction_data);

                // optional: make_outer_cv_confusion_matrices: this will output the individual outer-cross-validation confusion matrices (i.e. if outer-cv-folds = 5, then 5 respective confusion-matrices will be created, as well as the merged data confusion-matrix).
                if (make_outer_cv_confusion_matrices)
                {
                    // convert text results to confusion matrix and performance metrics
                    var ocv_prediction_file_data = perf.load_prediction_file(outer_cv_input.testing_text, null, prediction_data.predict_text, unrolled_index_data.calc_11p_thresholds);

                    // add any missing meta details to the confusion-matrix
                    update_ocv_cm(experiment_name, ocv_prediction_file_data, unrolled_index_data, group_key, p_previous_selected_columns, test_selected_columns, p_previous_selected_groups, test_selected_groups, outer_cv_input, prediction_data);

                    var local_ocv_cm_lines = new List<string>() { cm_header };
                    local_ocv_cm_lines.AddRange(ocv_prediction_file_data.cm_list.Select(a => $"{string.Join(",", exp_data.Select(c => c.value).ToList())},{a}").ToList());
                    if (local_ocv_cm_lines != null && local_ocv_cm_lines.Count > 0) { ocv_cm_lines.AddRange(local_ocv_cm_lines.Skip(ocv_cm_lines.Count == 0 ? 0 : 1)); }

                    var local_ocv_cm = perf.confusion_matrix.load(local_ocv_cm_lines);
                    if (local_ocv_cm != null && local_ocv_cm.Count > 1) { ocv_cm.AddRange(local_ocv_cm.Skip(ocv_cm.Count == 0 ? 0 : 1)); }

                    // save outer-cross-validation confusion-matrix CM for group
                    io_proxy.WriteAllLines(outer_cv_input.cm_fn, local_ocv_cm_lines);
                    io_proxy.WriteLine($@"{experiment_name}: Group OCV cache: Saved: [R({outer_cv_input.rcvi}/{unrolled_index_data.repetitions}) O({outer_cv_input.ocvi}/{unrolled_index_data.outer_cv_folds})] {unrolled_index_data.id_index_str()} {unrolled_index_data.id_fold_str()} {unrolled_index_data.id_ml_str()}. File: {outer_cv_input.cm_fn}.");
                }

                // delete temporary files
                io_proxy.Delete(outer_cv_input.training_fn);
                io_proxy.Delete(outer_cv_input.grid_fn);
                io_proxy.Delete(outer_cv_input.model_fn);
                io_proxy.Delete(outer_cv_input.testing_fn);
                io_proxy.Delete(outer_cv_input.predict_fn);
                // do not delete the confusion-matrix: io_proxy.Delete(outer_cv_input.cm_fn);
            }

            // 3. make confusion matrix from the merged prediction results
            var merged_prediction_text = prediction_data_list.SelectMany(a => a.predict_text).ToList();

            var prediction_file_data = perf.load_prediction_file(merged_cv_input.testing_text, null, merged_prediction_text, unrolled_index_data.calc_11p_thresholds);
            // add any missing details to the confusion-matrix
            update_merged_cm(experiment_name, prediction_file_data, unrolled_index_data, group_key, p_previous_selected_columns, test_selected_columns, p_previous_selected_groups, test_selected_groups, merged_cv_input, prediction_data_list);


            var mcv_cm_lines = new List<string>() { cm_header };
            mcv_cm_lines.AddRange(prediction_file_data.cm_list.Select(a => $"{string.Join(",", exp_data.Select(c => c.value).ToList())},{a}").ToList());


            // save CM for group
            io_proxy.WriteAllLines(merged_cv_input.cm_fn, mcv_cm_lines);
            io_proxy.WriteLine($@"{experiment_name}: Group MCV cache: Saved: {unrolled_index_data.id_index_str()} {unrolled_index_data.id_fold_str()} {unrolled_index_data.id_ml_str()}. File: {merged_cv_input.cm_fn}.");

            var mcv_cm = perf.confusion_matrix.load(mcv_cm_lines, -1);
            


            // todo: set mcv_cm
            return (ocv_cm_lines, ocv_cm, mcv_cm_lines, mcv_cm);
        }

        private static void update_merged_cm
        (
            string experiment_name,
            (List<perf.prediction> prediction_list, List<perf.confusion_matrix> cm_list) prediction_file_data,
            index_data unrolled_index,
            (string alphabet, string dimension, string category, string source, string @group, string member, string perspective) group_key,
            List<int> previous_selected_columns,
            List<int> test_selected_columns,
            List<int> previous_selected_groups,
            List<int> test_selected_groups,
            (int rcvi, int ocvi, string training_fn, string grid_fn, string model_fn, string testing_fn, string predict_fn, string cm_fn, List<string> training_text, List<string> testing_text, List<(int class_id, int training_size)> training_sizes, List<(int class_id, int testing_size)> testing_sizes) merged_cv_input,
            List<((long grid_dur, long train_dur, long predict_dur) dur, ((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double? cv_rate) grid, string[] predict_text)> prediction_data_list
        )
        {
            foreach (var cm in prediction_file_data.cm_list)
            {
                cm.x_class_name = init_dataset_ret.class_names?.FirstOrDefault(b => cm.class_id == b.class_id).class_name;
                cm.x_class_size = unrolled_index.idr.class_sizes?.First(b => b.class_id == cm.class_id).class_size ?? -1;
                cm.x_class_testing_size = merged_cv_input.testing_sizes?.First(b => b.class_id == cm.class_id).testing_size ?? -1;
                cm.x_class_training_size = merged_cv_input.training_sizes?.First(b => b.class_id == cm.class_id).training_size ?? -1;
                cm.x_class_weight = unrolled_index.class_weights?.FirstOrDefault(b => cm.class_id == b.class_id).class_weight;
                cm.x_coef0 = prediction_data_list.Where(a => a.grid.point.coef0 != null).Select(a => a.grid.point.coef0).DefaultIfEmpty(0).Average();
                cm.x_cost = prediction_data_list.Where(a => a.grid.point.cost != null).Select(a => a.grid.point.cost).DefaultIfEmpty(0).Average();
                cm.x_degree = prediction_data_list.Where(a => a.grid.point.degree != null).Select(a => a.grid.point.degree).DefaultIfEmpty(0).Average();
                cm.x_duration_grid_search = prediction_data_list.Select(a => a.dur.grid_dur).Sum().ToString(CultureInfo.InvariantCulture);
                cm.x_duration_testing = prediction_data_list.Select(a => a.dur.predict_dur).Sum().ToString(CultureInfo.InvariantCulture);
                cm.x_duration_training = prediction_data_list.Select(a => a.dur.train_dur).Sum().ToString(CultureInfo.InvariantCulture);
                cm.x_epsilon = prediction_data_list.Where(a => a.grid.point.epsilon != null).Select(a => a.grid.point.epsilon).DefaultIfEmpty(0).Average();
                cm.x_experiment_name = experiment_name;
                cm.x_gamma = prediction_data_list.Where(a => a.grid.point.gamma != null).Select(a => a.grid.point.gamma).DefaultIfEmpty(0).Average();
                cm.x_group_index = unrolled_index.group_index;
                cm.x_inner_cv_folds = unrolled_index.inner_cv_folds;
                cm.x_iteration_index = unrolled_index.iteration_index;
                cm.x_key_alphabet = group_key.alphabet;
                cm.x_key_category = group_key.category;
                cm.x_key_dimension = group_key.dimension;
                cm.x_key_group = group_key.@group;
                cm.x_key_member = group_key.member;
                cm.x_key_perspective = group_key.perspective;
                cm.x_key_source = group_key.source;
                cm.x_libsvm_cv = prediction_data_list.Where(a => a.grid.cv_rate != null).Select(a => (double)a.grid.cv_rate).DefaultIfEmpty(0).Average();
                cm.x_new_feature_count = test_selected_columns.Count;
                cm.x_new_group_count = test_selected_groups.Count;
                cm.x_old_feature_count = previous_selected_columns.Count;
                cm.x_old_group_count = previous_selected_groups.Count;
                cm.x_outer_cv_folds = unrolled_index.outer_cv_folds;
                cm.x_outer_cv_folds_to_run = unrolled_index.outer_cv_folds_to_run;
                cm.x_outer_cv_index = -1; //input.ocvi;
                cm.x_calc_11p_thresholds = unrolled_index.calc_11p_thresholds;
                cm.x_repetitions_index = -1; //input.rcvi;
                cm.x_repetitions_total = unrolled_index.repetitions;
                cm.x_scale_function = unrolled_index.scale_function;
                cm.x_svm_kernel = unrolled_index.svm_kernel;
                cm.x_svm_type = unrolled_index.svm_type;
                cm.x_total_groups = unrolled_index.total_groups;

                cm.calculate_ppf();
            }
        }

        private static void update_ocv_cm
        (
            string experiment_name,
            (List<perf.prediction> prediction_list, List<perf.confusion_matrix> cm_list) prediction_file_data,
            index_data unrolled_index,
            (string alphabet, string dimension, string category, string source, string @group, string member, string perspective) group_key,
            List<int> previous_selected_columns,
            List<int> test_selected_columns,
            List<int> previous_selected_groups,
            List<int> test_selected_groups,
            (int rcvi, int ocvi, string training_fn, string grid_fn, string model_fn, string testing_fn, string predict_fn, string cm_fn, List<string> training_text, List<string> testing_text, List<(int class_id, int training_size)> training_sizes, List<(int class_id, int testing_size)> testing_sizes) outer_cv_input,
            ((long grid_dur, long train_dur, long predict_dur) dur, ((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double? cv_rate) grid, string[] predict_text) prediction_data
        )
        {
            foreach (var cm in prediction_file_data.cm_list)
            {
                cm.x_class_name = init_dataset_ret.class_names?.FirstOrDefault(b => cm.class_id == b.class_id).class_name;
                cm.x_class_size = unrolled_index.idr.class_sizes?.First(b => b.class_id == cm.class_id).class_size ?? -1;
                cm.x_class_testing_size = outer_cv_input.testing_sizes?.First(b => b.class_id == cm.class_id).testing_size ?? -1;
                cm.x_class_training_size = outer_cv_input.training_sizes?.First(b => b.class_id == cm.class_id).training_size ?? -1;
                cm.x_class_weight = unrolled_index.class_weights?.FirstOrDefault(b => cm.class_id == b.class_id).class_weight;
                cm.x_coef0 = prediction_data.grid.point.coef0;
                cm.x_cost = prediction_data.grid.point.cost;
                cm.x_degree = prediction_data.grid.point.degree;
                cm.x_duration_grid_search = prediction_data.dur.grid_dur.ToString(CultureInfo.InvariantCulture);
                cm.x_duration_testing = prediction_data.dur.predict_dur.ToString(CultureInfo.InvariantCulture);
                cm.x_duration_training = prediction_data.dur.train_dur.ToString(CultureInfo.InvariantCulture);
                cm.x_epsilon = prediction_data.grid.point.epsilon;
                cm.x_experiment_name = experiment_name;
                cm.x_gamma = prediction_data.grid.point.gamma;
                cm.x_group_index = unrolled_index.group_index;
                cm.x_inner_cv_folds = unrolled_index.inner_cv_folds;
                cm.x_iteration_index = unrolled_index.iteration_index;
                cm.x_key_alphabet = group_key.alphabet;
                cm.x_key_category = group_key.category;
                cm.x_key_dimension = group_key.dimension;
                cm.x_key_group = group_key.@group;
                cm.x_key_member = group_key.member;
                cm.x_key_perspective = group_key.perspective;
                cm.x_key_source = group_key.source;
                cm.x_libsvm_cv = prediction_data.grid.cv_rate.GetValueOrDefault();
                cm.x_new_feature_count = test_selected_columns.Count;
                cm.x_new_group_count = test_selected_groups.Count;
                cm.x_old_feature_count = previous_selected_columns.Count;
                cm.x_old_group_count = previous_selected_groups.Count;
                cm.x_outer_cv_folds = unrolled_index.outer_cv_folds;
                cm.x_outer_cv_folds_to_run = unrolled_index.outer_cv_folds_to_run;
                cm.x_outer_cv_index = outer_cv_input.ocvi;
                cm.x_calc_11p_thresholds = unrolled_index.calc_11p_thresholds;
                cm.x_repetitions_index = outer_cv_input.rcvi;
                cm.x_repetitions_total = unrolled_index.repetitions;
                cm.x_scale_function = unrolled_index.scale_function;
                cm.x_svm_kernel = unrolled_index.svm_kernel;
                cm.x_svm_type = unrolled_index.svm_type;
                cm.x_total_groups = unrolled_index.total_groups;

                cm.calculate_ppf();
            }
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




        internal static List<(int rcvi, int ocvi, string training_fn, string grid_fn, string model_fn, string testing_fn, string predict_fn, string cm_fn, List<string> training_text, List<string> testing_text, List<(int class_id, int training_size)> training_sizes, List<(int class_id, int testing_size)> testing_sizes)>

            make_outer_cv_inputs(List<int> column_indexes, init_dataset_ret idr, string group_folder, index_data unrolled_index)

        {
            column_indexes = column_indexes.ToList();


            var indexes = new List<(int r, int o)>();
            for (var _repetitions_cv_index = 0; _repetitions_cv_index < unrolled_index.repetitions; _repetitions_cv_index++)
            {
                for (var _outer_cv_index = 0; _outer_cv_index < (unrolled_index.outer_cv_folds_to_run == 0 ? unrolled_index.outer_cv_folds : unrolled_index.outer_cv_folds_to_run); _outer_cv_index++)
                {
                    indexes.Add((_repetitions_cv_index, _outer_cv_index));
                }
            }

            var ocv_data = indexes.AsParallel()
                .AsOrdered()
                .Select(a =>
                {
                    var repetitions_index = a.r;
                    var outer_cv_index = a.o;



                    var filename = Path.Combine(group_folder, $@"o_{get_item_filename(unrolled_index, repetitions_index, outer_cv_index)}");
                    var train_fn = $@"{filename}.train.libsvm";
                    var grid_fn = $@"{filename}.grid.libsvm";
                    var model_fn = $@"{filename}.model.libsvm";
                    var testing_fn = $@"{filename}.test.libsvm";
                    var predict_fn = $@"{filename}.predict.libsvm";
                    var cm_fn = $@"{filename}.cm.csv";


                    var training_fold_indexes = idr.downsampled_training_class_folds.Select(a => (a.class_id, outer_cv_index: outer_cv_index, indexes: a.folds.Where(b => b.repetitions_index == repetitions_index && b.outer_cv_index != outer_cv_index).SelectMany(b => b.indexes).OrderBy(b => b).ToList())).ToList();
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

                                    return (fid: c.fid, fv: routines.scale(c.fv, x.non_zero, x.abs_sum, x.srsos, x.min, x.max, x.average, x.stdev, unrolled_index.scale_function));
                                })
                                .ToList()))
                            .ToList()))
                        .ToList();
                    var training_sizes = training_example_columns_scaled.Select(a => (class_id: a.class_id, training_size: a.examples.Count)).ToList();
                    var training_text = training_example_columns_scaled.AsParallel().AsOrdered().SelectMany(a => a.examples.Select(b => $@"{a.class_id} {String.Join(" ", b.columns.Where(c => c.fv != 0).OrderBy(c => c.fid).Select(c => $"{c.fid}:{c.fv:G17}").ToList())}").ToList()).ToList();
                    //merged_training_text.AddRange(training_text);



                    var testing_fold_indexes = idr.class_folds.Select(a => (a.class_id, outer_cv_index: outer_cv_index, indexes: a.folds.Where(b => b.repetitions_index == repetitions_index && b.outer_cv_index == outer_cv_index).SelectMany(b => b.indexes).OrderBy(b => b).ToList())).ToList();
                    var testing_examples = testing_fold_indexes.Select(a => (a.class_id, examples: a.indexes.Select(row_index => idr.dataset_instance_list_grouped.First(b => a.class_id == b.class_id).examples[row_index]).ToList())).ToList();
                    var testing_example_columns = testing_examples.AsParallel().AsOrdered().Select(a => (a.class_id, examples: a.examples.Select(b => (example: b, columns: column_indexes.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList() /*, columns_hash: hash.calc_hash(string.Join(" ", query_cols.Select(c => b.feature_data[c].fid == c ? b.feature_data[c] : b.feature_data.First(d => d.fid == c)).ToList()))*/)).ToList())).ToList();
                    if (testing_example_columns.Any(a => a.examples == null || a.examples.Count == 0)) { throw new Exception(); }

                    var testing_example_columns_scaled = testing_example_columns.AsParallel()
                        .AsOrdered()
                        .Select(a => (a.class_id, examples: a.examples.Select(b => (b.example, columns: b.columns.Select(c =>
                                {
                                    var x = training_scaling_params.First(y => y.fid == c.fid);

                                    return (fid: c.fid, fv: routines.scale(c.fv, x.non_zero, x.abs_sum, x.srsos, x.min, x.max, x.average, x.stdev, unrolled_index.scale_function));
                                })
                                .ToList()))
                            .ToList()))
                        .ToList();
                    var testing_text = testing_example_columns_scaled.AsParallel().AsOrdered().SelectMany(a => a.examples.Select(b => $@"{a.class_id} {String.Join(" ", b.columns.Where(c => c.fv != 0).OrderBy(c => c.fid).Select(c => $"{c.fid}:{c.fv:G17}").ToList())}").ToList()).ToList();
                    var testing_sizes = testing_example_columns_scaled.Select(a => (class_id: a.class_id, testing_size: a.examples.Count)).ToList();
                    //merged_testing_text.AddRange(testing_text);


                    return (repetitions_index, outer_cv_index, train_fn, grid_fn, model_fn, testing_fn, predict_fn, cm_fn, training_text, testing_text, training_sizes, testing_sizes);
                })
                .ToList();

            Parallel.ForEach(ocv_data,
                item =>
                {
                    io_proxy.WriteAllLines(item.train_fn, item.training_text);
                    io_proxy.WriteAllLines(item.testing_fn, item.testing_text);
                });

            var merged_training_text = ocv_data.SelectMany(a => a.training_text).ToList();
            var merged_testing_text = ocv_data.SelectMany(a => a.testing_text).ToList();
            var merged_training_sizes = ocv_data.SelectMany(a => a.training_sizes).GroupBy(a => a.class_id).Select(b => (class_id: b.Key, training_size: b.Select(c => c.training_size).Sum())).ToList();
            var merged_testing_sizes = ocv_data.SelectMany(a => a.testing_sizes).GroupBy(a => a.class_id).Select(b => (class_id: b.Key, testing_size: b.Select(c => c.testing_size).Sum())).ToList();


            // filenames for merging all repetition indexes and outer cv indexes... as if it were a single test.
            var merged_filename_prefix = Path.Combine(group_folder, $@"m_{get_iteration_filename(new[] { unrolled_index })}");
            var merged_train_fn = $@"{merged_filename_prefix}.train.libsvm";
            var merged_grid_fn = $@"{merged_filename_prefix}.grid.libsvm";
            var merged_model_fn = $@"{merged_filename_prefix}.model.libsvm";
            var merged_testing_fn = $@"{merged_filename_prefix}.test.libsvm";
            var merged_predict_fn = $@"{merged_filename_prefix}.predict.libsvm";
            var merged_cm_fn = $@"{merged_filename_prefix}.cm.csv";

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

        internal static string get_item_filename(index_data unrolled_index, int repetition_index, int outer_cv_index)
        {
            return $@"{get_iteration_filename(new[] { unrolled_index })}_ri[{repetition_index}]_oi[{outer_cv_index}]";
        }

        internal static string get_iteration_filename(
            IList<index_data> indexes)
        {
            static string get_initials(string name)
            {
                var initials = string.Join("", name.Replace("_", " ").Split().Where(a => a.Length > 0).Select(a => a.First()).ToList());
                return initials.Length > 2 ? initials.Substring(0, 2) : initials;
            }

            var iteration_index = get_ranges_str(indexes.Select(a => a.iteration_index).ToList());
            var group_index = get_ranges_str(indexes.Select(a => a.group_index).ToList());
            var total_groups = get_ranges_str(indexes.Select(a => a.total_groups).ToList());
            var calc_11p_thresholds = get_ranges_str(indexes.Select(a => a.calc_11p_thresholds ? 1 : 0).ToList());
            var repetitions = get_ranges_str(indexes.Select(a => a.repetitions).ToList());
            var outer_cv_folds = get_ranges_str(indexes.Select(a => a.outer_cv_folds).ToList());
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

            var p = new List<(string name, string value)>();
            p.Add((get_initials(nameof(iteration_index)), iteration_index));//it
            p.Add((get_initials(nameof(group_index)), group_index));//gi
            p.Add((get_initials(nameof(total_groups)), total_groups));//tg
            p.Add((get_initials(nameof(calc_11p_thresholds)), calc_11p_thresholds));//ot
            p.Add((get_initials(nameof(repetitions)), repetitions));//r
            p.Add((get_initials(nameof(outer_cv_folds)), outer_cv_folds));//oc
            p.Add((get_initials(nameof(class_weights)), class_weights));//cw
            p.Add((get_initials(nameof(svm_type)), svm_type));//st
            p.Add((get_initials(nameof(svm_kernel)), svm_kernel));//sk
            p.Add((get_initials(nameof(scale_function)), scale_function));//sf
            p.Add((get_initials(nameof(inner_cv_folds)), inner_cv_folds));//ic


            var iter_fn = string.Join("_", p.Select(a => $@"{a.name}[{a.value ?? ""}]").ToList());

            return iter_fn;
        }

        //internal static

        //    (
        //    (List<(int class_id, double weight)> class_weights, int? iteration_index, int? group_index, int? total_groups, int? svm_type, int? svm_kernel, int? scale_function, int? repetitions_index, int? repetitions, int? outer_cv_index, int? outer_cv_folds, int? inner_cv_folds) index,
        //    string filename
        //    )

        //    get_filename(/*bool mask, */List<(int class_id, double weight)> class_weights, int? iteration_index, int? group_index, int? total_groups, int? svm_type, int? svm_kernel, int? scale_function, int? repetitions_index, int? repetitions, int? outer_cv_index, int? outer_cv_folds, int? inner_cv_folds)//, int? group_index_first = null, int? group_index_last = null)//, int? id_index_first = null, int? id_index_last = null, int? instance_index_first = null, int? instance_index_last = null)
        //{
        //    // todo: add class_weights, svm_type, repetition index, outer cv index,
        //    // todo: add weight to CM class

        //    //int iteration_index
        //    //int group_index
        //    //int total_groups
        //    //bool calc_11p_thresholds
        //    //int repetitions
        //    //int outer_cv_folds
        //    //routines.libsvm_kernel_type svm_kernel
        //    //routines.scale_function scale_function
        //    //int inner_cv_folds
        //    //init_dataset_ret idr

        //    var hr = true;

        //    var w = class_weights != null && class_weights.Count > 0 ? class_weights.OrderBy(a=>a.class_id).Select(a => (key: "wi" + a.class_id, current: (int?)(a.weight * 100), goal: (int?)null)).Where(a => a.current != null || a.goal != null).ToList() : null;

        //    var filename_parts = new List<(string key, int? current, int? goal)>()
        //    {
        //        ("it", iteration_index != null ? iteration_index + (hr ? 1 : 0) : null, null),
        //        ("gr", group_index != null ? group_index + (hr ? 1 : 0) : null, total_groups),
        //        ("sv", svm_type != null ? svm_type + (hr ? 1 : 0) : null, null),
        //        ("kr", svm_kernel != null ? svm_kernel + (hr ? 1 : 0) : null, null),
        //        ("sc", scale_function != null ? scale_function + (hr ? 1 : 0) : null, null),
        //        ("rp", repetitions_index != null ? repetitions_index + (hr ? 1 : 0) : null, repetitions),
        //        ("oc", outer_cv_index != null ? outer_cv_index + (hr ? 1 : 0) : null, outer_cv_folds),
        //        ("ic", null, inner_cv_folds),
        //        //("xg", group_index_first != null ? group_index_first + (hr ? 1 : 0) : null, group_index_last != null ? group_index_last + (hr ? 1 : 0) : null),
        //        //("xi", id_index_first != null ? id_index_first + (hr ? 1 : 0) : null, id_index_last != null ? id_index_last + (hr ? 1 : 0) : null),
        //        //("xn", instance_index_first != null ? instance_index_first + (hr ? 1 : 0) : null, instance_index_last != null ? instance_index_last + (hr ? 1 : 0) : null),

        //    }/*.Where(a => a.current != null || a.goal != null)*/.ToList();

        //    if (w != null && w.Count > 0) { filename_parts.AddRange(w); }

        //    var filename = $@"{string.Join("_", filename_parts.Select(a => $@"{a.key}[{(a.current != null ? $"{a.current}" : (/*mask ? "*" :*/ "-"))}_{(a.goal != null ? $"{a.goal}" : (/*mask ? "*" :*/ "-"))}]").ToList())}";

        //    var index = (class_weights, iteration_index, group_index, total_groups, svm_type, svm_kernel, scale_function, repetitions_index, repetitions, outer_cv_index, outer_cv_folds, inner_cv_folds);

        //    return (index, filename);
        //}

        internal static ((long grid_dur, long train_dur, long predict_dur) dur, ((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double? cv_rate) grid, string[] predict_text)
            inner_cross_validation(/*init_dataset_ret idr,*/

                index_data unrolled_index,

                (int rcvi, int ocvi, string training_fn, string grid_fn, string model_fn, string testing_fn, string predict_fn, string cm_fn, List<string> training_text, List<string> testing_text, List<(int class_id, int training_size)> training_sizes, List<(int class_id, int testing_size)> testing_sizes) input)
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
            if (unrolled_index.inner_cv_folds >= 2)
            {
                sw_grid.Start();
                if (!string.IsNullOrWhiteSpace(input.grid_fn))
                {
                    var train_grid_stdout_file = "";
                    var train_grid_stderr_file = "";

                    train_grid_search_result = grid.grid_parameter_search(init_dataset_ret.libsvm_train_runtime, input.grid_fn, input.training_fn, train_grid_stdout_file, train_grid_stderr_file, unrolled_index.class_weights, unrolled_index.svm_type, unrolled_index.svm_kernel, unrolled_index.repetitions, input.rcvi, unrolled_index.outer_cv_folds, input.ocvi, unrolled_index.inner_cv_folds, libsvm_train_probability_estimates);
                }

                sw_grid.Stop();

            }

            var sw_grid_dur = sw_grid.ElapsedMilliseconds;


            // train
            var sw_train = new Stopwatch();
            sw_train.Start();
            var train_result = libsvm.train(init_dataset_ret.libsvm_train_runtime, input.training_fn, input.model_fn, train_stdout_filename, train_stderr_filename, train_grid_search_result.point.cost, train_grid_search_result.point.gamma, train_grid_search_result.point.epsilon, train_grid_search_result.point.coef0, train_grid_search_result.point.degree, null, unrolled_index.svm_type, unrolled_index.svm_kernel, null, probability_estimates: libsvm_train_probability_estimates);
            sw_train.Stop();
            var sw_train_dur = sw_train.ElapsedMilliseconds;

            if (log && !string.IsNullOrWhiteSpace(train_result.cmd_line)) io_proxy.WriteLine(train_result.cmd_line, module_name, method_name);
            if (log && !string.IsNullOrWhiteSpace(train_result.stdout)) train_result.stdout.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(train_result)}.{nameof(train_result.stdout)}: {a}", module_name, method_name));
            if (log && !string.IsNullOrWhiteSpace(train_result.stderr)) train_result.stderr.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(train_result)}.{nameof(train_result.stderr)}: {a}", module_name, method_name));


            // predict
            var sw_predict = new Stopwatch();
            sw_predict.Start();
            var predict_result = libsvm.predict(init_dataset_ret.libsvm_predict_runtime, input.testing_fn, input.model_fn, input.predict_fn, libsvm_train_probability_estimates, predict_stdout_filename, predict_stderr_filename);

            sw_predict.Stop();
            var sw_predict_dur = sw_train.ElapsedMilliseconds;

            if (log && !string.IsNullOrWhiteSpace(predict_result.cmd_line)) io_proxy.WriteLine(predict_result.cmd_line, module_name, method_name);
            if (log && !string.IsNullOrWhiteSpace(predict_result.stdout)) predict_result.stdout.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(predict_result)}.{nameof(predict_result.stdout)}: {a}", module_name, method_name));
            if (log && !string.IsNullOrWhiteSpace(predict_result.stderr)) predict_result.stderr.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(a => io_proxy.WriteLine($@"{nameof(predict_result)}.{nameof(predict_result.stderr)}: {a}", module_name, method_name));

            var predict_text = io_proxy.ReadAllLines(input.predict_fn);
            //io_proxy.WriteLine($@"Loaded {input.predict_fn}");

            return ((sw_grid_dur, sw_train_dur, sw_predict_dur), train_grid_search_result, predict_text);
        }



        //private static void test_group_kernel_scaling_perf(int instance_id, int total_instances, string experiment_name, int total_groups1, List<int> selected_groups, List<int> selected_columns, init_dataset_ret caller_ida, init_dataset_ret caller_idr, outer_cv_args caller_oca)
        //{
        //    // perhaps: alter 5 variables: (a) svm_kernel, (b) scale_function function, (c) num folds (r / o / i), (d) class class_weights, (e) class prediction thresholds... which are actually interesting though?
        //    // orv, ocv and icv...
        //    // svm_kernel?  not really... scaling? ...maybe but not so much.
        //    // class_weights, yes but lots of extra work...
        //    // class prediction thresholds... zero extra ML work but lots more data (10x) to process


        //    var module_name = nameof(program);
        //    var method_name = nameof(test_group_kernel_scaling_perf);
        //    io_proxy.WriteLine($@"{experiment_name}: Starting svm_kernel and scale_function ranking...", module_name, method_name);


        //    var iteration_index = -1;
        //    var group_index = -1;
        //    var make_outer_cv_confusion_matrices = false;

        //    remove_duplicate_columns(caller_idr, selected_columns);

        //    var iteration_folder = get_iteration_folder(outer_cv_args.results_root_folder, experiment_name, iteration_index);
        //    //var scores = new List<(int iteration_index, int group_index, int class_id, direction dir, double score1, double score2, double score3)>();


        //    var iteration_whole_group_cm_filename = Path.Combine(iteration_folder, $@"z_{get_filename(null, iteration_index, null, total_groups1, null, null, null, null, -1, null, -1, null, null, null, null, null, null, null)}.cm.csv");
        //    var iteration_instance_group_cm_filename = Path.Combine(iteration_folder, $@"x_{get_filename(null, iteration_index, null, total_groups1, null, null, null, null, -1, null, -1, null, null, null, null, null, instance_id, instance_id)}.cm.csv");

        //    var cache_files_loaded = new List<string>();
        //    var iteration_all_cm = new List<(string line, perf.confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)>();


        //    var kernel_types = ((routines.libsvm_kernel_type[])Enum.GetValues(typeof(routines.libsvm_kernel_type))).Where(a => a != routines.libsvm_kernel_type.precomputed).ToList();
        //    var scale_functions = ((routines.scale_function[])Enum.GetValues(typeof(routines.scale_function))).ToList();

        //    // limit the scale_function functions and svm_kernel types to reduce compute load
        //    scale_functions = new List<routines.scale_function>() { routines.scale_function.rescale };
        //    kernel_types = new List<routines.libsvm_kernel_type>() { routines.libsvm_kernel_type.rbf };

        //    var _r_start = 1;
        //    var _r_end = 1; // 5
        //    var _r_step = 1;

        //    var _o_start = 2;
        //    var _o_end = 2; // 20
        //    var _o_step = 2;

        //    var _i_start = 1;
        //    var _i_end = 1; // 20
        //    var _i_step = 2;

        //    var unrolled_index = 0;
        //    var unrolled_instance_index = 0;

        //    var indexes = new List<(int unrolled_index, int unrolled_instance_index, int _repetitions_cv_folds, int _outer_cv_folds, int _kernel_index, int _scale_function_index, int inner_cv_folds, init_dataset_ret ida, init_dataset_ret idr, outer_cv_args oca)>();
        //    var instance_indexes = new List<(int unrolled_index, int unrolled_instance_index, int _repetitions_cv_folds, int _outer_cv_folds, int _kernel_index, int _scale_function_index, int inner_cv_folds, init_dataset_ret ida, init_dataset_ret idr, outer_cv_args oca)>();




        //    for (var _repetitions_cv_folds = _r_start; _repetitions_cv_folds <= _r_end; _repetitions_cv_folds += _r_step)
        //    {
        //        for (var _outer_cv_folds = _o_start; _outer_cv_folds <= _o_end; _outer_cv_folds += _o_step)
        //        {
        //            var ida = (init_dataset_ret)null;
        //            var idr = (init_dataset_ret)null;

        //            // loop through number of kernels
        //            for (var _kernel_index = 0; _kernel_index < kernel_types.Count; _kernel_index++)
        //            {
        //                // loop through number of scale_function functions
        //                for (var _scale_function_index = 0; _scale_function_index < scale_functions.Count; _scale_function_index++)
        //                {
        //                    // loop through number of inner folds
        //                    for (var inner_cv_folds = _i_start; inner_cv_folds <= _i_end; inner_cv_folds += _i_step)
        //                    {

        //                        //for (var _weight_p = 0.0m; _weight_p <= 2.0m; _weight_p = _weight_p += 0.2m)
        //                        //{
        //                        //for (var _weight_n = 0.0m; _weight_n <= 2.0m; _weight_n = _weight_n += 0.2m)
        //                        //{

        //                        if (unrolled_instance_index == instance_id && (ida == null || idr == null))
        //                        {
        //                            ida = new init_dataset_ret(caller_ida);
        //                            unrolled_index.outer_cv_folds = _outer_cv_folds;
        //                            unrolled_index.repetitions = _repetitions_cv_folds;

        //                            idr = new init_dataset_ret(caller_idr);
        //                            idr.class_folds = idr.class_sizes.Select(a => (class_id: a.class_id, size: a.class_size, folds: routines.folds(a.class_size, unrolled_index.outer_cv_folds, unrolled_index.repetitions))).ToList();
        //                            idr.downsampled_training_class_folds = idr.class_folds.Select(a => (class_id: a.class_id, size: a.size, folds: a.folds.Select(b =>
        //                                    {
        //                                        var min_num_items_in_fold = idr.class_folds.Min(c => c.folds.Where(e => e.repetitions_index == b.repetitions_index && e.outer_cv_index == b.outer_cv_index).Min(e => e.indexes.Count));
        //                                        return (repetitions_index: b.repetitions_index, outer_cv_index: b.outer_cv_index, indexes: b.indexes.Take(min_num_items_in_fold).ToList());
        //                                    })
        //                                    .ToList()))
        //                                .ToList();

        //                        }

        //                        var oca = (outer_cv_args)null;

        //                        if (unrolled_instance_index == instance_id)
        //                        {
        //                            oca = new outer_cv_args(caller_oca);

        //                            unrolled_index.total_groups = -1;
        //                            unrolled_index.scale_function = scale_functions[_scale_function_index];
        //                            unrolled_index.svm_kernel = kernel_types[_kernel_index];
        //                            unrolled_index.calc_11p_thresholds = true;
        //                            unrolled_index.inner_cv_folds = inner_cv_folds;
        //                            //unrolled_index.class_weights = new List<(int class_id, double class_weight)>() { (+1, (double)_weight_p), (-1, (double)_weight_n) };

        //                            instance_indexes.Add((unrolled_index, unrolled_instance_index, _repetitions_cv_folds, _outer_cv_folds, _kernel_index, _scale_function_index, inner_cv_folds, ida, idr, oca));

        //                        }

        //                        indexes.Add((unrolled_index, unrolled_instance_index, _repetitions_cv_folds, _outer_cv_folds, _kernel_index, _scale_function_index, inner_cv_folds, ida, idr, oca));

        //                        //var group_folder = get_iteration_folder(outer_cv_args.results_root_folder, experiment_name, unrolled_index.iteration_index, unrolled_index.group_index);
        //                        //var group_merged_filename_prefix = Path.Combine(group_folder, $@"m_{get_filename(unrolled_index.class_weights, unrolled_index.iteration_index, unrolled_index.group_index, unrolled_index.total_groups, (int)unrolled_index.svm_type, (int)unrolled_index.svm_kernel, (int)unrolled_index.scale_function, null, unrolled_index.repetitions, null, unrolled_index.outer_cv_folds, unrolled_index.inner_cv_folds)}");
        //                        //var group_merged_cm_fn = $@"{group_merged_filename_prefix}.cm.csv";

        //                        unrolled_index++;

        //                        unrolled_instance_index++;
        //                        if (unrolled_instance_index >= total_instances) unrolled_instance_index = 0;
        //                        //}
        //                        //}
        //                    }
        //                }
        //            }
        //        }
        //    }

        //    instance_indexes.GroupBy(a => (a.ida, a.idr, a.oca)).ToList();

        //    var lcr = load_cache(load_x: true, load_m: true, wait_for_cache: false, ida, oca, iteration_index, experiment_name, iteration_whole_group_cm_filename, iteration_instance_group_cm_filename, 1, total_indexes, cache_files_loaded, iteration_all_cm, ?, ?);

        //    if (lcr.indexes_missing.Count > 0)
        //    {

        //        var apr1 = instance_indexes /*.Where(a => cnt_iid == instance_id)*/
        //            .AsParallel()
        //            .AsOrdered()
        //            .Select(qq =>
        //            {
        //                var note = $@"Index: {qq.unrolled_index}/{indexes.Count}. Iteration: {(iteration_index )} group: {(group_index )}. K({(int)qq.unrolled_index.svm_kernel}: {qq.unrolled_index.svm_kernel}) S({(int)qq.unrolled_index.scale_function}: {qq.unrolled_index.scale_function}) R({qq.unrolled_index.repetitions}) O({qq.unrolled_index.outer_cv_folds}) I({qq.unrolled_index.inner_cv_folds}) W({string.Join(", ", qq.unrolled_index.class_weights ?? new List<(int class_id, double class_weight)>())})";

        //                io_proxy.WriteLine($@"{experiment_name}: Starting: {note}", module_name, method_name);

        //                var group_folder = get_iteration_folder(outer_cv_args.results_root_folder, experiment_name, iteration_index, group_index);
        //                var group_merged_filename_prefix = Path.Combine(group_folder, $@"m_{get_filename(qq.unrolled_index.class_weights, iteration_index, group_index, qq.unrolled_index.total_groups, (int)qq.unrolled_index.svm_type, (int)qq.unrolled_index.svm_kernel, (int)qq.unrolled_index.scale_function, null, qq.unrolled_index.repetitions, null, qq.unrolled_index.outer_cv_folds, qq.unrolled_index.inner_cv_folds, null, null)}");
        //                var group_merged_cm_fn = $@"{group_merged_filename_prefix}.cm.csv";

        //                var iteration_all_cm = new List<(string line, perf.confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)>();

        //                if (io_proxy.is_file_available(group_merged_cm_fn))
        //                {
        //                    var lcs = perf.confusion_matrix.load(group_merged_cm_fn);

        //                    io_proxy.WriteLine($@"{experiment_name}: Group cache: Loaded for {note}. File: {group_merged_cm_fn}.", module_name, method_name);
        //                }

        //                return iteration_all_cm;
        //            }).ToList();

        //        iteration_all_cm.AddRange(apr1.SelectMany((a, i) => a.Skip(i == 0 && iteration_all_cm.Count == 0 ? 0 : 1)).ToList());

        //        // todo: add x_index_ohter property attribute to confusion_matrix to track which indexes are already done here.

        //        var apr2 = instance_indexes.Where(a => iteration_all_cm.Any(b => b.cm.cnt_iid == instance_id))
        //            .AsParallel()
        //            .AsOrdered()
        //            .Select(qq =>
        //            {
        //                var note = $@"Index: {qq.unrolled_index}/{indexes.Count}. Iteration: {(iteration_index )} group: {(group_index )}. K({(int)qq.unrolled_index.svm_kernel}: {qq.unrolled_index.svm_kernel}) S({(int)qq.unrolled_index.scale_function}: {qq.unrolled_index.scale_function}) R({qq.unrolled_index.repetitions}) O({qq.unrolled_index.outer_cv_folds}) I({qq.unrolled_index.inner_cv_folds}) W({string.Join(", ", qq.unrolled_index.class_weights ?? new List<(int class_id, double class_weight)>())})";

        //                io_proxy.WriteLine($@"{experiment_name}: Starting: {note}", module_name, method_name);

        //                var group_folder = get_iteration_folder(outer_cv_args.results_root_folder, experiment_name, iteration_index, group_index);
        //                var group_merged_filename_prefix = Path.Combine(group_folder, $@"m_{get_filename(qq.unrolled_index.class_weights, iteration_index, group_index, qq.unrolled_index.total_groups, (int)qq.unrolled_index.svm_type, (int)qq.unrolled_index.svm_kernel, (int)qq.unrolled_index.scale_function, null, qq.unrolled_index.repetitions, null, qq.unrolled_index.outer_cv_folds, qq.unrolled_index.inner_cv_folds, null, null)}");
        //                var group_merged_cm_fn = $@"{group_merged_filename_prefix}.cm.csv";

        //                var iteration_all_cm = new List<(string line, perf.confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)>();


        //                {
        //                    io_proxy.WriteLine($@"{experiment_name}: Group cache: Unavailable for {note}. File: {group_merged_cm_fn}.", module_name, method_name);

        //                    var is_group_selected = true;
        //                    var is_only_selection = false;
        //                    var is_last_winner = true;
        //                    var dir = direction.neutral;

        //                    var num_columns_added_from_last_iteration = 0;
        //                    var num_groups_added_from_last_iteration = 0;
        //                    var num_columns_added_from_highest_score_iteration = 0;
        //                    var num_groups_added_from_highest_score_iteration = 0;

        //                    var previous_selected_groups = selected_groups;
        //                    var previous_selected_columns = selected_columns;
        //                    var test_selected_groups = selected_groups;
        //                    var test_selected_columns = selected_columns;

        //                    var exp_data = new List<(string key, string value)>() { ($@"{nameof(is_group_selected)}", $@"{is_group_selected}"), ($@"{nameof(is_only_selection)}", $@"{is_only_selection}"), ($@"{nameof(is_last_winner)}", $@""), ($@"{nameof(num_columns_added_from_last_iteration)}", $@"{num_columns_added_from_last_iteration}"), ($@"{nameof(num_groups_added_from_last_iteration)}", $@"{num_groups_added_from_last_iteration}"), ($@"{nameof(num_columns_added_from_highest_score_iteration)}", $@"{num_columns_added_from_highest_score_iteration}"), ($@"{nameof(num_groups_added_from_highest_score_iteration)}", $@"{num_groups_added_from_highest_score_iteration}"), ($@"{nameof(dir)}", $@"{dir}"), ($@"{nameof(previous_selected_groups)}", $@"{string.Join(";", previous_selected_groups)}"), ($@"{nameof(previous_selected_columns)}", $@"{string.Join(";", previous_selected_columns)}"), ($@"{nameof(selected_groups)}", $@"{string.Join(";", selected_groups)}"), ($@"{nameof(selected_columns)}", $@"{string.Join(";", selected_columns)}"), ($@"{nameof(test_selected_groups)}", $@"{string.Join(";", test_selected_groups)}"), ($@"{nameof(test_selected_columns)}", $@"{string.Join(";", test_selected_columns)}"), };

        //                    var cm_header = $"{string.Join(",", exp_data.Select(a => a.key).ToList())},{string.Join(",", perf.confusion_matrix.csv_header)}";

        //                    //if (iteration_instance_all_groups_cm_lines.Count == 0) { iteration_instance_all_groups_cm_lines.Add(cm_header); }
        //                    //if (iteration_all_group_cm_list.Count == 0) iteration_all_group_cm_list.Add(cm_header);

        //                    // 1. make outer-cv files
        //                    var outer_cv_inputs = make_outer_cv_inputs(test_selected_columns, qq.ida, qq.idr, qq.oca, group_folder, iteration_index, group_index);
        //                    var merged_cv_input = outer_cv_inputs.First(a => a.ocvi == -1);

        //                    // 2. run libsvm
        //                    var prediction_data_list = new List<((long grid_dur, long train_dur, long predict_dur) dur, ((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double? cv_rate) grid, string[] predict_text)>();



        //                    foreach (var outer_cv_input in outer_cv_inputs)
        //                    {
        //                        if (outer_cv_input.ocvi == -1 || outer_cv_input.rcvi == -1) continue; // -1 is the index for the merged text

        //                        var prediction_data = inner_cross_validation(qq.ida, qq.idr, qq.oca, outer_cv_input);

        //                        prediction_data_list.Add(prediction_data);

        //                        if (make_outer_cv_confusion_matrices)
        //                        {
        //                            var ocv_prediction_file_data = perf.load_prediction_file(outer_cv_input.testing_text, null, prediction_data.predict_text, qq.unrolled_index.calc_11p_thresholds);

        //                            foreach (var ocv_cm in ocv_prediction_file_data.cm_list)
        //                            {
        //                                ocv_cm.x_id = qq.index;
        //                                ocv_cm.x_iteration_index = iteration_index;
        //                                ocv_cm.x_group_index = group_index;
        //                                ocv_cm.x_total_groups = qq.unrolled_index.total_groups;

        //                                ocv_cm.x_key_alphabet = "";
        //                                ocv_cm.x_key_dimension = "";
        //                                ocv_cm.x_key_category = "";
        //                                ocv_cm.x_key_source = "";
        //                                ocv_cm.x_key_group = "";
        //                                ocv_cm.x_key_member = "";
        //                                ocv_cm.x_key_perspective = "";

        //                                ocv_cm.x_experiment_name = experiment_name;
        //                                ocv_cm.x_scaling_function = qq.unrolled_index.scale_function.ToString();
        //                                ocv_cm.x_old_feature_count = previous_selected_columns.Count;
        //                                ocv_cm.x_new_feature_count = test_selected_columns.Count;
        //                                ocv_cm.x_old_group_count = previous_selected_groups.Count;
        //                                ocv_cm.x_new_group_count = test_selected_groups.Count;
        //                                ocv_cm.x_inner_cv_folds = qq.unrolled_index.inner_cv_folds;
        //                                ocv_cm.x_repetitions_cv_index = outer_cv_input.rcvi;
        //                                ocv_cm.x_repetitions_cv_folds = qq.unrolled_index.repetitions;
        //                                ocv_cm.x_outer_cv_index = outer_cv_input.ocvi;
        //                                ocv_cm.x_outer_cv_folds = qq.unrolled_index.outer_cv_folds;
        //                                ocv_cm.x_svm_type = qq.unrolled_index.svm_type.ToString();
        //                                ocv_cm.x_svm_kernel = qq.unrolled_index.svm_kernel.ToString();
        //                                ocv_cm.x_class_weight = qq.unrolled_index.class_weights?.FirstOrDefault(b => ocv_cm.class_id == b.class_id).class_weight;
        //                                ocv_cm.x_class_name = qq.unrolled_index.class_names?.FirstOrDefault(b => ocv_cm.class_id == b.class_id).class_name;
        //                                ocv_cm.x_class_size = qq.idr.class_sizes?.First(b => b.class_id == ocv_cm.class_id).class_size ?? -1;
        //                                ocv_cm.x_class_training_size = outer_cv_input.training_sizes?.First(b => b.class_id == ocv_cm.class_id).training_size ?? -1;
        //                                ocv_cm.x_class_testing_size = outer_cv_input.testing_sizes?.First(b => b.class_id == ocv_cm.class_id).testing_size ?? -1;
        //                                ocv_cm.x_cost = prediction_data.grid.point.cost;
        //                                ocv_cm.x_gamma = prediction_data.grid.point.gamma;
        //                                ocv_cm.x_coef0 = prediction_data.grid.point.coef0;
        //                                ocv_cm.x_epsilon = prediction_data.grid.point.epsilon;
        //                                ocv_cm.x_degree = prediction_data.grid.point.degree;
        //                                ocv_cm.x_libsvm_cv = prediction_data.grid.cv_rate.GetValueOrDefault();
        //                                ocv_cm.x_duration_grid_search = prediction_data.dur.grid_dur.ToString(CultureInfo.InvariantCulture);
        //                                ocv_cm.x_duration_training = prediction_data.dur.train_dur.ToString(CultureInfo.InvariantCulture);
        //                                ocv_cm.x_duration_testing = prediction_data.dur.predict_dur.ToString(CultureInfo.InvariantCulture);
        //                                ocv_cm.calculate_ppf();
        //                            }


        //                            var ocv_cm_lines = new List<string>() { cm_header };
        //                            ocv_cm_lines.AddRange(ocv_prediction_file_data.cm_list.Select(a => $"{string.Join(",", exp_data.Select(c => c.value).ToList())},{a}").ToList());
        //                            var ocv_lcs = perf.confusion_matrix.load(ocv_cm_lines);

        //                            if (ocv_lcs != null && ocv_lcs.Count > 1) { iteration_all_cm.AddRange(ocv_lcs.Skip(iteration_all_cm.Count == 0 ? 0 : 1)); }



        //                            // save OCV CM for group
        //                            io_proxy.WriteAllLines(outer_cv_input.cm_fn, ocv_cm_lines);
        //                            io_proxy.WriteLine($@"{experiment_name}: Group OCV cache: Saved for iteration {(iteration_index )} group {(group_index )} OCV R({outer_cv_input.rcvi}/{unrolled_index.repetitions}) O({outer_cv_input.ocvi}/{unrolled_index.outer_cv_folds}) {note}. File: {outer_cv_input.cm_fn}.", module_name, method_name);

        //                        }

        //                        io_proxy.Delete(outer_cv_input.training_fn);
        //                        io_proxy.Delete(outer_cv_input.grid_fn);
        //                        io_proxy.Delete(outer_cv_input.model_fn);
        //                        io_proxy.Delete(outer_cv_input.testing_fn);
        //                        io_proxy.Delete(outer_cv_input.predict_fn);
        //                        //io_proxy.Delete(outer_cv_input.cm_fn);
        //                    }

        //                    // 3. make confusion matrix from the merged prediction results
        //                    var merged_prediction_text = prediction_data_list.SelectMany(a => a.predict_text).ToList();

        //                    var prediction_file_data = perf.load_prediction_file(merged_cv_input.testing_text, null, merged_prediction_text, unrolled_index.calc_11p_thresholds);

        //                    foreach (var cm in prediction_file_data.cm_list)
        //                    {
        //                        cm.x_experiment_name = experiment_name;

        //                        cm.x_id = qq.index;
        //                        cm.x_iteration_index = iteration_index;
        //                        cm.x_group_index = group_index;
        //                        cm.x_total_groups = qq.unrolled_index.total_groups;

        //                        cm.x_key_alphabet = "";
        //                        cm.x_key_dimension = "";
        //                        cm.x_key_category = "";
        //                        cm.x_key_source = "";
        //                        cm.x_key_group = "";
        //                        cm.x_key_member = "";
        //                        cm.x_key_perspective = "";

        //                        cm.x_scaling_function = qq.unrolled_index.scale_function.ToString();
        //                        cm.x_old_feature_count = previous_selected_columns.Count;
        //                        cm.x_new_feature_count = test_selected_columns.Count;
        //                        cm.x_old_group_count = previous_selected_groups.Count;
        //                        cm.x_new_group_count = test_selected_groups.Count;
        //                        cm.x_inner_cv_folds = qq.unrolled_index.inner_cv_folds;
        //                        cm.x_repetitions_cv_index = -1; //input.rcvi;
        //                        cm.x_repetitions_cv_folds = qq.unrolled_index.repetitions;
        //                        cm.x_outer_cv_index = -1; //input.ocvi;
        //                        cm.x_outer_cv_folds = qq.unrolled_index.outer_cv_folds;
        //                        cm.x_svm_type = qq.unrolled_index.svm_type.ToString();
        //                        cm.x_svm_kernel = qq.unrolled_index.svm_kernel.ToString();
        //                        cm.x_class_weight = qq.unrolled_index.class_weights?.FirstOrDefault(b => cm.class_id == b.class_id).class_weight;
        //                        cm.x_class_name = qq.unrolled_index.class_names?.FirstOrDefault(b => cm.class_id == b.class_id).class_name;
        //                        cm.x_class_size = qq.idr.class_sizes?.First(b => b.class_id == cm.class_id).class_size ?? -1;
        //                        cm.x_class_training_size = merged_cv_input.training_sizes?.First(b => b.class_id == cm.class_id).training_size ?? -1;
        //                        cm.x_class_testing_size = merged_cv_input.testing_sizes?.First(b => b.class_id == cm.class_id).testing_size ?? -1;

        //                        cm.x_cost = prediction_data_list.Where(a => a.grid.point.cost != null).Select(a => a.grid.point.cost).DefaultIfEmpty(0).Average();
        //                        cm.x_gamma = prediction_data_list.Where(a => a.grid.point.gamma != null).Select(a => a.grid.point.gamma).DefaultIfEmpty(0).Average();
        //                        cm.x_coef0 = prediction_data_list.Where(a => a.grid.point.coef0 != null).Select(a => a.grid.point.coef0).DefaultIfEmpty(0).Average();
        //                        cm.x_epsilon = prediction_data_list.Where(a => a.grid.point.epsilon != null).Select(a => a.grid.point.epsilon).DefaultIfEmpty(0).Average();
        //                        cm.x_degree = prediction_data_list.Where(a => a.grid.point.degree != null).Select(a => a.grid.point.degree).DefaultIfEmpty(0).Average();
        //                        cm.x_libsvm_cv = prediction_data_list.Where(a => a.grid.cv_rate != null).Select(a => (double)a.grid.cv_rate).DefaultIfEmpty(0).Average();

        //                        cm.x_duration_grid_search = prediction_data_list.Select(a => a.dur.grid_dur).Sum().ToString(CultureInfo.InvariantCulture);
        //                        cm.x_duration_training = prediction_data_list.Select(a => a.dur.train_dur).Sum().ToString(CultureInfo.InvariantCulture);
        //                        cm.x_duration_testing = prediction_data_list.Select(a => a.dur.predict_dur).Sum().ToString(CultureInfo.InvariantCulture);

        //                        cm.calculate_ppf();
        //                    }



        //                    var merge_cm_lines = new List<string>() { cm_header };
        //                    merge_cm_lines.AddRange(prediction_file_data.cm_list.Select(a => $"{string.Join(",", exp_data.Select(c => c.value).ToList())},{a}").ToList());
        //                    var lcs = perf.confusion_matrix.load(merge_cm_lines);

        //                    if (lcs != null && lcs.Count > 1) { iteration_all_cm.AddRange(lcs.Skip(iteration_all_cm.Count == 0 ? 0 : 1)); }

        //                    // save CM for group
        //                    io_proxy.WriteAllLines( /*merged_cv_input.cm_fn*/group_merged_cm_fn, merge_cm_lines);
        //                    io_proxy.WriteLine($@"{experiment_name}: Group cache: Saved for {note}. File: {group_merged_cm_fn}.", module_name, method_name);
        //                }

        //                io_proxy.WriteLine($@"{experiment_name}: Finished: {note}.", module_name, method_name);

        //                return iteration_all_cm;
        //            });

        //        iteration_all_cm = tasks.SelectMany((a, i) => a.Result.iteration_all_cm.Skip(i == 0 ? 0 : 1)).ToList();
        //        tasks.Clear();
        //    }


        //    // 5. load results from other instances
        //    //
        //    if (instance_id == 0)
        //    {
        //        var instances_done = new List<string>() { iteration_instance_group_cm_filename, iteration_all_group_cm_filename };

        //        var total_loaded = iteration_all_cm.Skip(1).Count(a => a.cm.class_id == caller_oca.scoring_class_id && a.cm.x_prediction_threshold == null && a.cm.x_repetitions_cv_index == -1 && a.cm.x_outer_cv_index == -1);

        //        io_proxy.WriteLine($"{experiment_name}: Waiting for other instances to complete. Total Loaded: {total_loaded} / {total_indexes}.", module_name, method_name);

        //        while (total_loaded < total_indexes)
        //        {
        //            if (Directory.Exists(iteration_folder))
        //            {
        //                var dir_files = Directory.GetFiles(iteration_folder, "x_*.cm.csv", SearchOption.TopDirectoryOnly);

        //                foreach (var file in dir_files)
        //                {
        //                    if (instances_done.Contains(file)) continue;
        //                    instances_done.Add(file);

        //                    wait_any(tasks);

        //                    var task = Task.Run(() =>
        //                    {
        //                        var lcs = perf.confusion_matrix.load(file);

        //                        io_proxy.WriteLine($"{experiment_name}: Part cache (instances): Loaded. File: {file}.", module_name, method_name);

        //                        return (iteration_index, group_index, lcs);
        //                    });

        //                    tasks.Add(task);
        //                }

        //                Task.WaitAll(tasks.ToArray<Task>());
        //                iteration_all_cm.AddRange(tasks.SelectMany((a, i) => a.Result.iteration_all_cm.Skip(i == 0 && iteration_all_cm.Count == 0 ? 0 : 1)));
        //                tasks.Clear();
        //            }

        //            total_loaded = iteration_all_cm.Skip(1).Count(a => a.cm.class_id == caller_oca.scoring_class_id && a.cm.x_prediction_threshold == null && a.cm.x_repetitions_cv_index == -1 && a.cm.x_outer_cv_index == -1);

        //            if (total_loaded < total_indexes) { Task.Delay(new TimeSpan(0, 0, 15)).Wait(); }
        //        }


        //        // save CM with results from all instances
        //        io_proxy.WriteAllLines(iteration_all_group_cm_filename, iteration_all_cm.Select(a => a.line));
        //        io_proxy.WriteLine($"{experiment_name}: Full cache: Saved for iteration {(iteration_index )}. File: {iteration_all_group_cm_filename}.", module_name, method_name);
        //    }

        //}
    }
}
