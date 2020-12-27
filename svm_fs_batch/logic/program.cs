using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using svm_fs_batch.models;

namespace svm_fs_batch
{
    internal class program
    {
        public const string module_name = nameof(program);


        internal static void Main(string[] args)
        {
            //var rnd = new metrics_box();
            //rnd.set_cm(null, null, 20, 11, 126, 30);
            //Console.WriteLine(string.Join("\r\n", rnd.csv_values_array().Select((a, i) => $"{metrics_box.csv_header_values_array[i]} = '{a}'").ToArray()));
            //Console.WriteLine();
            
            //rnd.apply_imbalance_correction1();
            //Console.WriteLine(string.Join("\r\n", rnd.csv_values_array().Select((a, i) => $"{metrics_box.csv_header_values_array[i]} = '{a}'").ToArray()));
            //Console.WriteLine();

            //rnd.set_random_perf();
            //Console.WriteLine(string.Join("\r\n", rnd.csv_values_array().Select((a, i) => $"{metrics_box.csv_header_values_array[i]} = '{a}'").ToArray()));
            //Console.WriteLine();

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

            //var fake_args = $"-experiment_name=test -whole_array_index_first=0 -whole_array_index_last=9 -whole_array_step_size=2 -whole_array_length=5 -partition_array_index_first=4 -partition_array_index_last=5";
            var fake_args_list = new List<(string name, string value)>()
            {
                ("experiment_name", "test"),
                ("whole_array_index_first", "0"),
                ("whole_array_index_last", "0"),
                ("whole_array_step_size", "1"),
                ("whole_array_length", "1"),
                ("partition_array_index_first", "0"),
                ("partition_array_index_last", "0"),
                
                (nameof(svm_fs_batch.program_args.inner_folds), "1"),
                (nameof(svm_fs_batch.program_args.outer_cv_folds), "5"),
                (nameof(svm_fs_batch.program_args.outer_cv_folds_to_run), "1"),
                (nameof(svm_fs_batch.program_args.repetitions), "1"),
            };

            var fake_args = string.Join(" ", fake_args_list.Select(a => $"-{a.name}={a.value}").ToArray());
            args = fake_args.Split();

            var program_args = new program_args(args);

            if (program_args.setup)
            {
                setup.setup_pbs_job(main_cts, program_args);
                return;
            }


            // check experiment name is valid
            if (string.IsNullOrWhiteSpace(program_args.experiment_name)) { throw new ArgumentOutOfRangeException(nameof(args), $"{nameof(program_args.experiment_name)}: must specify experiment name"); }

            // check whole array indexes are valid
            if (program_args.whole_array_index_first <= -1) { throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(program_args.whole_array_index_first)} = {program_args.whole_array_index_first}"); }
            if (program_args.whole_array_index_last <= -1) { throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(program_args.whole_array_index_last)} = {program_args.whole_array_index_last}"); }
            if (program_args.whole_array_step_size <= 0) { throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(program_args.whole_array_step_size)} = {program_args.whole_array_step_size}"); }
            if (program_args.whole_array_length <= 0) { throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(program_args.whole_array_length)} = {program_args.whole_array_length}"); }

            // check partition array indexes are valid
            if (!routines.is_in_range(program_args.whole_array_index_first, program_args.whole_array_index_last, program_args.partition_array_index_first)) { throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(program_args.partition_array_index_first)} = {program_args.partition_array_index_first}"); }
            if (!routines.is_in_range(program_args.whole_array_index_first, program_args.whole_array_index_last, program_args.partition_array_index_last)) { throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(program_args.partition_array_index_last)} = {program_args.partition_array_index_last}"); }


            //var instance_id = get_instance_id(program_args.whole_array_index_first, program_args.whole_array_index_last, program_args.whole_array_step_size, program_args.partition_array_index_first, program_args.partition_array_index_last);

            var instance_id = routines.for_iterations(program_args.whole_array_index_first, program_args.partition_array_index_first, program_args.whole_array_step_size) - 1;
            var total_instance = routines.for_iterations(program_args.whole_array_index_first, program_args.whole_array_index_last, program_args.whole_array_step_size);

            if (instance_id < 0) { throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(instance_id)} = {instance_id}"); }
            if (total_instance != program_args.whole_array_length) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(program_args.whole_array_length)} = {program_args.whole_array_length}, {nameof(total_instance)} = {total_instance}");


            io_proxy.WriteLine($"Array job index: {instance_id} / {total_instance}. Partition array indexes: {program_args.partition_array_index_first}..{program_args.partition_array_index_last}.  Whole array indexes: {program_args.whole_array_index_first}..{program_args.whole_array_index_last}:{program_args.whole_array_step_size} (length: {program_args.whole_array_length}).");

            feature_selection.feature_selection_initialization
            (
                main_cts,
                program_args.experiment_name,
                instance_id,
                program_args.whole_array_length,
                //program_args.instance_array_index_start,
                //program_args.array_step,
                //program_args.instance_array_index_end,
                program_args.repetitions,
                program_args.outer_cv_folds,
                program_args.outer_cv_folds_to_run,
                program_args.inner_folds
            );
        }

        


        internal static string get_iteration_folder(string results_root_folder, string experiment_name, int iteration_index, /*string iteration_name = null,*/ int? group_index = null)
        {
            var hr = false;

            var it = $@"it_{(iteration_index + (hr ? 1 : 0))}" /*+ (!string.IsNullOrWhiteSpace(iteration_name) ? iteration_name : "")*/;

            if (group_index == null) return Path.Combine(results_root_folder, experiment_name, it);

            var gr = $@"gr_{(group_index + (hr ? 1 : 0))}";
            return Path.Combine(results_root_folder, experiment_name, it, gr);
        }


        internal static void update_merged_cm
        (
            CancellationTokenSource cts,
            dataset_loader dataset,
            (prediction[] prediction_list, confusion_matrix[] cm_list) prediction_file_data,
            index_data unrolled_index_data,
            outer_cv_input merged_cv_input,
            (TimeSpan? grid_dur, TimeSpan? train_dur, TimeSpan? predict_dur, grid_point grid_point, string[] predict_text)[] prediction_data_list,
            bool as_parallel = false
        )
        {
            const string method_name = nameof(update_merged_cm);

            if (cts.IsCancellationRequested) return;

            if (prediction_file_data.cm_list == null) throw new ArgumentOutOfRangeException(nameof(prediction_file_data),$@"{module_name}.{method_name}.{nameof(prediction_file_data)}.{nameof(prediction_file_data.cm_list)}");

            if (as_parallel)
            {
                Parallel.ForEach(prediction_file_data.cm_list, cm =>
                {
                    update_merged_cm_from_vector(prediction_data_list, cm);

                    update_merged_cm_single(cts, dataset, unrolled_index_data, merged_cv_input, cm); 

                });
            } else
            {
                foreach (var cm in prediction_file_data.cm_list)
                {
                    update_merged_cm_from_vector(prediction_data_list, cm);

                    update_merged_cm_single(cts, dataset, unrolled_index_data, merged_cv_input, cm);
                }
            }
        }

        private static void update_merged_cm_from_vector((TimeSpan? grid_dur, TimeSpan? train_dur, TimeSpan? predict_dur, grid_point grid_point, string[] predict_text)[] prediction_data_list, confusion_matrix cm)
        {
            if (cm.grid_point == null) { cm.grid_point = new grid_point(prediction_data_list?.Select(a => a.grid_point).ToArray()); }

            if ((cm.x_time_grid == null  || cm.x_time_grid == TimeSpan.Zero) &&  (prediction_data_list?.Any(a => a.grid_dur != null) ?? false)) cm.x_time_grid = new TimeSpan(prediction_data_list?.Select(a => a.grid_dur?.Ticks ?? 0).DefaultIfEmpty(0).Sum() ?? 0);
            if ((cm.x_time_train == null || cm.x_time_train == TimeSpan.Zero) && (prediction_data_list?.Any(a => a.train_dur != null) ?? false)) cm.x_time_train = new TimeSpan(prediction_data_list?.Select(a => a.train_dur?.Ticks ?? 0).DefaultIfEmpty(0).Sum() ?? 0);
            if ((cm.x_time_test == null  || cm.x_time_test == TimeSpan.Zero) &&  (prediction_data_list?.Any(a => a.predict_dur != null) ?? false)) cm.x_time_test = new TimeSpan(prediction_data_list?.Select(a => a.predict_dur?.Ticks ?? 0).DefaultIfEmpty(0).Sum() ?? 0);
        }

        internal static void update_merged_cm_single
        (
            CancellationTokenSource cts, 
            dataset_loader dataset, 
            index_data unrolled_index_data, 
            outer_cv_input merged_cv_input,
            confusion_matrix cm
        )
        {
            if (cts.IsCancellationRequested) return;

            cm.x_class_name = settings.class_names?.FirstOrDefault(b => cm.x_class_id == b.class_id).class_name;
            cm.x_class_size = dataset.class_sizes?.First(b => b.class_id == cm.x_class_id).class_size ?? -1;
            cm.x_class_test_size = merged_cv_input.test_sizes?.First(b => b.class_id == cm.x_class_id).test_size ?? -1;
            cm.x_class_train_size = merged_cv_input.train_sizes?.First(b => b.class_id == cm.x_class_id).train_size ?? -1;
            cm.x_class_weight = unrolled_index_data.class_weights?.FirstOrDefault(b => cm.x_class_id == b.class_id).class_weight;
            //cm.x_time_grid_search =  prediction_data_list?.Select(a => a.dur.grid_dur).DefaultIfEmpty(0).Sum();
            //cm.x_time_test = prediction_data_list?.Select(a => a.dur.predict_dur).DefaultIfEmpty(0).Sum();
            //cm.x_time_train = prediction_data_list?.Select(a => a.dur.train_dur).DefaultIfEmpty(0).Sum();
            cm.x_outer_cv_index = merged_cv_input.outer_cv_index;
            cm.x_repetitions_index = merged_cv_input.repetitions_index;
        }


        internal enum direction { none, forwards, neutral, backwards }


        internal static string get_item_filename(index_data unrolled_index, int repetition_index, int outer_cv_index)
        {
            return $@"{get_iteration_filename(new[] { unrolled_index })}_ri[{repetition_index}]_oi[{outer_cv_index}]";
        }
        
        internal static string get_iteration_filename(index_data[] indexes)
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

            var experiment_group_name = reduce(string.Join($@"_", indexes.Select(a => a.experiment_name).Distinct().ToArray()));
            var iteration_index = reduce(routines.find_ranges_str(indexes.Select(a => a.iteration_index).ToList()));
            var group_index = reduce(routines.find_ranges_str(indexes.Select(a => a.group_array_index).ToList()));
            var total_groups = reduce(routines.find_ranges_str(indexes.Select(a => a.total_groups).ToList()));
            var calc_11p_thresholds = reduce(routines.find_ranges_str(indexes.Select(a => a.calc_11p_thresholds ? 1 : 0).ToList()));
            var repetitions = reduce(routines.find_ranges_str(indexes.Select(a => a.repetitions).ToList()));
            var outer_cv_folds = reduce(routines.find_ranges_str(indexes.Select(a => a.outer_cv_folds).ToList()));
            var outer_cv_folds_to_run = reduce(routines.find_ranges_str(indexes.Select(a => a.outer_cv_folds_to_run).ToList()));
            var class_weights = string.Join("_",
                indexes
                    .Where(a => a.class_weights != null)
                    .SelectMany(a => a.class_weights)
                    .GroupBy(a => a.class_id)
                    .Select(a => $@"{a.Key}_{reduce(routines.find_ranges_str(a.Select(b => (int)(b.class_weight * 100)).ToList()))}")
                    .ToList());
            var svm_type = reduce(routines.find_ranges_str(indexes.Select(a => (int)a.svm_type).ToList()));
            var svm_kernel = reduce(routines.find_ranges_str(indexes.Select(a => (int)a.svm_kernel).ToList()));
            var scale_function = reduce(routines.find_ranges_str(indexes.Select(a => (int)a.scale_function).ToList()));
            var inner_cv_folds = reduce(routines.find_ranges_str(indexes.Select(a => a.inner_cv_folds).ToList()));

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


        
    }
}
