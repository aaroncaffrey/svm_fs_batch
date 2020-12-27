using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using svm_fs_batch.models;

namespace svm_fs_batch
{
    internal class cache_load
    {
        public const string module_name = nameof(cache_load);

        internal static index_data_container
            get_feature_selection_instructions
            (
                CancellationTokenSource cts,
                dataset_loader dataset,
                (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] groups,
                group_series_index[] job_group_series,
                string experiment_name,
                int iteration_index,
                int total_groups,
                 int instance_id,
                int total_instances,
                int repetitions,
                int outer_cv_folds,
                int outer_cv_folds_to_run,
                int inner_folds,

                int[] base_group_indexes,
                int[] group_indexes_to_test,
                int[] selected_group_indexes,
                int? previous_winner_group_index,
                int[] selection_excluded_group_indexes,
                List<int[]> previous_group_tests,
                bool as_parallel = true
            )
        {
            const string method_name = nameof(get_feature_selection_instructions);

            if (cts.IsCancellationRequested) return default;

            if (job_group_series == null || job_group_series.Length == 0) throw new ArgumentOutOfRangeException(nameof(job_group_series), $"{module_name}.{method_name}.{nameof(job_group_series)}");
            //var job_group_series = cache_load.job_group_series(cts, dataset, groups, experiment_name, iteration_index, base_group_indexes, group_indexes_to_test, selected_group_indexes, previous_winner_group_index, selection_excluded_group_indexes, previous_group_tests, as_parallel);

            var uip = new unrolled_indexes_parameters()
            {
                r_cv_series_start = repetitions,
                r_cv_series_end = repetitions,
                r_cv_series_step = 1,

                o_cv_series_start = outer_cv_folds,
                o_cv_series_end = outer_cv_folds,
                o_cv_series_step = 1,

                i_cv_series_start = inner_folds,
                i_cv_series_end = inner_folds,
                i_cv_series_step = 1,

                group_series = job_group_series
                //group_series_start = total_groups < 0 ? total_groups : 0,
                //group_series_end = total_groups < 0 ? total_groups : (total_groups > 0 ? total_groups - 1 : 0)

            };

            var unrolled_indexes = get_unrolled_indexes(cts, dataset, experiment_name, iteration_index, total_groups, instance_id, total_instances, uip, outer_cv_folds_to_run);

            return unrolled_indexes;
        }

        internal static group_series_index[] job_group_series(CancellationTokenSource cts, dataset_loader dataset, (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] groups, string experiment_name, int iteration_index, int[] base_group_indexes, int[] group_indexes_to_test, int[] selected_group_indexes, int? previous_winner_group_index, int[] selection_excluded_group_indexes, List<int[]> previous_group_tests, bool as_parallel)
        {
            var job_group_series = as_parallel ?
                group_indexes_to_test.AsParallel().AsOrdered().WithCancellation(cts.Token).Select(group_array_index => job_group_series_index_single(cts, dataset, groups, experiment_name, iteration_index, base_group_indexes, selected_group_indexes, previous_winner_group_index, selection_excluded_group_indexes, previous_group_tests, group_array_index)).ToArray()
                :
                group_indexes_to_test.Select(group_array_index => job_group_series_index_single(cts, dataset, groups, experiment_name, iteration_index, base_group_indexes, selected_group_indexes, previous_winner_group_index, selection_excluded_group_indexes, previous_group_tests, group_array_index)).ToArray();

            job_group_series = job_group_series.Where(a => a.selection_direction != program.direction.none).ToArray();
            return job_group_series;
        }

        internal static group_series_index job_group_series_index_single(CancellationTokenSource cts, dataset_loader dataset, (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] groups, string experiment_name, int iteration_index, int[] base_group_indexes, int[] selected_group_indexes, int? previous_winner_group_index, int[] selection_excluded_group_indexes, List<int[]> previous_group_tests, int group_array_index)
        {
            //var test_selected_groups = selected_groups.OrderBy(group_index => group_index).ToArray();
            //var test_selected_columns = selected_columns.OrderBy(col_index => col_index).ToArray();
            var gsi = new group_series_index();

            gsi.group_array_index = group_array_index;

            gsi.group_key = gsi.group_array_index > -1 && groups != null && groups.Length - 1 >= gsi.group_array_index ? groups[gsi.group_array_index].group_key : default;
            gsi.group_folder = program.get_iteration_folder(settings.results_root_folder, experiment_name, iteration_index, gsi.group_array_index);

            gsi.is_group_index_valid = gsi.group_array_index > -1 && routines.is_in_range(0, (groups?.Length ?? 0) - 1, gsi.group_array_index);
            gsi.is_group_selected = selected_group_indexes.Contains(gsi.group_array_index);
            gsi.is_group_only_selection = gsi.is_group_selected && selected_group_indexes.Length == 1;
            gsi.is_group_last_winner = gsi.group_array_index == previous_winner_group_index;
            gsi.is_group_base_group = base_group_indexes?.Contains(gsi.group_array_index) ?? false;
            gsi.is_group_blacklisted = selection_excluded_group_indexes?.Contains(gsi.group_array_index) ?? false;


            // if selected, remove.  if not selected, add.  if only group, no action.  if just added, no action.

            gsi.selection_direction = program.direction.none;

            if (gsi.is_group_blacklisted || (gsi.is_group_base_group && groups.Length > base_group_indexes.Length)) gsi.selection_direction = program.direction.none;

            else if (!gsi.is_group_index_valid /* || is_group_base_group*/) gsi.selection_direction = program.direction.neutral; // calibration

            else if (gsi.is_group_index_valid && gsi.is_group_selected && !gsi.is_group_base_group && !gsi.is_group_last_winner && !gsi.is_group_only_selection) gsi.selection_direction = program.direction.backwards;

            else if (gsi.is_group_index_valid && !gsi.is_group_selected && !gsi.is_group_base_group && !gsi.is_group_blacklisted) gsi.selection_direction = program.direction.forwards;

            else if (gsi.is_group_only_selection) gsi.selection_direction = program.direction.neutral;

            else throw new Exception();

            gsi.group_indexes = null;
            gsi.column_indexes = null;

            if (gsi.selection_direction == program.direction.none)
            {
                // no action
            }
            else if (gsi.selection_direction == program.direction.neutral)
            {
                gsi.group_indexes = selected_group_indexes;
                //gsi.column_indexes = selected_columns;
            }
            else if (gsi.selection_direction == program.direction.forwards)
            {
                gsi.group_indexes = selected_group_indexes.Union(new[] { gsi.group_array_index }).OrderBy(group_index => group_index).ToArray();
                //gsi.column_indexes = gsi.group_indexes.SelectMany(group_index => groups[group_index].columns).Union(new[] { 0 }).OrderBy(col_index => col_index).Distinct().ToArray();
            }
            else if (gsi.selection_direction == program.direction.backwards)
            {
                gsi.group_indexes = selected_group_indexes.Except(new[] { gsi.group_array_index }).OrderBy(group_index => group_index).ToArray();
                //gsi.column_indexes = gsi.group_indexes.SelectMany(group_index => groups[group_index].columns).Union(new[] { 0 }).OrderBy(col_index => col_index).Distinct().ToArray();
            }
            else throw new Exception();

            if (previous_group_tests.Any(a => a.SequenceEqual(gsi.group_indexes)))
            {
                gsi.selection_direction = program.direction.none;
                gsi.group_indexes = null;
                gsi.column_indexes = null;
            }

            gsi.column_indexes = gsi.group_indexes?.SelectMany(group_index => groups[group_index].columns).Union(new[] { 0 }).OrderBy(col_index => col_index).Distinct().ToArray();
            gsi.column_indexes = dataset_loader.remove_duplicate_columns(cts, dataset, gsi.column_indexes);

            if ((gsi.group_indexes != null && gsi.group_indexes.Length > 0) && (gsi.column_indexes == null || gsi.column_indexes.Length <= 1)) throw new Exception();

            return gsi;
        }


        internal static index_data_container get_unrolled_indexes
        (
            CancellationTokenSource cts,
            dataset_loader dataset,
            string experiment_name,
            int iteration_index,
            int total_groups,
             int instance_id,
            int total_instances,
            unrolled_indexes_parameters uip,
            int outer_cv_folds_to_run = 0
        //int[] selection_excluded_groups = null
        )
        {
            const string method_name = nameof(get_unrolled_indexes);

            if (uip.svm_types == null || uip.svm_types.Length == 0) uip.svm_types = new[] { routines.libsvm_svm_type.c_svc };
            if (uip.kernels == null || uip.kernels.Length == 0) uip.kernels = new[] { routines.libsvm_kernel_type.rbf };
            if (uip.scales == null || uip.scales.Length == 0) uip.scales = new[] { scaling.scale_function.rescale };

            if (uip.r_cv_series == null || uip.r_cv_series.Length == 0) uip.r_cv_series = routines.range(uip.r_cv_series_start, uip.r_cv_series_end, uip.r_cv_series_step);
            if (uip.o_cv_series == null || uip.o_cv_series.Length == 0) uip.o_cv_series = routines.range(uip.o_cv_series_start, uip.o_cv_series_end, uip.o_cv_series_step);
            if (uip.i_cv_series == null || uip.i_cv_series.Length == 0) uip.i_cv_series = routines.range(uip.i_cv_series_start, uip.i_cv_series_end, uip.i_cv_series_step);
            //if (p.group_series == null || p.group_series.Length == 0) p.group_series = routines.range(p.group_series_start, p.group_series_end, 1);

            //if (selection_excluded_groups != null && selection_excluded_groups.Length > 0)
            //{
            //    p.group_series = p.group_series.Except(selection_excluded_groups).ToArray();
            //}

            if (uip.r_cv_series == null || uip.r_cv_series.Length == 0) throw new ArgumentOutOfRangeException(nameof(uip), $@"{module_name}.{method_name}");
            if (uip.o_cv_series == null || uip.o_cv_series.Length == 0) throw new ArgumentOutOfRangeException(nameof(uip), $@"{module_name}.{method_name}");
            if (uip.i_cv_series == null || uip.i_cv_series.Length == 0) throw new ArgumentOutOfRangeException(nameof(uip), $@"{module_name}.{method_name}");
            if (uip.group_series == null || uip.group_series.Length == 0) throw new ArgumentOutOfRangeException(nameof(uip), $@"{module_name}.{method_name}");

            var unrolled_whole_index = 0;
            //var unrolled_partition_indexes = new int[total_instances];
            //var unrolled_instance_id = 0;

            var r_cv_series_len = uip.r_cv_series.Length;
            var o_cv_series_len = uip.o_cv_series.Length;
            var group_series_len = uip.group_series.Length;
            var p_svm_types_len = uip.svm_types.Length;
            var p_kernels_len = uip.kernels.Length;
            var p_scales_len = uip.scales.Length;
            var i_cv_series_len = uip.i_cv_series.Length;
            var p_class_weight_sets_len = (uip.class_weight_sets?.Length ?? 1) > 0 ? (uip.class_weight_sets?.Length ?? 1) : 1;

            var len_product = r_cv_series_len * o_cv_series_len * group_series_len * p_svm_types_len * p_kernels_len * p_scales_len * i_cv_series_len * p_class_weight_sets_len;
            var indexes_whole = new index_data[len_product];

            for (var z_r_cv_series_index = 0; z_r_cv_series_index < uip.r_cv_series.Length; z_r_cv_series_index++)//1
            {
                for (var z_o_cv_series_index = 0; z_o_cv_series_index < uip.o_cv_series.Length; z_o_cv_series_index++)//2
                {
                    // for the current number of R and O, set the folds (which vary according to the values of R and O).
                    var (class_folds, down_sampled_train_class_folds) = routines.folds(cts, dataset.class_sizes, uip.r_cv_series[z_r_cv_series_index], uip.o_cv_series[z_o_cv_series_index] /*, outer_cv_folds_to_run*/);

                    for (var z_svm_types_index = 0; z_svm_types_index < uip.svm_types.Length; z_svm_types_index++)//4
                    {
                        for (var z_kernels_index = 0; z_kernels_index < uip.kernels.Length; z_kernels_index++)//5
                        {
                            for (var z_scales_index = 0; z_scales_index < uip.scales.Length; z_scales_index++)//6
                            {
                                for (var z_i_cv_series_index = 0; z_i_cv_series_index < uip.i_cv_series.Length; z_i_cv_series_index++)//7
                                {
                                    for (var z_class_weight_sets_index = 0; z_class_weight_sets_index <= (uip.class_weight_sets?.Length ?? 0); z_class_weight_sets_index++)//8
                                    {
                                        if (uip.class_weight_sets != null && uip.class_weight_sets.Length > 0 && z_class_weight_sets_index >= uip.class_weight_sets.Length) continue;
                                        var class_weights = uip.class_weight_sets == null || z_class_weight_sets_index >= uip.class_weight_sets.Length ? null : uip.class_weight_sets[z_class_weight_sets_index];

                                        for (var z_group_series_index = 0; z_group_series_index < uip.group_series.Length; z_group_series_index++)//3
                                        {
                                            var index_data = new index_data()
                                            {
                                                //unrolled_partition_index = unrolled_partition_indexes[unrolled_instance_id],
                                                //unrolled_instance_id = unrolled_instance_id,

                                                group_array_index = uip.group_series[z_group_series_index].group_array_index,
                                                selection_direction = uip.group_series[z_group_series_index].selection_direction,
                                                group_array_indexes = uip.group_series[z_group_series_index].group_indexes,
                                                column_array_indexes = uip.group_series[z_group_series_index].column_indexes,
                                                num_groups = uip.group_series[z_group_series_index].group_indexes.Length,
                                                num_columns = uip.group_series[z_group_series_index].column_indexes.Length,
                                                group_folder = uip.group_series[z_group_series_index].group_folder,
                                                group_key = uip.group_series[z_group_series_index].group_key,

                                                experiment_name = experiment_name,
                                                unrolled_whole_index = unrolled_whole_index,
                                                iteration_index = iteration_index,
                                                calc_11p_thresholds = uip.calc_11p_thresholds,
                                                repetitions = uip.r_cv_series[z_r_cv_series_index],
                                                outer_cv_folds = uip.o_cv_series[z_o_cv_series_index],
                                                outer_cv_folds_to_run = outer_cv_folds_to_run,
                                                svm_type = uip.svm_types[z_svm_types_index],
                                                svm_kernel = uip.kernels[z_kernels_index],
                                                scale_function = uip.scales[z_scales_index],
                                                inner_cv_folds = uip.i_cv_series[z_i_cv_series_index],
                                                total_instances = total_instances,
                                                total_whole_indexes = len_product,

                                                class_weights = class_weights,
                                                class_folds = class_folds,
                                                down_sampled_train_class_folds = down_sampled_train_class_folds,
                                                total_groups = total_groups,
                                                //is_job_completed = false,

                                                unrolled_instance_id = -1,
                                                unrolled_partition_index = -1,
                                                total_partition_indexes = -1,
                                            };

                                            indexes_whole[unrolled_whole_index++] = index_data;

                                            //unrolled_partition_indexes[unrolled_instance_id++]++;

                                            //if (unrolled_instance_id >= total_instances) unrolled_instance_id = 0;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (unrolled_whole_index != len_product) throw new Exception();

            var index_data_container = redistribute_work(cts, indexes_whole, instance_id, total_instances);

            return index_data_container;
        }

        internal static index_data_container redistribute_work(CancellationTokenSource cts, index_data_container index_data_container, int instance_id, int total_instances)
        {
            return redistribute_work(cts, index_data_container.indexes_whole, instance_id, total_instances);
        }

        internal static index_data_container redistribute_work(CancellationTokenSource cts, index_data[] indexes_whole, int instance_id, int total_instances, bool as_parallel = true)
        {
            const string method_name = nameof(redistribute_work);

            if (cts.IsCancellationRequested) return default;

            if (indexes_whole == null || indexes_whole.Length == 0) throw new ArgumentOutOfRangeException(nameof(indexes_whole), $@"{module_name}.{method_name}.{nameof(indexes_whole)}");

            var index_data_container = new index_data_container();
            index_data_container.indexes_whole = indexes_whole;

            var unrolled_whole_index = 0;
            var unrolled_partition_indexes = new int[total_instances];
            var unrolled_instance_id = 0;

            for (var x = 0; x < index_data_container.indexes_whole.Length; x++)
            {
                // unique id for index_data
                index_data_container.indexes_whole[x].unrolled_whole_index = unrolled_whole_index++;
                index_data_container.indexes_whole[x].unrolled_partition_index = unrolled_partition_indexes[unrolled_instance_id];
                index_data_container.indexes_whole[x].total_whole_indexes = index_data_container.indexes_whole.Length;

                // instance id
                index_data_container.indexes_whole[x].total_instances = total_instances;
                index_data_container.indexes_whole[x].unrolled_instance_id = unrolled_instance_id;

                unrolled_partition_indexes[unrolled_instance_id++]++;

                if (unrolled_instance_id >= total_instances) unrolled_instance_id = 0;
            }

            if (as_parallel)
            {
                Parallel.For(0, index_data_container.indexes_whole.Length, index =>
                {
                    index_data_container.indexes_whole[index].total_partition_indexes = unrolled_partition_indexes[index_data_container.indexes_whole[index].unrolled_instance_id];
                });
            }
            else
            {
                for (var index = 0; index < index_data_container.indexes_whole.Length; index++)
                {
                    index_data_container.indexes_whole[index].total_partition_indexes = unrolled_partition_indexes[index_data_container.indexes_whole[index].unrolled_instance_id];
                }
            }

            index_data_container.indexes_partitions = as_parallel
                ?
                index_data_container.indexes_whole.AsParallel().AsOrdered().WithCancellation(cts.Token).GroupBy(a => a.unrolled_instance_id).OrderBy(a => a.Key).Select(a => a.ToArray()).ToArray()
                :
                index_data_container.indexes_whole.GroupBy(a => a.unrolled_instance_id).OrderBy(a => a.Key).Select(a => a.ToArray()).ToArray();

            index_data_container.indexes_partition = index_data_container.indexes_partitions.FirstOrDefault(a => (a.FirstOrDefault()?.unrolled_instance_id ?? null) == instance_id);

            if (index_data_container.indexes_partitions.Any(a => a.FirstOrDefault().total_partition_indexes != unrolled_partition_indexes[index_data_container.indexes_whole[a.FirstOrDefault().unrolled_instance_id].unrolled_instance_id])) throw new Exception($@"{module_name}.{method_name}");

            //index_data_container.indexes_partition = index_data_container.indexes_whole.AsParallel().AsOrdered().WithCancellation(cts.Token).Where(a => a.unrolled_instance_id == instance_id).ToArray();

            // shuffle with actual random seed, otherwise same 'random' work would be selected
            // if finished own work, do others  and save  group file

            return index_data_container;
        }



        internal static void load_cache(
            CancellationTokenSource cts, 
            //(dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] groups, 
            int instance_id, 
            int iteration_index,
            string experiment_name,
            bool wait_for_cache, 
            List<string> cache_files_already_loaded, 
            List<(index_data id, confusion_matrix cm)> iteration_cm_sd_list,
            index_data_container index_data_container,
            (index_data id, confusion_matrix cm, rank_score rs)[] last_iteration_id_cm_rs,
            (index_data id, confusion_matrix cm, rank_score rs) last_winner_id_cm_rs, 
            (index_data id, confusion_matrix cm, rank_score rs) best_winner_id_cm_rs,
            bool as_parallel = true
            )
        {
            const string method_name = nameof(load_cache);

            if (cts.IsCancellationRequested) return;// default;

            const string summary_tag = @"_summary.cm.csv";
            const string full_tag = @"_full.cm.csv";

            // a single group may have multiple tests... e.g. different number of inner-cv, outer-cv, class_weights, etc...
            // therefore, group_index existing isn't enough, must also have the various other parameters



            if (instance_id < 0) throw new ArgumentOutOfRangeException(nameof(instance_id), $@"{module_name}.{method_name}");
            if (iteration_index < 0) throw new ArgumentOutOfRangeException(nameof(iteration_index), $@"{module_name}.{method_name}");
            if (string.IsNullOrWhiteSpace(experiment_name)) throw new ArgumentOutOfRangeException(nameof(experiment_name), $@"{module_name}.{method_name}");
            if (cache_files_already_loaded == null) throw new ArgumentOutOfRangeException(nameof(cache_files_already_loaded), $@"{module_name}.{method_name}");
            if (iteration_cm_sd_list == null) throw new ArgumentOutOfRangeException(nameof(iteration_cm_sd_list), $@"{module_name}.{method_name}");
            if (last_iteration_id_cm_rs == null && iteration_index != 0) throw new ArgumentOutOfRangeException(nameof(last_iteration_id_cm_rs), $@"{module_name}.{method_name}");
            if (last_winner_id_cm_rs == default && iteration_index != 0) throw new ArgumentOutOfRangeException(nameof(last_winner_id_cm_rs), $@"{module_name}.{method_name}");
            if (best_winner_id_cm_rs == default && iteration_index != 0) throw new ArgumentOutOfRangeException(nameof(best_winner_id_cm_rs), $@"{module_name}.{method_name}");

            // check which indexes are missing.
            update_missing(cts, instance_id, iteration_cm_sd_list, index_data_container);

            // if all indexes loaded, return
            if (!index_data_container.indexes_missing_whole.Any()) return;// index_data_container;


            var iteration_folder = program.get_iteration_folder(settings.results_root_folder, experiment_name, iteration_index);

            var cache_level_whole = (name: "whole", marker: 'z');
            var cache_level_partition = (name: "partition", marker: 'x');
            var cache_level_group = (name: "group", marker: 'm');
            var cache_levels = new (string name, char marker)[] { cache_level_whole, cache_level_partition, cache_level_group };

            do
            {
                if (!Directory.Exists(iteration_folder)) continue;

                // limit m to current partition?

                foreach (var cache_level in cache_levels)
                {
                    // don't load if already loaded..
                    if (!index_data_container.indexes_missing_whole.Any()) break;

                    // only load m* for the partition... pt1.
                    if (cache_level == cache_level_group && !index_data_container.indexes_missing_partition.Any()) continue;

                    // load cache, if exists (z, x, m)
                    var cache_files1 = Directory.GetFiles(iteration_folder, $"{cache_level.marker}_*.cm.csv", cache_level == cache_level_group ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly);//.ToList();

                    // don't load any files already previously loaded...
                    cache_files1 = cache_files1.Except(cache_files_already_loaded).Distinct().ToArray();

                    var cache_files2 = cache_files1
                        .Select(filename =>
                        {
                            var full = filename.EndsWith(full_tag) ? filename : filename.Replace(summary_tag, full_tag);
                            var summary = filename.EndsWith(summary_tag) ? filename : filename.Replace(full_tag, summary_tag);

                            return (full, summary);
                            //return full;
                        })
                        .Distinct()
                        .ToArray();

                    cache_files1 = cache_files2.Select(filename =>
                        {
                            if (cache_files_already_loaded.Contains(filename.full)) return null;
                            //if (cache_files_already_loaded.Contains(filename.summary)) return null;

                            if (cache_files1.Contains(filename.full)) return filename.full;
                            //if (cache_files1.Contains(filename.summary)) return filename.summary;

                            return null;
                        })
                        .Where(a => a != null)
                        .ToArray();

                    // don't load if 'm' and not in partition
                    if (cache_level == cache_level_group)
                    {
                        // only load m* for the hpc instance partition... pt2.
                        var merge_files =
                            as_parallel ?
                                index_data_container.indexes_missing_partition
                                    .AsParallel()
                                    .AsOrdered()
                                    .WithCancellation(cts.Token)
                                    .Select(a =>
                                    {

                                        //var summary = $@"{Path.Combine(program.get_iteration_folder(settings.results_root_folder, experiment_name, a.iteration_index, a.group_array_index), $@"m_{program.get_iteration_filename(new[] { a })}")}{summary_tag}";
                                        var full = $@"{Path.Combine(program.get_iteration_folder(settings.results_root_folder, experiment_name, a.iteration_index, a.group_array_index), $@"m_{program.get_iteration_filename(new[] { a })}")}{full_tag}";

                                        if (cache_files1.Contains(full)) return full;
                                        //if (cache_files1.Contains(summary)) return summary;

                                        return null;
                                    })
                                    .Where(a => a != null)
                                    .ToList() :
                                index_data_container.indexes_missing_partition
                                    .Select(a =>
                                    {

                                        //var summary = $@"{Path.Combine(program.get_iteration_folder(settings.results_root_folder, experiment_name, a.iteration_index, a.group_array_index), $@"m_{program.get_iteration_filename(new[] { a })}")}{summary_tag}";
                                        var full = $@"{Path.Combine(program.get_iteration_folder(settings.results_root_folder, experiment_name, a.iteration_index, a.group_array_index), $@"m_{program.get_iteration_filename(new[] { a })}")}{full_tag}";

                                        if (cache_files1.Contains(full)) return full;
                                        //if (cache_files1.Contains(summary)) return summary;

                                        return null;
                                    })
                                    .Where(a => a != null)
                                    .ToList();

                        cache_files1 = cache_files1.Intersect(merge_files).ToArray();
                    }


                    load_cache_file_list(cts, /*groups,*/ instance_id, cache_files_already_loaded, iteration_cm_sd_list, index_data_container, cache_files1, as_parallel); //last_iteration_id_cm_rs, last_winner_id_cm_rs, best_winner_id_cm_rs,
                }

                if (wait_for_cache && index_data_container.indexes_missing_whole.Any()) { io_proxy.wait(cts, 15, 30); }

            } while (wait_for_cache && index_data_container.indexes_missing_whole.Any());

            //return index_data_container;
        }

        private static void
            load_cache_file_list
            (
                CancellationTokenSource cts,
                //(dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] groups,
                int instance_id,
                List<string> cache_files_already_loaded,
                List<(index_data id, confusion_matrix cm)> iteration_id_cm,
                index_data_container index_data_container,
                //(index_data id, confusion_matrix cm, rank_score rs)[] last_iteration_id_cm_rs,
                //(index_data id, confusion_matrix cm, rank_score rs) last_winner_id_cm_rs,
                //(index_data id, confusion_matrix cm, rank_score rs) best_winner_id_cm_rs,
                string[] cache_files,
                bool as_parallel = true
            )
        {
            const string method_name = nameof(load_cache_file_list);

            if (cts.IsCancellationRequested) return;
            if (cache_files == null || cache_files.Length == 0) return;

            //var gk_list1 = groups.Select(a => a.group_key).ToArray();
            //var gk_list2 = groups.SelectMany(a => a.group_column_headers).ToArray();
            // load and parse cm files
            var cache_files_cm_list =
                as_parallel ?
                    cache_files
                        .AsParallel()
                        .AsOrdered()
                        .WithCancellation(cts.Token)
                        .Select(cm_fn =>
                        {
                            if (cts.IsCancellationRequested) return null;
                            if (io_proxy.is_file_available(cts, cm_fn, false, module_name, method_name))
                            {
                                var cm = confusion_matrix.load(cts, cm_fn);
                                if (cm != null && cm.Length > 0)
                                {
                                    for (var cm_index = 0; cm_index < cm.Length; cm_index++)
                                    {
                                        if (cm[cm_index].unrolled_index_data != null)
                                        {
                                            var id = index_data.find_reference(index_data_container.indexes_whole, cm[cm_index].unrolled_index_data);
                                            if (id == null) throw new Exception();
                                            cm[cm_index].unrolled_index_data = id;

                                            //var gk = dataset_group_key.find_reference(gk_list1, cm[cm_index].unrolled_index_data.group_key);
                                            //if (gk == null) gk = dataset_group_key.find_reference(gk_list2, cm[cm_index].unrolled_index_data.group_key);
                                            //if (gk != null) cm[cm_index].unrolled_index_data.group_key = gk;
                                        }
                                    }
                                }
                                return cm;
                            }
                            return null;
                        })
                        .ToArray() :
                    cache_files
                        .Select(cm_fn =>
                        {
                            if (cts.IsCancellationRequested) return null;
                            if (io_proxy.is_file_available(cts, cm_fn, false, module_name, method_name))
                            {
                                var cm = confusion_matrix.load(cts, cm_fn);
                                if (cm != null && cm.Length > 0)
                                {
                                    for (var cm_index = 0; cm_index < cm.Length; cm_index++)
                                    {
                                        if (cm[cm_index].unrolled_index_data != null)
                                        {
                                            var id = index_data.find_reference(index_data_container.indexes_whole, cm[cm_index].unrolled_index_data);
                                            if (id == null) throw new Exception();
                                            cm[cm_index].unrolled_index_data = id;
                                        }
                                    }
                                }
                                return cm;
                            }
                            return null;
                        })
                        .ToArray();

            // record filenames to list of files already loaded
            //lock (cache_files_already_loaded)
            {
                cache_files_already_loaded.AddRange(cache_files.Where((a, i) => cache_files_cm_list[i] != null && cache_files_cm_list[i].Length > 0 && cache_files_cm_list[i].Any(cm => cm != null)).ToList());
            }

            for (var cms_index = 0; cms_index < cache_files_cm_list.Length; cms_index++)
            {
                var cm_file = cache_files[cms_index];

                if (!index_data_container.indexes_missing_whole.Any()) break;

                var cache_files_cm_list_cms_index = cache_files_cm_list[cms_index];

                if (cache_files_cm_list_cms_index == null || cache_files_cm_list_cms_index.Length == 0) continue;

                //lock (iteration_cm_sd_list)
                {
                    // limit cm to those indexes not already loaded
                    var id_cm_sd_append = as_parallel ?
                        cache_files_cm_list_cms_index
                            .AsParallel()
                            .AsOrdered()
                            .WithCancellation(cts.Token)
                            .Select((cm, cm_index) => (id: cm?.unrolled_index_data, cm: cm))
                            .Where(a => a.id != null && a.cm != null)
                            .ToArray()
                        :
                        cache_files_cm_list_cms_index
                            .Select((cm, cm_index) => (id: cm?.unrolled_index_data, cm: cm))
                            .Where(a => a.id != null && a.cm != null)
                            .ToArray();


                    if (id_cm_sd_append.Any())
                    {
                        iteration_id_cm.AddRange(id_cm_sd_append);

                        update_missing(cts, instance_id, iteration_id_cm, index_data_container, as_parallel);

                        io_proxy.WriteLine($@"Loaded cache: {cm_file}. id_cm_sd_append: {id_cm_sd_append.Length}. iteration_cm_all: {iteration_id_cm.Count}. indexes_loaded_whole: {index_data_container.indexes_loaded_whole.Length}. indexes_loaded_partition: {index_data_container.indexes_loaded_partition.Length}. indexes_missing_whole: {index_data_container.indexes_missing_whole.Length}. indexes_missing_partition: {index_data_container.indexes_missing_partition.Length}.", module_name, method_name);
                    }
                }
            }
        }

        internal static void
            update_missing(
                CancellationTokenSource cts,
                int instance_id,
                List<(index_data id, confusion_matrix cm)> iteration_cm_all,
                index_data_container index_data_container,
                bool as_parallel = true
            )
        {
            // this method checks 'iteration_cm_all' to see which results are already loaded and which results are missing
            // note: if necessary, call redistribute_work().

            var iteration_cm_all_id = iteration_cm_all.Select(a => a.id).ToArray();
            //var iteration_cm_all_cm = iteration_cm_all.Select(a => a.cm).ToArray();

            // find loaded indexes
            index_data_container.indexes_loaded_whole =
                iteration_cm_all_id == null || iteration_cm_all_id.Length == 0 ?
                    Array.Empty<index_data>() :
                (
                    as_parallel ?
                    index_data_container.indexes_whole.AsParallel().AsOrdered().WithCancellation(cts.Token).Where(id => index_data.find_reference(iteration_cm_all_id, id) != null).ToArray() :
                    index_data_container.indexes_whole.Where(id => index_data.find_reference(iteration_cm_all_id, id) != null).ToArray()
                );

            index_data_container.indexes_missing_whole = index_data_container.indexes_whole.Except(index_data_container.indexes_loaded_whole).ToArray();

            index_data_container.indexes_loaded_partitions = as_parallel ?
                index_data_container.indexes_partitions.AsParallel().AsOrdered().WithCancellation(cts.Token).Select(ip => ip.Intersect(index_data_container.indexes_loaded_whole).ToArray()).ToArray() :
                index_data_container.indexes_partitions.Select(ip => ip.Intersect(index_data_container.indexes_loaded_whole).ToArray()).ToArray();

            index_data_container.indexes_loaded_partition = index_data_container.indexes_loaded_partitions.Length > instance_id ? index_data_container.indexes_loaded_partitions[instance_id] : null;

            index_data_container.indexes_missing_partitions = as_parallel ?
                index_data_container.indexes_partitions.AsParallel().AsOrdered().WithCancellation(cts.Token).Select((ip, i) => ip.Except(index_data_container.indexes_loaded_partitions[i]).ToArray()).ToArray() :
                index_data_container.indexes_partitions.Select((ip, i) => ip.Except(index_data_container.indexes_loaded_partitions[i]).ToArray()).ToArray();

            index_data_container.indexes_missing_partition = index_data_container.indexes_missing_partitions.Length > instance_id ? index_data_container.indexes_missing_partitions[instance_id] : null;
        }
    }
}
/*internal static (index_data[] indexes_whole, index_data[] indexes_partition) get_unrolled_indexes_check_bias(CancellationTokenSource cts, int search_type, dataset_loader dataset, string experiment_name, int iteration_index, int total_groups, int instance_id, int total_instances)
        {
            if (cts.IsCancellationRequested) return default;


            // for a specific set of features (i.e. the final result from feature selection):

            // what happens to performance if we vary the scale & kernel (is it stable, does it increase, decrease, or randomise?)

            if (search_type == 0)
            {
                var p1 = new unrolled_indexes_parameters
                {
                    group_series_start = -1,
                    group_series_end = -1,

                    calc_11p_thresholds = true,

                    scales = new[]
                    {
                        scaling.scale_function.none,
                        scaling.scale_function.normalisation,
                        scaling.scale_function.rescale,
                        scaling.scale_function.standardisation,
                        scaling.scale_function.L0_norm,
                        scaling.scale_function.L1_norm,
                        scaling.scale_function.L2_norm,
                    },

                    kernels = new[]
                    {
                        routines.libsvm_kernel_type.linear,
                        routines.libsvm_kernel_type.polynomial,
                        routines.libsvm_kernel_type.sigmoid,
                        routines.libsvm_kernel_type.rbf
                    }
                };

                var variations_1 = get_unrolled_indexes(cts, dataset, experiment_name, iteration_index, total_groups, instance_id, total_instances, p1);
                return variations_1;
            }
            else if (search_type == 1)
            {

                // what happens if we vary the repetitions, outer-cv, inner-cv
                var p2 = new unrolled_indexes_parameters
                {
                    group_series_start = -1,
                    group_series_end = -1,

                    calc_11p_thresholds = true,

                    // repetitions: (10 - (1 - 1)) / 1 = 10

                    r_cv_series_start = 1,
                    r_cv_series_end = 10,
                    r_cv_series_step = 1,

                    // outer-cv: (10 - (2 - 1)) / 1 = 9

                    o_cv_series_start = 2,
                    o_cv_series_end = 10,
                    o_cv_series_step = 1,

                    // inner-cv: (10 - (2 - 1)) / 1 = 9

                    i_cv_series_start = 1, // start at 1 to test skipping inner-cv, to show what performance increase is obtained through inner-cv
                    i_cv_series_end = 10,
                    i_cv_series_step = 1
                };

                var variations_2 = get_unrolled_indexes(cts, dataset, experiment_name, iteration_index,total_groups, instance_id, total_instances, p2);

                return variations_2;
            }
            else if (search_type == 2)
            {

                // what happens if we vary the weights 
                var weight_values = new double[]
                {
                    200,
                    100,
                    50,
                    10,
                    5,
                    2,
                    1,
                    0.75,
                    0.5,
                    0.25,
                    0,
                    -0.25,
                    -0.5,
                    -0.75,
                    -1,
                    -2,
                    -5,
                    -10,
                    -50,
                    -100,
                    -200
                }; // default: 1 // 21 values

                var p3 = new unrolled_indexes_parameters
                {
                    group_series_start = -1,
                    group_series_end = -1,

                    calc_11p_thresholds = true,
                    class_weight_sets = new (int class_id, double class_weight)[weight_values.Length * weight_values.Length][]
                };

                var k = 0;
                for (var wi_index = 0; wi_index < weight_values.Length; wi_index++)
                {
                    for (var wj_index = 0; wj_index < weight_values.Length; wj_index++)
                    {
                        p3.class_weight_sets[k++] = new (int class_id, double class_weight)[2] { (+1, weight_values[wi_index]), (-1, weight_values[wj_index]) };
                    }
                }

                var variations_3 = get_unrolled_indexes(cts, dataset, experiment_name, iteration_index, total_groups, instance_id, total_instances, p3);

                return variations_3;
            }
            else
            {
                // return null when no tests are left
                return (null, null);
            }
        }*/