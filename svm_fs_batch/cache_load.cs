using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal class cache_load
    {
        public const string module_name = nameof(cache_load);

        internal static (index_data[] indexes_whole, index_data[] indexes_partition) get_unrolled_indexes_basic(dataset_loader dataset, string experiment_name, int iteration_index, /*string iteration_name,*/ int total_groups, int instance_index, int total_instances,
             int repetitions, int outer_cv_folds, int outer_cv_folds_to_run, int inner_folds, int[] selection_excluded_groups)
        {
            var p = new unrolled_indexes_parameters()
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

                group_series_start = total_groups < 0 ? total_groups : 0,
                group_series_end = total_groups < 0 ? total_groups : (total_groups > 0 ? total_groups - 1 : 0)
            };

            return get_unrolled_indexes(dataset, experiment_name, iteration_index, total_groups, instance_index, total_instances, p, outer_cv_folds_to_run, selection_excluded_groups);
        }

        internal static (index_data[] indexes_whole, index_data[] indexes_partition) get_unrolled_indexes_check_bias(int search_type, dataset_loader dataset, string experiment_name, int iteration_index, int total_groups, int instance_index, int total_instances)
        {
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

                var variations_1 = get_unrolled_indexes(dataset, experiment_name, iteration_index, /*iteration_name,*/ total_groups, instance_index, total_instances, p1);
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

                var variations_2 = get_unrolled_indexes(dataset, experiment_name, iteration_index, /*iteration_name,*/ total_groups, instance_index, total_instances, p2);

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

                var variations_3 = get_unrolled_indexes(dataset, experiment_name, iteration_index, /*iteration_name,*/ total_groups, instance_index, total_instances, p3);

                return variations_3;
            }
            else
            {
                // return null when no tests are left
                return (null, null);
            }
        }

        public static int for_iterations(int start, int end, int step)
        {
            // step should never be zero
            if (step == 0) throw new DivideByZeroException();

            return end >= start ? ((end - start) / step) + 1 : 0;
        }

        public static int[] range(int start, int end, int step)
        {
            var ix = for_iterations(start, end, step);

            var ret = new int[ix];

            var step_sum = start;

            for (var i = 0; i < ix; i++)
            {
                ret[i] = step_sum;

                step_sum += step;
            }

            return ret;
        }

        internal static (index_data[] indexes_whole, index_data[] indexes_partition) get_unrolled_indexes
        (
            dataset_loader dataset,
            string experiment_name,
            int iteration_index,
            //string iteration_name,
            int total_groups,
            int instance_index,
            int total_instances,
            unrolled_indexes_parameters p,
            int outer_cv_folds_to_run = 0,
            int[] selection_excluded_groups = null
        )
        {
            if (p.svm_types == null || p.svm_types.Length == 0) p.svm_types = new[] { routines.libsvm_svm_type.c_svc };
            if (p.kernels == null || p.kernels.Length == 0) p.kernels = new[] { routines.libsvm_kernel_type.rbf };
            if (p.scales == null || p.scales.Length == 0) p.scales = new[] { scaling.scale_function.rescale };

            var r_cv_series = range(p.r_cv_series_start, p.r_cv_series_end, p.r_cv_series_step);
            var o_cv_series = range(p.o_cv_series_start, p.o_cv_series_end, p.o_cv_series_step);
            var i_cv_series = range(p.i_cv_series_start, p.i_cv_series_end, p.i_cv_series_step);
            var group_series = range(p.group_series_start, p.group_series_end, 1);

            if (selection_excluded_groups != null && selection_excluded_groups.Length > 0)
            {
                group_series = group_series.Except(selection_excluded_groups).ToArray();
            }

            if (r_cv_series == null || r_cv_series.Length == 0) throw new Exception();
            if (o_cv_series == null || o_cv_series.Length == 0) throw new Exception();
            if (i_cv_series == null || i_cv_series.Length == 0) throw new Exception();
            if (group_series == null || group_series.Length == 0) throw new Exception();

            var unrolled_whole_index = 0;
            var unrolled_partition_indexes = new int[total_instances];
            var unrolled_instance_index = 0;

            var r_cv_series_len = r_cv_series.Length;
            var o_cv_series_len = o_cv_series.Length;
            var group_series_len = group_series.Length;
            var p_svm_types_len = p.svm_types.Length;
            var p_kernels_len = p.kernels.Length;
            var p_scales_len = p.scales.Length;
            var i_cv_series_len = i_cv_series.Length;
            var p_class_weight_sets_len = (p.class_weight_sets?.Length ?? 1) > 0 ? (p.class_weight_sets?.Length ?? 1) : 1;

            var indexes_whole = new index_data[
                r_cv_series_len *
                o_cv_series_len *
                group_series_len *
                p_svm_types_len *
                p_kernels_len *
                p_scales_len *
                i_cv_series_len *
                p_class_weight_sets_len
            ];

            for (var z_r_cv_series_index = 0; z_r_cv_series_index < r_cv_series.Length; z_r_cv_series_index++)//1
            {
                for (var z_o_cv_series_index = 0; z_o_cv_series_index < o_cv_series.Length; z_o_cv_series_index++)//2
                {
                    // for the current number of R and O, set the folds (which vary according to the values of R and O).
                    var (class_folds, down_sampled_training_class_folds) = routines.folds(dataset.class_sizes, r_cv_series[z_r_cv_series_index], o_cv_series[z_o_cv_series_index] /*, outer_cv_folds_to_run*/);

                    for (var z_group_series_index = 0; z_group_series_index < group_series.Length; z_group_series_index++)//3
                    {
                        for (var z_svm_types_index = 0; z_svm_types_index < p.svm_types.Length; z_svm_types_index++)//4
                        {
                            for (var z_kernels_index = 0; z_kernels_index < p.kernels.Length; z_kernels_index++)//5
                            {
                                for (var z_scales_index = 0; z_scales_index < p.scales.Length; z_scales_index++)//6
                                {
                                    for (var z_i_cv_series_index = 0; z_i_cv_series_index < i_cv_series.Length; z_i_cv_series_index++)//7
                                    {
                                        for (var z_class_weight_sets_index = 0; z_class_weight_sets_index <= (p.class_weight_sets?.Length ?? 0); z_class_weight_sets_index++)//8
                                        {
                                            if (p.class_weight_sets != null && p.class_weight_sets.Length > 0 && z_class_weight_sets_index >= p.class_weight_sets.Length) continue;

                                            var class_weights = p.class_weight_sets == null || z_class_weight_sets_index >= p.class_weight_sets.Length ? null : p.class_weight_sets[z_class_weight_sets_index];

                                            var index_data = new index_data()
                                            {
                                                experiment_group_name = experiment_name,
                                                unrolled_whole_index = unrolled_whole_index,
                                                unrolled_partition_index = unrolled_partition_indexes[unrolled_instance_index],
                                                unrolled_instance_index = unrolled_instance_index,
                                                iteration_index = iteration_index,
                                                //iteration_name = iteration_name,
                                                group_array_index = group_series[z_group_series_index],
                                                total_groups = total_groups,
                                                calc_11p_thresholds = p.calc_11p_thresholds,
                                                repetitions = r_cv_series[z_r_cv_series_index],
                                                outer_cv_folds = o_cv_series[z_o_cv_series_index],
                                                outer_cv_folds_to_run = outer_cv_folds_to_run,
                                                svm_type = p.svm_types[z_svm_types_index],
                                                svm_kernel = p.kernels[z_kernels_index],
                                                scale_function = p.scales[z_scales_index],
                                                inner_cv_folds = i_cv_series[z_i_cv_series_index],
                                                total_instances = total_instances,
                                                total_whole_indexes = -1,
                                                total_partition_indexes = -1,
                                                class_weights = class_weights,
                                                class_folds = class_folds,
                                                down_sampled_training_class_folds = down_sampled_training_class_folds,
                                            };

                                            indexes_whole[unrolled_whole_index++] = index_data;

                                            unrolled_partition_indexes[unrolled_instance_index++]++;

                                            if (unrolled_instance_index >= total_instances) unrolled_instance_index = 0;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            var indexes_partition = indexes_whole.AsParallel().AsOrdered().Where(a => a.unrolled_instance_index == instance_index).ToArray();

            Parallel.For(0,
                indexes_whole.Length,
                index =>
                //for (var index = 0; index < indexes_whole.Count; index++)
                {
                    indexes_whole[index].total_whole_indexes = unrolled_whole_index;
                    indexes_whole[index].total_partition_indexes = unrolled_partition_indexes[indexes_whole[index].unrolled_instance_index];
                });


            return (indexes_whole, indexes_partition);
        }


        internal static (index_data[] indexes_whole, index_data[] indexes_partition, index_data[] indexes_loaded_whole, index_data[] indexes_loaded_partition, index_data[] indexes_missing_whole, index_data[] indexes_missing_partition)
            load_cache
            (
                int instance_index,
                int iteration_index,
                //string iteration_name,
                string experiment_name,
                bool wait_for_cache,
                List<string> cache_files_already_loaded,
                List<(index_data id, confusion_matrix cm, score_data sd)> iteration_cm_sd_list,
                index_data[] indexes_whole,
                index_data[] indexes_partition,
                (index_data id, confusion_matrix cm, score_data sd, rank_data rd)[] last_iteration_cm_sd_rd_list,
                (index_data id, confusion_matrix cm, score_data sd, rank_data rd) last_winner_cm_sd_rd,
                (index_data id, confusion_matrix cm, score_data sd, rank_data rd) best_winner_cm_sd_rd
            )
        {
            // a single group may have multiple tests... e.g. different number of inner-cv, outer-cv, class_weights, etc...
            // therefore, group_index existing isn't enough, must also have the various other parameters

            const string method_name = nameof(load_cache);

            if (instance_index < 0) throw new ArgumentOutOfRangeException(nameof(instance_index));
            if (iteration_index < 0) throw new ArgumentOutOfRangeException(nameof(iteration_index));
            if (string.IsNullOrWhiteSpace(experiment_name)) throw new ArgumentOutOfRangeException(nameof(experiment_name));
            if (cache_files_already_loaded == null) throw new ArgumentOutOfRangeException(nameof(cache_files_already_loaded));
            if (iteration_cm_sd_list == null) throw new ArgumentOutOfRangeException(nameof(iteration_cm_sd_list));
            if (last_iteration_cm_sd_rd_list == null && iteration_index != 0) throw new ArgumentOutOfRangeException(nameof(last_iteration_cm_sd_rd_list));
            if (last_winner_cm_sd_rd == default && iteration_index != 0) throw new ArgumentOutOfRangeException(nameof(last_winner_cm_sd_rd));
            if (best_winner_cm_sd_rd == default && iteration_index != 0) throw new ArgumentOutOfRangeException(nameof(best_winner_cm_sd_rd));

            // check which indexes are missing.
            var loaded_state = get_missing(instance_index, iteration_cm_sd_list, indexes_whole, indexes_partition);

            // if all indexes loaded, return
            if (!loaded_state.indexes_missing_whole.Any()) return loaded_state;


            var iteration_folder = program.get_iteration_folder(settings.results_root_folder, experiment_name, iteration_index/*, iteration_name*/);

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
                    if (!loaded_state.indexes_missing_whole.Any()) break;

                    // only load m* for the partition... pt1.
                    if (cache_level == cache_level_group && !loaded_state.indexes_missing_partition.Any()) continue;

                    // load cache, if exists (z, x, m)
                    var cache_files1 = Directory.GetFiles(iteration_folder, $"{cache_level.marker}_*.cm.csv", cache_level == cache_level_group ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly).ToList();

                    // don't load any files already previously loaded...
                    cache_files1 = cache_files1.Except(cache_files_already_loaded).Distinct().ToList();

                    var cache_files2 = cache_files1
                        .Select(a =>
                        {
                            var summary = a.Contains("_summary.cm.csv") ? a : a.Replace(".cm.csv", "_summary.cm.csv");
                            var full = a.Replace("_summary.cm.csv", ".cm.csv");

                            return (summary, full);
                        })
                        .Distinct()
                        .ToList();

                    cache_files1 = cache_files2.Select(a =>
                        {
                            if (cache_files_already_loaded.Contains(a.summary)) return null;
                            if (cache_files_already_loaded.Contains(a.full)) return null;

                            if (cache_files1.Contains(a.full)) return a.full;

                            if (cache_files1.Contains(a.summary)) return a.summary;

                            return null;
                        })
                        .Where(a => a != null)
                        .ToList();

                    // don't load if 'm' and not in partition
                    if (cache_level == cache_level_group)
                    {
                        // only load m* for the hpc instance partition... pt2.
                        var merge_files = loaded_state.indexes_missing_partition
                            .AsParallel()
                            .AsOrdered()
                            .Select(a =>
                            {
                                var summary = $@"{Path.Combine(program.get_iteration_folder(settings.results_root_folder, experiment_name, a.iteration_index, /*iteration_name,*/ a.group_array_index), $@"m_{program.get_iteration_filename(new[] { a })}")}_summary.cm.csv";
                                var full = $@"{Path.Combine(program.get_iteration_folder(settings.results_root_folder, experiment_name, a.iteration_index, /*iteration_name,*/ a.group_array_index), $@"m_{program.get_iteration_filename(new[] { a })}")}.cm.csv";

                                if (cache_files1.Contains(full)) return full;

                                if (cache_files1.Contains(summary)) return summary;

                                return null;
                            })
                            .Where(a => a != null)
                            .ToList();

                        cache_files1 = cache_files1.Intersect(merge_files).ToList();
                    }


                    var cache_files_cm_list = cache_files1
                        .AsParallel()
                        .AsOrdered()
                        .Select(cm_fn =>
                        {
                            if (io_proxy.is_file_available(cm_fn))
                            {
                                var cm = confusion_matrix.load(cm_fn);

                                return cm;
                            }

                            return null;
                        })
                        .ToList();

                    //lock (cache_files_already_loaded)
                    {
                        cache_files_already_loaded.AddRange(cache_files1.Where((a, i) => cache_files_cm_list[i] != null && cache_files_cm_list[i].Any(cm => cm != null)).ToList());
                    }

                    for (var cms_index = 0; cms_index < cache_files_cm_list.Count; cms_index++)
                    {
                        var cm_file = cache_files1[cms_index];

                        if (!loaded_state.indexes_missing_whole.Any()) break;

                        var cache_files_cm_list_cms_index = cache_files_cm_list[cms_index];

                        if (cache_files_cm_list_cms_index == default) continue;

                        //lock (iteration_cm_sd_list)
                        {
                            // limit cm to those indexes not already loaded
                            var id_cm_sd_append = cache_files_cm_list_cms_index
                                .Select((cm, cm_index) =>
                                {
                                    if (cm == null) return (null, null, null);


                                    var id1 = loaded_state.indexes_missing_whole
                                        .FirstOrDefault(id2 =>
                                            cm != null &&
                                            id2 != null &&
                                            id2.iteration_index == cm.x_iteration_index &&
                                            id2.group_array_index == cm.x_group_array_index &&
                                            id2.total_groups == cm.x_total_groups &&
                                            id2.calc_11p_thresholds == cm.x_calc_11p_thresholds &&
                                            id2.repetitions == cm.x_repetitions_total &&
                                            id2.outer_cv_folds == cm.x_outer_cv_folds &&
                                            id2.svm_type == cm.x_svm_type &&
                                            id2.svm_kernel == cm.x_svm_kernel &&
                                            id2.scale_function == cm.x_scale_function &&
                                            id2.inner_cv_folds == cm.x_inner_cv_folds);

                                    if (id1 == null) return (null, null, null);

                                    var same_group = last_iteration_cm_sd_rd_list
                                    .FirstOrDefault
                                    (a =>
                                        a.sd.group_array_index == cm.x_group_array_index.Value &&
                                        a.sd.iteration_index + 1 == cm.x_iteration_index.Value &&
                                        a.sd.class_id == cm.x_class_id.Value
                                    );

                                    var sd = new score_data(
                                        cm: cm,
                                        same_group: same_group.sd,
                                        last_winner: last_winner_cm_sd_rd.sd,
                                        best_winner: best_winner_cm_sd_rd.sd
                                    );

                                    return (id:id1, cm:cm, sd:sd);
                                }).Where(a=>a.id != null && a.cm != null && a.sd != null).ToArray();


                            if (id_cm_sd_append.Any())
                            {
                                iteration_cm_sd_list.AddRange(id_cm_sd_append);

                                loaded_state = get_missing(instance_index, iteration_cm_sd_list, indexes_whole, indexes_partition);

                                io_proxy.WriteLine($@"Loaded {cache_level} cache: {cm_file}. id_cm_sd_append: {id_cm_sd_append.Length}. iteration_cm_all: {iteration_cm_sd_list.Count}. indexes_loaded_whole: {loaded_state.indexes_loaded_whole.Length}. indexes_loaded_partition: {loaded_state.indexes_loaded_partition.Length}. indexes_missing_whole: {loaded_state.indexes_missing_whole.Length}. indexes_missing_partition: {loaded_state.indexes_missing_partition.Length}.", module_name, method_name);
                            }
                        }
                    }
                }

                if (wait_for_cache && loaded_state.indexes_missing_whole.Any()) { Task.Delay(new TimeSpan(0, 0, 15)).Wait(); }

            } while (wait_for_cache && loaded_state.indexes_missing_whole.Any());

            return loaded_state;
        }

        internal static (index_data[] indexes_whole, index_data[] indexes_partition, index_data[] indexes_loaded_whole, index_data[] indexes_loaded_partition, index_data[] indexes_missing_whole, index_data[] indexes_missing_partition)
            get_missing(
                int instance_index,
                List<(index_data index_data, confusion_matrix cm, score_data sd)> iteration_cm_all,
                index_data[] indexes_whole,
                index_data[] indexes_partition
            )
        {
            // check iteration_cm_all to see what is already loaded and what is missing

            //lock (iteration_cm_all)
            {
                //var iteration_cm_all_cm = iteration_cm_all.Select(a => a.cm).ToArray();

                //lock (indexes_whole)
                {
                    if (indexes_partition == null || indexes_partition.Length == 0)
                    {
                        indexes_partition = indexes_whole.Where(a => a.unrolled_instance_index == instance_index).ToArray();
                    }

                    //var loaded_whole = indexes_whole
                    //    .Where
                    //    (a => iteration_cm_all_cm
                    //        .Any
                    //        (cm =>
                    //            a != default &&
                    //            cm != default &&
                    //            a.iteration_index == cm.x_iteration_index &&
                    //            a.group_array_index == cm.x_group_array_index &&
                    //            a.total_groups == cm.x_total_groups &&
                    //            a.calc_11p_thresholds == cm.x_calc_11p_thresholds &&
                    //            a.repetitions == cm.x_repetitions_total &&
                    //            a.outer_cv_folds == cm.x_outer_cv_folds &&
                    //            a.svm_kernel == cm.x_svm_kernel &&
                    //            a.scale_function == cm.x_scale_function &&
                    //            a.inner_cv_folds == cm.x_inner_cv_folds
                    //        )
                    //    ).ToArray();

                    var loaded_whole = indexes_whole
                        .Where
                        (id => iteration_cm_all
                            .Any
                            (ica =>
                                ica.index_data != default &&
                                id != default &&
                                id.iteration_index == ica.index_data.iteration_index &&
                                id.group_array_index == ica.index_data.group_array_index &&
                                id.total_groups == ica.index_data.total_groups &&
                                id.calc_11p_thresholds == ica.index_data.calc_11p_thresholds &&
                                id.repetitions == ica.index_data.repetitions &&
                                id.outer_cv_folds == ica.index_data.outer_cv_folds &&
                                id.svm_type == ica.index_data.svm_type &&
                                id.svm_kernel == ica.index_data.svm_kernel &&
                                id.scale_function == ica.index_data.scale_function &&
                                id.inner_cv_folds == ica.index_data.inner_cv_folds
                            )
                        ).ToArray();

                    var loaded_partition = instance_index > -1 ? loaded_whole.Where(a => a.unrolled_instance_index == instance_index).ToArray() : loaded_whole;

                    var missing_whole = indexes_whole.Except(loaded_whole).ToArray();

                    var missing_partition = instance_index > -1 ? missing_whole.Where(a => a.unrolled_instance_index == instance_index).ToArray() : missing_whole;

                    return (indexes_whole, indexes_partition, loaded_whole, loaded_partition, missing_whole, missing_partition);
                }
            }
        }
    }
}
