using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal class cache_load
    {
        
        internal static (List<index_data> indexes_whole, List<index_data> indexes_partition) get_unrolled_indexes_basic(dataset_loader dataset, int iteration_index, string iteration_name, int total_groups, int instance_index, int total_instances,
             int repetitions, int outer_cv_folds, int outer_cv_folds_to_run, int inner_folds)
        {
            var p = new unrolled_indexes_parameters()
            {
                // the parameter below are for bias checking... make sure performance isn't biased towards a certain value of r/o/i below
                r_cv_start = repetitions,
                r_cv_end = repetitions,
                r_cv_step = 1,
                
                o_cv_start = outer_cv_folds,
                o_cv_end = outer_cv_folds,
                o_cv_step = 1,

                i_cv_start = inner_folds,
                i_cv_end = inner_folds,
                i_cv_step = 1
            };

            return get_unrolled_indexes(dataset, iteration_index, iteration_name, total_groups, instance_index, total_instances, p, outer_cv_folds_to_run);
            
            //calc_11p_thresholds, svm_types, kernels, scales, class_weights, r_cv_start, r_cv_end, r_cv_step, o_cv_start, o_cv_end, o_cv_step, i_cv_start, i_cv_end, i_cv_step);
        }

        internal static (List<index_data> indexes_whole, List<index_data> indexes_partition) get_unrolled_indexes_check_bias(int search_type, dataset_loader dataset, int iteration_index, string iteration_name, int total_groups, int instance_index, int total_instances)
        {
            // for a specific set of features (i.e. the final result from feature selection):

            // what happens if we vary the scale & kernel

            if (search_type == 0)
            {
                var p1 = new unrolled_indexes_parameters
                {
                    group_start = -1,
                    group_end = -1,

                    calc_11p_thresholds = true,

                    scales = new List<scaling.scale_function>()
                    {
                        scaling.scale_function.none,
                        scaling.scale_function.normalisation,
                        scaling.scale_function.rescale,
                        scaling.scale_function.standardisation,
                        scaling.scale_function.L0_norm,
                        scaling.scale_function.L1_norm,
                        scaling.scale_function.L2_norm,
                    },

                    kernels = new List<routines.libsvm_kernel_type>()
                    {
                        routines.libsvm_kernel_type.linear,
                        routines.libsvm_kernel_type.polynomial,
                        routines.libsvm_kernel_type.sigmoid,
                        routines.libsvm_kernel_type.rbf
                    }
                };

                var variations_1 = get_unrolled_indexes(dataset, iteration_index, iteration_name, total_groups, instance_index, total_instances, p1);
                return variations_1;
            }
            else if (search_type == 1)
            {

                // what happens if we vary the repetitions, outer-cv, inner-cv
                var p2 = new unrolled_indexes_parameters
                {
                    group_start = -1,
                    group_end = -1,

                    calc_11p_thresholds = true,

                    // repetitions: (10 - (1 - 1)) / 1 = 10

                    r_cv_start = 1,
                    r_cv_end = 10,
                    r_cv_step = 1,

                    // outer-cv: (10 - (2 - 1)) / 1 = 9

                    o_cv_start = 2,
                    o_cv_end = 10,
                    o_cv_step = 1,

                    // inner-cv: (10 - (2 - 1)) / 1 = 9

                    i_cv_start = 1, // start at 1 to test skipping inner-cv, to show what performance increase is obtained through inner-cv
                    i_cv_end = 10,
                    i_cv_step = 1
                };

                var variations_2 = get_unrolled_indexes(dataset, iteration_index, iteration_name, total_groups, instance_index, total_instances, p2);

                return variations_2;
            }
            else if (search_type == 2)
            {

                // what happens if we vary the weights 
                var weight_values = new List<double>()
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
                    group_start = -1,
                    group_end = -1,

                    calc_11p_thresholds = true,
                    class_weight_sets = new List<List<(int class_id, double class_weight)>>()
                };

                foreach (var wi in weight_values)
                {
                    foreach (var wj in weight_values)
                    {
                        p3.class_weight_sets.Add(new List<(int class_id, double class_weight)>() { (+1, wi), (-1, wj) });
                    }
                }

                var variations_3 = get_unrolled_indexes(dataset, iteration_index, iteration_name, total_groups, instance_index, total_instances, p3);

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

        internal static (List<index_data> indexes_whole, List<index_data> indexes_partition) get_unrolled_indexes
        (
            dataset_loader dataset,
            int iteration_index, 
            string iteration_name,
            int total_groups,
            int instance_index, 
            int total_instances, 
            unrolled_indexes_parameters p,
            int outer_cv_folds_to_run = 0
        )
        {
            // set default scales functions and svm_kernel types
            //if (kernels == null) kernels = ((routines.libsvm_kernel_type[])Enum.GetValues(typeof(routines.libsvm_kernel_type))).Where(a => a != routines.libsvm_kernel_type.precomputed).ToList();
            //if (scales == null) scales = ((scaling.scale_function[])Enum.GetValues(typeof(scaling.scale_function))).ToList();

            // limit the scale_function functions and svm_kernel types to reduce compute load
            //if (class_weights == null) class_weights = new List<(int class_id, double class_weight)>();

            if (p.class_weight_sets == null || p.class_weight_sets.Count == 0)
            {
                p.class_weight_sets = new List<List<(int class_id, double class_weight)>> { new List<(int class_id, double class_weight)>() };
            }
            else
            {
                for (var i = 0; i < p.class_weight_sets.Count; i++)
                {
                    if (p.class_weight_sets[i] == null) p.class_weight_sets[i] = new List<(int class_id, double class_weight)>();
                }
            }

            if (p.svm_types == null || p.svm_types.Count == 0) p.svm_types = new List<routines.libsvm_svm_type>() { routines.libsvm_svm_type.c_svc };
            if (p.kernels == null || p.kernels.Count == 0) p.kernels = new List<routines.libsvm_kernel_type>() { routines.libsvm_kernel_type.rbf };
            if (p.scales == null || p.scales.Count == 0) p.scales = new List<scaling.scale_function>() { scaling.scale_function.rescale };


            var r_cv_series_index = -1;
            var r_cv_series = new int[for_iterations(p.r_cv_start, p.r_cv_end, p.r_cv_step)];
            for (var _repetitions_cv_folds = p.r_cv_start; _repetitions_cv_folds <= p.r_cv_end && r_cv_series_index < r_cv_series.Length - 1; _repetitions_cv_folds += p.r_cv_step) r_cv_series[++r_cv_series_index] = _repetitions_cv_folds;

            // todo: outer_cv_folds_to_run
            var o_cv_series_index = -1;

            var o_cv_series_count = for_iterations(p.o_cv_start, p.o_cv_end, p.o_cv_step);
            var o_cv_series = new int[o_cv_series_count];
            for (var _outer_cv_folds = p.o_cv_start; _outer_cv_folds <= p.o_cv_end && o_cv_series_index < o_cv_series.Length - 1; _outer_cv_folds += p.o_cv_step) o_cv_series[++o_cv_series_index] = _outer_cv_folds;

            var i_cv_series_index = -1;
            var i_cv_series = new int[for_iterations(p.i_cv_start, p.i_cv_end, p.i_cv_step)];
            for (var inner_cv_folds = p.i_cv_start; inner_cv_folds <= p.i_cv_end && i_cv_series_index < i_cv_series.Length - 1; inner_cv_folds += p.i_cv_step) i_cv_series[++i_cv_series_index] = inner_cv_folds;



            // should group series be the loop group index or the mixture of groups?

            var group_series_index = -1;
            var group_series = new int[total_groups]; // this is currently only the group_index , could also add the selected_group_indexes

            if (p.group_start == -1 && p.group_end == -1) group_series = new int[1];

            var group_end = p.group_start >= 0 && p.group_end == -1 ? total_groups - 1 : p.group_end;

            for (var g_series = p.group_start; g_series <= group_end; g_series++)
            {
                // default - set group_series to 0,1,2,3,...,(total_groups-1)
                group_series[++group_series_index] = g_series; //group_series.Add(g_series);
            }

            if (r_cv_series == null || r_cv_series.Length == 0) throw new Exception();
            if (o_cv_series == null || o_cv_series.Length == 0) throw new Exception();
            if (i_cv_series == null || i_cv_series.Length == 0) throw new Exception();
            if (group_series == null || group_series.Length == 0) throw new Exception();

            var unrolled_whole_index = 0;
            var unrolled_partition_indexes = new int[total_instances];
            var unrolled_instance_index = 0;
            var indexes_whole = new List<index_data>();

            foreach (var repetitions in r_cv_series) // e.g. 1 or 5, or 1,2,3,4,5,...
            {
                foreach (var outer_cv_folds in o_cv_series) // e.g. 5 or 10, or 1,2,3,4,5,...
                {
                    //var ida = (init_dataset_ret)null;
                    

                    

                    // for the current number of R and O, set the folds (which vary according to the values of R and O).
                    var (class_folds, down_sampled_training_class_folds) = routines.folds(dataset.class_sizes, repetitions, outer_cv_folds/*, outer_cv_folds_to_run*/);


                    foreach (var group_index in group_series) // ~7000
                    {
                        foreach (var svm_type in p.svm_types)
                        {
                            // loop through number of kernels
                            foreach (var svm_kernel in p.kernels)
                            {
                                // loop through number of scale_function functions
                                foreach (var scale_function in p.scales)
                                {
                                    // loop through number of inner folds
                                    foreach (var inner_folds in i_cv_series)
                                    {

                                        // todo: add class_weights,  
                                        // note: repetition index, outer cv index, etc. are not included because all folds represent 1 job

                                        foreach (var class_weights in p.class_weight_sets)
                                        {
                                            var index_data = new index_data()
                                            {
                                                unrolled_whole_index = unrolled_whole_index,
                                                unrolled_partition_index = unrolled_partition_indexes[unrolled_instance_index],
                                                unrolled_instance_index = unrolled_instance_index,
                                                iteration_index = iteration_index,
                                                iteration_name= iteration_name,
                                                group_array_index = group_index,
                                                total_groups = total_groups,
                                                calc_11p_thresholds = p.calc_11p_thresholds,
                                                repetitions = repetitions,
                                                outer_cv_folds = outer_cv_folds,
                                                outer_cv_folds_to_run = outer_cv_folds_to_run,
                                                svm_type = svm_type,
                                                svm_kernel = svm_kernel,
                                                scale_function = scale_function,
                                                inner_cv_folds = inner_folds,
                                                total_instances = total_instances,
                                                total_whole_indexes = -1,
                                                total_partition_indexes = -1,
                                                
                                                class_weights = class_weights,
                                                class_folds = class_folds,
                                                down_sampled_training_class_folds = down_sampled_training_class_folds,
                                            };

                                            indexes_whole.Add(index_data);

                                            unrolled_partition_indexes[unrolled_instance_index]++;

                                            unrolled_whole_index++;

                                            unrolled_instance_index++;
                                            if (unrolled_instance_index >= total_instances) unrolled_instance_index = 0;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            var indexes_partition = indexes_whole.Where(a => a.unrolled_instance_index == instance_index).ToList();

            //var total_whole_indexes = indexes_whole.Count;
            
            for (var index = 0; index < indexes_whole.Count; index++)
            {
                indexes_whole[index].total_whole_indexes = unrolled_whole_index;
                indexes_whole[index].total_partition_indexes = unrolled_partition_indexes[indexes_whole[index].unrolled_instance_index];
            }


            return (indexes_whole, indexes_partition);
        }


        internal static (List<index_data> indexes_loaded_whole, List<index_data> indexes_loaded_partition, List<index_data> indexes_missing_whole, List<index_data> indexes_missing_partition)
            load_cache
            (
                int instance_index,
                int iteration_index,
                string iteration_name,
                string experiment_name,
                bool wait_for_cache,
                List<string> cache_files_already_loaded,
                List<confusion_matrix> iteration_cm_all,
                List<index_data> indexes_whole,
                List<index_data> indexes_partition
            )
        {
            // a single group may have multiple tests... e.g. different number of inner-cv, outer-cv, class_weights, etc...
            // therefore, group_index existing isn't enough, must also have the various other parameters

            const string module_name = nameof(program);
            const string method_name = nameof(load_cache);

            if (cache_files_already_loaded == null) throw new Exception();
            if (iteration_cm_all == null) throw new Exception();

            // check which indexes are missing.
            var loaded_state = get_missing(instance_index, iteration_cm_all, indexes_whole);

            // if all indexes loaded, return
            if (!loaded_state.indexes_missing_whole.Any()) return loaded_state;


            var iteration_folder = program.get_iteration_folder(settings.results_root_folder, experiment_name, iteration_index, iteration_name);

            do
            {
                if (!Directory.Exists(iteration_folder)) continue;

                var cache_levels = new string[] { "z", "x", "m" };

                // limit m to current partition?

                foreach (var cache_level in cache_levels)
                {
                    // don't load if already loaded..
                    if (!loaded_state.indexes_missing_whole.Any()) break;

                    if (string.Equals(cache_level, cache_levels.Last(), StringComparison.InvariantCultureIgnoreCase))
                    {
                        // only load m* for the partition... pt1
                        if (!loaded_state.indexes_missing_partition.Any()) continue;
                    }

                    // load cache, if exists (z, x, m)
                    var cache_files = Directory.GetFiles(iteration_folder, $"{cache_level}_*.cm.csv", string.Equals(cache_level, "m", StringComparison.InvariantCultureIgnoreCase) ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly).ToList();

                    // don't load any files already previously loaded...
                    cache_files = cache_files.Except(cache_files_already_loaded).Distinct().ToList();

                    // don't load if 'm' and not in partition
                    if (string.Equals(cache_level, cache_levels.Last(), StringComparison.InvariantCultureIgnoreCase))
                    {
                        // only load m* for the partition... pt2
                        //var merge_files = indexes_partition.Select(a => $@"{Path.Combine(program.get_iteration_folder(init_dataset_ret.results_root_folder, experiment_name, a.iteration_index, a.group_index), $@"m_{program.get_iteration_filename(new[] { a })}")}.cm.csv").ToList();
                        var merge_files = loaded_state.indexes_missing_partition.Select(a => $@"{Path.Combine(program.get_iteration_folder(settings.results_root_folder, experiment_name, a.iteration_index, iteration_name, a.group_array_index), $@"m_{program.get_iteration_filename(new[] { a })}")}.cm.csv").ToList();

                        cache_files = cache_files.Intersect(merge_files).ToList();
                    }

                    
                    var cache_files_cm_list = cache_files.AsParallel()
                        .AsOrdered()
                        .Select(cm_fn =>
                        {
                            if (io_proxy.is_file_available(cm_fn))
                            {
                                var cm = confusion_matrix.load(cm_fn);

                                return cm;
                            }

                            return default;
                        })
                        .ToList();

                    lock (cache_files_already_loaded) { cache_files_already_loaded.AddRange(cache_files.Where((a, i) => cache_files_cm_list[i] != default && cache_files_cm_list[i].Any(b => b != default /*&& b.cm != default*/)).ToList()); }

                    for (var cms_index = 0; cms_index < cache_files_cm_list.Count; cms_index++)
                    {
                        var cm_file = cache_files[cms_index];

                        if (!loaded_state.indexes_missing_whole.Any()) break;

                        var cache_files_cm_list_cms_index = cache_files_cm_list[cms_index];

                        if (cache_files_cm_list_cms_index == default) continue;

                        lock (iteration_cm_all)
                        { 
                            // limit cm to those indexes not already loaded
                            var cm_append = cache_files_cm_list_cms_index.Where((cm, i) => 
                                                               cm != null && 
                                                               loaded_state.indexes_missing_whole.Any(a => 
                                                                   a.iteration_index == cm.x_iteration_index && 
                                                                   a.group_array_index == cm.x_group_array_index && 
                                                                   a.total_groups == cm.x_total_groups && 
                                                                   a.calc_11p_thresholds == cm.x_calc_11p_thresholds && 
                                                                   a.repetitions == cm.x_repetitions_total && 
                                                                   a.outer_cv_folds == cm.x_outer_cv_folds && 
                                                                   a.svm_type == cm.x_svm_type && 
                                                                   a.svm_kernel == cm.x_svm_kernel && 
                                                                   a.scale_function == cm.x_scale_function && 
                                                                   a.inner_cv_folds == cm.x_inner_cv_folds)
                                                               ).ToList();

                            // maybe: cm_append = cm_append.Where(a => iteration_cm_all.All(b => b.line != a.line)).ToList();

                            if (cm_append.Any())
                            {
                                iteration_cm_all.AddRange(cm_append);

                                loaded_state = get_missing(instance_index, iteration_cm_all, indexes_whole);

                                io_proxy.WriteLine($@"Loaded {cache_level} cache: {cm_file}. cm_append: {cm_append.Count}. iteration_cm_all: {iteration_cm_all.Count}. indexes_loaded_whole: {loaded_state.indexes_loaded_whole.Count}. indexes_loaded_partition: {loaded_state.indexes_loaded_partition.Count}. indexes_missing_whole: {loaded_state.indexes_missing_whole.Count}. indexes_missing_partition: {loaded_state.indexes_missing_partition.Count}.", module_name, method_name);
                            }
                        }
                    }
                }

                if (wait_for_cache && loaded_state.indexes_missing_whole.Any()) { Task.Delay(new TimeSpan(0, 0, 15)).Wait(); }

            } while (wait_for_cache && loaded_state.indexes_missing_whole.Any());

            return loaded_state;
        }

        internal static (List<index_data> indexes_loaded_whole, List<index_data> indexes_loaded_partition, List<index_data> indexes_missing_whole, List<index_data> indexes_missing_partition) get_missing(
            int instance_index,
                List<confusion_matrix> iteration_cm_all,
                List<index_data> indexes_whole
            )
        {
            // check iteration_cm_all to see what is already loaded and what is missing

            var loaded_whole = indexes_whole
                .Where(a => iteration_cm_all
                                    .Any(cm => a != default && cm != default && cm != default && a.iteration_index == cm.x_iteration_index && a.group_array_index == cm.x_group_array_index && a.total_groups == cm.x_total_groups && a.calc_11p_thresholds == cm.x_calc_11p_thresholds && a.repetitions == cm.x_repetitions_total && a.outer_cv_folds == cm.x_outer_cv_folds && a.svm_kernel == cm.x_svm_kernel && a.scale_function == cm.x_scale_function && a.inner_cv_folds == cm.x_inner_cv_folds)
                )
                .ToList();

            var loaded_partition = instance_index > -1 ? loaded_whole.Where(a => a.unrolled_instance_index == instance_index).ToList() : loaded_whole;


            var missing_whole = indexes_whole.Except(loaded_whole).ToList();

            var missing_partition = instance_index > -1 ? missing_whole.Where(a => a.unrolled_instance_index == instance_index).ToList() : missing_whole;


            return (loaded_whole, loaded_partition, missing_whole, missing_partition);
        }
    }
}
