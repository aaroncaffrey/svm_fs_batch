using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal class cache_load
    {
        internal class get_unrolled_indexes_parameters
        {
            internal bool output_threshold_adjustment_performance = false;
            
            internal List<routines.libsvm_svm_type> svm_types = new List<routines.libsvm_svm_type>() { routines.libsvm_svm_type.c_svc };
            internal List<routines.scale_function> scales = new List<routines.scale_function>() { routines.scale_function.rescale };
            internal List<routines.libsvm_kernel_type> kernels = new List<routines.libsvm_kernel_type>() { routines.libsvm_kernel_type.rbf };
            
            internal List<List<(int class_id, double class_weight)>> class_weight_sets = new List<List<(int class_id, double class_weight)>>();
            
            // number of times to run tests (repeats) (default: 1 [0 would be none] ... 1 to 1 at step 1)
            internal int r_cv_start = 1;
            internal int r_cv_end = 1;
            internal int r_cv_step = 1;
            
            // number of outer-cross-validations (default: 5 ... 5 to 5 at step 1)
            internal int o_cv_start = 5;
            internal int o_cv_end = 5;
            internal int o_cv_step = 1;
            
            // number of inner-cross-validations (default: 5 ... 5 to 5 at step 1)
            internal int i_cv_start = 5;
            internal int i_cv_end = 5;
            internal int i_cv_step = 1;
        }

        internal static

          (List<(
              int unrolled_index,
              int unrolled_instance_index,
              int iteration_index,
              int group_index,
              int total_groups,
              bool output_threshold_adjustment_performance,
              int repetitions,
              int outer_cv_folds,
              List<(int class_id, double class_weight)> class_weights,
              routines.libsvm_svm_type svm_type,
              routines.libsvm_kernel_type svm_kernel,
              routines.scale_function scale_function,
              int inner_cv_folds,
              program.init_dataset_ret idr
              )> indexes_whole,

          List<(
              int unrolled_index,
              int unrolled_instance_index,
              int iteration_index,
              int group_index,
              int total_groups,
              bool output_threshold_adjustment_performance,
              int repetitions,
              int outer_cv_folds,
              List<(int class_id, double class_weight)> class_weights,
              routines.libsvm_svm_type svm_type,
              routines.libsvm_kernel_type svm_kernel,
              routines.scale_function scale_function,
              int inner_cv_folds,
              program.init_dataset_ret idr
              )> indexes_partition)

          get_unrolled_indexes_basic(

              program.init_dataset_ret caller_idr,
              int iteration_index,
              int total_groups,
              int instance_index,
              int total_instances
              )
        {
            //bool output_threshold_adjustment_performance = false;

            //var svm_types = new List<routines.libsvm_svm_type>() { routines.libsvm_svm_type.c_svc };
            //var scales = new List<routines.scale_function>() { routines.scale_function.rescale };
            //var kernels = new List<routines.libsvm_kernel_type>() { routines.libsvm_kernel_type.rbf };

            //var class_weights = new List<List<(int class_id, double class_weight)>>();

            //// number of times to run tests (repeats) (default: 1 [0 would be none] ... 1 to 1 at step 1)
            //int r_cv_start = 1;
            //int r_cv_end = 1;
            //int r_cv_step = 1;

            //// number of outer-cross-validations (default: 5 ... 5 to 5 at step 1)
            //int o_cv_start = 5;
            //int o_cv_end = 5;
            //int o_cv_step = 1;

            //// number of inner-cross-validations (default: 5 ... 5 to 5 at step 1)
            //int i_cv_start = 5;
            //int i_cv_end = 5;
            //int i_cv_step = 1;


            var p = new get_unrolled_indexes_parameters();

            return get_unrolled_indexes(caller_idr, iteration_index, total_groups, instance_index, total_instances, p);//output_threshold_adjustment_performance, svm_types, kernels, scales, class_weights, r_cv_start, r_cv_end, r_cv_step, o_cv_start, o_cv_end, o_cv_step, i_cv_start, i_cv_end, i_cv_step);
        }

        internal static

         (List<(
             int unrolled_index,
             int unrolled_instance_index,
             int iteration_index,
             int group_index,
             int total_groups,
             bool output_threshold_adjustment_performance,
             int repetitions,
             int outer_cv_folds,
             List<(int class_id, double class_weight)> class_weights,
             routines.libsvm_svm_type svm_type,
             routines.libsvm_kernel_type svm_kernel,
             routines.scale_function scale_function,
             int inner_cv_folds,
             program.init_dataset_ret idr
             )> indexes_whole,

         List<(
             int unrolled_index,
             int unrolled_instance_index,
             int iteration_index,
             int group_index,
             int total_groups,
             bool output_threshold_adjustment_performance,
             int repetitions,
             int outer_cv_folds,
             List<(int class_id, double class_weight)> class_weights,
             routines.libsvm_svm_type svm_type,
             routines.libsvm_kernel_type svm_kernel,
             routines.scale_function scale_function,
             int inner_cv_folds,
             program.init_dataset_ret idr
             )> indexes_partition)

         get_unrolled_indexes_deeper_search(

             program.init_dataset_ret caller_idr,
             int iteration_index,
             int total_groups,
             int instance_index,
             int total_instances
             )
        {
            // for a specific set of features (i.e. the final result from feature selection):

            // what happens if we vary the scale & kernel
            var p1 = new get_unrolled_indexes_parameters
            {
                output_threshold_adjustment_performance = true,

                scales = new List<routines.scale_function>()
                {
                    routines.scale_function.none, routines.scale_function.normalisation, routines.scale_function.rescale, routines.scale_function.standardisation, routines.scale_function.L0_norm, routines.scale_function.L1_norm, routines.scale_function.L2_norm,
                },

                kernels = new List<routines.libsvm_kernel_type>()
                {
                    routines.libsvm_kernel_type.linear, routines.libsvm_kernel_type.polynomial, routines.libsvm_kernel_type.sigmoid, routines.libsvm_kernel_type.rbf
                },

                
            };

            // what happens if we vary the repetitions, outer-cv, inner-cv
            var p2 = new get_unrolled_indexes_parameters
            {
                output_threshold_adjustment_performance = true,

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



            // what happens if we vary the weights 
            var weight_values = new List<double>() { 200, 100, 50, 10, 5, 2, 1, 0.75, 0.5, 0.25, 0, -0.25, -0.5, -0.75, -1, -2, -5, -10, -50, -100, -200 }; // default: 1 // 21 values

            var p3 = new get_unrolled_indexes_parameters
            {
                output_threshold_adjustment_performance = true, 
                class_weight_sets = new List<List<(int class_id, double class_weight)>>()
            };

            foreach (var wi in weight_values)
            {
                foreach (var wj in weight_values)
                {
                    p2.class_weight_sets.Add(new List<(int class_id, double class_weight)>()
                    {
                        (+1, wi),
                        (-1, wj)
                    });
                }
            }
            


            // 
            var variations_1 = get_unrolled_indexes(caller_idr, iteration_index, total_groups, instance_index, total_instances, p1);

            //
            var variations_2 = get_unrolled_indexes(caller_idr, iteration_index, total_groups, instance_index, total_instances, p2);
            
            //
            var variations_3 = get_unrolled_indexes(caller_idr, iteration_index, total_groups, instance_index, total_instances, p3);



            // join variations 1 & 2 & 3
            var indexes_whole = variations_1.indexes_whole.ToList();
            indexes_whole.AddRange(variations_2.indexes_whole.ToList());
            indexes_whole.AddRange(variations_3.indexes_whole.ToList());

            var indexes_partition = variations_1.indexes_partition.ToList();
            indexes_partition.AddRange(variations_2.indexes_partition.ToList());
            indexes_partition.AddRange(variations_3.indexes_partition.ToList());

            return (indexes_whole, indexes_partition);
        }

        public static int for_iterations(int start, int end, int step)
        {
            // step should never be zero
            if (step == 0) throw new DivideByZeroException();

            return end >= start ? ((end - start) / step) + 1 : 0;
        }

        internal static

        (List<(
            int unrolled_index,
            int unrolled_instance_index,
            int iteration_index,
            int group_index,
            int total_groups,
            bool output_threshold_adjustment_performance,
            int repetitions,
            int outer_cv_folds,
            List<(int class_id, double class_weight)> class_weights,
            routines.libsvm_svm_type svm_type,
            routines.libsvm_kernel_type svm_kernel,
            routines.scale_function scale_function,
            int inner_cv_folds,
            program.init_dataset_ret idr
            )> indexes_whole,

        List<(
            int unrolled_index,
            int unrolled_instance_index,
            int iteration_index,
            int group_index,
            int total_groups,
            bool output_threshold_adjustment_performance,
            int repetitions,
            int outer_cv_folds,
            List<(int class_id, double class_weight)> class_weights,
            routines.libsvm_svm_type svm_type,
            routines.libsvm_kernel_type svm_kernel,
            routines.scale_function scale_function,
            int inner_cv_folds,
            program.init_dataset_ret idr
            )> indexes_partition)

        get_unrolled_indexes(

            program.init_dataset_ret caller_idr,
            int iteration_index,
            int total_groups,
            int instance_index,
            int total_instances,

            get_unrolled_indexes_parameters p
            //bool output_threshold_adjustment_performance = false,

            //List<routines.libsvm_svm_type> svm_types = null,
            //List<routines.libsvm_kernel_type> kernels = null,
            //List<routines.scale_function> scales = null,
            //List<List<(int class_id, double class_weight)>> class_weight_sets = null,

            //int r_cv_start = 1, int r_cv_end = 1, int r_cv_step = 1, 
            //int o_cv_start = 5, int o_cv_end = 5, int o_cv_step = 1, 
            //int i_cv_start = 5, int i_cv_end = 5, int i_cv_step = 1
            )
        {
            // set default scales functions and svm_kernel types
            //if (kernels == null) kernels = ((routines.libsvm_kernel_type[])Enum.GetValues(typeof(routines.libsvm_kernel_type))).Where(a => a != routines.libsvm_kernel_type.precomputed).ToList();
            //if (scales == null) scales = ((routines.scale_function[])Enum.GetValues(typeof(routines.scale_function))).ToList();

            // limit the scale_function functions and svm_kernel types to reduce compute load
            //if (class_weights == null) class_weights = new List<(int class_id, double class_weight)>();
            if (p.class_weight_sets == null || p.class_weight_sets.Count == 0)
            {
                p.class_weight_sets = new List<List<(int class_id, double class_weight)>> {new List<(int class_id, double class_weight)>()};
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
            if (p.scales == null || p.scales.Count == 0) p.scales = new List<routines.scale_function>() { routines.scale_function.rescale };


            var r_cv_series_index = -1;
            var r_cv_series = new int[for_iterations(p.r_cv_start, p.r_cv_end, p.r_cv_step)];
            for (var _repetitions_cv_folds = p.r_cv_start; _repetitions_cv_folds <= p.r_cv_end; _repetitions_cv_folds += p.r_cv_step) r_cv_series[++r_cv_series_index]=_repetitions_cv_folds;

            var o_cv_series_index = -1;
            var o_cv_series = new int[for_iterations(p.o_cv_start, p.o_cv_end, p.o_cv_step)];
            for (var _outer_cv_folds = p.o_cv_start; _outer_cv_folds <= p.o_cv_end; _outer_cv_folds += p.o_cv_step) o_cv_series[++o_cv_series_index]=_outer_cv_folds;

            var i_cv_series_index = -1;
            var i_cv_series = new int[for_iterations(p.i_cv_start, p.i_cv_end, p.i_cv_step)];
            for (var inner_cv_folds = p.i_cv_start; inner_cv_folds <= p.i_cv_end; inner_cv_folds += p.i_cv_step) i_cv_series[++i_cv_series_index] = inner_cv_folds;

            

            // should group series be the loop group index or the mixture of groups?
            var group_series_index = -1;
            var group_series = new int[total_groups]; // this is currently only the group_index , could also add the selected_group_indexes
            for (var g_series = 0; g_series < total_groups; g_series++) group_series[++group_series_index]=g_series; //group_series.Add(g_series);

            if (r_cv_series == null || r_cv_series.Length == 0) throw new Exception();
            if (o_cv_series == null || o_cv_series.Length == 0) throw new Exception();
            if (i_cv_series == null || i_cv_series.Length == 0) throw new Exception();
            if (group_series == null || group_series.Length == 0) throw new Exception();

            //var _r_start = 1;
            //var _r_end = 1; // 5
            //var _r_step = 1;

            //var _o_start = 2;
            //var _o_end = 2; // 20
            //var _o_step = 2;

            //var _i_start = 1;
            //var _i_end = 1; // 20
            //var _i_step = 2;

            var unrolled_index = 0;
            var unrolled_instance_index = 0;

            

            var indexes_whole = new List<(
                int unrolled_index,
                int unrolled_instance_index,
                int iteration_index,
                int group_index,
                int total_groups,
                bool output_threshold_adjustment_performance,
                int repetitions,
                int outer_cv_folds,
                List<(int class_id, double class_weight)> class_weights,
                routines.libsvm_svm_type svm_type,
                routines.libsvm_kernel_type svm_kernel,
                routines.scale_function scale_function,
                int inner_cv_folds,
                program.init_dataset_ret idr
                )>();


            foreach (var repetitions in r_cv_series) // e.g. 1 or 5, or 1,2,3,4,5,...
            {
                foreach (var outer_folds in o_cv_series) // e.g. 5 or 10, or 1,2,3,4,5,...
                {
                    //var ida = (init_dataset_ret)null;
                    var idr = (program.init_dataset_ret)null;

                    idr = new program.init_dataset_ret(caller_idr);
                    idr.set_folds(repetitions, outer_folds);

                    //idr.class_folds = idr.class_sizes.Select(a => (class_id: a.class_id, size: a.class_size, folds: routines.folds(a.class_size, repetitions, outer_folds))).ToList();
                    //
                    //idr.downsampled_training_class_folds = idr.class_folds.Select(a => (class_id: a.class_id, size: a.size, folds: a.folds.Select(b =>
                    //        {
                    //            var min_num_items_in_fold = idr.class_folds.Min(c => c.folds.Where(e => e.repetitions_index == b.repetitions_index && e.outer_cv_index == b.outer_cv_index).Min(e => e.indexes.Count));
                    //            return (repetitions_index: b.repetitions_index, outer_cv_index: b.outer_cv_index, indexes: b.indexes.Take(min_num_items_in_fold).ToList());
                    //        })
                    //        .ToList()))
                    //    .ToList();

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
                                            indexes_whole.Add((unrolled_index, unrolled_instance_index, iteration_index, group_index, total_groups, p.output_threshold_adjustment_performance, repetitions, outer_folds, class_weights, svm_type, svm_kernel, scale_function, inner_folds, idr));

                                            unrolled_index++;

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

            return (indexes_whole, indexes_partition);
        }


        internal static
             (
            List<(
                int unrolled_index,
                int unrolled_instance_index,
                int iteration_index,
                int group_index,
                int total_groups,
                bool output_threshold_adjustment_performance,
                int repetitions,
                int outer_cv_folds,
                List<(int class_id, double class_weight)> class_weights,
                routines.libsvm_svm_type svm_type,
                routines.libsvm_kernel_type svm_kernel,
                routines.scale_function scale_function,
                int inner_cv_folds,
                program.init_dataset_ret idr
                )> indexes_loaded_whole,

            List<(
                int unrolled_index,
                int unrolled_instance_index,
                int iteration_index,
                int group_index,
                int total_groups,
                bool output_threshold_adjustment_performance,
                int repetitions,
                int outer_cv_folds,
                List<(int class_id, double class_weight)> class_weights,
                routines.libsvm_svm_type svm_type,
                routines.libsvm_kernel_type svm_kernel,
                routines.scale_function scale_function,
                int inner_cv_folds,
                program.init_dataset_ret idr
                )> indexes_loaded_partition,

            List<(
                int unrolled_index,
                int unrolled_instance_index,
                int iteration_index,
                int group_index,
                int total_groups,
                bool output_threshold_adjustment_performance,
                int repetitions,
                int outer_cv_folds,
                List<(int class_id, double class_weight)> class_weights,
                routines.libsvm_svm_type svm_type,
                routines.libsvm_kernel_type svm_kernel,
                routines.scale_function scale_function,
                int inner_cv_folds,
                program.init_dataset_ret idr
                )> indexes_missing_whole,

            List<(
                int unrolled_index,
                int unrolled_instance_index,
                int iteration_index,
                int group_index,
                int total_groups,
                bool output_threshold_adjustment_performance,
                int repetitions,
                int outer_cv_folds,
                List<(int class_id, double class_weight)> class_weights,
                routines.libsvm_svm_type svm_type,
                routines.libsvm_kernel_type svm_kernel,
                routines.scale_function scale_function,
                int inner_cv_folds,
                program.init_dataset_ret idr
                )> indexes_missing_partition
            )

            load_cache
            (
                int instance_index,
                int iteration_index,
                string experiment_name,
                //bool load_z,
                //bool load_x,
                //bool load_m,
                bool wait_for_cache,
                //init_dataset_ret ida,
                //outer_cv_args oca,
                //int iteration_index,
                //string experiment_name,
                //string full_iteration_fn,
                //string partition_iteration_fn,
                //int total_groups,
                //int total_ids,
                ///*List<string> extra_cache_files,*/
                List<string> cache_files_already_loaded,
                List<(string line, perf.confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)> iteration_cm_all,
                //int group_index_start,
                //int group_index_last

                List<(
                    int unrolled_index,
                    int unrolled_instance_index,
                    int iteration_index,
                    int group_index,
                    int total_groups,
                    bool output_threshold_adjustment_performance,
                    int repetitions,
                    int outer_cv_folds,
                    List<(int class_id, double class_weight)> class_weights,
                    routines.libsvm_svm_type svm_type,
                    routines.libsvm_kernel_type svm_kernel,
                    routines.scale_function scale_function,
                    int inner_cv_folds,
                    program.init_dataset_ret idr
                    )> indexes_whole,

                List<(
                    int unrolled_index,
                    int unrolled_instance_index,
                    int iteration_index,
                    int group_index,
                    int total_groups,
                    bool output_threshold_adjustment_performance,
                    int repetitions,
                    int outer_cv_folds,
                    List<(int class_id, double class_weight)> class_weights,
                    routines.libsvm_svm_type svm_type,
                    routines.libsvm_kernel_type svm_kernel,
                    routines.scale_function scale_function,
                    int inner_cv_folds,
                    program.init_dataset_ret idr
                    )> indexes_partition
            //,
            //bool output_threshold_adjustement_performance
            )
        {
            // a single group may have multiple tests... e.g. different number of inner-cv, outer-cv, class_weights, etc...
            // therefore, group_index existing isn't enough, must also have the various other parameters

            var module_name = nameof(program);
            var method_name = nameof(load_cache);

            if (cache_files_already_loaded == null) throw new Exception();
            if (iteration_cm_all == null) throw new Exception();

            // check which indexes are missing.
            var loaded_state = get_missing(instance_index, iteration_cm_all, indexes_whole);

            // if all indexes loaded, return
            if (!loaded_state.indexes_missing_whole.Any()) return loaded_state;


            var iteration_folder = program.get_iteration_folder(program.init_dataset_ret.results_root_folder, experiment_name, iteration_index);

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
                        //var merge_files = indexes_partition.Select(a => $@"{Path.Combine(program.get_iteration_folder(program.init_dataset_ret.results_root_folder, experiment_name, a.iteration_index, a.group_index), $@"m_{program.get_iteration_filename(new[] { a })}")}.cm.csv").ToList();
                        var merge_files = loaded_state.indexes_missing_partition.Select(a => $@"{Path.Combine(program.get_iteration_folder(program.init_dataset_ret.results_root_folder, experiment_name, a.iteration_index, a.group_index), $@"m_{program.get_iteration_filename(new[] { a })}")}.cm.csv").ToList();

                        cache_files = cache_files.Intersect(merge_files).ToList();
                    }


                    var cms = cache_files.AsParallel()
                        .AsOrdered()
                        .Select(cm_fn =>
                        {
                            if (io_proxy.is_file_available(cm_fn))
                            {
                                var cm = perf.confusion_matrix.load(cm_fn);

                                return cm;
                            }

                            return default;
                        })
                        .ToList();

                    lock (cache_files_already_loaded) { cache_files_already_loaded.AddRange(cache_files.Where((a, i) => cms[i] != default && cms[i].Any(b => b != default && b.cm != default)).ToList()); }

                    for (var cms_index = 0; cms_index < cms.Count; cms_index++)
                    {
                        var cm_file = cache_files[cms_index];

                        if (!loaded_state.indexes_missing_whole.Any()) break;

                        var cm = cms[cms_index];

                        if (cm == default) continue;

                        lock (iteration_cm_all)
                        {
                            // limit cm to those indexes not already loaded
                            var cm_append = cm.Where((b, i) => (i == 0 && iteration_cm_all.Count == 0 && b != default) || (b != default && b.cm != default && loaded_state.indexes_missing_whole.Any(a => a.iteration_index == b.cm.x_iteration_index && a.group_index == b.cm.x_group_index && a.total_groups == b.cm.x_total_groups && a.output_threshold_adjustment_performance == b.cm.x_output_threshold_adjustment_performance && a.repetitions == b.cm.x_repetitions_total && a.outer_cv_folds == b.cm.x_outer_cv_folds && a.svm_type == b.cm.x_svm_type && a.svm_kernel == b.cm.x_svm_kernel && a.scale_function == b.cm.x_scale_function && a.inner_cv_folds == b.cm.x_inner_cv_folds))).ToList();

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

        internal static
            (
            List<(
                int unrolled_index,
                int unrolled_instance_index,
                int iteration_index,
                int group_index,
                int total_groups,
                bool output_threshold_adjustment_performance,
                int repetitions,
                int outer_cv_folds,
                List<(int class_id, double class_weight)> class_weights,
                routines.libsvm_svm_type svm_type,
                routines.libsvm_kernel_type svm_kernel,
                routines.scale_function scale_function,
                int inner_cv_folds,
                program.init_dataset_ret idr
                )> indexes_loaded_whole,

            List<(
                int unrolled_index,
                int unrolled_instance_index,
                int iteration_index,
                int group_index,
                int total_groups,
                bool output_threshold_adjustment_performance,
                int repetitions,
                int outer_cv_folds,
                List<(int class_id, double class_weight)> class_weights,
                routines.libsvm_svm_type svm_type,
                routines.libsvm_kernel_type svm_kernel,
                routines.scale_function scale_function,
                int inner_cv_folds,
                program.init_dataset_ret idr
                )> indexes_loaded_partition,

            List<(
                int unrolled_index,
                int unrolled_instance_index,
                int iteration_index,
                int group_index,
                int total_groups,
                bool output_threshold_adjustment_performance,
                int repetitions,
                int outer_cv_folds,
                List<(int class_id, double class_weight)> class_weights,
                routines.libsvm_svm_type svm_type,
                routines.libsvm_kernel_type svm_kernel,
                routines.scale_function scale_function,
                int inner_cv_folds,
                program.init_dataset_ret idr
                )> indexes_missing_whole,

            List<(
                int unrolled_index,
                int unrolled_instance_index,
                int iteration_index,
                int group_index,
                int total_groups,
                bool output_threshold_adjustment_performance,
                int repetitions,
                int outer_cv_folds,
                List<(int class_id, double class_weight)> class_weights,
                routines.libsvm_svm_type svm_type,
                routines.libsvm_kernel_type svm_kernel,
                routines.scale_function scale_function,
                int inner_cv_folds,
                program.init_dataset_ret idr
                )> indexes_missing_partition
            )

            get_missing(

                int instance_index,

                List<(string line, perf.confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)> iteration_cm_all,

                List<(
                    int unrolled_index,
                    int unrolled_instance_index,
                    int iteration_index,
                    int group_index,
                    int total_groups,
                    bool output_threshold_adjustment_performance,
                    int repetitions,
                    int outer_cv_folds,
                    List<(int class_id, double class_weight)> class_weights,
                    routines.libsvm_svm_type svm_type,
                    routines.libsvm_kernel_type svm_kernel,
                    routines.scale_function scale_function,
                    int inner_cv_folds,
                    program.init_dataset_ret idr
                    )> indexes_whole
            )
        {
            // check iteration_cm_all to see what is already loaded and what is missing

            var loaded_whole = indexes_whole.Where(a =>
                iteration_cm_all.Any(b => a != default && b != default && b.cm != default && a.iteration_index == b.cm.x_iteration_index && a.group_index == b.cm.x_group_index && a.total_groups == b.cm.x_total_groups && a.output_threshold_adjustment_performance == b.cm.x_output_threshold_adjustment_performance && a.repetitions == b.cm.x_repetitions_total && a.outer_cv_folds == b.cm.x_outer_cv_folds && a.svm_kernel == b.cm.x_svm_kernel && a.scale_function == b.cm.x_scale_function && a.inner_cv_folds == b.cm.x_inner_cv_folds)).ToList();

            var loaded_partition = instance_index > -1 ? loaded_whole.Where(a => a.unrolled_instance_index == instance_index).ToList() : loaded_whole;


            var missing_whole = indexes_whole.Except(loaded_whole).ToList();

            var missing_partition = instance_index > -1 ? missing_whole.Where(a => a.unrolled_instance_index == instance_index).ToList() : missing_whole;


            return (loaded_whole, loaded_partition, missing_whole, missing_partition);
        }
    }
}
