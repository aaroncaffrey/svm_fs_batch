using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace svm_fs_batch
{
    internal class feature_selection
    {
        public const string module_name = nameof(feature_selection);

        internal static void feature_selection_initialization(
            CancellationTokenSource cts,
            //dataset_loader dataset,
            string experiment_name,
            int instance_id,
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
            const string method_name = nameof(feature_selection_initialization);
            if (cts.IsCancellationRequested) return;

            var find_best_group_features_first = false;
            var check_individual_last = true;


            // Load dataset
            var dataset_names = "[1i.aaindex]";
            var dataset = new dataset_loader(cts, dataset_names);

            // Get the feature groups within the dataset
            var groups1 = dataset_group_methods.get_main_groups(cts, dataset, file_tag: true, alphabet: true, stats: true, dimension: true, category: true, source: true, @group: true, member: false, perspective: false);

            // Limit for testing
            groups1 = groups1.Take(10).ToArray();

            // Feature select within each group first, to reduce number of columns
            if (find_best_group_features_first)
            {
                io_proxy.WriteLine($@"Finding best of {groups1.Sum(a => a.columns.Length)} individual columns within the {groups1.Length} groups", module_name, method_name);

                // get best features in each group
                var groups1_reduce_input = dataset_group_methods.get_sub_groups(cts, groups1, file_tag: true, alphabet: true, stats: true, dimension: true, category: true, source: true, @group: true, member: true, perspective: true);

                // There is 1 performance test per instance (i.e. each nested cross validation performance test [1-repetition, 5-fold outer, 5-fold inner])
                // This means that if number of groups is less than number of instances, some instances could be idle... problem, but rare enough to ignore.

                var groups1_reduce_output = groups1_reduce_input
                    //.AsParallel()
                    //.AsOrdered()
                    //.WithCancellation(cts.Token)
                    .Select
                    (
                        (group, group_index) =>
                        feature_selection_worker
                        (
                            cts: cts,
                            dataset: dataset,
                            groups: @group,
                            preselect_all_groups: true,
                            //save_status: true,
                            //cache_full: false,
                            //cache_summary: true,
                            base_group_indexes: null,
                            experiment_name: $"{experiment_name}_stage1_{group_index}",
                            instance_id: instance_id,
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
                preselect_all_groups: false,
                //save_status: true,
                //cache_full: true,
                //cache_summary: true,
                base_group_indexes: null,
                experiment_name: $"{experiment_name}_stage2",
                instance_id: instance_id,
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
                    preselect_all_groups: true,
                    //save_status: true,
                    //cache_full: false,
                    //cache_summary: true,
                    base_group_indexes: null,
                    experiment_name: $"{experiment_name}_stage3",
                    instance_id: instance_id,
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
            (index_data id, confusion_matrix cm, rank_score rs) best_winner_data,
            List<(index_data id, confusion_matrix cm, rank_score rs)> winners
        )
        feature_selection_worker
        (
            CancellationTokenSource cts,
            dataset_loader dataset,
            (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] groups,
            bool preselect_all_groups,// preselect all groups
            int[] base_group_indexes,//always include these groups
            string experiment_name,
             int instance_id,
            int total_instances,
            int repetitions,
            int outer_cv_folds,
            int outer_cv_folds_to_run,
            int inner_folds,
            double min_score_increase = 0.005,
            int max_iterations = 100,
            int limit_iteration_not_higher_than_all = 14,
            int limit_iteration_not_higher_than_last = 7,
            bool make_outer_cv_confusion_matrices = false,
            bool as_parallel = true
        )
        {
            const string method_name = nameof(feature_selection_worker);
            //const bool cache_full = true;
            //const bool cache_summary = true;
            const bool overwrite_cache = false;

            if (cts.IsCancellationRequested) return default;

            var total_groups = groups.Length;

            base_group_indexes = base_group_indexes?.OrderBy(a => a).Distinct().ToArray();
            //var base_column_indexes = base_group_indexes?.SelectMany(base_group_index => groups[base_group_index].columns).OrderBy(a => a).Distinct().ToArray();

            (index_data id, confusion_matrix cm, rank_score rs) best_winner_id_cm_rs = default;
            (index_data id, confusion_matrix cm, rank_score rs) last_winner_id_cm_rs = default;

            var all_winners_id_cm_rs = new List<(index_data id, confusion_matrix cm, rank_score rs)>();
            //var last_iteration_id_cm_rs = Array.Empty<(index_data id, confusion_matrix cm, rank_score rs)>();

            var all_iteration_id_cm_rs = new List<(index_data id, confusion_matrix cm, rank_score rs)[]>();
            var previous_group_tests = new List<int[]>();

            var cache_files_loaded = new List<string>();
            //var deep_search_index = -1;
            var feature_selection_finished = false;
            var iteration_index = 0;
            //var iteration_name = "";
            var iterations_not_higher_than_best = 0;
            var iterations_not_higher_than_last = 0;
            //var last_iteration_folder = "";

            //var all_iterations_winners_cm = new List<confusion_matrix>();
            //var feature_selection_status_list = new List<feature_selection_status>();

            var selection_excluded_groups = new List<int>();

            //io_proxy.WriteLine($@"{experiment_name}: Groups: (array indexes: [{(array_index_start)}..{(array_index_last)}]). Total groups: {total_groups}. (This instance #{(instance_index)}/#{total_instances}. array indexes: [{(array_index_start)}..{(array_index_last)}]). All indexes: [0..{(total_groups - 1)}].)", module_name, method_name);
            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Total groups: {(groups?.Length ?? 0)}.", module_name, method_name);
            var calibrate = false;




            while (!feature_selection_finished)
            {
                if (cts.IsCancellationRequested) return default;

                var last_iteration_id_cm_rs = all_iteration_id_cm_rs?.LastOrDefault();

                // get list of work to do this iteration
                //(index_data[] indexes_whole, index_data[] indexes_partition) unrolled_indexes = default;

                var selected_groups = last_winner_id_cm_rs.id?.group_array_indexes?.ToArray() ?? Array.Empty<int>();
                //var selected_columns = last_winner_id_cm_rs.id?.column_array_indexes?.ToArray() ?? Array.Empty<int>();

                var selection_excluded_groups2 = last_winner_id_cm_rs.id != null ? selection_excluded_groups.Concat(new List<int>() { last_winner_id_cm_rs.id.group_array_index }).ToArray() : selection_excluded_groups.ToArray();

                if (preselect_all_groups)
                {
                    selected_groups = selected_groups.Union(Enumerable.Range(0, groups.Length).ToArray()).ToArray();
                    //selected_columns = selected_groups.SelectMany(group_index => groups[group_index].columns).Union(new[] { 0 }).OrderBy(column_index => column_index).Distinct().ToArray();
                    preselect_all_groups = false;
                    calibrate = true;
                }

                if (base_group_indexes != null && base_group_indexes.Length > 0)
                {
                    selected_groups = selected_groups.Union(base_group_indexes).OrderBy(a => a).ToArray();
                    //selected_columns = selected_groups.SelectMany(group_index => groups[group_index].columns).Union(new[] { 0 }).OrderBy(column_index => column_index).Distinct().ToArray();
                }

                //if (calibrate && (selected_groups == null || selected_groups.Length == 0 || selected_columns == null || selected_columns.Length <= 1 /* class id */))
                if (calibrate && (selected_groups == null || selected_groups.Length == 0))
                {
                    io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: iteration: {iteration_index}, selected_groups.Length = {(selected_groups?.Length ?? 0)}.");
                    throw new Exception();
                }

                //if (selected_columns != null && selected_columns.Length > 0 && selected_columns[0] != 0)
                //{
                //    selected_columns = selected_columns.Union(new[] { 0 }).OrderBy(a => a).ToArray();
                //}

                var group_indexes_to_test = calibrate ? new[] { -1 } : Enumerable.Range(0, groups.Length).Except(selection_excluded_groups2).ToArray();
                io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: iteration: {iteration_index}, group_indexes_to_test.Length = {(group_indexes_to_test?.Length ?? 0)}.");

                var previous_winner_group_index = last_winner_id_cm_rs.id?.group_array_index;

                if (group_indexes_to_test == null || group_indexes_to_test.Length == 0)
                {
                    break;
                }

                var job_group_series = cache_load.job_group_series(cts, dataset, groups, experiment_name, iteration_index, base_group_indexes, group_indexes_to_test, selected_groups, previous_winner_group_index, selection_excluded_groups2, previous_group_tests, as_parallel);
                io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: iteration: {iteration_index}, job_group_series.Length = {(job_group_series?.Length ?? 0)}.");

                if (job_group_series == null || job_group_series.Length == 0)
                {
                    break;
                }

                var index_data_container = cache_load.get_feature_selection_instructions(cts, dataset, groups, job_group_series, experiment_name, iteration_index, total_groups, instance_id, total_instances, repetitions, outer_cv_folds, outer_cv_folds_to_run, inner_folds, base_group_indexes, group_indexes_to_test, selected_groups, previous_winner_group_index, selection_excluded_groups2, previous_group_tests);
                io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: iteration: {iteration_index}, index_data_container.indexes_whole.Length = {(index_data_container?.indexes_whole?.Length ?? 0)}, index_data_container.indexes_partition.Length = {(index_data_container?.indexes_partition?.Length ?? 0)}.");

                if (index_data_container.indexes_whole == null || index_data_container.indexes_whole.Length == 0)
                {
                    break;
                }


                // iteration_all_cm is a list of all merged results (i.e. the individual outer-cross-validation partitions merged)
                var iteration_whole_results = new List<(index_data id, confusion_matrix cm)>();

                // get folder and file names for this iteration (iteration_folder & iteration_whole_cm_filename are the same for all partitions; iteration_partition_cm_filename is specific to the partition)
                var iteration_folder = program.get_iteration_folder(settings.results_root_folder, experiment_name, iteration_index);
                var iteration_whole_cm_filename_full = Path.Combine(iteration_folder, $@"z_{program.get_iteration_filename(index_data_container.indexes_whole)}_full.cm.csv");
                var iteration_whole_cm_filename_summary = Path.Combine(iteration_folder, $@"z_{program.get_iteration_filename(index_data_container.indexes_whole)}_summary.cm.csv");
                var iteration_partition_cm_filename_full = Path.Combine(iteration_folder, $@"x_{program.get_iteration_filename(index_data_container.indexes_partition)}_full.cm.csv");
                var iteration_partition_cm_filename_summary = Path.Combine(iteration_folder, $@"x_{program.get_iteration_filename(index_data_container.indexes_partition)}_summary.cm.csv");



                // load cache (first try whole iteration, then try partition, then try individual work items)
                cache_load.load_cache(
                    cts: cts,
                    instance_index: instance_id,
                    iteration_index: iteration_index,
                    experiment_name: experiment_name,
                    wait_for_cache: false,
                    cache_files_already_loaded: cache_files_loaded,
                    iteration_cm_sd_list: iteration_whole_results,
                    index_data_container: index_data_container,
                    last_iteration_id_cm_rs: last_iteration_id_cm_rs,
                    last_winner_id_cm_rs: last_winner_id_cm_rs,
                    best_winner_id_cm_rs: best_winner_id_cm_rs);

                // check if this partition is loaded....
                while (index_data_container.indexes_missing_partition.Any())
                {
                    // after loading the whole iteration cache, all partitions cache, and partition individual merged items cache, and if there are still partition items missing... 
                    // use parallel to split the missing items into cpu bound partitions
                    var indexes_missing_partition_results =
                        as_parallel ?
                            index_data_container
                        .indexes_missing_partition
                        .AsParallel()
                        .AsOrdered()
                        .WithCancellation(cts.Token)
                        .Select
                        (unrolled_index_data =>
                            cross_validate.cross_validate_performance(
                                cts: cts,
                                dataset: dataset,
                                experiment_name: experiment_name,
                                unrolled_index_data: unrolled_index_data,
                                make_outer_cv_confusion_matrices: make_outer_cv_confusion_matrices)
                        )
                        .ToList()
                            :
                    index_data_container
                        .indexes_missing_partition
                        .Select
                        (unrolled_index_data =>
                            cross_validate.cross_validate_performance(
                                cts: cts,
                                dataset: dataset,
                                experiment_name: experiment_name,
                                unrolled_index_data: unrolled_index_data,
                                make_outer_cv_confusion_matrices: make_outer_cv_confusion_matrices)
                        )
                        .ToList();

                    iteration_whole_results.AddRange(indexes_missing_partition_results.Where(a => a != default).SelectMany(a => a).ToArray());

                    // all partition should be loaded by this point, check
                    cache_load.update_missing(cts, instance_id, iteration_whole_results, index_data_container);

                    io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Partition {(index_data_container.indexes_missing_partition.Length > 0 ? $@"{index_data_container.indexes_missing_partition.Length} incomplete" : $@"complete")} for iteration {(iteration_index)}.");
                }

                // save partition cache
                {
                    // 4. save CM for all groups of this hpc instance (from index start to index end) merged outer-cv results
                    //var instance_id2 = instance_id;
                    var iteration_partition_results = iteration_whole_results.Where((cm_sd, i) => cm_sd.id.unrolled_instance_id == instance_id).ToArray();

                    confusion_matrix.save(cts, iteration_partition_cm_filename_full, iteration_partition_cm_filename_summary, overwrite_cache, iteration_partition_results);
                    io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Partition cache: Saved for iteration {(iteration_index)} group. Files: {iteration_partition_cm_filename_full}, {iteration_partition_cm_filename_summary}.");
                }


                // check if all partitions are loaded....
                while (index_data_container.indexes_missing_whole.Any())
                {
                    // 5. load results from other instances (into iteration_whole_results)

                    cache_load.load_cache(cts, instance_id, iteration_index, experiment_name, true, cache_files_loaded, iteration_whole_results, index_data_container, last_iteration_id_cm_rs, last_winner_id_cm_rs, best_winner_id_cm_rs);

                    io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Partition {(index_data_container.indexes_missing_whole.Length > 0 ? $@"{index_data_container.indexes_missing_whole.Length} incomplete" : $@"complete")} for iteration {(iteration_index)}.");
                }

                if (instance_id == 0)
                {
                    // make sure z saved, in case stopped after completed work, before saving z cache file
                    // save CM with results from all instances
                    confusion_matrix.save(cts, iteration_whole_cm_filename_full, iteration_whole_cm_filename_summary, overwrite_cache, iteration_whole_results.ToArray());
                    io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Full cache: Saved for iteration {(iteration_index)}. Files: {iteration_whole_cm_filename_full}, {iteration_whole_cm_filename_summary}.");
                }


                // 6. find winner (highest performance of any group of any class [within scoring_metrics' and 'scoring_class_ids'])
                //      ensure ordering will be consistent between instances
                //      ensure score_data instances are created, may not have been if from cache.

                var iteration_whole_results_fixed = iteration_whole_results
                    .Where
                    (cm_sd =>
                        cm_sd.id != null &&
                        cm_sd.cm != null &&
                        cm_sd.cm.x_class_id != null && // class id exists
                        cm_sd.cm.x_class_id.Value == scoring_args.scoring_class_id && // ...and is the scoring class id
                        cm_sd.cm.x_prediction_threshold == null && // not a threshold altered metric
                        cm_sd.cm.x_repetitions_index == -1 && // merged
                        cm_sd.cm.x_outer_cv_index == -1 && // merged
                        cm_sd.id.iteration_index == iteration_index // this iteration
                    )
                    .Select(a =>
                    {
                        var fs_score = a.cm.metrics.get_values_by_names(scoring_args.scoring_metrics).Average();

                        return (a.id, a.cm, fs_score);
                    })
                    .ToList();

                var iteration_whole_results_fixed_with_ranks = set_ranks(cts, iteration_whole_results_fixed, last_iteration_id_cm_rs);

                iteration_whole_results.Clear();
                iteration_whole_results = null;

                iteration_whole_results_fixed.Clear();
                iteration_whole_results_fixed = null;

                var this_iteration_winner_id_cm_rs = iteration_whole_results_fixed_with_ranks[0];
                all_winners_id_cm_rs.Add(this_iteration_winner_id_cm_rs);
                var iteration_winner_group = this_iteration_winner_id_cm_rs.id.group_array_index > -1 ? groups[this_iteration_winner_id_cm_rs.id.group_array_index] : default;
                var iteration_winner_group_key = iteration_winner_group != default ? iteration_winner_group.group_key : default;



                var available_groups = Enumerable.Range(0, groups.Length).ToArray();
                available_groups = available_groups.Except(this_iteration_winner_id_cm_rs.id.group_array_indexes).ToArray();
                if (base_group_indexes != null && base_group_indexes.Length > 0) available_groups = available_groups.Except(base_group_indexes).ToArray();
                if (selection_excluded_groups != null && selection_excluded_groups.Count > 0) available_groups = available_groups.Except(selection_excluded_groups).ToArray();
                var num_available_groups = available_groups.Length;



                {
                    var zero_score = iteration_whole_results_fixed_with_ranks.Where<(index_data id, confusion_matrix cm, rank_score rs)>(a => a.rs.fs_score == 0).Select(a => a.id.group_array_index).ToArray();
                    if (zero_score.Length > 0)
                    {
                        selection_excluded_groups.AddRange(zero_score);
                        io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}:  Excluding groups with zero scores: {string.Join(", ", zero_score)}.");
                    }
                }

                {
                    const int poor_trend_iterations = 5;

                    if (num_available_groups > poor_trend_iterations && iteration_index >= (poor_trend_iterations - 1))
                    {
                        // take bottom 10% for last 5 (poor_trend_iterations) iterations
                        var bottom_indexes = all_iteration_id_cm_rs.TakeLast(poor_trend_iterations).SelectMany(a => a.Where(b => b.rs.fs_score_percentile <= 0.1).ToArray()).Select(a => a.id.group_array_index).ToArray();
                        var bottom_indexes_count = bottom_indexes.Distinct().Select(a => (group_array_index: a, count: bottom_indexes.Count(b => a == b))).ToArray();

                        // if group_array_index was in bottom 10% the last 5 (poor_trend_iterations) times, then blacklist
                        var always_poor = bottom_indexes_count.Where(a => a.count >= poor_trend_iterations).Select(a => a.group_array_index).ToArray();

                        if (always_poor.Length > 0)
                        {
                            selection_excluded_groups.AddRange(always_poor);
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}:  Excluding groups with always poor scores: {string.Join(", ", always_poor)}.");
                        }
                    }
                }


                if (!feature_selection_finished)
                {
                    var score_increase_from_last = this_iteration_winner_id_cm_rs.rs.fs_score - (last_winner_id_cm_rs.rs?.fs_score ?? 0d);
                    var score_increase_from_best = this_iteration_winner_id_cm_rs.rs.fs_score - (best_winner_id_cm_rs.rs?.fs_score ?? 0d);

                    iterations_not_higher_than_last = score_increase_from_last > 0 ? 0 : iterations_not_higher_than_last + 1;
                    iterations_not_higher_than_best = score_increase_from_best > 0 ? 0 : iterations_not_higher_than_best + 1;

                    if (score_increase_from_best > 0)
                    {
                        best_winner_id_cm_rs = this_iteration_winner_id_cm_rs;
                    }



                    var groups_not_available = num_available_groups == 0;
                    var not_higher_than_last_limit_reached = iterations_not_higher_than_last >= limit_iteration_not_higher_than_last;
                    var not_higher_than_best_limit_reached = iterations_not_higher_than_best >= limit_iteration_not_higher_than_all;
                    var max_iterations_reached = max_iterations > 0 && iteration_index + 1 >= max_iterations;
                    var score_increase_not_reached = min_score_increase > 0 && score_increase_from_last < min_score_increase;

                    //if (feature_selection_finished) io_proxy.WriteLine($@"{nameof(feature_selection_finished)}: {nameof(feature_selection_finished)} = {feature_selection_finished}");
                    if (not_higher_than_last_limit_reached) io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(not_higher_than_last_limit_reached)} = {not_higher_than_last_limit_reached}");
                    if (not_higher_than_best_limit_reached) io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(not_higher_than_best_limit_reached)} = {not_higher_than_best_limit_reached}");
                    if (groups_not_available) io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(groups_not_available)} = {groups_not_available}");
                    if (max_iterations_reached) io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(max_iterations_reached)} = {max_iterations_reached}");
                    if (score_increase_not_reached) io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(score_increase_not_reached)} = {score_increase_not_reached}");

                    feature_selection_finished = /*feature_selection_finished ||*/ not_higher_than_last_limit_reached || not_higher_than_best_limit_reached || groups_not_available;
                }

                // if main program instance, then save iteration winner to file
                if (instance_id == 0)
                {
                    {
                        // Save the CM ranked for the current iteration (winner rank #0)
                        var iteration_cm_ranks_fn1 = Path.Combine(iteration_folder, $@"iteration_ranks_cm_{iteration_index}_full.csv");
                        var iteration_cm_ranks_fn2 = Path.Combine(iteration_folder, $@"iteration_ranks_cm_{iteration_index}_summary.csv");
                        if (io_proxy.is_file_available(cts, iteration_cm_ranks_fn1) && io_proxy.is_file_available(cts, iteration_cm_ranks_fn2))
                        {
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Already saved for iteration {(iteration_index)}. Files: {iteration_cm_ranks_fn1}, {iteration_cm_ranks_fn2}.");
                        }
                        else
                        {
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Unavailable for iteration {(iteration_index)}. Files: {iteration_cm_ranks_fn1}, {iteration_cm_ranks_fn2}.");
                            confusion_matrix.save(cts, iteration_cm_ranks_fn1, iteration_cm_ranks_fn2, overwrite_cache, iteration_whole_results_fixed_with_ranks);
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Saved for iteration {(iteration_index)}. Files: {iteration_cm_ranks_fn1}, {iteration_cm_ranks_fn2}.");
                        }
                    }

                    {
                        // Save the CM of winners from all iterations
                        var winners_cm_fn1 = Path.Combine(iteration_folder, $@"winners_cm_{iteration_index}_full.csv");
                        var winners_cm_fn2 = Path.Combine(iteration_folder, $@"winners_cm_{iteration_index}_summary.csv");
                        if (io_proxy.is_file_available(cts, winners_cm_fn1) && io_proxy.is_file_available(cts, winners_cm_fn2))
                        {
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Already saved for iteration {(iteration_index)}. Files: {winners_cm_fn1}, {winners_cm_fn2}.");
                        }
                        else
                        {
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Unavailable for iteration {(iteration_index)}. Files: {winners_cm_fn1}, {winners_cm_fn2}.");
                            confusion_matrix.save(cts, winners_cm_fn1, winners_cm_fn2, overwrite_cache, all_winners_id_cm_rs.ToArray());
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Saved for iteration {(iteration_index)}. Files: {winners_cm_fn1}, {winners_cm_fn2}.");
                        }
                    }

                    {
                        // Save the prediction list for misclassification analysis
                        var prediction_list_filename = Path.Combine(iteration_folder, $@"iteration_prediction_list_{iteration_index}.csv");
                        if (io_proxy.is_file_available(cts, prediction_list_filename))
                        {
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Already saved for iteration {(iteration_index)}. File: {prediction_list_filename}.");
                        }
                        else
                        {
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Unavailable for iteration {(iteration_index)}. File: {prediction_list_filename}.");
                            prediction.save(cts, prediction_list_filename, iteration_whole_results_fixed_with_ranks);
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Saved for iteration {(iteration_index)}. File: {prediction_list_filename}.");
                        }

                    }
                }

                io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Finished: iteration {(iteration_index)}.  {(feature_selection_finished ? "Finished" : "Not finished")}.");


                last_winner_id_cm_rs = this_iteration_winner_id_cm_rs;
                all_iteration_id_cm_rs.Add(iteration_whole_results_fixed_with_ranks);
                iteration_index++;
                calibrate = false;
                preselect_all_groups = false;
                previous_group_tests.AddRange(job_group_series.Select(a => a.group_indexes).ToArray());
            }

            io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Finished: all iterations of feature selection for {total_groups} groups.");
            io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Finished: winning score = {best_winner_id_cm_rs.rs.fs_score}, total columns = {best_winner_id_cm_rs.id.num_columns}.");


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

            var best_winner_groups = best_winner_id_cm_rs.id.group_array_indexes.Select(group_index => groups[group_index]).ToArray();

            return (best_winner_groups, best_winner_id_cm_rs, all_winners_id_cm_rs);
        }



        internal static
            (index_data id, confusion_matrix cm, rank_score rs)[] set_ranks
        (
            CancellationTokenSource cts,
            List<(index_data id, confusion_matrix cm, double fs_score)> id_cm_score,
            (index_data id, confusion_matrix cm, rank_score rs)[] last_iteration_id_cm_rs,
            bool as_parallel = true
        )
        {
            if (cts.IsCancellationRequested) return default;

            var last_winner_id_cm_rs = last_iteration_id_cm_rs?.FirstOrDefault();

            // ensure consistent reordering (i.e. for items with equal tied scores when processing may have been done out of order)
            id_cm_score = id_cm_score.OrderBy(a => a.id.group_array_index).ThenBy(a => a.cm.x_class_id).ToList();

            // order descending by score
            id_cm_score = id_cm_score
                .OrderByDescending((a => a.fs_score))
                .ThenBy(a => a.id.num_columns)
                .ToList();

            // unexpected but possible edge case, if winner is the same as last time (due to random variance), then take the next group instead.
            if (id_cm_score[0].id.group_array_index == (last_iteration_id_cm_rs?.FirstOrDefault().id.group_array_index ?? -1) && id_cm_score.Count > 1)
            {
                var ix0 = id_cm_score[0];
                var ix1 = id_cm_score[1];

                id_cm_score[0] = ix1;
                id_cm_score[1] = ix0;
            }


            var max_rank = id_cm_score.Count - 1;

            var ranks_list = Enumerable.Range(0, id_cm_score.Count).ToArray();
            var ranks_list_scaling = new scaling(ranks_list) { rescale_scale_min = 0, rescale_scale_max = 1 };

            var scores_list = id_cm_score.Select(a => a.fs_score).ToArray();
            var scores_list_scaling = new scaling(scores_list) { rescale_scale_min = 0, rescale_scale_max = 1 };

            // make rank_data instances, which track the ranks (performance) of each group over time, to allow for optimisation decisions and detection of variant features

            var id_cm_rs = as_parallel ? id_cm_score
                    .AsParallel()
                    .AsOrdered()
                    .WithCancellation(cts.Token)
                    .Select((a, index) =>
                    {
                        var last_rs = last_iteration_id_cm_rs?.FirstOrDefault
                        (a =>
                            a.id.iteration_index == id_cm_score[index].id.iteration_index &&
                            a.id.group_array_index == id_cm_score[index].id.group_array_index &&
                            a.cm.x_class_id == id_cm_score[index].cm.x_class_id
                        ) ?? default;



                        var rs = new rank_score()
                        {
                            group_array_index = a.id.group_array_index,
                            iteration_index = a.id.iteration_index,

                            fs_rank_index = max_rank - index,
                            fs_rank_index_percentile = ranks_list_scaling.scale(max_rank - index, scaling.scale_function.rescale),

                            fs_score = a.fs_score,
                            fs_score_percentile = scores_list_scaling.scale(a.fs_score, scaling.scale_function.rescale)
                        };

                        return (a.id, a.cm, rs);
                    })
                    .ToArray() :
            id_cm_score
                .Select((a, index) =>
                {
                    var last_rs = last_iteration_id_cm_rs?.FirstOrDefault
                    (a =>
                        a.id.iteration_index == id_cm_score[index].id.iteration_index &&
                        a.id.group_array_index == id_cm_score[index].id.group_array_index &&
                        a.cm.x_class_id == id_cm_score[index].cm.x_class_id
                    ) ?? default;



                    var rs = new rank_score()
                    {
                        group_array_index = a.id.group_array_index,
                        iteration_index = a.id.iteration_index,

                        fs_rank_index = max_rank - index,
                        fs_rank_index_percentile = ranks_list_scaling.scale(max_rank - index, scaling.scale_function.rescale),

                        fs_score = a.fs_score,
                        fs_score_percentile = scores_list_scaling.scale(a.fs_score, scaling.scale_function.rescale)
                    };

                    return (a.id, a.cm, rs);
                })
                .ToArray();

            return id_cm_rs;
        }
    }
}
