﻿using System;
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
            //groups1 = groups1.Take(11).ToArray();

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
                            preselect_all_columns: true,
                            save_status: true,
                            cache_full: false,
                            cache_summary: true,
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
                preselect_all_columns: false,
                save_status: true,
                cache_full: true,
                cache_summary: true,
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
                    preselect_all_columns: true,
                    save_status: true,
                    cache_full: false,
                    cache_summary: true,
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
            (index_data id, confusion_matrix cm, score_data sd, rank_data rd) best_winner_data,
            List<(index_data id, confusion_matrix cm, score_data sd, rank_data rd)> winners
        )
        feature_selection_worker
        (
            CancellationTokenSource cts,
            dataset_loader dataset,
            (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] groups,
            bool preselect_all_columns,// preselect all groups
            bool save_status,
            bool cache_full,
            bool cache_summary,
            int[] base_group_indexes,//always include these groups
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
            double min_score_increase = 0.005,
            int max_iterations = 100,
            int limit_iteration_not_higher_than_all = 14,
            int limit_iteration_not_higher_than_last = 7,
            bool make_outer_cv_confusion_matrices = false
        )
        {
            const string method_name = nameof(feature_selection_worker);

            const bool overwrite_cache = false;

            if (cts.IsCancellationRequested) return default;

            var total_groups = groups.Length;

            base_group_indexes = base_group_indexes?.OrderBy(a => a).Distinct().ToArray();
            //var base_column_indexes = base_group_indexes?.SelectMany(base_group_index => groups[base_group_index].columns).OrderBy(a => a).Distinct().ToArray();

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
            //var last_iteration_folder = "";

            //var all_iterations_winners_cm = new List<confusion_matrix>();
            var feature_selection_status_list = new List<feature_selection_status>();

            var selection_excluded_groups = new List<int>();

            //io_proxy.WriteLine($@"{experiment_name}: Groups: (array indexes: [{(array_index_start)}..{(array_index_last)}]). Total groups: {total_groups}. (This instance #{(instance_index)}/#{total_instances}. array indexes: [{(array_index_start)}..{(array_index_last)}]). All indexes: [0..{(total_groups - 1)}].)", module_name, method_name);
            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Total groups: {total_groups}.", module_name, method_name);
            var calibrate = false;


            while (!feature_selection_finished)
            {
                if (cts.IsCancellationRequested) return default;

                // get list of work to do this iteration
                (index_data[] indexes_whole, index_data[] indexes_partition) unrolled_indexes = default;

                var selected_groups = last_winner_cm_sd_rd.sd?.index_data.group_array_indexes?.ToArray() ?? Array.Empty<int>();
                var selected_columns = last_winner_cm_sd_rd.sd?.index_data.column_array_indexes?.ToArray() ?? Array.Empty<int>();

                var selection_excluded_groups2 = last_winner_cm_sd_rd.sd != null ? selection_excluded_groups.Concat(new List<int>() { last_winner_cm_sd_rd.sd.index_data.group_array_index }).ToArray() : selection_excluded_groups.ToArray();

                if (preselect_all_columns)
                {
                    selected_groups = selected_groups.Union(Enumerable.Range(0, groups.Length).ToArray()).ToArray();
                    selected_columns = selected_groups.SelectMany(group_index => groups[group_index].columns).Union(new[] { 0 }).OrderBy(column_index => column_index).Distinct().ToArray();
                    preselect_all_columns = false;
                    calibrate = true;
                }

                if (base_group_indexes != null && base_group_indexes.Length > 0)
                {
                    selected_groups = selected_groups.Union(base_group_indexes).OrderBy(a => a).ToArray();
                    selected_columns = selected_groups.SelectMany(group_index => groups[group_index].columns).Union(new[] { 0 }).OrderBy(column_index => column_index).Distinct().ToArray();
                }

                if (calibrate && (selected_groups == null || selected_groups.Length == 0 || selected_columns == null || selected_columns.Length <= 1 /* class id */))
                {
                    throw new Exception();
                }

                if (selected_columns != null && selected_columns.Length > 0 && selected_columns[0] != 0)
                {
                    selected_columns = selected_columns.Union(new[] { 0 }).OrderBy(a => a).ToArray();
                }

                var group_indexes_to_test = calibrate ? new[] { -1 } : Enumerable.Range(0, groups.Length).Except(selection_excluded_groups2).ToArray();
                var previous_winner_group_index = last_winner_cm_sd_rd.sd?.index_data.group_array_index;

                //var job_group_indexes = cache_load.get_feature_selection_instructions(cts, dataset, groups, base_group_indexes, group_indexes_to_test, selected_groups, previous_winner_group_index, selection_excluded_groups, selected_columns);
                //var index_data_container = cache_load.get_feature_selection_instructions(cts, dataset, groups, selected_groups, selected_columns, experiment_name, iteration_id, calibrate ? -1 : total_groups, instance_id, total_instances, repetitions, outer_cv_folds, outer_cv_folds_to_run, inner_folds, selection_excluded_groups2);

                var index_data_container = cache_load.get_feature_selection_instructions(cts, dataset, groups, experiment_name, iteration_index, total_groups, instance_id, total_instances, repetitions, outer_cv_folds, outer_cv_folds_to_run, inner_folds, base_group_indexes, group_indexes_to_test, selected_groups, previous_winner_group_index, selection_excluded_groups2, selected_columns);

                var total_whole_indexes = unrolled_indexes.indexes_whole.Length;
                var total_partition_indexes = unrolled_indexes.indexes_partition.Length;
                io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: iteration: {iteration_index}, total_whole_indexes = {total_whole_indexes}, total_partition_indexes = {total_partition_indexes}.");

                // iteration_all_cm is a list of all merged results (i.e. the individual outer-cross-validation partitions merged)
                var iteration_whole_results = new List<(index_data id, confusion_matrix cm, score_data sd)>();

                // get folder and file names for this iteration (iteration_folder & iteration_whole_cm_filename are the same for all partitions; iteration_partition_cm_filename is specific to the partition)
                var iteration_folder = program.get_iteration_folder(settings.results_root_folder, experiment_name, iteration_index /*, iteration_name*/);
                var iteration_whole_cm_filename1 = cache_full ? Path.Combine(iteration_folder, $@"z_{program.get_iteration_filename(unrolled_indexes.indexes_whole)}_full.cm.csv") : null;
                var iteration_whole_cm_filename2 = cache_summary ? Path.Combine(iteration_folder, $@"z_{program.get_iteration_filename(unrolled_indexes.indexes_whole)}_summary.cm.csv") : null;
                var iteration_partition_cm_filename1 = cache_full ? Path.Combine(iteration_folder, $@"x_{program.get_iteration_filename(unrolled_indexes.indexes_partition)}_full.cm.csv") : null;
                var iteration_partition_cm_filename2 = cache_summary ? Path.Combine(iteration_folder, $@"x_{program.get_iteration_filename(unrolled_indexes.indexes_partition)}_summary.cm.csv") : null;


                // load cache (first try whole iteration, then try partition, then try individual work items)
                cache_load.load_cache(cts, instance_id, iteration_index, experiment_name, false, cache_files_loaded, iteration_whole_results, index_data_container, last_iteration_cm_sd_rd_list, last_winner_cm_sd_rd, best_winner_cm_sd_rd);

                // check if this partition is loaded....
                while (index_data_container.indexes_missing_partition.Any())
                {
                    // after loading the whole iteration cache, all partitions cache, and partition individual merged items cache, and if there are still partition items missing... 
                    // use parallel to split the missing items into cpu bound partitions
                    var indexes_missing_partition_results = index_data_container
                        .indexes_missing_partition
                        .AsParallel()
                        .AsOrdered()
                        .WithCancellation(cts.Token)
                        .Select
                        (unrolled_index_data =>
                            cross_validate.parallel_index_run(cts: cts, dataset: dataset, experiment_name: experiment_name, unrolled_index_data: unrolled_index_data, last_iteration_cm_sd_rd_list: last_iteration_cm_sd_rd_list, last_winner_cm_sd_rd: last_winner_cm_sd_rd, best_winner_cm_sd_rd: best_winner_cm_sd_rd, make_outer_cv_confusion_matrices: make_outer_cv_confusion_matrices)
                        )
                        .ToList();

                    iteration_whole_results.AddRange(indexes_missing_partition_results.Where(a => a != default).SelectMany(a => a).ToArray());

                    // all partition should be loaded by this point, check
                    cache_load.update_missing(cts, instance_id, iteration_whole_results, index_data_container);

                    io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Partition {(index_data_container.indexes_missing_partition.Any() ? "incomplete" : "complete")} for iteration {(iteration_index)}.");
                }

                // save partition cache
                {
                    // 4. save CM for all groups of this hpc instance (from index start to index end) merged outer-cv results
                    //var instance_id2 = instance_id;
                    var iteration_partition_results = iteration_whole_results.Where((cm_sd, i) => cm_sd.id.unrolled_instance_id == instance_id).ToArray();

                    confusion_matrix.save(cts, cache_full ? iteration_partition_cm_filename1 : null, cache_summary ? iteration_partition_cm_filename2 : null, overwrite_cache, iteration_partition_results);
                    io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Partition cache: Saved for iteration {(iteration_index)} group. Files: {iteration_partition_cm_filename1}, {iteration_partition_cm_filename2}.");
                }


                // check if all partitions are loaded....
                while (index_data_container.indexes_missing_whole.Any())
                {
                    // 5. load results from other instances (into iteration_whole_results)

                    cache_load.load_cache(cts, instance_id, iteration_index, experiment_name, true, cache_files_loaded, iteration_whole_results, index_data_container, last_iteration_cm_sd_rd_list, last_winner_cm_sd_rd, best_winner_cm_sd_rd);

                    io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Partition {(index_data_container.indexes_missing_whole.Any() ? "incomplete" : "complete")} for iteration {(iteration_index)}.");
                }

                if (instance_id == 0)
                {
                    // make sure z saved, in case stopped after completed work, before saving z cache file
                    // save CM with results from all instances
                    confusion_matrix.save(cts, cache_full ? iteration_whole_cm_filename1 : null, cache_summary ? iteration_whole_cm_filename2 : null, overwrite_cache, iteration_whole_results.ToArray());
                    io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Full cache: Saved for iteration {(iteration_index)}. Files: {iteration_whole_cm_filename1}, {iteration_whole_cm_filename2}.");
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
                        cm_sd.cm.unrolled_index_data.iteration_index == iteration_index //&&
                        //cm_sd.cm.unrolled_index_data.group_array_index != null
                    )
                    .Select
                    (cm_sd =>
                    {

                        var sd = cm_sd.sd;

                        if (sd == null) // could be null if loaded from cache ( only cm loaded, not sd/rd) .
                        {
                            // score for the same group at the last iteration
                            var same_group = last_iteration_cm_sd_rd_list
                                .FirstOrDefault
                                (a =>
                                    a.sd.index_data.group_array_index == cm_sd.cm.unrolled_index_data.group_array_index &&
                                    a.sd.index_data.iteration_index + 1 == cm_sd.cm.unrolled_index_data.iteration_index &&
                                    a.sd.class_id == cm_sd.cm.x_class_id.Value
                                );

                            sd = new score_data(
                                cm_sd.id,
                                cm_sd.cm,
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
                var this_iteration_winner_cm_sd_rd = iteration_whole_results_fixed_with_ranks[0];
                all_winners_cm_sd_rd_list.Add(this_iteration_winner_cm_sd_rd);
                var iteration_winner_group = this_iteration_winner_cm_sd_rd.sd.index_data.group_array_index > -1 ? groups[this_iteration_winner_cm_sd_rd.sd.index_data.group_array_index] : default;
                var iteration_winner_group_key = iteration_winner_group != default ? iteration_winner_group.group_key : default;



                var available_groups = Enumerable.Range(0, groups.Length).ToArray();
                available_groups = available_groups.Except(this_iteration_winner_cm_sd_rd.sd.index_data.group_array_indexes).ToArray();
                if (base_group_indexes != null && base_group_indexes.Length > 0) available_groups = available_groups.Except(base_group_indexes).ToArray();
                if (selection_excluded_groups != null && selection_excluded_groups.Count > 0) available_groups = available_groups.Except(selection_excluded_groups).ToArray();
                var num_available_groups = available_groups.Length;



                {
                    var zero_score = iteration_whole_results_fixed_with_ranks.Where(a => a.sd.same_group_score.value == 0).Select(a => a.sd.index_data.group_array_index).ToArray();
                    if (zero_score.Length > 0)
                    {
                        selection_excluded_groups.AddRange(zero_score);
                        io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}:  Excluding groups with zero scores: {string.Join(", ", zero_score)}.");
                    }
                }

                {
                    if (num_available_groups > 5)
                    {
                        var always_poor = iteration_whole_results_fixed_with_ranks.Where(a =>
                            a.rd.rank_score.value_history.Length >= 5 &&
                            a.rd.rank_score.value_history.TakeLast(5).All(b => b.value <= 0.1 /* bottom 10% of ranks for last 5 iterations */))
                            .Select(a => a.sd.index_data.group_array_index).ToArray();

                        if (always_poor.Length > 0)
                        {
                            selection_excluded_groups.AddRange(always_poor);
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}:  Excluding groups with always poor scores: {string.Join(", ", always_poor)}.");
                        }
                    }
                }


                if (!feature_selection_finished)
                {
                    iterations_not_higher_than_last = this_iteration_winner_cm_sd_rd.sd.is_score_higher_than_last_winner ? 0 : iterations_not_higher_than_last + 1;
                    iterations_not_higher_than_best = this_iteration_winner_cm_sd_rd.sd.is_score_higher_than_best_winner ? 0 : iterations_not_higher_than_best + 1;

                    if (this_iteration_winner_cm_sd_rd.sd.is_score_higher_than_best_winner)
                    {
                        best_winner_cm_sd_rd = this_iteration_winner_cm_sd_rd;
                    }



                    var groups_not_available = num_available_groups == 0;
                    var not_higher_than_last_limit_reached = iterations_not_higher_than_last >= limit_iteration_not_higher_than_last;
                    var not_higher_than_best_limit_reached = iterations_not_higher_than_best >= limit_iteration_not_higher_than_all;
                    var max_iterations_reached = max_iterations > 0 && iteration_index + 1 >= max_iterations;
                    var score_increase_not_reached = min_score_increase > 0 && (this_iteration_winner_cm_sd_rd.sd.same_group_score.value - (last_winner_cm_sd_rd.sd?.same_group_score?.value ?? 0)) < min_score_increase;

                    //if (feature_selection_finished) io_proxy.WriteLine($@"{nameof(feature_selection_finished)}: {nameof(feature_selection_finished)} = {feature_selection_finished}");
                    if (not_higher_than_last_limit_reached) io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(not_higher_than_last_limit_reached)} = {not_higher_than_last_limit_reached}");
                    if (not_higher_than_best_limit_reached) io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(not_higher_than_best_limit_reached)} = {not_higher_than_best_limit_reached}");
                    if (groups_not_available) io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(groups_not_available)} = {groups_not_available}");
                    if (max_iterations_reached) io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(max_iterations_reached)} = {max_iterations_reached}");
                    if (score_increase_not_reached) io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: {nameof(feature_selection_finished)}: {nameof(score_increase_not_reached)} = {score_increase_not_reached}");

                    feature_selection_finished = /*feature_selection_finished ||*/ not_higher_than_last_limit_reached || not_higher_than_best_limit_reached || groups_not_available;
                }

                // if main program instance, then save iteration winner to file
                if (save_status && instance_id == 0)
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
                                io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Already saved for iteration {(iteration_index)}. File: {all_iterations_status_fn}.");
                            }
                            else
                            {
                                io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Unavailable for iteration {(iteration_index)}. File: {all_iterations_status_fn}.");
                                feature_selection_status.save(cts, all_iterations_status_fn, feature_selection_status_list);
                                io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Saved for iteration {(iteration_index)}. File: {all_iterations_status_fn}.");
                            }
                        }
                    }

                    {
                        // Save the CM ranked for the current iteration (winner rank #0)
                        var iteration_cm_ranks_fn1 = Path.Combine(iteration_folder, $@"iteration_ranks_cm_{iteration_index}.csv");
                        var iteration_cm_ranks_fn2 = Path.Combine(iteration_folder, $@"iteration_ranks_cm_{iteration_index}_summary.csv");
                        if (io_proxy.is_file_available(cts, iteration_cm_ranks_fn1) && io_proxy.is_file_available(cts, iteration_cm_ranks_fn2))
                        {
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Already saved for iteration {(iteration_index)}. Files: {iteration_cm_ranks_fn1}, {iteration_cm_ranks_fn2}.");
                        }
                        else
                        {
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Unavailable for iteration {(iteration_index)}. Files: {iteration_cm_ranks_fn1}, {iteration_cm_ranks_fn2}.");
                            confusion_matrix.save(cts, cache_full ? iteration_cm_ranks_fn1 : null, cache_summary ? iteration_cm_ranks_fn2 : null, overwrite_cache, iteration_whole_results_fixed_with_ranks);
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Saved for iteration {(iteration_index)}. Files: {iteration_cm_ranks_fn1}, {iteration_cm_ranks_fn2}.");
                        }
                    }

                    {
                        // Save the CM of winners from all iterations
                        var winners_cm_fn1 = Path.Combine(iteration_folder, $@"winners_cm_{iteration_index}.csv");
                        var winners_cm_fn2 = Path.Combine(iteration_folder, $@"winners_cm_{iteration_index}_summary.csv");
                        if (io_proxy.is_file_available(cts, winners_cm_fn1) && io_proxy.is_file_available(cts, winners_cm_fn2))
                        {
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Already saved for iteration {(iteration_index)}. Files: {winners_cm_fn1}, {winners_cm_fn2}.");
                        }
                        else
                        {
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Unavailable for iteration {(iteration_index)}. Files: {winners_cm_fn1}, {winners_cm_fn2}.");
                            confusion_matrix.save(cts, cache_full ? winners_cm_fn1 : null, cache_summary ? winners_cm_fn2 : null, overwrite_cache, all_winners_cm_sd_rd_list.ToArray());
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
                            prediction.save(cts, prediction_list_filename, iteration_whole_results_fixed_with_ranks.Select(a => a.cm).ToArray());
                            io_proxy.WriteLine($@"[{instance_id}/{total_instances}] {experiment_name}: Saved for iteration {(iteration_index)}. File: {prediction_list_filename}.");
                        }

                    }
                }

                io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Finished: iteration {(iteration_index)}.  {(feature_selection_finished ? "Finished" : "Not finished")}.");

                last_winner_cm_sd_rd = this_iteration_winner_cm_sd_rd;
                last_iteration_cm_sd_rd_list = iteration_whole_results_fixed_with_ranks;
                //last_iteration_folder = iteration_folder;
                iteration_index++;
                calibrate = false;
            }

            io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Finished: all iterations of feature selection for {total_groups} groups.");
            io_proxy.WriteLine($"[{instance_id}/{total_instances}] {experiment_name}: Finished: winning score = {best_winner_cm_sd_rd.sd.same_group_score.value}, total columns = {best_winner_cm_sd_rd.sd.index_data.num_columns}.");


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

            var best_winner_groups = best_winner_cm_sd_rd.sd.index_data.group_array_indexes.Select(group_index => groups[group_index]).ToArray();

            return (best_winner_groups, best_winner_cm_sd_rd, all_winners_cm_sd_rd_list);
        }
    }
}