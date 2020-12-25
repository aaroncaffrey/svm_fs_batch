using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal class cross_validate
    {
        public const string module_name = nameof(cross_validate);

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

        internal static (index_data id, confusion_matrix cm)[] cross_validate_performance(
           CancellationTokenSource cts,
           dataset_loader dataset,
           //(dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] groups,
           string experiment_name,
           index_data unrolled_index_data,
           (index_data id, confusion_matrix cm, rank_score rs)[] last_iteration_id_cm_rs,
           (index_data id, confusion_matrix cm, rank_score rs) last_winner_id_cm_rs,
           (index_data id, confusion_matrix cm, rank_score rs) best_winner_id_cm_rs,
           bool make_outer_cv_confusion_matrices = false,
           bool overwrite_cache = false,
           bool save_group_cache = false,
           bool save_full = false,
           bool save_summary = false)
        {
            if (cts.IsCancellationRequested) return default;

            if (dataset == null) throw new ArgumentOutOfRangeException(nameof(dataset));
            if (string.IsNullOrWhiteSpace(unrolled_index_data.experiment_name)) throw new ArgumentOutOfRangeException(nameof(experiment_name));
            if (unrolled_index_data == null) throw new ArgumentOutOfRangeException(nameof(unrolled_index_data));

            io_proxy.WriteLine($@"{unrolled_index_data.experiment_name}: Start parallel index: {unrolled_index_data?.id_index_str()} {unrolled_index_data?.id_fold_str()} {unrolled_index_data?.id_ml_str()}");

            // todo: re-check for group cache file?
            io_proxy.WriteLine($@"{unrolled_index_data.experiment_name}: Group cache: Unavailable for iteration {(unrolled_index_data.iteration_index)} group {(unrolled_index_data.group_array_index)}/{unrolled_index_data.total_groups}");

            var ocv_result = outer_cross_validation(cts: cts, dataset: dataset, unrolled_index_data: unrolled_index_data, make_outer_cv_confusion_matrices: make_outer_cv_confusion_matrices, overwrite_cache: overwrite_cache, save_group_cache: save_group_cache, save_full: save_full, save_summary: save_summary);

            io_proxy.WriteLine($@"{unrolled_index_data.experiment_name}: Finished parallel index: {unrolled_index_data?.id_index_str()} {unrolled_index_data?.id_fold_str()} {unrolled_index_data?.id_ml_str()}");

            var group_cm_sd_list = ocv_result.mcv_cm.Select(cm =>
            {
                //var same_group = last_iteration_id_cm_rs.FirstOrDefault(a => a.id.group_array_index == cm.unrolled_index_data.group_array_index && a.id.iteration_index + 1 == cm.unrolled_index_data.iteration_index && a.cm.x_class_id == cm.x_class_id.Value);

                //var sd = new score_data(unrolled_index_data, cm, same_group: same_group.sd, last_winner: last_winner_id_cm_rs.sd, best_winner: best_winner_id_cm_rs.sd);

                return (unrolled_index_data, cm);
            })
                .ToArray();

            return group_cm_sd_list;
        }

        private static (index_data id, confusion_matrix[] ocv_cm, confusion_matrix[] mcv_cm) outer_cross_validation
        (
            CancellationTokenSource cts,
            dataset_loader dataset,
            //string experiment_name,
            //selection_test_info selection_test_info,
            //int[] test_selected_columns,
            index_data unrolled_index_data,
            //string group_folder,
            bool make_outer_cv_confusion_matrices,
            //dataset_group_key group_key,

            bool overwrite_cache = false,
            bool save_group_cache = false,
            bool save_full = false,
            bool save_summary = false
        )
        {
            const string method_name = nameof(outer_cross_validation);

            if (cts.IsCancellationRequested) return default;

            if (dataset == null) throw new ArgumentOutOfRangeException(nameof(dataset));
            //if (string.IsNullOrWhiteSpace(experiment_name)) throw new ArgumentOutOfRangeException(nameof(experiment_name));
            if (unrolled_index_data == null) throw new ArgumentOutOfRangeException(nameof(unrolled_index_data));
            if (unrolled_index_data.column_array_indexes == null || unrolled_index_data.column_array_indexes.Length == 0) throw new ArgumentOutOfRangeException(nameof(unrolled_index_data), $@"{module_name}.{method_name}.{nameof(unrolled_index_data)}.{nameof(unrolled_index_data.column_array_indexes)}");
            //if (selection_test_info == null) throw new ArgumentOutOfRangeException(nameof(selection_test_info));
            //if (group_key == null) throw new ArgumentOutOfRangeException(nameof(group_key));

            // 1. make outer-cv files
            var outer_cv_inputs = make_outer_cv_inputs(
                cts: cts,
                dataset: dataset,
                //column_indexes: test_selected_columns,
                //group_folder: group_folder,
                unrolled_index: unrolled_index_data
                );


            // 2. run libsvm
            var outer_cv_inputs_result = outer_cv_inputs
                .Where(a => a.outer_cv_index != -1 && a.repetitions_index != -1)
                .AsParallel()
                .AsOrdered()
                .WithCancellation(cts.Token)
                .Select(outer_cv_input => outer_cross_validation_single(cts, dataset, unrolled_index_data, outer_cv_input, make_outer_cv_confusion_matrices, overwrite_cache, save_group_cache, save_full, save_summary))
                .ToArray();

            // 1a. the ocvi index -1 is merged data
            var merged_cv_input = outer_cv_inputs.First(a => a.outer_cv_index == -1);

            var ocv_cm = make_outer_cv_confusion_matrices ? outer_cv_inputs_result.Where(a => a.ocv_cm != null).SelectMany(a => a.ocv_cm).ToArray() : null;
            var prediction_data_list = outer_cv_inputs_result.Select(a => a.prediction_data).ToArray();

            // 3. make confusion matrix from the merged prediction results
            // note: repeated 'labels' lines will be ignored
            var merged_prediction_text = prediction_data_list.SelectMany(a => a.predict_text).ToArray();

            var merged_test_class_sample_id_list = merged_cv_input.test_fold_indexes.SelectMany(a => a.test_indexes).ToArray();

            var prediction_file_data = performance_measure.load_prediction_file(cts, merged_cv_input.test_text, null, merged_prediction_text, unrolled_index_data.calc_11p_thresholds, merged_test_class_sample_id_list);
            for (var cm_index = 0; cm_index < prediction_file_data.cm_list.Length; cm_index++) { prediction_file_data.cm_list[cm_index].unrolled_index_data = unrolled_index_data; }

            var mcv_cm = prediction_file_data.cm_list;

            // add any missing details to the confusion-matrix
            program.update_merged_cm(
                cts: cts,
                dataset: dataset,
                prediction_file_data: prediction_file_data,
                unrolled_index_data: unrolled_index_data,
                merged_cv_input: merged_cv_input,
                prediction_data_list: prediction_data_list
                );

            // save CM for group
            if (save_group_cache && (save_full || save_summary))
            {
                confusion_matrix.save(cts, save_full ? merged_cv_input.cm_fn1 : null, save_summary ? merged_cv_input.cm_fn2 : null, overwrite_cache, mcv_cm);
                io_proxy.WriteLine($@"{unrolled_index_data.experiment_name}: Group MCV cache: Saved: {unrolled_index_data?.id_index_str()} {unrolled_index_data?.id_fold_str()} {unrolled_index_data?.id_ml_str()}. Files: {merged_cv_input.cm_fn1}, {merged_cv_input.cm_fn2}.");
            }
            else
            {
                io_proxy.WriteLine($@"{unrolled_index_data.experiment_name}: Group MCV cache: Save disabled: {unrolled_index_data?.id_index_str()} {unrolled_index_data?.id_fold_str()} {unrolled_index_data?.id_ml_str()}. Files: {merged_cv_input.cm_fn1}, {merged_cv_input.cm_fn2}.");

                if (!save_group_cache && !string.IsNullOrWhiteSpace(unrolled_index_data.group_folder)) io_proxy.delete_directory(unrolled_index_data.group_folder);
            }


            return (unrolled_index_data, ocv_cm, mcv_cm);
        }

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
                //int[] column_indexes,
                //string group_folder,
                index_data unrolled_index
                )
        {
            const string method_name = nameof(make_outer_cv_inputs);

            if (cts.IsCancellationRequested) return default;

            const bool preserve_fid = false; // whether to keep the original FID in the libsvm training/testing files (note: if not, bear in mind that features with zero values are removed, so this must not distort the ordering...).

            if (unrolled_index.column_array_indexes == null || unrolled_index.column_array_indexes.Length == 0) throw new ArgumentOutOfRangeException(nameof(unrolled_index), $@"{module_name}.{method_name}.{nameof(unrolled_index)}.{nameof(unrolled_index.column_array_indexes)}");
            if (unrolled_index.repetitions <= 0) throw new ArgumentOutOfRangeException(nameof(unrolled_index), $@"{module_name}.{method_name}.{nameof(unrolled_index)}.{nameof(unrolled_index.repetitions)}");
            if (unrolled_index.outer_cv_folds <= 0) throw new ArgumentOutOfRangeException(nameof(unrolled_index), $@"{module_name}.{method_name}.{nameof(unrolled_index)}.{nameof(unrolled_index.outer_cv_folds)}");
            if (unrolled_index.outer_cv_folds_to_run < 0 || unrolled_index.outer_cv_folds_to_run > unrolled_index.outer_cv_folds) throw new ArgumentOutOfRangeException(nameof(unrolled_index), $@"{module_name}.{method_name}.{nameof(unrolled_index)}.{nameof(unrolled_index.outer_cv_folds_to_run)}");


            // ensure columns in correct order, and has class id
            unrolled_index.column_array_indexes = unrolled_index.column_array_indexes.OrderBy(a => a).ToArray();
            if (unrolled_index.column_array_indexes[0] != 0)
            {
                unrolled_index.column_array_indexes = (new int[] { 0 }).Concat(unrolled_index.column_array_indexes).ToArray();
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

                    var filename = Path.Combine(unrolled_index.group_folder, $@"o_{program.get_item_filename(unrolled_index, repetitions_index, outer_cv_index)}");
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
                                    .SelectMany(b => b.class_sample_indexes)
                                    .OrderBy(b => b)
                                    .ToArray()
                            )
                        ).ToArray();

                    var train_sizes = train_fold_indexes.Select(a => (class_id: a.class_id, train_size: a.train_indexes?.Length ?? 0)).ToArray();
                    var train_row_values = dataset.get_row_features(cts, train_fold_indexes, unrolled_index.column_array_indexes);
                    var train_scaling = dataset_loader.get_scaling_params(train_row_values, unrolled_index.column_array_indexes);
                    var train_row_scaled_values = dataset_loader.get_scaled_rows(train_row_values, /*column_indexes,*/ train_scaling, unrolled_index.scale_function);
                    var train_text = train_row_scaled_values.AsParallel().AsOrdered().WithCancellation(cts.Token).Select(row => $@"{(int)row[0]} {string.Join(" ", row.Skip(1 /* skip class id column */).Select((col_val, x_index) => col_val != 0 ? $@"{(preserve_fid ? unrolled_index.column_array_indexes[x_index] : (x_index + 1))}:{col_val:G17}" : $@"").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray())}").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray();

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
                                    .SelectMany(b => b.class_sample_indexes)
                                    .OrderBy(b => b)
                                    .ToArray()
                            )
                        ).ToArray();

                    var test_sizes = test_fold_indexes.Select(a => (class_id: a.class_id, test_size: a.test_indexes?.Length ?? 0)).ToArray();
                    var test_row_values = dataset.get_row_features(cts, test_fold_indexes, unrolled_index.column_array_indexes);
                    var test_scaling = train_scaling; /* scale test data with training data */
                    var test_row_scaled_values = dataset_loader.get_scaled_rows(test_row_values, /*column_indexes,*/ test_scaling, unrolled_index.scale_function);
                    var test_text = test_row_scaled_values.AsParallel().AsOrdered().WithCancellation(cts.Token).Select(row => $@"{(int)row[0]} {string.Join(" ", row.Skip(1 /* skip class id column */).Select((col_val, x_index) => col_val != 0 ? $@"{(preserve_fid ? unrolled_index.column_array_indexes[x_index] : (x_index + 1))}:{col_val:G17}" : $@"").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray())}").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray();

                    return (repetitions_index, outer_cv_index, train_fn, grid_fn, model_fn, test_fn, predict_fn, cm_fn1, cm_fn2, train_text, test_text, train_sizes, test_sizes, train_fold_indexes, test_fold_indexes);
                })
                .ToArray();

            Parallel.ForEach(ocv_data,
                item =>
                {
                    if (cts.IsCancellationRequested) return;

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
            var merged_filename_prefix = Path.Combine(unrolled_index.group_folder, $@"m_{program.get_iteration_filename(new[] { unrolled_index })}");
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
            ocv_data = (new[] { merged_cv_input }).Concat(ocv_data).ToArray();


            //var test_class_sample_id_list = merged_cv_input.test_fold_indexes.SelectMany(a => a.test_indexes).ToList();


            var save_merged_files = false;
            if (save_merged_files)
            {
                io_proxy.WriteAllLines(cts, merged_train_fn, merged_train_text);
                io_proxy.WriteAllLines(cts, merged_test_fn, merged_test_text);
            }

            return ocv_data;
        }


        private static (((long grid_dur, long train_dur, long predict_dur) dur, grid_point grid_point, string[] predict_text) prediction_data, confusion_matrix[] ocv_cm)
            outer_cross_validation_single
            (
                CancellationTokenSource cts,
                dataset_loader dataset,
                index_data unrolled_index_data,
                (int repetitions_index, int outer_cv_index, string train_fn, string grid_fn, string model_fn, string test_fn, string predict_fn, string cm_fn1, string cm_fn2, string[] train_text, string[] test_text, (int class_id, int training_size)[] train_sizes, (int class_id, int testing_size)[] test_sizes, (int class_id, int[] train_indexes)[] train_fold_indexes, (int class_id, int[] test_indexes)[] test_fold_indexes) outer_cv_input,
                bool make_outer_cv_confusion_matrices = false,
                bool overwrite_cache = false,
                bool save_group_cache = false,
                bool save_full = false,
                bool save_summary = false
            )
        {
            confusion_matrix[] ocv_cm = null;

            // call libsvm... returns raw prediction file data from doing: parameter search -> train (with best parameters) -> predict
            var prediction_data = inner_cross_validation(cts, unrolled_index_data, outer_cv_input);

            // optional: make_outer_cv_confusion_matrices: this will output the individual outer-cross-validation confusion matrices (i.e. if outer-cv-folds = 5, then 5 respective confusion-matrices will be created, as well as the merged data confusion-matrix).
            if (make_outer_cv_confusion_matrices)
            {
                var ocv_test_class_sample_id_list = outer_cv_input.test_fold_indexes.SelectMany(a => a.test_indexes).ToArray();

                // convert text results to confusion matrix and performance metrics
                var ocv_prediction_file_data = performance_measure.load_prediction_file(cts, outer_cv_input.test_text, null, prediction_data.predict_text, unrolled_index_data.calc_11p_thresholds, ocv_test_class_sample_id_list);
                for (var cm_index = 0; cm_index < ocv_prediction_file_data.cm_list.Length; cm_index++) { ocv_prediction_file_data.cm_list[cm_index].unrolled_index_data = unrolled_index_data; }

                // add any missing meta details to the confusion-matrix
                program.update_merged_cm(cts: cts, dataset: dataset, prediction_file_data: ocv_prediction_file_data, unrolled_index_data: unrolled_index_data, merged_cv_input: outer_cv_input, prediction_data_list: new[] { prediction_data });

                //ocv_cm.AddRange(ocv_prediction_file_data.cm_list);
                ocv_cm = ocv_prediction_file_data.cm_list;

                if (save_group_cache && (save_full || save_summary))
                {
                    // save outer-cross-validation confusion-matrix CM for group
                    confusion_matrix.save(cts, save_full ? outer_cv_input.cm_fn1 : null, save_summary ? outer_cv_input.cm_fn2 : null, overwrite_cache, ocv_prediction_file_data.cm_list);
                    io_proxy.WriteLine($@"{unrolled_index_data.experiment_name}: Group OCV cache: Saved: [R({outer_cv_input.repetitions_index}/{unrolled_index_data.repetitions}) O({outer_cv_input.outer_cv_index}/{unrolled_index_data.outer_cv_folds})] {unrolled_index_data.id_index_str()} {unrolled_index_data.id_fold_str()} {unrolled_index_data.id_ml_str()}. Files: {outer_cv_input.cm_fn1}, {outer_cv_input.cm_fn2}.");
                }
                else
                {
                    io_proxy.WriteLine($@"{unrolled_index_data.experiment_name}: Group OCV cache: Save disabled: [R({outer_cv_input.repetitions_index}/{unrolled_index_data.repetitions}) O({outer_cv_input.outer_cv_index}/{unrolled_index_data.outer_cv_folds})] {unrolled_index_data.id_index_str()} {unrolled_index_data.id_fold_str()} {unrolled_index_data.id_ml_str()}. Files: {outer_cv_input.cm_fn1}, {outer_cv_input.cm_fn2}.");
                }
            }

            // delete temporary files
            io_proxy.delete_file(outer_cv_input.train_fn);
            io_proxy.delete_file(outer_cv_input.grid_fn);
            io_proxy.delete_file(outer_cv_input.model_fn);
            io_proxy.delete_file(outer_cv_input.test_fn);
            io_proxy.delete_file(outer_cv_input.predict_fn);
            // note: do not delete the confusion-matrix: io_proxy.Delete(outer_cv_input.cm_fn);

            return (prediction_data, ocv_cm);
        }
    }
}
