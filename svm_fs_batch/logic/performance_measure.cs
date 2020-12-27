﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal static class performance_measure
    {
        public const string module_name = nameof(performance_measure);

        internal static List<confusion_matrix> count_prediction_error(

            CancellationTokenSource cts,
            prediction[] prediction_list,
            double? threshold = null,
            int? threshold_class = null,
            bool calculate_auc = true,
            bool as_parallel = false
            )
        {
            if (cts.IsCancellationRequested) return default;

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(prediction_list),prediction_list.ToString()),
            //    (nameof(threshold),threshold.ToString()),
            //    (nameof(threshold_class),threshold_class.ToString()),
            //    (nameof(calculate_auc),calculate_auc.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(count_prediction_error)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var actual_class_id_list = prediction_list.Select(a => a.real_class_id).Distinct().OrderBy(a => a).ToList();

            var confusion_matrix_list = new List<confusion_matrix>();

            for (var i = 0; i < actual_class_id_list.Count; i++)
            {
                var actual_class_id = actual_class_id_list[i];

                var confusion_matrix1 = new confusion_matrix()
                {
                    x_class_id = actual_class_id,
                    metrics = new metrics_box()
                    {
                        cm_P = prediction_list.Count(b => actual_class_id == b.real_class_id),
                        cm_N = prediction_list.Count(b => actual_class_id != b.real_class_id)
                    },
                    x_prediction_threshold = threshold,
                    x_prediction_threshold_class = threshold_class,
                    thresholds = prediction_list.Select(a => a.probability_estimates.FirstOrDefault(b => b.class_id == actual_class_id).probability_estimate)/*.Distinct()*/.OrderByDescending(a => a).ToArray(),
                    predictions = prediction_list.ToArray()
                };
                confusion_matrix_list.Add(confusion_matrix1);

            }

            for (var prediction_list_index = 0; prediction_list_index < prediction_list.Length; prediction_list_index++)
            {
                var prediction = prediction_list[prediction_list_index];

                var actual_class_matrix = confusion_matrix_list.First(a => a.x_class_id == prediction.real_class_id);
                var predicted_class_matrix = confusion_matrix_list.First(a => a.x_class_id == prediction.predicted_class_id);

                if (prediction.real_class_id == prediction.predicted_class_id)
                {
                    actual_class_matrix.metrics.cm_P_TP++;

                    for (var index = 0; index < confusion_matrix_list.Count; index++)
                    {
                        if (confusion_matrix_list[index].x_class_id != prediction.real_class_id)
                        {
                            confusion_matrix_list[index].metrics.cm_N_TN++;
                        }
                    }
                }

                else if (prediction.real_class_id != prediction.predicted_class_id)
                {
                    actual_class_matrix.metrics.cm_P_FN++;

                    predicted_class_matrix.metrics.cm_N_FP++;
                }
            }

            if (as_parallel)
            {
                Parallel.ForEach(confusion_matrix_list, cm =>
                {
                    cm.calculate_theshold_metrics(cts, cm.metrics, calculate_auc, prediction_list);
                });
            }
            else
            {
                foreach (var cm in confusion_matrix_list)
                {
                    cm.calculate_theshold_metrics(cts, cm.metrics, calculate_auc, prediction_list);
                }
            }

            return confusion_matrix_list;
        }



        internal static double area_under_curve_trapz((double x, double y)[] coordinate_list)//, bool interpolation = true)
        {

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(coordinate_list),coordinate_list.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(area_under_curve_trapz)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            //var coords = new List<(double x1, double x2, double y1, double y2)>();
            coordinate_list = coordinate_list.Distinct().ToArray();
            coordinate_list = coordinate_list.OrderBy(a => a.x).ThenBy(a => a.y).ToArray();
            var auc = coordinate_list.Select((c, i) => i >= coordinate_list.Length - 1 ? 0 : (coordinate_list[i + 1].x - coordinate_list[i].x) * ((coordinate_list[i].y + coordinate_list[i + 1].y) / 2)).Sum();
            return auc;
        }


        internal static prediction[] load_prediction_file_probability_values(CancellationTokenSource cts, (string test_file, string test_comments_file, string prediction_file, string test_class_sample_id_list_file)[] files, bool as_parallel = false)
        {
            // method untested
            const string method_name = nameof(load_prediction_file_probability_values);

            if (cts.IsCancellationRequested) return default;

            var lines =
                as_parallel ?
                    files.AsParallel().AsOrdered().WithCancellation(cts.Token).Select((a, i) =>
                    (
                        test_file_lines: io_proxy.ReadAllLines(cts, a.test_file, module_name, method_name).ToArray(),
                        test_comments_file_lines: io_proxy.ReadAllLines(cts, a.test_comments_file, module_name, method_name).ToArray(),
                        prediction_file_lines: io_proxy.ReadAllLines(cts, a.prediction_file, module_name, method_name).ToArray(),
                        test_class_sample_id_list_lines: !string.IsNullOrWhiteSpace(a.test_class_sample_id_list_file) ? io_proxy.ReadAllLines(cts, a.test_class_sample_id_list_file, module_name, method_name).ToArray() : null
                    )).ToArray()
                :
                    files.Select((a, i) =>
                    (
                        test_file_lines: io_proxy.ReadAllLines(cts, a.test_file, module_name, method_name).ToArray(),
                        test_comments_file_lines: io_proxy.ReadAllLines(cts, a.test_comments_file, module_name, method_name).ToArray(),
                        prediction_file_lines: io_proxy.ReadAllLines(cts, a.prediction_file, module_name, method_name).ToArray(),
                        test_class_sample_id_list_lines: !string.IsNullOrWhiteSpace(a.test_class_sample_id_list_file) ? io_proxy.ReadAllLines(cts, a.test_class_sample_id_list_file, module_name, method_name).ToArray() : null
                    )).ToArray();

            // prediction file MAY have a header, but only if probability estimates are enabled

            //var test_has_headers = false;
            //var test_comments_has_headers = true;

            // check any prediction file has labels on first line
            var prediction_has_headers = lines.Any(a => a.prediction_file_lines.FirstOrDefault().StartsWith("labels", StringComparison.Ordinal));

            if (prediction_has_headers)
            {
                // check all labels in all prediction files match
                if (lines.Select(a => a.prediction_file_lines.FirstOrDefault()).Distinct().Count() != 1) { throw new ArgumentOutOfRangeException(nameof(files)); }
            }

            lines = as_parallel ?

                lines.AsParallel().AsOrdered().WithCancellation(cts.Token).Select((a, i) => (
                a.test_file_lines,
                a.test_comments_file_lines.Skip(1 /* skip header */).ToArray(),
                a.prediction_file_lines.Skip(i > 0 && prediction_has_headers ? 1 : 0).ToArray(),
                a.test_class_sample_id_list_lines
            )).ToArray() :
                lines.Select((a, i) => (
                    a.test_file_lines,
                    a.test_comments_file_lines.Skip(1 /* skip header */).ToArray(),
                    a.prediction_file_lines.Skip(i > 0 && prediction_has_headers ? 1 : 0).ToArray(),
                    a.test_class_sample_id_list_lines
                )).ToArray();

            var test_file_lines = lines.SelectMany(a => a.test_file_lines).ToArray();
            var test_comments_file_lines = lines.SelectMany(a => a.test_comments_file_lines).ToArray();
            var prediction_file_lines = lines.SelectMany(a => a.prediction_file_lines).ToArray();

            var test_class_sample_id_list = lines.Where(a => a.test_class_sample_id_list_lines != null).SelectMany(a => a.test_class_sample_id_list_lines).Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();
            if (test_class_sample_id_list.Length == 0) test_class_sample_id_list = null;

            return load_prediction_file_probability_values_from_text(cts, test_file_lines, test_comments_file_lines, prediction_file_lines, test_class_sample_id_list);
        }

        internal static prediction[] load_prediction_file_probability_values(CancellationTokenSource cts, string test_file, string test_comments_file, string prediction_file, string test_sample_id_list_file = null)
        {

            const string method_name = nameof(load_prediction_file_probability_values);

            if (cts.IsCancellationRequested) return default;

            //if (string.IsNullOrWhiteSpace(test_file) || !io_proxy.Exists(test_file, nameof(performance_measure), nameof(load_prediction_file_regression_values)) || new FileInfo(test_file).Length == 0)
            //{
            //    throw new Exception($@"Error: Test data file not found: ""{test_file}"".");
            //}

            //if (!io_proxy.is_file_available(test_file))
            //{
            //    throw new Exception($@"Error: Test data file not available for access: ""{test_file}"".");
            //}

            //if (string.IsNullOrWhiteSpace(prediction_file) || !io_proxy.Exists(prediction_file, nameof(performance_measure), nameof(load_prediction_file_regression_values)) || new FileInfo(prediction_file).Length == 0)
            //{
            //    throw new Exception($@"Error: Prediction output file not found: ""{prediction_file}"".");
            //}

            //if (!io_proxy.is_file_available(prediction_file))
            //{
            //    throw new Exception($@"Error: Prediction output file not available for access: ""{prediction_file}"".");
            //}

            var test_file_lines = io_proxy.ReadAllLines(cts, test_file, module_name, method_name);

            var test_comments_file_lines = !string.IsNullOrWhiteSpace(test_comments_file) && io_proxy.Exists(test_comments_file/*, module_name, method_name*/) ? io_proxy.ReadAllLines(cts, test_comments_file, module_name, method_name) : null;

            var prediction_file_lines = io_proxy.ReadAllLines(cts, prediction_file, module_name, method_name);

            var test_sample_id_list_lines = !string.IsNullOrWhiteSpace(test_sample_id_list_file) ? io_proxy.ReadAllLines(cts, test_sample_id_list_file, module_name, method_name) : null;
            var test_class_sample_id_list = test_sample_id_list_lines?.Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();

            return load_prediction_file_probability_values_from_text(cts, test_file_lines, test_comments_file_lines, prediction_file_lines, test_class_sample_id_list);
        }

        internal static prediction[] load_prediction_file_probability_values_from_text(CancellationTokenSource cts, string[] test_file_lines, string[] test_comments_file_lines, string[] prediction_file_lines, int[] test_class_sample_id_list, bool as_parallel = false)
        {
            if (cts.IsCancellationRequested) return default;

            if (test_file_lines == null || test_file_lines.Length == 0)
            {
                throw new ArgumentNullException(nameof(test_file_lines));
            }

            if (prediction_file_lines == null || prediction_file_lines.Length == 0)
            {
                throw new ArgumentNullException(nameof(prediction_file_lines));
            }

            // remove comments from test_file_lines (comments start with #)
            test_file_lines = test_file_lines.Select(a =>
            {
                var hash_index = a.IndexOf('#', StringComparison.Ordinal);

                if (hash_index > -1)
                {
                    return a.Substring(0, hash_index).Trim();
                }

                return a.Trim();
            }).ToArray();



            var test_file_comments_header = test_comments_file_lines?.FirstOrDefault()?.Split(',') ?? null;

            if (test_comments_file_lines != null && test_comments_file_lines.Length > 0)
            {
                test_comments_file_lines = test_comments_file_lines.Skip(1).ToArray();
            }


            var test_file_data = test_file_lines
                .Where(a => !string.IsNullOrWhiteSpace(a) && int.TryParse(a.Trim().Split().First(), NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var line_actual_class_id))
                .Select(a => a.Trim().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries)).ToArray();



            if (test_file_lines.Length == 0) return null;


            var prediction_file_data = prediction_file_lines
                .Where(a => !string.IsNullOrWhiteSpace(a) && int.TryParse(a.Trim().Split().First(), NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var line_predicted_class_id))
                .Select(a => a.Trim().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries))
                .ToArray();

            //if (prediction_file_lines.Count == 0) return null;
            if (prediction_file_data.Length == 0) return null;

            var probability_estimate_class_labels = new List<int>();

            if (prediction_file_lines.Where(a => a.Trim().StartsWith($@"labels")).Distinct().Count() > 1) /* should be 0 or 1 for the same model */
            {
                throw new Exception($@"Error: more than one set of labels in the same file.");
            }

            if (prediction_file_lines.First().Trim().Split().First() == $@"labels")
            {
                probability_estimate_class_labels = prediction_file_lines.First().Trim().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries).Skip(1).Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToList();
            }

            if (test_comments_file_lines != null && test_file_data.Length != test_comments_file_lines.Length)
            {
                throw new Exception($@"Error: test file and test comments file have different instance length: [ {test_file_data.Length} : {test_comments_file_lines.Length} ].");
            }

            if (test_file_data.Length != prediction_file_data.Length)
            {
                throw new Exception($@"Error: test file and prediction file have different instance length: [ {test_file_data.Length} : {prediction_file_data.Length} ].");
            }

            if (test_class_sample_id_list != null && test_class_sample_id_list.Length > 0 && test_class_sample_id_list.Length != test_file_data.Length)
            {
                throw new Exception($@"Error: test sample ids and test file data do not match: [ {test_file_data.Length} : {prediction_file_data.Length} ].");
            }

            var total_predictions = test_file_data.Length;


            var prediction_list =
                as_parallel ?
                Enumerable.Range(0, total_predictions).AsParallel().AsOrdered().WithCancellation(cts.Token).Select(prediction_index =>
                {
                    var probability_estimates = prediction_file_data[prediction_index].Length <= 1 ?
                        Array.Empty<(int class_id, double probability_estimate)>()
                        :
                        prediction_file_data[prediction_index]
                            .Skip(1 /* skip predicted class id */)
                            .Select((a, i) => (class_id: probability_estimate_class_labels[i], probability_estimate: double.TryParse(a, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var pe_out) ? pe_out : default))
                            .OrderBy(a => a.class_id)
                            .ToArray();

                    //var probability_estimates_stated = probability_estimates != null && probability_estimates.Count > 0 && probability_estimate_class_labels != null && probability_estimate_class_labels.Count > 0;

                    var prediction = new prediction()
                    {
                        prediction_index = prediction_index,
                        class_sample_id = test_class_sample_id_list != null && test_class_sample_id_list.Length - 1 >= prediction_index ? test_class_sample_id_list[prediction_index] : -1,
                        comment = test_comments_file_lines != null && test_comments_file_lines.Length > 0 ? test_comments_file_lines[prediction_index].Split(',').Select((a, i) => (comment_header: ((test_file_comments_header?.Length ?? 0) - 1 >= i ? test_file_comments_header[i] : ""), comment_value: a)).ToArray() : Array.Empty<(string comment_header, string comment_value)>(),
                        real_class_id = int.TryParse(test_file_data[prediction_index][0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_real_class_id) ? out_real_class_id : default,
                        predicted_class_id = int.TryParse(prediction_file_data[prediction_index][0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_predicted_class_id) ? out_predicted_class_id : default,
                        probability_estimates = probability_estimates,
                        //test_row_vector = test_file_data[prediction_index].Skip(1/* skip class id column */).ToArray(),
                    };

                    return prediction;
                }).ToArray() :
                Enumerable.Range(0, total_predictions).Select(prediction_index =>
                {
                    var probability_estimates = prediction_file_data[prediction_index].Length <= 1 ?
                        Array.Empty<(int class_id, double probability_estimate)>()
                        :
                        prediction_file_data[prediction_index]
                            .Skip(1 /* skip predicted class id */)
                            .Select((a, i) => (class_id: probability_estimate_class_labels[i], probability_estimate: double.TryParse(a, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var pe_out) ? pe_out : default))
                            .OrderBy(a => a.class_id)
                            .ToArray();

                    //var probability_estimates_stated = probability_estimates != null && probability_estimates.Count > 0 && probability_estimate_class_labels != null && probability_estimate_class_labels.Count > 0;

                    var prediction = new prediction()
                    {
                        prediction_index = prediction_index,
                        class_sample_id = test_class_sample_id_list != null && test_class_sample_id_list.Length - 1 >= prediction_index ? test_class_sample_id_list[prediction_index] : -1,
                        comment = test_comments_file_lines != null && test_comments_file_lines.Length > 0 ? test_comments_file_lines[prediction_index].Split(',').Select((a, i) => (comment_header: ((test_file_comments_header?.Length ?? 0) - 1 >= i ? test_file_comments_header[i] : ""), comment_value: a)).ToArray() : Array.Empty<(string comment_header, string comment_value)>(),
                        real_class_id = int.TryParse(test_file_data[prediction_index][0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_real_class_id) ? out_real_class_id : default,
                        predicted_class_id = int.TryParse(prediction_file_data[prediction_index][0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_predicted_class_id) ? out_predicted_class_id : default,
                        probability_estimates = probability_estimates,
                        //test_row_vector = test_file_data[prediction_index].Skip(1/* skip class id column */).ToArray(),
                    };

                    return prediction;
                }).ToArray();

            return prediction_list;
        }

        //internal static (List<prediction> prediction_list, List<confusion_matrix> cm_list) load_prediction_file(List<(string test_file, string test_comments_file, string prediction_file)> files, bool calc_11p_thresholds)
        //{


        //    var prediction_list = load_prediction_file_regression_values(test_file, test_comments_file, prediction_file);
        //    var cm_list = load_prediction_file(prediction_list, calc_11p_thresholds);

        //    return (prediction_list, cm_list);

        //}

        internal static (prediction[] prediction_list, confusion_matrix[] cm_list) load_prediction_file(CancellationTokenSource cts, string test_file, string test_comments_file, string prediction_file, bool calc_11p_thresholds)
        {
            if (cts.IsCancellationRequested) return default;
            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(test_file),test_io_proxy.ToString()),
            //    (nameof(prediction_file),prediction_io_proxy.ToString()),
            //    (nameof(calc_11p_thresholds),calc_11p_thresholds.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(load_prediction_file)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var prediction_list = load_prediction_file_probability_values(cts, test_file, test_comments_file, prediction_file);
            var cm_list = load_prediction_file(cts, prediction_list, calc_11p_thresholds);

            return (prediction_list, cm_list);
        }

        //internal static (List<prediction> prediction_list, List<confusion_matrix> cm_list) load_prediction_file(string[] test_file_lines, string[] test_comments_file_lines, string[] prediction_file_lines, bool calc_11p_thresholds, int[] test_class_sample_id_list)
        internal static (prediction[] prediction_list, confusion_matrix[] cm_list) load_prediction_file(CancellationTokenSource cts, string[] test_file_lines, string[] test_comments_file_lines, string[] prediction_file_lines, bool calc_11p_thresholds, int[] test_class_sample_id_list)
        {
            if (cts.IsCancellationRequested) return default;

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(test_file),test_io_proxy.ToString()),
            //    (nameof(prediction_file),prediction_io_proxy.ToString()),
            //    (nameof(calc_11p_thresholds),calc_11p_thresholds.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(load_prediction_file)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var prediction_list = load_prediction_file_probability_values_from_text(cts, test_file_lines, test_comments_file_lines, prediction_file_lines, test_class_sample_id_list);
            var cm_list = load_prediction_file(cts, prediction_list, calc_11p_thresholds);

            return (prediction_list, cm_list);
        }

        internal static confusion_matrix[] load_prediction_file(CancellationTokenSource cts, prediction[] prediction_list, bool calc_11p_thresholds)
        {
            if (cts.IsCancellationRequested) return default;

            if (prediction_list == null || prediction_list.Length == 0) throw new ArgumentOutOfRangeException(nameof(prediction_list));

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(prediction_list),prediction_list.ToString()),
            //    (nameof(calc_11p_thresholds),calc_11p_thresholds.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(load_prediction_file)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var class_id_list = prediction_list.SelectMany(a => new int[] { a.real_class_id, a.predicted_class_id }).Distinct().OrderBy(a => a).ToArray();

            if (class_id_list == null || class_id_list.Length == 0) throw new Exception();

            var confusion_matrix_list = new List<confusion_matrix>();

            // make confusion matrix performance scores with default decision boundary threshold
            var default_confusion_matrix_list = count_prediction_error(cts, prediction_list);
            confusion_matrix_list.AddRange(default_confusion_matrix_list);


            if (class_id_list.Length >= 2 && calc_11p_thresholds)
            {
                // make confusion matrix performance scores with altered default decision boundary threshold
                for (var class_id_x = 0; class_id_x < class_id_list.Length; class_id_x++)
                {
                    for (var class_id_y = 0; class_id_y < class_id_list.Length; class_id_y++)
                    {
                        if (class_id_x >= class_id_y) continue;

                        var negative_id = class_id_list[class_id_x];
                        var positive_id = class_id_list[class_id_y];

                        var threshold_prediction_list = eleven_points.Select(th => (positive_threshold: th, prediction_list: prediction_list.Select(p => new prediction(p)
                        {
                            predicted_class_id = p.probability_estimates.First(e => e.class_id == positive_id).probability_estimate >= th ? positive_id : negative_id,
                        }).ToArray())).ToArray();

                        var threshold_confusion_matrix_list = threshold_prediction_list.SelectMany(a => count_prediction_error(cts, a.prediction_list, a.positive_threshold, positive_id, false)).ToArray();


                        for (var i = 0; i < threshold_confusion_matrix_list.Length; i++)
                        {
                            var class_default_cm = default_confusion_matrix_list.First(a => a.x_class_id == threshold_confusion_matrix_list[i].x_class_id);

                            // note: AUC_ROC and AUC_PR don't change when altering the default threshold since the predicted class isn't a factor in their calculation.

                            threshold_confusion_matrix_list[i].metrics.p_ROC_AUC_Approx_All = class_default_cm.metrics.p_ROC_AUC_Approx_All;
                            threshold_confusion_matrix_list[i].metrics.p_ROC_AUC_Approx_11p = class_default_cm.metrics.p_ROC_AUC_Approx_11p;
                            threshold_confusion_matrix_list[i].roc_xy_str_all = class_default_cm.roc_xy_str_all;
                            threshold_confusion_matrix_list[i].roc_xy_str_11p = class_default_cm.roc_xy_str_11p;

                            threshold_confusion_matrix_list[i].metrics.p_PR_AUC_Approx_All = class_default_cm.metrics.p_PR_AUC_Approx_All;
                            threshold_confusion_matrix_list[i].metrics.p_PR_AUC_Approx_11p = class_default_cm.metrics.p_PR_AUC_Approx_11p;
                            threshold_confusion_matrix_list[i].pr_xy_str_all = class_default_cm.pr_xy_str_all;
                            threshold_confusion_matrix_list[i].pr_xy_str_11p = class_default_cm.pr_xy_str_11p;
                            threshold_confusion_matrix_list[i].pri_xy_str_all = class_default_cm.pri_xy_str_all;
                            threshold_confusion_matrix_list[i].pri_xy_str_11p = class_default_cm.pri_xy_str_11p;
                        }

                        confusion_matrix_list.AddRange(threshold_confusion_matrix_list);
                    }
                }
            }

            return confusion_matrix_list.ToArray();
        }

        internal static double brier(prediction[] prediction_list, int positive_id)
        {
            if (prediction_list.Any(a => a.probability_estimates == null || a.probability_estimates.Length == 0)) return default;

            prediction_list = prediction_list.OrderByDescending(a => a.probability_estimates.First(b => b.class_id == positive_id).probability_estimate).ToArray();

            // Calc Brier
            var brier_score = ((double)1 / (double)prediction_list.Length)
                              * (prediction_list.Sum(a => Math.Pow(a.probability_estimates.First(b => b.class_id == a.predicted_class_id).probability_estimate - (a.real_class_id == a.predicted_class_id ? 1 : 0), 2)));

            return brier_score;
        }

        internal enum threshold_type
        {
            all_thresholds,
            eleven_points
        }

        internal static readonly double[] eleven_points = new double[] { 1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0 };

        internal static (double roc_auc_approx, double roc_auc_actual, double pr_auc_approx, double pri_auc_approx, double ap, double api, (double x, double y)[] roc_xy, (double x, double y)[] pr_xy, (double x, double y)[] pri_xy)
            Calculate_ROC_PR_AUC(CancellationTokenSource cts, prediction[] prediction_list, int positive_id, threshold_type threshold_type = threshold_type.all_thresholds)
        {
            if (cts.IsCancellationRequested) return default;

            if (prediction_list.Any(a => a.probability_estimates == null || a.probability_estimates.Length == 0)) return default;

            // Assume binary classifier - get negative class id
            var class_ids = prediction_list.Select(a => a.real_class_id).Union(prediction_list.Select(c => c.predicted_class_id)).Distinct().OrderBy(a => a).ToArray();
            var negative_id = class_ids.First(class_id => class_id != positive_id);

            // Calc P
            var p = (double)prediction_list.Count(a => a.real_class_id == positive_id);

            // Calc N
            var n = (double)prediction_list.Count(a => a.real_class_id != positive_id);

            // Order predictions descending by positive class probability
            prediction_list = prediction_list.OrderByDescending(a => a.probability_estimates.FirstOrDefault(b => b.class_id == positive_id).probability_estimate).ToArray();

            var threshold_confusion_matrix_list = get_theshold_confusion_matrices(cts, prediction_list, positive_id, threshold_type, negative_id);


            // Average Precision (Not Approximated)
            var ap = calc_average_precision(threshold_confusion_matrix_list);

            // Average Precision Interpolated (Not Approximated)
            var api = calc_average_precision_interpolated(threshold_confusion_matrix_list);


            // PR Curve Coordinates
            var pr_plot_coords = calc_pr_plot(threshold_confusion_matrix_list, interpolate: false);

            // PR Approx
            var pr_auc_approx = area_under_curve_trapz(pr_plot_coords);


            // PRI Curve Coordinates
            var pri_plot_coords = calc_pr_plot(threshold_confusion_matrix_list, interpolate: true);

            // PRI Approx
            var pri_auc_approx = area_under_curve_trapz(pri_plot_coords);


            // ROC Curve Coordinates
            var roc_plot_coords = calc_roc_plot(threshold_confusion_matrix_list);

            // ROC Approx
            var roc_auc_approx = area_under_curve_trapz(roc_plot_coords);

            // ROC (Not Approximated, and Not reduced to Eleven Points - Incompatible with 11 points)
            var roc_auc_actual = calc_roc_auc(prediction_list, positive_id, p, n);

            return (roc_auc_approx: roc_auc_approx, roc_auc_actual: roc_auc_actual, pr_auc_approx: pr_auc_approx, pri_auc_approx: pri_auc_approx, ap: ap, api: api, roc_xy: roc_plot_coords, pr_xy: pr_plot_coords, pri_xy: pri_plot_coords);
        }

        private static confusion_matrix[] get_theshold_confusion_matrices(CancellationTokenSource cts, prediction[] prediction_list, int positive_id, threshold_type threshold_type, int negative_id)
        {
            // Get thresholds list (either all thresholds or 11 points)
            double[] thresholds = null;

            if (threshold_type == threshold_type.all_thresholds) { thresholds = prediction_list.Select(a => a.probability_estimates.FirstOrDefault(b => b.class_id == positive_id).probability_estimate).Distinct().OrderByDescending(a => a).ToArray(); } else if (threshold_type == threshold_type.eleven_points) { thresholds = eleven_points; } else { throw new NotSupportedException(); }

            // Calc predictions at each threshold
            var threshold_prediction_list = thresholds.Select(t => (positive_threshold: t, prediction_list: prediction_list.Select(pl => new prediction(pl) { predicted_class_id = pl.probability_estimates.FirstOrDefault(e => e.class_id == positive_id).probability_estimate >= t ? positive_id : negative_id, }).ToArray())).ToArray();

            // Calc confusion matrices at each threshold
            var threshold_confusion_matrix_list = threshold_prediction_list.SelectMany(a => count_prediction_error(cts, a.prediction_list, a.positive_threshold, positive_id, false))
                .Where(a => a.x_class_id == positive_id)
                .ToArray();

            return threshold_confusion_matrix_list;
        }

        private static double calc_roc_auc(prediction[] prediction_list, int positive_id, double p, double n)
        {
            var total_neg_for_threshold = prediction_list.Select((a, i) => (actual_class: a.real_class_id, total_neg_at_point: prediction_list.Where((b, j) => j <= i && b.real_class_id != positive_id).Count())).ToList();

            var roc_auc_actual = ((double)1 / (double)(p * n)) *
                                 (double)prediction_list.Select((a, i) =>
                                    {
                                        if (a.real_class_id != positive_id) return 0;
                                        var total_n_at_current_threshold = total_neg_for_threshold[i].total_neg_at_point;

                                         //var n_more_than_current_n = total_neg_for_threshold.Count(b => b.actual_class == negative_id && b.total_neg_at_point > total_n_at_current_threshold);
                                         var n_more_than_current_n = total_neg_for_threshold.Count(b => b.actual_class != positive_id && b.total_neg_at_point > total_n_at_current_threshold);

                                        return n_more_than_current_n;
                                    })
                                     .Sum();
            return roc_auc_actual;
        }

        private static (double x, double y)[] calc_roc_plot(confusion_matrix[] threshold_confusion_matrix_list)
        {
            if (threshold_confusion_matrix_list == null || threshold_confusion_matrix_list.Length == 0) return null;
            var xy1 = threshold_confusion_matrix_list.Select(a => (x: a.metrics.p_FPR, y: a.metrics.p_TPR)).Distinct().ToArray();

            var need_start = !xy1.Any(a => a.x == 0.0 && a.y == 0.0);
            var need_end = !xy1.Any(a => a.x == 1.0 && a.y == 1.0);
            if (need_start || need_end)
            {
                var xy2 = new (double x, double y)[xy1.Length + (need_start ? 1 : 0) + (need_end ? 1 : 0)];
                if (need_start) { xy2[0] = (0.0, 0.0); }
                if (need_end) { xy2[^1] = (1.0, 1.0); }
                Array.Copy(xy1, 0, xy2, need_start ? 1 : 0, xy1.Length);
                xy1 = xy2;
            }

            //todo: check whether 'roc_auc_approx' should be calculated before or after 'OrderBy' y,x statement, or if doesn't matter.
            xy1 = xy1.OrderBy(a => a.y).ThenBy(a => a.x).ToArray();
            return xy1;
        }

        //private static (double x, double y)[] calc_pri_plot(confusion_matrix[] threshold_confusion_matrix_list)
        //{
        //    if (threshold_confusion_matrix_list == null || threshold_confusion_matrix_list.Length == 0) return null;
        //
        //    var size = threshold_confusion_matrix_list.Length;
        //    var need_start = (threshold_confusion_matrix_list.First().metrics.TPR != 0.0);
        //    var need_end = (threshold_confusion_matrix_list.Last().metrics.TPR != 1.0);
        //    var xy = new (double x, double y)[size + (need_start ? 1 : 0) + (need_end ? 1 : 0)];
        //
        //    var pri_plot_coords = threshold_confusion_matrix_list.Select(a =>
        //        {
        //            var max_ppv = threshold_confusion_matrix_list.Where(b => b.metrics.TPR >= a.metrics.TPR).Max(b => b.metrics.PPV);
        //            if (double.IsNaN(max_ppv)) max_ppv = a.metrics.PPV; // 0;
        //
        //            return (x: a.metrics.TPR, y: max_ppv);
        //        })
        //        .ToArray();
        //
        //    Array.Copy(pri_plot_coords, 0, xy, need_start ? 1 : 0, pri_plot_coords.Length);
        //
        //    if (need_start) { xy[0] = ((double)0.0, threshold_confusion_matrix_list.First().metrics.PPV); }
        //    if (need_end) { var m = threshold_confusion_matrix_list.First(); xy[^1] = ((double)1.0, (double)m.metrics.P / ((double)m.metrics.P + (double)m.metrics.N)); }
        //
        //    return xy;
        //}

        private static (double x, double y)[] calc_pr_plot(confusion_matrix[] threshold_confusion_matrix_list, bool interpolate)
        {
            if (threshold_confusion_matrix_list == null || threshold_confusion_matrix_list.Length == 0) return null;

            var xy1 = threshold_confusion_matrix_list.Select(a =>
            {
                var max_ppv = threshold_confusion_matrix_list.Where(b => b.metrics.p_TPR >= a.metrics.p_TPR).Max(b => b.metrics.p_PPV);
                if (double.IsNaN(max_ppv)) max_ppv = a.metrics.p_PPV; // 0;

                return (x: a.metrics.p_TPR, y: (interpolate ? max_ppv : a.metrics.p_PPV));
            }).ToArray();

            var need_start = xy1.First().x != 0.0;
            var need_end = xy1.Last().x != 1.0;
            if (need_start || need_end)
            {
                var xy2 = new (double x, double y)[xy1.Length + (need_start ? 1 : 0) + (need_end ? 1 : 0)];
                Array.Copy(xy1, 0, xy2, need_start ? 1 : 0, xy1.Length);

                if (need_start) { xy2[0] = ((double)0.0, xy1.First().y); }

                if (need_end)
                {
                    var m = threshold_confusion_matrix_list.First();
                    xy2[^1] = ((double)1.0, (double)m.metrics.cm_P / ((double)m.metrics.cm_P + (double)m.metrics.cm_N));
                }

                xy1 = xy2;
            }

            return xy1;
        }

        private static double calc_average_precision_interpolated(confusion_matrix[] threshold_confusion_matrix_list)
        {
            var api = threshold_confusion_matrix_list.Select((a, i) =>
                {
                    var max_ppv = threshold_confusion_matrix_list.Where(b => b.metrics.p_TPR >= a.metrics.p_TPR).Max(b => b.metrics.p_PPV);

                    if (double.IsNaN(max_ppv) /* || max_ppv == 0*/) max_ppv = a.metrics.p_PPV; // = 0? =1? unknown: should it be a.PPV, 0, or 1 when there are no true results?

                    var delta_tpr = Math.Abs(a.metrics.p_TPR - (i == 0 ? 0 : threshold_confusion_matrix_list[i - 1].metrics.p_TPR));
                    //var _ap = a.PPV * delta_tpr;
                    var _api = max_ppv * delta_tpr;

                    if (double.IsNaN(_api)) _api = 0;

                    return _api;
                })
                .Sum();
            return api;
        }

        private static double calc_average_precision(confusion_matrix[] threshold_confusion_matrix_list)
        {
            var ap = threshold_confusion_matrix_list.Select((a, i) =>
                {
                    //var max_p = threshold_confusion_matrix_list.Where(b => b.TPR >= a.TPR).Max(b => b.PPV);
                    var delta_tpr = Math.Abs(a.metrics.p_TPR - (i == 0 ? 0 : threshold_confusion_matrix_list[i - 1].metrics.p_TPR));
                    var _ap = a.metrics.p_PPV * delta_tpr;

                    if (double.IsNaN(_ap)) _ap = 0;
                    //var _api = max_p * delta_tpr;
                    return _ap;
                })
                .Sum();
            return ap;
        }
    }
}
