using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal static class perf
    {
        public const string module_name = nameof(perf);

        internal static List<confusion_matrix> count_prediction_error(
            IList<prediction> prediction_list,
            double? threshold = null,
            int? threshold_class = null,
            bool calculate_auc = true
            )
        {

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
                        P = prediction_list.Count(b => actual_class_id == b.real_class_id),
                        N = prediction_list.Count(b => actual_class_id != b.real_class_id)
                    },
                    x_prediction_threshold = threshold,
                    x_prediction_threshold_class = threshold_class,
                    thresholds = prediction_list.Select(a => a.probability_estimates.FirstOrDefault(b => b.class_id == actual_class_id).probability_estimate)/*.Distinct()*/.OrderByDescending(a => a).ToList(),
                    predictions = prediction_list.ToList()
                };
                confusion_matrix_list.Add(confusion_matrix1);

            }

            for (var prediction_list_index = 0; prediction_list_index < prediction_list.Count; prediction_list_index++)
            {
                var prediction = prediction_list[prediction_list_index];

                var actual_class_matrix = confusion_matrix_list.First(a => a.x_class_id == prediction.real_class_id);
                var predicted_class_matrix = confusion_matrix_list.First(a => a.x_class_id == prediction.predicted_class_id);

                if (prediction.real_class_id == prediction.predicted_class_id)
                {
                    actual_class_matrix.metrics.TP++;

                    for (var index = 0; index < confusion_matrix_list.Count; index++)
                    {
                        if (confusion_matrix_list[index].x_class_id != prediction.real_class_id)
                        {
                            confusion_matrix_list[index].metrics.TN++;
                        }
                    }
                }

                else if (prediction.real_class_id != prediction.predicted_class_id)
                {
                    actual_class_matrix.metrics.FN++;

                    predicted_class_matrix.metrics.FP++;
                }
            }


            Parallel.ForEach(confusion_matrix_list,
                cm =>
                    //foreach (var cm in confusion_matrix_list)
                {
                    cm.calculate_metrics(cm.metrics, calculate_auc, prediction_list);
                    //if (calculate_auc)
                    //{
                    //    var (brier_score, roc_auc, roc_auc2, pr_auc, ap, api, roc_xy, pr_xy) = performance_measure.Calculate_ROC_PR_AUC(prediction_list, cm.class_id.Value);
                    //    cm.Brier = brier_score;

                    //    cm.ROC_AUC_Approx = roc_auc;
                    //    cm.ROC_AUC2 = roc_auc2;

                    //    cm.PR_AUC_Approx = pr_auc;
                    //    cm.AP = ap;
                    //    cm.API = api;

                    //    cm.pr_xy_str = string.Join("/",pr_xy.Select(a => $"{Math.Round(a.x,6)};{Math.Round(a.y,6)}").ToList());
                    //    cm.roc_xy_str = string.Join("/",roc_xy.Select(a => $"{Math.Round(a.x,6)};{Math.Round(a.y,6)}").ToList());
                    //}


                    //cm.TPR = (cm.TP + cm.FN) == (double) 0.0 ? (double) 0.0 : (double) (cm.TP) / (double) (cm.TP + cm.FN);
                    //cm.TNR = (cm.TN + cm.FP) == (double) 0.0 ? (double) 0.0 : (double) (cm.TN) / (double) (cm.TN + cm.FP);
                    //cm.PPV = (cm.TP + cm.FP) == (double) 0.0 ? (double) 0.0 : (double) (cm.TP) / (double) (cm.TP + cm.FP);
                    //cm.NPV = (cm.TN + cm.FN) == (double) 0.0 ? (double) 0.0 : (double) (cm.TN) / (double) (cm.TN + cm.FN);
                    //cm.FNR = (cm.FN + cm.TP) == (double) 0.0 ? (double) 0.0 : (double) (cm.FN) / (double) (cm.FN + cm.TP);
                    //cm.FPR = (cm.FP + cm.TN) == (double) 0.0 ? (double) 0.0 : (double) (cm.FP) / (double) (cm.FP + cm.TN);
                    //cm.FDR = (cm.FP + cm.TP) == (double) 0.0 ? (double) 0.0 : (double) (cm.FP) / (double) (cm.FP + cm.TP);
                    //cm.FOR = (cm.FN + cm.TN) == (double) 0.0 ? (double) 0.0 : (double) (cm.FN) / (double) (cm.FN + cm.TN);
                    //cm.ACC = (cm.P + cm.N) == (double) 0.0 ? (double) 0.0 : (double) (cm.TP + cm.TN) / (double) (cm.P + cm.N);
                    //cm.F1S = (cm.PPV + cm.TPR) == (double) 0.0 ? (double) 0.0 : (double) (2 * cm.PPV * cm.TPR) / (double) (cm.PPV + cm.TPR);
                    //cm.G1S = (cm.PPV + cm.TPR) == (double) 0.0 ? (double) 0.0 : (double) Math.Sqrt((double) (cm.PPV * cm.TPR));
                    //cm.MCC = ((double) Math.Sqrt((double) ((cm.TP + cm.FP) * (cm.TP + cm.FN) * (cm.TN + cm.FP) * (cm.TN + cm.FN))) == (double) 0.0) ? (double) 0.0 : ((cm.TP * cm.TN) - (cm.FP * cm.FN)) / (double) Math.Sqrt((double) ((cm.TP + cm.FP) * (cm.TP + cm.FN) * (cm.TN + cm.FP) * (cm.TN + cm.FN)));
                    //cm.Informedness = (cm.TPR + cm.TNR) - (double) 1.0;
                    //cm.Markedness = (cm.PPV + cm.NPV) - (double) 1.0;
                    //cm.BalancedAccuracy = (double) (cm.TPR + cm.TNR) / (double) 2.0;

                    //cm.LRP = (1 - cm.TNR)== (double)0.0 ? (double)0.0 : (cm.TPR) / (1 - cm.TNR);
                    //cm.LRN = (cm.TNR)==(double)0.0?(double)0.0:(1 - cm.TPR) / (cm.TNR);


                    //cm.F1B_00 = fbeta2(cm.PPV, cm.TPR, (double) 0.0);
                    //cm.F1B_01 = fbeta2(cm.PPV, cm.TPR, (double) 0.1);
                    //cm.F1B_02 = fbeta2(cm.PPV, cm.TPR, (double) 0.2);
                    //cm.F1B_03 = fbeta2(cm.PPV, cm.TPR, (double) 0.3);
                    //cm.F1B_04 = fbeta2(cm.PPV, cm.TPR, (double) 0.4);
                    //cm.F1B_05 = fbeta2(cm.PPV, cm.TPR, (double) 0.5);
                    //cm.F1B_06 = fbeta2(cm.PPV, cm.TPR, (double) 0.6);
                    //cm.F1B_07 = fbeta2(cm.PPV, cm.TPR, (double) 0.7);
                    //cm.F1B_08 = fbeta2(cm.PPV, cm.TPR, (double) 0.8);
                    //cm.F1B_09 = fbeta2(cm.PPV, cm.TPR, (double) 0.9);
                    //cm.F1B_10 = fbeta2(cm.PPV, cm.TPR, (double) 1.0);

                    //cm.calculate_ppf();
                });

            return confusion_matrix_list;
        }



        internal static double area_under_curve_trapz(List<(double x, double y)> coordinate_list)//, bool interpolation = true)
        {

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(coordinate_list),coordinate_list.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(area_under_curve_trapz)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            //var coords = new List<(double x1, double x2, double y1, double y2)>();
            coordinate_list = coordinate_list.Distinct().ToList();
            coordinate_list = coordinate_list.OrderBy(a => a.x).ThenBy(a => a.y).ToList();
            var auc = coordinate_list.Select((c, i) => i >= coordinate_list.Count - 1 ? 0 : (coordinate_list[i + 1].x - coordinate_list[i].x) * ((coordinate_list[i].y + coordinate_list[i + 1].y) / 2)).Sum();
            return auc;
        }


        internal static List<prediction> load_prediction_file_regression_values(IList<(string test_file, string test_comments_file, string prediction_file, string test_class_sample_id_list_file)> files)
        {
            // method untested
            const string method_name = nameof(load_prediction_file_regression_values);

            var lines = files.AsParallel().AsOrdered().Select((a, i) => 
                (
                    test_file_lines: io_proxy.ReadAllLines(a.test_file, module_name, method_name).ToList(),
                    test_comments_file_lines: io_proxy.ReadAllLines(a.test_comments_file, module_name, method_name).ToList(),
                    prediction_file_lines: io_proxy.ReadAllLines(a.prediction_file, module_name, method_name).ToList(),
                    test_class_sample_id_list_lines: !string.IsNullOrWhiteSpace(a.test_class_sample_id_list_file) ? io_proxy.ReadAllLines(a.test_class_sample_id_list_file, module_name, method_name).ToList() : null
                )).ToList();

            // prediction file MAY have a header, but only if probability estimates are enabled

            //var test_has_headers = false;
            //var test_comments_has_headers = true;
            
            // check any prediction file has labels on first line
            var prediction_has_headers = lines.Any(a => a.prediction_file_lines.FirstOrDefault().StartsWith("labels", StringComparison.InvariantCulture));

            if (prediction_has_headers)
            {
                // check all labels in all prediction files match
                if (lines.Select(a => a.prediction_file_lines.FirstOrDefault()).Distinct().Count() != 1) { throw new ArgumentOutOfRangeException(nameof(files)); }
            }

            lines = lines.AsParallel().AsOrdered().Select((a, i) => (
                a.test_file_lines, 
                a.test_comments_file_lines.Skip(1 /* skip header */).ToList(),
                a.prediction_file_lines.Skip(i > 0 && prediction_has_headers ? 1 : 0).ToList(),
                a.test_class_sample_id_list_lines
            )).ToList();

            var test_file_lines = lines.SelectMany(a => a.test_file_lines).ToList();
            var test_comments_file_lines = lines.SelectMany(a => a.test_comments_file_lines).ToList();
            var prediction_file_lines = lines.SelectMany(a => a.prediction_file_lines).ToList();

            var test_class_sample_id_list = lines.Where(a=> a.test_class_sample_id_list_lines != null).SelectMany(a => a.test_class_sample_id_list_lines).Select(a => int.Parse(a)).ToList();
            if (test_class_sample_id_list.Count == 0) test_class_sample_id_list = null;

            return load_prediction_file_regression_values_from_text(test_file_lines, test_comments_file_lines, prediction_file_lines, test_class_sample_id_list);
        }

        internal static List<prediction> load_prediction_file_regression_values(string test_file, string test_comments_file, string prediction_file, string test_sample_id_list_file = null)
        {
            
            const string method_name = nameof(load_prediction_file_regression_values);

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

            var test_file_lines = io_proxy.ReadAllLines(test_file, module_name, method_name).ToList();

            var test_comments_file_lines = !string.IsNullOrWhiteSpace(test_comments_file) && io_proxy.Exists(test_comments_file/*, module_name, method_name*/) ? io_proxy.ReadAllLines(test_comments_file, module_name, method_name).ToList() : null;

            var prediction_file_lines = io_proxy.ReadAllLines(prediction_file, module_name, method_name).ToList();

            var test_sample_id_list_lines = !string.IsNullOrWhiteSpace(test_sample_id_list_file) ? io_proxy.ReadAllLines(test_sample_id_list_file, module_name, method_name).ToList() : null;
            var test_class_sample_id_list = test_sample_id_list_lines != null ? test_sample_id_list_lines.Select(a => int.Parse(a)).ToList() : null;

            return load_prediction_file_regression_values_from_text(test_file_lines, test_comments_file_lines, prediction_file_lines, test_class_sample_id_list);
        }

        internal static List<prediction> load_prediction_file_regression_values_from_text(IList<string> test_file_lines, IList<string> test_comments_file_lines, IList<string> prediction_file_lines, IList<int> test_class_sample_id_list)
        {
            // todo: output misclassification file

            if (test_file_lines == null || test_file_lines.Count == 0)
            {
                throw new ArgumentNullException(nameof(test_file_lines));
            }

            if (prediction_file_lines == null || prediction_file_lines.Count == 0)
            {
                throw new ArgumentNullException(nameof(prediction_file_lines));
            }

            // remove comments from test_file_lines (comments start with #)
            test_file_lines = test_file_lines.Select(a =>
            {
                var hash_index = a.IndexOf('#', StringComparison.InvariantCulture);

                if (hash_index > -1)
                {
                    return a.Substring(0, hash_index).Trim();
                }

                return a.Trim();
            }).ToList();



            var test_file_comments_header = test_comments_file_lines?.FirstOrDefault()?.Split(',') ?? null;

            if (test_comments_file_lines != null && test_comments_file_lines.Count > 0)
            {
                test_comments_file_lines = test_comments_file_lines.Skip(1).ToList();
            }


            var test_file_data = test_file_lines
                .Where(a => !string.IsNullOrWhiteSpace(a) && int.TryParse(a.Trim().Split().First(), NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var line_actual_class_id))
                .Select(a => a.Trim().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries)).ToList();



            if (test_file_lines.Count == 0) return null;


            var prediction_file_data = prediction_file_lines
                .Where(a => !string.IsNullOrWhiteSpace(a) && int.TryParse(a.Trim().Split().First(), NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var line_predicted_class_id))
                .Select(a => a.Trim().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries))
                .ToList();

            //if (prediction_file_lines.Count == 0) return null;
            if (prediction_file_data.Count == 0) return null;

            var probability_estimate_class_labels = new List<int>();

            if (prediction_file_lines.Where(a => a.Trim().StartsWith($@"labels")).Distinct().Count() > 1) /* should be 0 or 1 for the same model */
            {
                throw new Exception($@"Error: more than one set of labels in the same file.");
            }

            if (prediction_file_lines.First().Trim().Split().First() == $@"labels")
            {
                probability_estimate_class_labels = prediction_file_lines.First().Trim().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries).Skip(1).Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToList();
            }

            if (test_comments_file_lines != null && test_file_data.Count != test_comments_file_lines.Count)
            {
                throw new Exception($@"Error: test file and test comments file have different instance length: [ {test_file_data.Count} : {test_comments_file_lines.Count} ].");
            }

            if (test_file_data.Count != prediction_file_data.Count)
            {
                throw new Exception($@"Error: test file and prediction file have different instance length: [ {test_file_data.Count} : {prediction_file_data.Count} ].");
            }

            if (test_class_sample_id_list != null && test_class_sample_id_list.Count > 0 && test_class_sample_id_list.Count != test_file_data.Count)
            {
                throw new Exception($@"Error: test sample ids and test file data do not match: [ {test_file_data.Count} : {prediction_file_data.Count} ].");
            }

            var total_predictions = test_file_data.Count;


            var prediction_list = Enumerable.Range(0, total_predictions).AsParallel().AsOrdered().Select(prediction_index =>
            {
                var probability_estimates = prediction_file_data[prediction_index].Length <= 1 ?
                    new List<(int class_id, double probability_estimate)>()
                    :
                    prediction_file_data[prediction_index]
                        .Skip(1 /* skip predicted class id */)
                        .Select((a, i) => (class_id: probability_estimate_class_labels[i], probability_estimate: double.TryParse(a, NumberStyles.Float, CultureInfo.InvariantCulture, out var pe_out) ? pe_out : default))
                        .OrderBy(a => a.class_id)
                        .ToList();

                //var probability_estimates_stated = probability_estimates != null && probability_estimates.Count > 0 && probability_estimate_class_labels != null && probability_estimate_class_labels.Count > 0;

                var prediction = new prediction()
                {
                    prediction_index = prediction_index,
                    class_sample_id = test_class_sample_id_list != null && test_class_sample_id_list.Count - 1 >= prediction_index ? test_class_sample_id_list[prediction_index] : -1,
                    comment = test_comments_file_lines != null && test_comments_file_lines.Count > 0 ? test_comments_file_lines[prediction_index].Split(',').Select((a, i) => (comment_header: ((test_file_comments_header?.Length??0) - 1 >= i ? test_file_comments_header[i] : ""), comment_value: a)).ToList() : new List<(string comment_header, string comment_value)>(),
                    real_class_id = int.TryParse(test_file_data[prediction_index][0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_real_class_id) ? out_real_class_id : default,
                    predicted_class_id = int.TryParse(prediction_file_data[prediction_index][0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_predicted_class_id) ? out_predicted_class_id : default,
                    probability_estimates = probability_estimates,
                    test_row_vector = test_file_data[prediction_index].Skip(1/* skip class id column */).ToArray(),
                };

                return prediction;
            }).ToList();

            return prediction_list;
        }

        //internal static (List<prediction> prediction_list, List<confusion_matrix> cm_list) load_prediction_file(List<(string test_file, string test_comments_file, string prediction_file)> files, bool calc_11p_thresholds)
        //{


        //    var prediction_list = load_prediction_file_regression_values(test_file, test_comments_file, prediction_file);
        //    var cm_list = load_prediction_file(prediction_list, calc_11p_thresholds);

        //    return (prediction_list, cm_list);

        //}

        internal static (List<prediction> prediction_list, List<confusion_matrix> cm_list) load_prediction_file(string test_file, string test_comments_file, string prediction_file, bool calc_11p_thresholds)
        {

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(test_file),test_io_proxy.ToString()),
            //    (nameof(prediction_file),prediction_io_proxy.ToString()),
            //    (nameof(calc_11p_thresholds),calc_11p_thresholds.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(load_prediction_file)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var prediction_list = load_prediction_file_regression_values(test_file, test_comments_file, prediction_file);
            var cm_list = load_prediction_file(prediction_list, calc_11p_thresholds);

            return (prediction_list, cm_list);
        }

        internal static (List<prediction> prediction_list, List<confusion_matrix> cm_list) load_prediction_file(IList<string> test_file_lines, IList<string> test_comments_file_lines, IList<string> prediction_file_lines, bool calc_11p_thresholds, IList<int> test_class_sample_id_list)
        {

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(test_file),test_io_proxy.ToString()),
            //    (nameof(prediction_file),prediction_io_proxy.ToString()),
            //    (nameof(calc_11p_thresholds),calc_11p_thresholds.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(load_prediction_file)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var prediction_list = load_prediction_file_regression_values_from_text(test_file_lines, test_comments_file_lines, prediction_file_lines, test_class_sample_id_list);
            var cm_list = load_prediction_file(prediction_list, calc_11p_thresholds);

            return (prediction_list, cm_list);
        }

        internal static List<confusion_matrix> load_prediction_file(List<prediction> prediction_list, bool calc_11p_thresholds)
        {

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(prediction_list),prediction_list.ToString()),
            //    (nameof(calc_11p_thresholds),calc_11p_thresholds.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(load_prediction_file)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var class_id_list = prediction_list.SelectMany(a => new int[] { a.real_class_id, a.predicted_class_id }).Distinct().OrderBy(a => a).ToList();

            var default_confusion_matrix_list = count_prediction_error(prediction_list);

            var confusion_matrix_list = new List<confusion_matrix>();
            confusion_matrix_list.AddRange(default_confusion_matrix_list);

            if (class_id_list.Count >= 2 && calc_11p_thresholds)
            {
                var positive_id = class_id_list.Max();
                var negative_id = class_id_list.Min();

                var thresholds = new List<double>();
                for (var i = 0.00m; i <= 1.00m; i += 0.10m)//0.05m)
                {
                    thresholds.Add((double)i);
                }

                var threshold_prediction_list = thresholds.Select(t => (positive_threshold: t, prediction_list: prediction_list.Select(p => new prediction(p)
                {
                    predicted_class_id = p.probability_estimates.First(e => e.class_id == positive_id).probability_estimate >= t ? positive_id : negative_id,
                }).ToList())).ToList();

                var threshold_confusion_matrix_list = threshold_prediction_list.SelectMany(a => count_prediction_error(a.prediction_list, a.positive_threshold, positive_id, false)).ToList();

                for (var i = 0; i < threshold_confusion_matrix_list.Count; i++)
                {
                    var default_cm = default_confusion_matrix_list.First(a => a.x_class_id == threshold_confusion_matrix_list[i].x_class_id);

                    threshold_confusion_matrix_list[i].metrics.ROC_AUC_Approx_All = default_cm.metrics.ROC_AUC_Approx_All;
                    threshold_confusion_matrix_list[i].metrics.ROC_AUC_Approx_11p = default_cm.metrics.ROC_AUC_Approx_11p;
                    threshold_confusion_matrix_list[i].roc_xy_str_all = default_cm.roc_xy_str_all;
                    threshold_confusion_matrix_list[i].roc_xy_str_11p = default_cm.roc_xy_str_11p;

                    threshold_confusion_matrix_list[i].metrics.PR_AUC_Approx_All = default_cm.metrics.PR_AUC_Approx_All;
                    threshold_confusion_matrix_list[i].metrics.PR_AUC_Approx_11p = default_cm.metrics.PR_AUC_Approx_11p;
                    threshold_confusion_matrix_list[i].pr_xy_str_all = default_cm.pr_xy_str_all;
                    threshold_confusion_matrix_list[i].pr_xy_str_11p = default_cm.pr_xy_str_11p;
                    threshold_confusion_matrix_list[i].pri_xy_str_all = default_cm.pri_xy_str_all;
                    threshold_confusion_matrix_list[i].pri_xy_str_11p = default_cm.pri_xy_str_11p;
                }

                confusion_matrix_list.AddRange(threshold_confusion_matrix_list);
            }

            return confusion_matrix_list;
        }

        internal static double brier(IList<prediction> prediction_list, int positive_id)
        {
            if (prediction_list.Any(a => a.probability_estimates == null || a.probability_estimates.Count == 0)) return default;

            prediction_list = prediction_list.OrderByDescending(a => a.probability_estimates.First(b => b.class_id == positive_id).probability_estimate).ToList();

            // Calc Brier
            var brier_score = ((double)1 / (double)prediction_list.Count)
                              * (prediction_list.Sum(a => Math.Pow(a.probability_estimates.First(b => b.class_id == a.predicted_class_id).probability_estimate - (a.real_class_id == a.predicted_class_id ? 1 : 0), 2)));

            return brier_score;
        }

        internal enum threshold_type
        {
            all_thresholds,
            eleven_points
        }

        internal static (/*double brier_score,*/ double roc_auc_approx, double roc_auc_actual, double pr_auc_approx, double pri_auc_approx, double ap, double api, List<(double x, double y)> roc_xy, List<(double x, double y)> pr_xy, List<(double x, double y)> pri_xy)
            Calculate_ROC_PR_AUC(IList<prediction> prediction_list, int positive_id, threshold_type threshold_type = threshold_type.all_thresholds)
        {
            if (prediction_list.Any(a => a.probability_estimates == null || a.probability_estimates.Count == 0)) return default;

            // Assume binary classifier - get negative class id
            var negative_id = prediction_list.First(a => a.real_class_id != positive_id).real_class_id;

            // Calc P
            var p = prediction_list.Count(a => a.real_class_id == positive_id);

            // Calc N
            var n = prediction_list.Count(a => a.real_class_id == negative_id);

            // Order predictions descending by positive class probability
            prediction_list = prediction_list.OrderByDescending(a => a.probability_estimates.FirstOrDefault(b => b.class_id == positive_id).probability_estimate).ToList();

            // Get thresholds list (either all thresholds or 11 points)
            List<double> thresholds = null;

            if (threshold_type == threshold_type.all_thresholds)
            {
                thresholds = prediction_list.Select(a => a.probability_estimates.FirstOrDefault(b => b.class_id == positive_id).probability_estimate).Distinct().OrderByDescending(a => a).ToList();
            }
            else if (threshold_type == threshold_type.eleven_points)
            {
                thresholds = new List<double>()
                {
                    1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0
                };
            }
            else
            {
                throw new NotSupportedException();
            }

            // Calc predictions at each threshold
            var threshold_prediction_list = thresholds.Select(t => (positive_threshold: t, prediction_list: prediction_list.Select(pl => new prediction(pl)
            {
                predicted_class_id = pl.probability_estimates.FirstOrDefault(e => e.class_id == positive_id).probability_estimate >= t ? positive_id : negative_id,
            }).ToList())).ToList();

            // Calc confusion matrices at each threshold
            var threshold_confusion_matrix_list = threshold_prediction_list.SelectMany(a => count_prediction_error(a.prediction_list, a.positive_threshold, positive_id, false)).ToList();
            threshold_confusion_matrix_list = threshold_confusion_matrix_list.Where(a => a.x_class_id == positive_id).ToList();

            //// Calc Brier
            //var brier_score = ((double)1 / (double)prediction_list.Count)
            //                  * (prediction_list.Sum(a => Math.Pow(a.probability_estimates.First(b => b.class_id == a.default_predicted_class).probability_estimate - (a.actual_class == a.default_predicted_class ? 1 : 0), 2)));

            // Average Precision (Not Approximated)
            var ap = threshold_confusion_matrix_list.Select((a, i) =>
            {
                //var max_p = threshold_confusion_matrix_list.Where(b => b.TPR >= a.TPR).Max(b => b.PPV);
                var delta_tpr = Math.Abs(a.metrics.TPR - (i == 0 ? 0 : threshold_confusion_matrix_list[i - 1].metrics.TPR));
                var _ap = a.metrics.PPV * delta_tpr;

                if (double.IsNaN(_ap)) _ap = 0;
                //var _api = max_p * delta_tpr;
                return _ap;
            }).Sum();

            // Average Precision Interpolated (Not Approximated)
            var api = threshold_confusion_matrix_list.Select((a, i) =>
            {
                var max_ppv = threshold_confusion_matrix_list.Where(b => b.metrics.TPR >= a.metrics.TPR).Max(b => b.metrics.PPV);

                if (double.IsNaN(max_ppv)/* || max_ppv == 0*/) max_ppv = a.metrics.PPV; // = 0? =1? unknown: should it be a.PPV, 0, or 1 when there are no true results?

                var delta_tpr = Math.Abs(a.metrics.TPR - (i == 0 ? 0 : threshold_confusion_matrix_list[i - 1].metrics.TPR));
                //var _ap = a.PPV * delta_tpr;
                var _api = max_ppv * delta_tpr;

                if (double.IsNaN(_api)) _api = 0;

                return _api;
            }).Sum();

            // PR Curve Coordinates
            var pr_plot_coords = threshold_confusion_matrix_list.Select(a => (x: a.metrics.TPR, y: a.metrics.PPV)).ToList();

            if (pr_plot_coords.First().x != 0.0)
            {
                pr_plot_coords.Insert(0, ((double)0.0, pr_plot_coords.First().y));
            }

            if (pr_plot_coords.Last().x != 1.0 && threshold_confusion_matrix_list.Count > 0)
            {
                var m = threshold_confusion_matrix_list.First();
                pr_plot_coords.Add(((double)1.0, (double)m.metrics.P / ((double)m.metrics.P + (double)m.metrics.N)));
            }

            // PRI Curve Coordinates
            var pri_plot_coords = threshold_confusion_matrix_list.Select(a =>
            {
                var max_ppv = threshold_confusion_matrix_list.Where(b => b.metrics.TPR >= a.metrics.TPR).Max(b => b.metrics.PPV);
                if (double.IsNaN(max_ppv)) max_ppv = a.metrics.PPV;// 0;

                return (x: a.metrics.TPR, y: max_ppv);
            }).ToList();

            if (pri_plot_coords.First().x != 0.0)
            {
                pri_plot_coords.Insert(0, ((double)0.0, pri_plot_coords.First().y));
            }

            if (pri_plot_coords.Last().x != 1.0 && threshold_confusion_matrix_list.Count > 0)
            {
                var m = threshold_confusion_matrix_list.First();
                pri_plot_coords.Add(((double)1.0, (double)m.metrics.P / ((double)m.metrics.P + (double)m.metrics.N)));
            }

            // ROC Curve Coordinates
            var roc_plot_coords = threshold_confusion_matrix_list.Select(a => (x: a.metrics.FPR, y: a.metrics.TPR)).ToList();
            if (!roc_plot_coords.Any(a => a.x == (double)0.0 && a.y == (double)0.0)) roc_plot_coords.Insert(0, ((double)0.0, (double)0.0));
            if (!roc_plot_coords.Any(a => a.x == (double)1.0 && a.y == (double)1.0)) roc_plot_coords.Add(((double)1.0, (double)1.0));
            roc_plot_coords = roc_plot_coords.Distinct().ToList();

            // ROC Approx
            var roc_auc_approx = area_under_curve_trapz(roc_plot_coords);
            roc_plot_coords = roc_plot_coords.OrderBy(a => a.y).ThenBy(a => a.x).ToList();

            // PR Approx
            var pr_auc_approx = area_under_curve_trapz(pr_plot_coords);

            // PRI Approx
            var pri_auc_approx = area_under_curve_trapz(pri_plot_coords);

            // ROC (Not Approx & Not Eleven Point - Incompatible)

            var total_neg_for_threshold = prediction_list.Select((a, i) => (actual_class: a.real_class_id, total_neg_at_point: prediction_list.Where((b, j) => j <= i && b.real_class_id == negative_id).Count())).ToList();
            var roc_auc_actual = ((double)1 / (double)(p * n)) * (double)prediction_list
                                  .Select((a, i) =>
                                  {
                                      if (a.real_class_id != positive_id) return 0;
                                      var total_n_at_current_threshold = total_neg_for_threshold[i].total_neg_at_point;

                                      var n_more_than_current_n = total_neg_for_threshold.Count(b => b.actual_class == negative_id && b.total_neg_at_point > total_n_at_current_threshold);

                                      return n_more_than_current_n;
                                  }).Sum();

            return (/*brier_score: brier_score,*/ roc_auc_approx: roc_auc_approx, roc_auc_actual: roc_auc_actual, pr_auc_approx: pr_auc_approx, pri_auc_approx: pri_auc_approx, ap: ap, api: api, roc_xy: roc_plot_coords, pr_xy: pr_plot_coords, pri_xy: pri_plot_coords);
        }
    }
}
