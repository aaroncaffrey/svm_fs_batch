using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal class confusion_matrix
    {
        public const string module_name = nameof(confusion_matrix);
        internal static readonly confusion_matrix empty = new confusion_matrix() { unrolled_index_data = index_data.empty, grid_point = grid_point.empty, metrics = metrics_box.empty };

        internal index_data unrolled_index_data;
        internal grid_point grid_point;
        internal string x_duration_grid_search;
        internal string x_duration_training;
        internal string x_duration_testing;
        internal double? x_prediction_threshold = -1;
        internal double? x_prediction_threshold_class;
        internal int x_repetitions_index = -1;
        internal int x_outer_cv_index = -1;
        internal int? x_class_id;
        internal double? x_class_weight;
        internal string x_class_name;
        internal double x_class_size;
        internal double x_class_training_size;
        internal double x_class_testing_size;
        internal metrics_box metrics;
        internal string roc_xy_str_all;
        internal string roc_xy_str_11p;
        internal string pr_xy_str_all;
        internal string pr_xy_str_11p;
        internal string pri_xy_str_all;
        internal string pri_xy_str_11p;
        internal double[] thresholds;
        internal prediction[] predictions;

        // note: load does not load the rd/sd part of the cm file.  this has to be recalculated after loading the cm.

        internal static void save(CancellationTokenSource cts, string cm_full_filename, string cm_summary_filename, bool overwrite, confusion_matrix[] x_list)
        {
            if (cts.IsCancellationRequested) return;

            save(cts, cm_full_filename, cm_summary_filename, overwrite, null, x_list, null);
        }

        internal static void save(CancellationTokenSource cts, string cm_full_filename, string cm_summary_filename, bool overwrite, (index_data id, confusion_matrix cm, rank_score rs)[] x_list)
        {
            if (cts.IsCancellationRequested) return;

            save(cts, cm_full_filename, cm_summary_filename, overwrite,
                x_list.Select(a => a.id).ToArray(),
                x_list.Select(a => a.cm).ToArray(),
                x_list.Select(a => a.rs).ToArray());
        }

        internal static void save(CancellationTokenSource cts, string cm_full_filename, string cm_summary_filename, bool overwrite, (index_data id, confusion_matrix cm)[] x_list)
        {
            if (cts.IsCancellationRequested) return;

            save(cts, cm_full_filename, cm_summary_filename, overwrite,
                x_list.Select(a => a.id).ToArray(),
                x_list.Select(a => a.cm).ToArray(),
                null);
        }


        internal static void save(CancellationTokenSource cts, string cm_full_filename, string cm_summary_filename, bool overwrite, index_data[] id_list, confusion_matrix[] cm_list, rank_score[] rs_list)
        {
            const string method_name = nameof(save);

            if (cts.IsCancellationRequested) return;

            if (cm_full_filename == cm_summary_filename) throw new Exception($@"Filenames are the same.");

            var save_full_req = !string.IsNullOrWhiteSpace(cm_full_filename);
            var save_summary_req = !string.IsNullOrWhiteSpace(cm_summary_filename);
            if (!save_full_req && !save_summary_req) throw new Exception($@"No filenames provided to save data to.");

            var lens = new int[] { id_list?.Length ?? 0, cm_list?.Length ?? 0, rs_list?.Length ?? 0 }.Where(a => a > 0).ToArray();
            var lens_distinct_count = lens.Distinct().Count();
            if (lens.Length == 0 || lens_distinct_count > 1) throw new Exception($@"Array length of {nameof(id_list)}, {nameof(cm_list)}, and {nameof(rs_list)} do not match.");
            var lens_max = lens.Max();

            var save_full = save_full_req && (overwrite || !io_proxy.is_file_available(cts, cm_full_filename, module_name, method_name));
            var save_summary = save_summary_req && (overwrite || !io_proxy.is_file_available(cts, cm_summary_filename, module_name, method_name));

            if (save_full_req && !save_full)
            {
                io_proxy.WriteLine($@"Not overwriting file: {cm_full_filename}", module_name, method_name);
            }

            if (save_summary_req && !save_summary)
            {
                io_proxy.WriteLine($@"Not overwriting file: {cm_summary_filename}", module_name, method_name);
            }

            if (!save_full && !save_summary)
            {
                return;
            }


            var lines1 = save_full ? new string[lens.Max() + 1] : null;
            var lines2 = save_summary ? new string[lens.Max() + 1] : null;

            var csv_header_values_array = new List<string>();
            if (rs_list != null && rs_list.Length > 0) { csv_header_values_array.AddRange(rank_score.csv_header_values_array); }
            if (id_list != null && id_list.Length > 0) { csv_header_values_array.AddRange(index_data.csv_header_values_array); }
            if (cm_list != null && cm_list.Length > 0) { csv_header_values_array.AddRange(confusion_matrix.csv_header_values_array); }

            var csv_header_values_string = string.Join(",", csv_header_values_array);
            if (lines1 != null) lines1[0] = csv_header_values_string;
            if (lines2 != null) lines2[0] = csv_header_values_string;



            Parallel.For(0,
                lens_max,
                i =>
                {
                    var values1 = lines1 != null ? new List<string>() : null;
                    if (values1 != null)
                    {
                        values1?.AddRange(rs_list != null && rs_list.Length > i ? rs_list[i].csv_values_array() : rank_score.empty.csv_values_array());
                        values1?.AddRange(id_list != null && id_list.Length > i ? id_list[i].csv_values_array() : index_data.empty.csv_values_array());
                        values1?.AddRange(cm_list != null && cm_list.Length > i ? cm_list[i].csv_values_array(false) : confusion_matrix.empty.csv_values_array(false));

                        if (lines1 != null) lines1[i + 1] = string.Join(",", values1);
                    }

                    var values2 = lines2 != null ? new List<string>() : null;
                    if (values2 != null)
                    {
                        values2?.AddRange(rs_list != null && rs_list.Length > i ? rs_list[i].csv_values_array() : rank_score.empty.csv_values_array());
                        values2?.AddRange(id_list != null && id_list.Length > i ? id_list[i].csv_values_array() : index_data.empty.csv_values_array());
                        values2?.AddRange(cm_list != null && cm_list.Length > i ? cm_list[i].csv_values_array(true) : confusion_matrix.empty.csv_values_array(true));

                        if (lines2 != null) lines2[i + 1] = string.Join(",", values2);
                    }
                });

            if (lines1 != null && lines1.Length > 0)
            {
                io_proxy.WriteAllLines(cts, cm_full_filename, lines1, module_name, method_name);
                io_proxy.WriteLine($@"Saved: {cm_full_filename} ({lines1.Length} lines)", module_name, method_name);
            }

            if (lines2 != null && lines2.Length > 0)
            {
                io_proxy.WriteAllLines(cts, cm_summary_filename, lines2, module_name, method_name);
                io_proxy.WriteLine($@"Saved: {cm_summary_filename} ({lines2.Length} lines)", module_name, method_name);
            }
        }

        internal static confusion_matrix[] load(CancellationTokenSource cts, string filename, int column_offset = -1)
        {
            if (cts.IsCancellationRequested) return default;

            var lines = io_proxy.ReadAllLines(cts, filename);
            var ret = load(cts, lines, column_offset);//, filename);
            return ret;
        }

        internal static confusion_matrix[] load(CancellationTokenSource cts, string[] lines, int column_offset = -1)//, string cm_fn = null)
        {
            if (cts.IsCancellationRequested) return default;

            var line_header = lines[0].Split(',');

            var has_header_line = false;

            if (column_offset == -1)
            {
                // find column position in csv of the header, and set column_offset accordingly
                for (var i = 0; i <= (line_header.Length - csv_header_values_array.Length); i++)
                {
                    if (line_header.Skip(i).Take(csv_header_values_array.Length).SequenceEqual(csv_header_values_array))
                    {
                        has_header_line = true;
                        column_offset = i;
                        break;
                    }
                }
#if DEBUG
                if (column_offset == -1) { throw new ArgumentOutOfRangeException(nameof(column_offset)); }
#endif

                if (column_offset == -1) column_offset = 0;
            }


            var cm_list = lines
                .Skip(has_header_line ? 1 : 0)
                .Where(a => !string.IsNullOrWhiteSpace(a))
                .AsParallel()
                .AsOrdered()
                .WithCancellation(cts.Token)
                .Select(line =>
                {
                    if (cts.IsCancellationRequested) return default;

                    var s_all = line.Split(',');

                    var column_count = s_all.Length - column_offset;

                    if (column_count < csv_header_values_array.Length) return null;

                    var x_type = s_all
                        .Skip(column_offset)
                        .AsParallel()
                        .AsOrdered()
                        .WithCancellation(cts.Token)
                        .Select(as_str =>
                        {
                            var as_double = double.TryParse(as_str, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var out_double) ? out_double : (double?)null;
                            var as_int = int.TryParse(as_str, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_int) ? out_int : (int?)null;
                            var as_bool = as_int == 1 && as_double == 1 ? (bool?)true : (as_int == 0 && as_double == 0 ? (bool?)false : (bool?)null);
                            if (as_bool == null && bool.TryParse(as_str, out var out_bool)) as_bool = (bool?)out_bool;

                            return (as_str, as_int, as_double, as_bool);
                        })
                        .ToArray();


                    var k = 0;

                    // skip and don't load rank_score values
                    k += rank_score.csv_header_values_array.Length;

                    // load index_data to be able to later match this confusion_matrix instance with its index_data instance 
                    var unrolled_index_data = new index_data(x_type, k);
                    k += index_data.csv_header_values_array.Length;

                    var grid_point = new grid_point() { cost = x_type[k++].as_double, gamma = x_type[k++].as_double, epsilon = x_type[k++].as_double, coef0 = x_type[k++].as_double, degree = x_type[k++].as_double, cv_rate = x_type[k++].as_double, };

                    var cm = new confusion_matrix()
                    {

                        unrolled_index_data = unrolled_index_data,

                        grid_point = grid_point,

                        x_duration_grid_search = x_type[k++].as_str,
                        x_duration_training = x_type[k++].as_str,
                        x_duration_testing = x_type[k++].as_str,
                        x_prediction_threshold = x_type[k++].as_double,
                        x_prediction_threshold_class = x_type[k++].as_double,
                        x_repetitions_index = x_type[k++].as_int ?? 0,
                        x_outer_cv_index = x_type[k++].as_int ?? 0,

                        x_class_id = x_type[k++].as_int,
                        x_class_weight = x_type[k++].as_double,
                        x_class_name = x_type[k++].as_str,
                        x_class_size = x_type[k++].as_double ?? 0,
                        x_class_training_size = x_type[k++].as_double ?? 0,
                        x_class_testing_size = x_type[k++].as_double ?? 0,

                        metrics = new metrics_box()
                        {
                            P = x_type[k++].as_double ?? 0,
                            N = x_type[k++].as_double ?? 0,
                            TP = x_type[k++].as_double ?? 0,
                            FP = x_type[k++].as_double ?? 0,
                            TN = x_type[k++].as_double ?? 0,
                            FN = x_type[k++].as_double ?? 0,
                            TPR = x_type[k++].as_double ?? 0,
                            TNR = x_type[k++].as_double ?? 0,
                            PPV = x_type[k++].as_double ?? 0,
                            Precision = x_type[k++].as_double ?? 0,
                            Prevalence = x_type[k++].as_double ?? 0,
                            MCR = x_type[k++].as_double ?? 0,
                            ER = x_type[k++].as_double ?? 0,
                            NER = x_type[k++].as_double ?? 0,
                            CNER = x_type[k++].as_double ?? 0,
                            Kappa = x_type[k++].as_double ?? 0,
                            Overlap = x_type[k++].as_double ?? 0,
                            RND_ACC = x_type[k++].as_double ?? 0,
                            Support = x_type[k++].as_double ?? 0,
                            BaseRate = x_type[k++].as_double ?? 0,
                            YoudenIndex = x_type[k++].as_double ?? 0,
                            NPV = x_type[k++].as_double ?? 0,
                            FNR = x_type[k++].as_double ?? 0,
                            FPR = x_type[k++].as_double ?? 0,
                            FDR = x_type[k++].as_double ?? 0,
                            FOR = x_type[k++].as_double ?? 0,
                            ACC = x_type[k++].as_double ?? 0,
                            GMean = x_type[k++].as_double ?? 0,
                            F1S = x_type[k++].as_double ?? 0,
                            G1S = x_type[k++].as_double ?? 0,
                            MCC = x_type[k++].as_double ?? 0,
                            Informedness = x_type[k++].as_double ?? 0,
                            Markedness = x_type[k++].as_double ?? 0,
                            BalancedAccuracy = x_type[k++].as_double ?? 0,
                            ROC_AUC_Approx_All = x_type[k++].as_double ?? 0,
                            ROC_AUC_Approx_11p = x_type[k++].as_double ?? 0,
                            ROC_AUC_All = x_type[k++].as_double ?? 0,
                            PR_AUC_Approx_All = x_type[k++].as_double ?? 0,
                            PR_AUC_Approx_11p = x_type[k++].as_double ?? 0,
                            PRI_AUC_Approx_All = x_type[k++].as_double ?? 0,
                            PRI_AUC_Approx_11p = x_type[k++].as_double ?? 0,
                            AP_All = x_type[k++].as_double ?? 0,
                            AP_11p = x_type[k++].as_double ?? 0,
                            API_All = x_type[k++].as_double ?? 0,
                            API_11p = x_type[k++].as_double ?? 0,
                            Brier_Inverse_All = x_type[k++].as_double ?? 0,
                            LRP = x_type[k++].as_double ?? 0,
                            LRN = x_type[k++].as_double ?? 0,
                            DOR = x_type[k++].as_double ?? 0,
                            PrevalenceThreshold = x_type[k++].as_double ?? 0,
                            CriticalSuccessIndex = x_type[k++].as_double ?? 0,
                            F1B_00 = x_type[k++].as_double ?? 0,
                            F1B_01 = x_type[k++].as_double ?? 0,
                            F1B_02 = x_type[k++].as_double ?? 0,
                            F1B_03 = x_type[k++].as_double ?? 0,
                            F1B_04 = x_type[k++].as_double ?? 0,
                            F1B_05 = x_type[k++].as_double ?? 0,
                            F1B_06 = x_type[k++].as_double ?? 0,
                            F1B_07 = x_type[k++].as_double ?? 0,
                            F1B_08 = x_type[k++].as_double ?? 0,
                            F1B_09 = x_type[k++].as_double ?? 0,
                            F1B_10 = x_type[k++].as_double ?? 0,
                        },

                        roc_xy_str_all = x_type.Length > k ? x_type[k++].as_str : "",
                        roc_xy_str_11p = x_type.Length > k ? x_type[k++].as_str : "",
                        pr_xy_str_all = x_type.Length > k ? x_type[k++].as_str : "",
                        pr_xy_str_11p = x_type.Length > k ? x_type[k++].as_str : "",
                        pri_xy_str_all = x_type.Length > k ? x_type[k++].as_str : "",
                        pri_xy_str_11p = x_type.Length > k ? x_type[k++].as_str : "",
                        thresholds = x_type.Length > k ? x_type[k++].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => double.TryParse(a, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var th_out) ? th_out : -1).ToArray() : null,
                        predictions = x_type.Length > k ? x_type[k++].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => new prediction(a.Split('|'))).ToArray() : null,
                    };

                    return cm;
                })
                .Where(a => a != null)
                .ToArray();

            return cm_list;
        }




        internal void calculate_metrics(CancellationTokenSource cts, metrics_box metrics, bool calculate_auc, prediction[] prediction_list)
        {
            if (cts.IsCancellationRequested) return;

            if (x_class_id != null && prediction_list != null && prediction_list.Length > 0 && prediction_list.Any(a => a.probability_estimates != null && a.probability_estimates.Length > 0))
            {
                var p_brier_score_all = performance_measure.brier(prediction_list, x_class_id.Value);
                metrics.Brier_Inverse_All = 1 - p_brier_score_all;

                if (calculate_auc)
                {
                    //var (p_brier_score_all, p_roc_auc_all, p_roc_auc2_all, p_pr_auc_all, p_pri_auc_all, p_ap_all, p_api_all, p_roc_xy_all, p_pr_xy_all, p_pri_xy_all) = performance_measure.Calculate_ROC_PR_AUC(prediction_list, class_id.Value, false);
                    //var (p_brier_score_11p, p_roc_auc_11p, p_roc_auc2_11p, p_pr_auc_11p, p_pri_auc_11p, p_ap_11p, p_api_11p, p_roc_xy_11p, p_pr_xy_11p, p_pri_xy_11p) = performance_measure.Calculate_ROC_PR_AUC(prediction_list, class_id.Value, true);

                    var (p_roc_auc_approx_all, p_roc_auc_actual_all, p_pr_auc_approx_all, p_pri_auc_approx_all, p_ap_all, p_api_all, p_roc_xy_all, p_pr_xy_all, p_pri_xy_all) = performance_measure.Calculate_ROC_PR_AUC(cts, prediction_list, x_class_id.Value, performance_measure.threshold_type.all_thresholds);
                    var (p_roc_auc_approx_11p, p_roc_auc_actual_11p, p_pr_auc_approx_11p, p_pri_auc_approx_11p, p_ap_11p, p_api_11p, p_roc_xy_11p, p_pr_xy_11p, p_pri_xy_11p) = performance_measure.Calculate_ROC_PR_AUC(cts, prediction_list, x_class_id.Value, performance_measure.threshold_type.eleven_points);


                    metrics.ROC_AUC_Approx_All = p_roc_auc_approx_all;
                    metrics.ROC_AUC_Approx_11p = p_roc_auc_approx_11p;

                    metrics.ROC_AUC_All = p_roc_auc_actual_all;
                    //ROC_AUC_11p = p_roc_auc_actual_11p;

                    metrics.PR_AUC_Approx_All = p_pr_auc_approx_all;
                    metrics.PR_AUC_Approx_11p = p_pr_auc_approx_11p;

                    metrics.PRI_AUC_Approx_All = p_pri_auc_approx_all;
                    metrics.PRI_AUC_Approx_11p = p_pri_auc_approx_11p;

                    metrics.AP_All = p_ap_all;
                    metrics.AP_11p = p_ap_11p;
                    metrics.API_All = p_api_all;
                    metrics.API_11p = p_api_11p;

                    // PR (x: a.TPR, y: a.PPV)
                    // ROC (x: a.FPR, y: a.TPR)

                    // roc x,y points (11 point and all thresholds)
                    roc_xy_str_all = p_roc_xy_all != null && p_roc_xy_all.Count > 0 ? $"FPR;TPR/{string.Join("/", p_roc_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";
                    roc_xy_str_11p = p_roc_xy_11p != null && p_roc_xy_11p.Count > 0 ? $"FPR;TPR/{string.Join("/", p_roc_xy_11p.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";

                    // precision-recall chart x,y points (11 point and all thresholds)
                    pr_xy_str_all = p_pr_xy_all != null && p_pr_xy_all.Count > 0 ? $"TPR;PPV/{string.Join("/", p_pr_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";
                    pr_xy_str_11p = p_pr_xy_11p != null && p_pr_xy_11p.Count > 0 ? $"TPR;PPV/{string.Join("/", p_pr_xy_11p.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";

                    // precision-recall interpolated x,y points (11 point and all thresholds)
                    pri_xy_str_all = p_pri_xy_all != null && p_pri_xy_all.Count > 0 ? $"TPR;PPV/{string.Join("/", p_pri_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";
                    pri_xy_str_11p = p_pri_xy_11p != null && p_pri_xy_11p.Count > 0 ? $"TPR;PPV/{string.Join("/", p_pri_xy_11p.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";
                }
            }

            const double zero = 0.0;
            const double one = 1.0;
            const double two = 2.0;

            metrics.Support = (metrics.N) == zero ? zero : (double)(metrics.TP + metrics.FP) / (double)(metrics.N);
            metrics.BaseRate = (metrics.N) == zero ? zero : (double)(metrics.TP + metrics.FN) / (double)(metrics.N);

            metrics.Prevalence = (metrics.P + metrics.N) == zero ? zero : (metrics.FN + metrics.TP) / (metrics.P + metrics.N);

            metrics.MCR = (metrics.P + metrics.N) == zero ? zero : (metrics.FP + metrics.FN) / (metrics.P + metrics.N);
            metrics.TPR = (metrics.TP + metrics.FN) == zero ? zero : (double)(metrics.TP) / (double)(metrics.TP + metrics.FN);
            metrics.TNR = (metrics.TN + metrics.FP) == zero ? zero : (double)(metrics.TN) / (double)(metrics.TN + metrics.FP);
            metrics.Precision = (metrics.TP + metrics.FP) == zero ? zero : (double)(metrics.TP) / (double)(metrics.TP + metrics.FP);

            metrics.Overlap = (metrics.TP + metrics.FP + metrics.FN) == zero ? zero : metrics.TP / (metrics.TP + metrics.FP + metrics.FN);

            // null error rate
            metrics.NER = (metrics.P + metrics.N) == zero ? zero : (metrics.P > metrics.N ? metrics.P : metrics.N) / (metrics.P + metrics.N);

            // class null error rate
            metrics.CNER = (metrics.P + metrics.N) == zero ? zero : metrics.P / (metrics.P + metrics.N);

            // positive predictive value (differs from precision, can be equal)
            metrics.PPV = (metrics.TPR * metrics.Prevalence + (one - metrics.TNR) * (one - metrics.Prevalence)) == zero ? zero : (metrics.TPR * metrics.Prevalence) / (metrics.TPR * metrics.Prevalence + (one - metrics.TNR) * (one - metrics.Prevalence));

            // negative predictive value
            metrics.NPV = (metrics.TN + metrics.FN) == zero ? zero : (double)(metrics.TN) / (double)(metrics.TN + metrics.FN);

            // false negative rate
            metrics.FNR = (metrics.FN + metrics.TP) == zero ? zero : (double)(metrics.FN) / (double)(metrics.FN + metrics.TP);

            // false positive rate
            metrics.FPR = (metrics.FP + metrics.TN) == zero ? zero : (double)(metrics.FP) / (double)(metrics.FP + metrics.TN);

            // false discovery rate
            metrics.FDR = (metrics.FP + metrics.TP) == zero ? zero : (double)(metrics.FP) / (double)(metrics.FP + metrics.TP);

            // false omission rate
            metrics.FOR = (metrics.FN + metrics.TN) == zero ? zero : (double)(metrics.FN) / (double)(metrics.FN + metrics.TN);

            // accuracy
            metrics.ACC = (metrics.P + metrics.N) == zero ? zero : (double)(metrics.TP + metrics.TN) / (double)(metrics.P + metrics.N);

            // test error rate (inaccuracy)
            metrics.ER = one - metrics.ACC;

            metrics.YoudenIndex = metrics.TPR + metrics.TNR - one;


            //Kappa = (totalAccuracy - randomAccuracy) / (1 - randomAccuracy)
            //totalAccuracy = (TP + TN) / (TP + TN + FP + FN)
            //randomAccuracy = referenceLikelihood(F) * resultLikelihood(F) + referenceLikelihood(T) * resultLikelihood(T)
            //randomAccuracy = (ActualFalse * PredictedFalse + ActualTrue * PredictedTrue) / Total * Total
            //randomAccuracy = (TN + FP) * (TN + FN) + (FN + TP) * (FP + TP) / Total * Total

            metrics.RND_ACC = ((metrics.TN + metrics.FP) * (metrics.TN + metrics.FN) + (metrics.FN + metrics.TP) * (metrics.FP + metrics.TP)) / ((metrics.TP + metrics.TN + metrics.FP + metrics.FN) * (metrics.TP + metrics.TN + metrics.FP + metrics.FN));

            metrics.Kappa = (one - metrics.RND_ACC) == zero ? zero : (metrics.ACC - metrics.RND_ACC) / (one - metrics.RND_ACC);

            // Geometric Mean score
            metrics.GMean = Math.Sqrt(metrics.TPR * metrics.TNR);

            // F1 score
            metrics.F1S = (metrics.PPV + metrics.TPR) == zero ? zero : (double)(2 * metrics.PPV * metrics.TPR) / (double)(metrics.PPV + metrics.TPR);

            // G1 score (same as Fowlkes–Mallows index?)
            metrics.G1S = (metrics.PPV + metrics.TPR) == zero ? zero : (double)Math.Sqrt((double)(metrics.PPV * metrics.TPR));

            // Matthews correlation coefficient (MCC)
            metrics.MCC = ((double)Math.Sqrt((double)((metrics.TP + metrics.FP) * (metrics.TP + metrics.FN) * (metrics.TN + metrics.FP) * (metrics.TN + metrics.FN))) == zero) ? zero : ((metrics.TP * metrics.TN) - (metrics.FP * metrics.FN)) / (double)Math.Sqrt((double)((metrics.TP + metrics.FP) * (metrics.TP + metrics.FN) * (metrics.TN + metrics.FP) * (metrics.TN + metrics.FN)));
            metrics.Informedness = (metrics.TPR + metrics.TNR) - one;
            metrics.Markedness = (metrics.PPV + metrics.NPV) - one;
            metrics.BalancedAccuracy = (metrics.TPR + metrics.TNR) / two;

            // likelihood ratio for positive results
            metrics.LRP = (one - metrics.TNR) == zero ? zero : (metrics.TPR) / (one - metrics.TNR);

            // likelihood ratio for negative results
            metrics.LRN = (metrics.TNR) == zero ? zero : (one - metrics.TPR) / (metrics.TNR);

            // Diagnostic odds ratio
            metrics.DOR = metrics.LRN == zero ? zero : metrics.LRP / metrics.LRN;

            // Prevalence Threshold
            metrics.PrevalenceThreshold = (metrics.TPR + metrics.TNR - one) == zero ? zero : (Math.Sqrt(metrics.TPR * (-metrics.TNR + one)) + metrics.TNR - one) / (metrics.TPR + metrics.TNR - one);

            // Threat Score / Critical Success Index
            metrics.CriticalSuccessIndex = (metrics.TP + metrics.FN + metrics.FP) == zero ? zero : metrics.TP / (metrics.TP + metrics.FN + metrics.FP);

            // Fowlkes–Mallows index - (same as G1 score?):
            // var FM_Index = Math.Sqrt(PPV * TPR);

            metrics.F1B_00 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, 0.0d);
            metrics.F1B_01 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, 0.1d);
            metrics.F1B_02 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, 0.2d);
            metrics.F1B_03 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, 0.3d);
            metrics.F1B_04 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, 0.4d);
            metrics.F1B_05 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, 0.5d);
            metrics.F1B_06 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, 0.6d);
            metrics.F1B_07 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, 0.7d);
            metrics.F1B_08 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, 0.8d);
            metrics.F1B_09 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, 0.9d);
            metrics.F1B_10 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, 1.0d);
        }


        internal static readonly string[] csv_header_values_array =
                index_data.csv_header_values_array.Select(a => $"id_{a}").ToArray()
                .Concat(grid_point.csv_header_values_array.Select(a => $"gp_{a}").ToArray())
                .Concat(
                new string[]
                {
                    nameof(x_duration_grid_search),
                    nameof(x_duration_training),
                    nameof(x_duration_testing),
                    nameof(x_prediction_threshold),
                    nameof(x_prediction_threshold_class),
                    nameof(x_repetitions_index),
                    nameof(x_outer_cv_index),

                    nameof(x_class_id),
                    nameof(x_class_weight),
                    nameof(x_class_name),
                    nameof(x_class_size),
                    nameof(x_class_training_size),
                    nameof(x_class_testing_size),
                })
                .Concat(metrics_box.csv_header_values_array.Select(a => $"m_{a}").ToArray())
                .Concat(new string[]
                {
                    nameof(roc_xy_str_all),
                    nameof(roc_xy_str_11p),
                    nameof(pr_xy_str_all),
                    nameof(pr_xy_str_11p),
                    nameof(pri_xy_str_all),
                    nameof(pri_xy_str_11p),

                    nameof(thresholds),
                    nameof(predictions),
                }).ToArray();

        internal static readonly string csv_header_string = string.Join(",", csv_header_values_array);


        internal string[] csv_values_array(bool summary = false)
        {
            return
                (unrolled_index_data?.csv_values_array() ?? index_data.empty.csv_values_array())
                .Concat(grid_point?.csv_values_array() ?? grid_point.empty.csv_values_array())
                .Concat(new string[]
                {
                    x_duration_grid_search ?? "",
                    x_duration_training ?? "",
                    x_duration_testing ?? "",
                    x_prediction_threshold?.ToString("G17", NumberFormatInfo.InvariantInfo),
                    x_prediction_threshold_class?.ToString("G17", NumberFormatInfo.InvariantInfo),
                    x_repetitions_index.ToString(CultureInfo.InvariantCulture),
                    x_outer_cv_index.ToString(CultureInfo.InvariantCulture),

                    x_class_id?.ToString(CultureInfo.InvariantCulture),
                    x_class_weight?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    x_class_name ?? "",
                    x_class_size.ToString("G17", NumberFormatInfo.InvariantInfo),
                    x_class_training_size.ToString("G17", NumberFormatInfo.InvariantInfo),
                    x_class_testing_size.ToString("G17", NumberFormatInfo.InvariantInfo),
                })
                .Concat(metrics?.csv_values_array() ?? metrics_box.empty.csv_values_array())
                .Concat(new string[] {
                    !summary ? roc_xy_str_all ?? "" : "",
                    !summary ? roc_xy_str_11p ?? "" : "",
                    !summary ? pr_xy_str_all ?? "" : "",
                    !summary ? pr_xy_str_11p ?? "" : "",
                    !summary ? pri_xy_str_all ?? "" : "",
                    !summary ? pri_xy_str_11p ?? "" : "",
                    !summary ? string.Join(';',thresholds?.Select(a=> $"{a:G17}").ToArray() ?? Array.Empty<string>()) : "",
                    !summary ? string.Join(";", predictions?.Select(a=> string.Join("|", a?.csv_values_array() ?? prediction.empty.csv_values_array())).ToArray() ?? Array.Empty<string>()) : "",
                })
                .Select(a => a?.Replace(",", ";", StringComparison.OrdinalIgnoreCase) ?? "")
                .ToArray();
        }

        public string csv_values_string()
        {
            return string.Join(",", csv_values_array());
        }
    }
}
