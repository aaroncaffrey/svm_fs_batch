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
        internal TimeSpan? x_time_grid;
        internal TimeSpan? x_time_train;
        internal TimeSpan? x_time_test;
        internal double? x_prediction_threshold = -1;
        internal double? x_prediction_threshold_class;
        internal int x_repetitions_index = -1;
        internal int x_outer_cv_index = -1;
        internal int? x_class_id;
        internal double? x_class_weight;
        internal string x_class_name;
        internal double x_class_size;
        internal double x_class_train_size;
        internal double x_class_test_size;
        internal metrics_box metrics;
        internal (double x, double y)[] roc_xy_str_all;
        internal (double x, double y)[] roc_xy_str_11p;
        internal (double x, double y)[] pr_xy_str_all;
        internal (double x, double y)[] pr_xy_str_11p;
        internal (double x, double y)[] pri_xy_str_all;
        internal (double x, double y)[] pri_xy_str_11p;
        internal double[] thresholds;
        internal prediction[] predictions;

        // note: load does not load the rd/sd part of the cm file.  this has to be recalculated after loading the cm.

        internal static void save(CancellationTokenSource cts, string cm_full_filename, string cm_summary_filename, bool overwrite, confusion_matrix[] x_list, bool as_parallel = true)
        {
            if (cts.IsCancellationRequested) return;

            save(cts, cm_full_filename, cm_summary_filename, overwrite, null, x_list, null, as_parallel);
        }

        internal static void save(CancellationTokenSource cts, string cm_full_filename, string cm_summary_filename, bool overwrite, (index_data id, confusion_matrix cm, rank_score rs)[] x_list, bool as_parallel = true)
        {
            if (cts.IsCancellationRequested) return;

            save(
                cts: cts,
                cm_full_filename: cm_full_filename,
                cm_summary_filename: cm_summary_filename,
                overwrite: overwrite,
                id_list: x_list.Select(a => a.id).ToArray(),
                cm_list: x_list.Select(a => a.cm).ToArray(),
                rs_list: x_list.Select(a => a.rs).ToArray()
                , as_parallel);
        }

        internal static void save(CancellationTokenSource cts, string cm_full_filename, string cm_summary_filename, bool overwrite, (index_data id, confusion_matrix cm)[] x_list, bool as_parallel = true)
        {
            if (cts.IsCancellationRequested) return;

            save(
                cts: cts,
                cm_full_filename: cm_full_filename,
                cm_summary_filename: cm_summary_filename,
                overwrite: overwrite,
                id_list: x_list.Select(a => a.id).ToArray(),
                cm_list: x_list.Select(a => a.cm).ToArray(),
                rs_list: null
                , as_parallel);
        }


        internal static void save(CancellationTokenSource cts, string cm_full_filename, string cm_summary_filename, bool overwrite, index_data[] id_list, confusion_matrix[] cm_list, rank_score[] rs_list, bool as_parallel = true)
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


            var lines_full = save_full ? new string[lens.Max() + 1] : null;
            var lines_summary = save_summary ? new string[lens.Max() + 1] : null;

            var csv_header_values_array = new List<string>();
            //if (rs_list != null && rs_list.Length > 0) { csv_header_values_array.AddRange(rank_score.csv_header_values_array); }
            //if (id_list != null && id_list.Length > 0) { csv_header_values_array.AddRange(index_data.csv_header_values_array); }
            //if (cm_list != null && cm_list.Length > 0) { csv_header_values_array.AddRange(confusion_matrix.csv_header_values_array); }

            csv_header_values_array.AddRange(rank_score.csv_header_values_array);
            csv_header_values_array.AddRange(index_data.csv_header_values_array);
            csv_header_values_array.AddRange(confusion_matrix.csv_header_values_array);

            var csv_header_values_string = string.Join(",", csv_header_values_array);
            if (lines_full != null) lines_full[0] = csv_header_values_string;
            if (lines_summary != null) lines_summary[0] = csv_header_values_string;


            if (as_parallel)
            {
                Parallel.For(0,
                    lens_max,
                    i =>
                    {
                        var values1 = new List<string>();

                        values1?.AddRange(rs_list != null && rs_list.Length > i ? rs_list[i].csv_values_array() : rank_score.empty.csv_values_array());
                        values1?.AddRange(id_list != null && id_list.Length > i ? id_list[i].csv_values_array() : index_data.empty.csv_values_array());
                        values1?.AddRange(cm_list != null && cm_list.Length > i ? cm_list[i].csv_values_array() : confusion_matrix.empty.csv_values_array());

                        if (lines_full != null) lines_full[i + 1] = string.Join(",", values1);
                        if (lines_summary != null) lines_summary[i + 1] = string.Join(",", values1.Select(a => a.Length <= 255 ? a : "").ToArray());
                    });
            }
            else
            {
                for (var i = 0; i < lens_max; i++)
                {
                    var values1 = new List<string>();

                    values1?.AddRange(rs_list != null && rs_list.Length > i ? rs_list[i].csv_values_array() : rank_score.empty.csv_values_array());
                    values1?.AddRange(id_list != null && id_list.Length > i ? id_list[i].csv_values_array() : index_data.empty.csv_values_array());
                    values1?.AddRange(cm_list != null && cm_list.Length > i ? cm_list[i].csv_values_array() : confusion_matrix.empty.csv_values_array());

                    if (lines_full != null) lines_full[i + 1] = string.Join(",", values1);
                    if (lines_summary != null) lines_summary[i + 1] = string.Join(",", values1.Select(a => a.Length <= 255 ? a : "").ToArray());
                }
            }

            if (lines_full != null && lines_full.Length > 0)
            {
                io_proxy.WriteAllLines(cts, cm_full_filename, lines_full, module_name, method_name);
                io_proxy.WriteLine($@"Saved: {cm_full_filename} ({lines_full.Length} lines)", module_name, method_name);
            }

            if (lines_summary != null && lines_summary.Length > 0)
            {
                io_proxy.WriteAllLines(cts, cm_summary_filename, lines_summary, module_name, method_name);
                io_proxy.WriteLine($@"Saved: {cm_summary_filename} ({lines_summary.Length} lines)", module_name, method_name);
            }
        }

        internal static confusion_matrix[] load(CancellationTokenSource cts, string filename, int column_offset = -1, bool as_parallel = true)
        {
            if (cts.IsCancellationRequested) return default;

            var lines = io_proxy.ReadAllLines(cts, filename);
            var ret = load(cts, lines, column_offset, as_parallel);//, filename);
            return ret;
        }

        internal static confusion_matrix[] load(CancellationTokenSource cts, string[] lines, int column_offset = -1, bool as_parallel = true)
        {
            if (cts.IsCancellationRequested) return default;

            var line_header = lines[0].Split(',');

            var has_header_line = false;

            if (column_offset == -1)
            {
                // find column position in csv of the header, and set column_offset accordingly
                for (var i = 0; i <= (line_header.Length - csv_header_values_array.Length); i++)
                {
                    if (line_header.Skip(i).Take(csv_header_values_array.Length).SequenceEqual(csv_header_values_array, StringComparer.OrdinalIgnoreCase))
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


            var cm_list =
                as_parallel
                ?
                    lines
                    .Skip(has_header_line ? 1 : 0)
                    .Where(a => !string.IsNullOrWhiteSpace(a))
                    .AsParallel()
                    .AsOrdered()
                    .WithCancellation(cts.Token)
                    .Select(line => load_line(cts, column_offset, line))
                    .Where(a => a != null)
                    .ToArray()
                :
                    lines
                    .Skip(has_header_line ? 1 : 0)
                    .Where(a => !string.IsNullOrWhiteSpace(a))
                    .Select(line => load_line(cts, column_offset, line))
                    .Where(a => a != null)
                    .ToArray();


            return cm_list;
        }

        private static confusion_matrix load_line(CancellationTokenSource cts, int column_offset, string line)
        {
            if (cts.IsCancellationRequested) return default;

            var s_all = line.Split(',');

            var column_count = s_all.Length - column_offset;

            //todo: check why confusion matrix is missing from 'line'
            if (column_count < csv_header_values_array.Length) return null;

            var x_type = x_types.get_x_types(s_all, cts, true); //routines.x_types(cts, s_all, true);


            var k = 0;

            // skip and don't load rank_score values
            k += rank_score.csv_header_values_array.Length;

            // load index_data to be able to later match this confusion_matrix instance with its index_data instance 
            var unrolled_index_data = new index_data(x_type, k);
            k += index_data.csv_header_values_array.Length;


            k = column_offset;

            var grid_point = new grid_point() { cost = x_type[k++].as_double, gamma = x_type[k++].as_double, epsilon = x_type[k++].as_double, coef0 = x_type[k++].as_double, degree = x_type[k++].as_double, cv_rate = x_type[k++].as_double, };

            var cm = new confusion_matrix()
            {
                unrolled_index_data = unrolled_index_data,
                grid_point = grid_point,
                x_time_grid = !string.IsNullOrWhiteSpace(x_type[k++].as_str) ? new TimeSpan(x_type[k].as_long ?? 0) : (TimeSpan?)null,
                x_time_train = !string.IsNullOrWhiteSpace(x_type[k++].as_str) ? new TimeSpan(x_type[k].as_long ?? 0) : (TimeSpan?)null,
                x_time_test = !string.IsNullOrWhiteSpace(x_type[k++].as_str) ? new TimeSpan(x_type[k].as_long ?? 0) : (TimeSpan?)null,
                x_prediction_threshold = x_type[k++].as_double,
                x_prediction_threshold_class = x_type[k++].as_double,
                x_repetitions_index = x_type[k++].as_int ?? 0,
                x_outer_cv_index = x_type[k++].as_int ?? 0,
                x_class_id = x_type[k++].as_int,
                x_class_weight = x_type[k++].as_double,
                x_class_name = x_type[k++].as_str,
                x_class_size = x_type[k++].as_double ?? 0,
                x_class_train_size = x_type[k++].as_double ?? 0,
                x_class_test_size = x_type[k++].as_double ?? 0,
                metrics = new metrics_box()
                {
                    cm_P = x_type[k++].as_double ?? 0,
                    cm_N = x_type[k++].as_double ?? 0,
                    cm_P_TP = x_type[k++].as_double ?? 0,
                    cm_N_FP = x_type[k++].as_double ?? 0,
                    cm_N_TN = x_type[k++].as_double ?? 0,
                    cm_P_FN = x_type[k++].as_double ?? 0,
                    p_TPR = x_type[k++].as_double ?? 0,
                    p_TNR = x_type[k++].as_double ?? 0,
                    p_PPV = x_type[k++].as_double ?? 0,
                    p_Precision = x_type[k++].as_double ?? 0,
                    p_Prevalence = x_type[k++].as_double ?? 0,
                    p_MCR = x_type[k++].as_double ?? 0,
                    p_ER = x_type[k++].as_double ?? 0,
                    p_NER = x_type[k++].as_double ?? 0,
                    p_CNER = x_type[k++].as_double ?? 0,
                    p_Kappa = x_type[k++].as_double ?? 0,
                    p_Overlap = x_type[k++].as_double ?? 0,
                    p_RND_ACC = x_type[k++].as_double ?? 0,
                    p_Support = x_type[k++].as_double ?? 0,
                    p_BaseRate = x_type[k++].as_double ?? 0,
                    p_YoudenIndex = x_type[k++].as_double ?? 0,
                    p_NPV = x_type[k++].as_double ?? 0,
                    p_FNR = x_type[k++].as_double ?? 0,
                    p_FPR = x_type[k++].as_double ?? 0,
                    p_FDR = x_type[k++].as_double ?? 0,
                    p_FOR = x_type[k++].as_double ?? 0,
                    p_ACC = x_type[k++].as_double ?? 0,
                    p_GMean = x_type[k++].as_double ?? 0,
                    p_F1S = x_type[k++].as_double ?? 0,
                    p_G1S = x_type[k++].as_double ?? 0,
                    p_MCC = x_type[k++].as_double ?? 0,
                    p_Informedness = x_type[k++].as_double ?? 0,
                    p_Markedness = x_type[k++].as_double ?? 0,
                    p_BalancedAccuracy = x_type[k++].as_double ?? 0,
                    p_ROC_AUC_Approx_All = x_type[k++].as_double ?? 0,
                    p_ROC_AUC_Approx_11p = x_type[k++].as_double ?? 0,
                    p_ROC_AUC_All = x_type[k++].as_double ?? 0,
                    p_PR_AUC_Approx_All = x_type[k++].as_double ?? 0,
                    p_PR_AUC_Approx_11p = x_type[k++].as_double ?? 0,
                    p_PRI_AUC_Approx_All = x_type[k++].as_double ?? 0,
                    p_PRI_AUC_Approx_11p = x_type[k++].as_double ?? 0,
                    p_AP_All = x_type[k++].as_double ?? 0,
                    p_AP_11p = x_type[k++].as_double ?? 0,
                    p_API_All = x_type[k++].as_double ?? 0,
                    p_API_11p = x_type[k++].as_double ?? 0,
                    p_Brier_Inverse_All = x_type[k++].as_double ?? 0,
                    p_LRP = x_type[k++].as_double ?? 0,
                    p_LRN = x_type[k++].as_double ?? 0,
                    p_DOR = x_type[k++].as_double ?? 0,
                    p_PrevalenceThreshold = x_type[k++].as_double ?? 0,
                    p_CriticalSuccessIndex = x_type[k++].as_double ?? 0,
                    p_F1B_00 = x_type[k++].as_double ?? 0,
                    p_F1B_01 = x_type[k++].as_double ?? 0,
                    p_F1B_02 = x_type[k++].as_double ?? 0,
                    p_F1B_03 = x_type[k++].as_double ?? 0,
                    p_F1B_04 = x_type[k++].as_double ?? 0,
                    p_F1B_05 = x_type[k++].as_double ?? 0,
                    p_F1B_06 = x_type[k++].as_double ?? 0,
                    p_F1B_07 = x_type[k++].as_double ?? 0,
                    p_F1B_08 = x_type[k++].as_double ?? 0,
                    p_F1B_09 = x_type[k++].as_double ?? 0,
                    p_F1B_10 = x_type[k++].as_double ?? 0,
                },
                //roc_xy_str_all = x_type.Length > k ? x_type[k++].as_str : "",
                //roc_xy_str_11p = x_type.Length > k ? x_type[k++].as_str : "",
                //pr_xy_str_all = x_type.Length > k ? x_type[k++].as_str : "",
                //pr_xy_str_11p = x_type.Length > k ? x_type[k++].as_str : "",
                //pri_xy_str_all = x_type.Length > k ? x_type[k++].as_str : "",
                //pri_xy_str_11p = x_type.Length > k ? x_type[k++].as_str : "",
                roc_xy_str_all = x_type.Length > k && !string.IsNullOrWhiteSpace(x_type[k++].as_str) ? x_type[k].as_str.Split(';').Skip(1/* skip axis names*/).Select(a => { var xy = a.Split(':', StringSplitOptions.RemoveEmptyEntries).Select(b => double.Parse(b, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray(); return (xy[0], xy[1]); }).ToArray() : null,
                roc_xy_str_11p = x_type.Length > k && !string.IsNullOrWhiteSpace(x_type[k++].as_str) ? x_type[k].as_str.Split(';').Skip(1/* skip axis names*/).Select(a => { var xy = a.Split(':', StringSplitOptions.RemoveEmptyEntries).Select(b => double.Parse(b, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray(); return (xy[0], xy[1]); }).ToArray() : null,
                pr_xy_str_all = x_type.Length > k && !string.IsNullOrWhiteSpace(x_type[k++].as_str) ? x_type[k].as_str.Split(';').Skip(1/* skip axis names*/).Select(a => { var xy = a.Split(':', StringSplitOptions.RemoveEmptyEntries).Select(b => double.Parse(b, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray(); return (xy[0], xy[1]); }).ToArray() : null,
                pr_xy_str_11p = x_type.Length > k && !string.IsNullOrWhiteSpace(x_type[k++].as_str) ? x_type[k].as_str.Split(';').Skip(1/* skip axis names*/).Select(a => { var xy = a.Split(':', StringSplitOptions.RemoveEmptyEntries).Select(b => double.Parse(b, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray(); return (xy[0], xy[1]); }).ToArray() : null,
                pri_xy_str_all = x_type.Length > k && !string.IsNullOrWhiteSpace(x_type[k++].as_str) ? x_type[k].as_str.Split(';').Skip(1/* skip axis names*/).Select(a => { var xy = a.Split(':', StringSplitOptions.RemoveEmptyEntries).Select(b => double.Parse(b, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray(); return (xy[0], xy[1]); }).ToArray() : null,
                pri_xy_str_11p = x_type.Length > k && !string.IsNullOrWhiteSpace(x_type[k++].as_str) ? x_type[k].as_str.Split(';').Skip(1/* skip axis names*/).Select(a => { var xy = a.Split(':', StringSplitOptions.RemoveEmptyEntries).Select(b => double.Parse(b, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray(); return (xy[0], xy[1]); }).ToArray() : null,

                thresholds = x_type.Length > k && !string.IsNullOrWhiteSpace(x_type[k++].as_str) ? x_type[k].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => double.TryParse(a, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var th_out) ? th_out : -1).ToArray() : null,
                predictions = x_type.Length > k && !string.IsNullOrWhiteSpace(x_type[k++].as_str) ? x_type[k].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => new prediction(a.Split('|'))).ToArray() : null,
            };

            return cm;
        }


        internal void calculate_theshold_metrics(CancellationTokenSource cts, metrics_box metrics, bool calculate_auc, prediction[] prediction_list)
        {
            if (cts.IsCancellationRequested) return;

            if (x_class_id != null && prediction_list != null && prediction_list.Length > 0 && prediction_list.Any(a => a.probability_estimates != null && a.probability_estimates.Length > 0))
            {
                var p_brier_score_all = performance_measure.brier(prediction_list, x_class_id.Value);
                metrics.p_Brier_Inverse_All = 1 - p_brier_score_all;

                if (calculate_auc)
                {
                    //var (p_brier_score_all, p_roc_auc_all, p_roc_auc2_all, p_pr_auc_all, p_pri_auc_all, p_ap_all, p_api_all, p_roc_xy_all, p_pr_xy_all, p_pri_xy_all) = performance_measure.Calculate_ROC_PR_AUC(prediction_list, class_id.Value, false);
                    //var (p_brier_score_11p, p_roc_auc_11p, p_roc_auc2_11p, p_pr_auc_11p, p_pri_auc_11p, p_ap_11p, p_api_11p, p_roc_xy_11p, p_pr_xy_11p, p_pri_xy_11p) = performance_measure.Calculate_ROC_PR_AUC(prediction_list, class_id.Value, true);

                    var (p_roc_auc_approx_all, p_roc_auc_actual_all, p_pr_auc_approx_all, p_pri_auc_approx_all, p_ap_all, p_api_all, p_roc_xy_all, p_pr_xy_all, p_pri_xy_all) = performance_measure.Calculate_ROC_PR_AUC(cts, prediction_list, x_class_id.Value, performance_measure.threshold_type.all_thresholds);
                    var (p_roc_auc_approx_11p, p_roc_auc_actual_11p, p_pr_auc_approx_11p, p_pri_auc_approx_11p, p_ap_11p, p_api_11p, p_roc_xy_11p, p_pr_xy_11p, p_pri_xy_11p) = performance_measure.Calculate_ROC_PR_AUC(cts, prediction_list, x_class_id.Value, performance_measure.threshold_type.eleven_points);


                    metrics.p_ROC_AUC_Approx_All = p_roc_auc_approx_all;
                    metrics.p_ROC_AUC_Approx_11p = p_roc_auc_approx_11p;

                    metrics.p_ROC_AUC_All = p_roc_auc_actual_all;
                    //ROC_AUC_11p = p_roc_auc_actual_11p;

                    metrics.p_PR_AUC_Approx_All = p_pr_auc_approx_all;
                    metrics.p_PR_AUC_Approx_11p = p_pr_auc_approx_11p;

                    metrics.p_PRI_AUC_Approx_All = p_pri_auc_approx_all;
                    metrics.p_PRI_AUC_Approx_11p = p_pri_auc_approx_11p;

                    metrics.p_AP_All = p_ap_all;
                    metrics.p_AP_11p = p_ap_11p;
                    metrics.p_API_All = p_api_all;
                    metrics.p_API_11p = p_api_11p;

                    // PR (x: a.TPR, y: a.PPV)
                    // ROC (x: a.FPR, y: a.TPR)

                    // roc x,y points (11 point and all thresholds)
                    //roc_xy_str_all = p_roc_xy_all != null && p_roc_xy_all.Count > 0 ? $"FPR;TPR/{string.Join("/", p_roc_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToArray())}" : "";
                    //roc_xy_str_11p = p_roc_xy_11p != null && p_roc_xy_11p.Count > 0 ? $"FPR;TPR/{string.Join("/", p_roc_xy_11p.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToArray())}" : "";
                    //// precision-recall chart x,y points (11 point and all thresholds)
                    //pr_xy_str_all = p_pr_xy_all != null && p_pr_xy_all.Count > 0 ? $"TPR;PPV/{string.Join("/", p_pr_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToArray())}" : "";
                    //pr_xy_str_11p = p_pr_xy_11p != null && p_pr_xy_11p.Count > 0 ? $"TPR;PPV/{string.Join("/", p_pr_xy_11p.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToArray())}" : "";
                    //// precision-recall interpolated x,y points (11 point and all thresholds)
                    //pri_xy_str_all = p_pri_xy_all != null && p_pri_xy_all.Count > 0 ? $"TPR;PPV/{string.Join("/", p_pri_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToArray())}" : "";
                    //pri_xy_str_11p = p_pri_xy_11p != null && p_pri_xy_11p.Count > 0 ? $"TPR;PPV/{string.Join("/", p_pri_xy_11p.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToArray())}" : "";

                    //roc x, y points(11 point and all thresholds)
                    roc_xy_str_all = p_roc_xy_all;
                    roc_xy_str_11p = p_roc_xy_11p;
                    // precision-recall chart x,y points (11 point and all thresholds)
                    pr_xy_str_all = p_pr_xy_all;
                    pr_xy_str_11p = p_pr_xy_11p;
                    // precision-recall interpolated x,y points (11 point and all thresholds)
                    pri_xy_str_all = p_pri_xy_all;
                    pri_xy_str_11p = p_pri_xy_11p;
                }
            }

            metrics.calculate_metrics();
        }


        internal static readonly string[] csv_header_values_array =
                //index_data.csv_header_values_array.Select(a => $"id_{a}").ToArray()
                //.Concat(grid_point.csv_header_values_array.Select(a => $"gp_{a}").ToArray())
                (grid_point.csv_header_values_array.Select(a => $"gp_{a}").ToArray())
                .Concat(
                new string[]
                {
                    nameof(x_time_grid),
                    nameof(x_time_train),
                    nameof(x_time_test),
                    nameof(x_prediction_threshold),
                    nameof(x_prediction_threshold_class),
                    nameof(x_repetitions_index),
                    nameof(x_outer_cv_index),

                    nameof(x_class_id),
                    nameof(x_class_weight),
                    nameof(x_class_name),
                    nameof(x_class_size),
                    nameof(x_class_train_size),
                    nameof(x_class_test_size),
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


        internal string[] csv_values_array()//bool summary = false)
        {
            //const bool summary = false;
            return
                //(unrolled_index_data?.csv_values_array() ?? index_data.empty.csv_values_array())
                //.Concat(grid_point?.csv_values_array() ?? grid_point.empty.csv_values_array())
                (grid_point?.csv_values_array() ?? grid_point.empty.csv_values_array())
                .Concat(new string[]
                {
                    x_time_grid?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    x_time_train?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    x_time_test?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    x_prediction_threshold?.ToString("G17", NumberFormatInfo.InvariantInfo),
                    x_prediction_threshold_class?.ToString("G17", NumberFormatInfo.InvariantInfo),
                    x_repetitions_index.ToString(NumberFormatInfo.InvariantInfo),
                    x_outer_cv_index.ToString(NumberFormatInfo.InvariantInfo),

                    x_class_id?.ToString(NumberFormatInfo.InvariantInfo),
                    x_class_weight?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    x_class_name ?? "",
                    x_class_size.ToString("G17", NumberFormatInfo.InvariantInfo),
                    x_class_train_size.ToString("G17", NumberFormatInfo.InvariantInfo),
                    x_class_test_size.ToString("G17", NumberFormatInfo.InvariantInfo),
                })
                .Concat(metrics?.csv_values_array() ?? metrics_box.empty.csv_values_array())
                .Concat(new string[] {
                    
                    //// roc x,y points (11 point and all thresholds) // $"{Math.Round(a.x, 6)}:{Math.Round(a.y, 6)}"?
                    roc_xy_str_all != null && roc_xy_str_all.Length > 0 ? ($"FPR:TPR;{string.Join(";", roc_xy_str_all?.Select(a => $"{a.x:G17}:{a.y:G17}").ToArray() ?? Array.Empty<string>())}" ?? "") : "",
                    roc_xy_str_11p != null && roc_xy_str_11p.Length > 0 ? ($"FPR:TPR;{string.Join(";", roc_xy_str_11p?.Select(a=> $"{a.x:G17}:{a.y:G17}").ToArray() ?? Array.Empty<string>())}" ?? "") : "",

                    //// precision-recall chart x,y points (11 point and all thresholds)
                    pr_xy_str_all != null && pr_xy_str_all.Length > 0 ? ($"TPR:PPV;{string.Join(";", pr_xy_str_all?.Select(a=> $"{a.x:G17}:{a.y:G17}").ToArray() ?? Array.Empty<string>())}" ?? "") : "",
                    pr_xy_str_11p != null && pr_xy_str_11p.Length > 0 ? ($"TPR:PPV;{string.Join(";", pr_xy_str_11p?.Select(a=> $"{a.x:G17}:{a.y:G17}").ToArray() ?? Array.Empty<string>())}" ?? "") : "",

                    //// precision-recall interpolated x,y points (11 point and all thresholds)
                    pri_xy_str_all != null && pri_xy_str_all.Length > 0 ? ($"TPR:PPV;{string.Join(";", pri_xy_str_all?.Select(a=> $"{a.x:G17}:{a.y:G17}").ToArray() ?? Array.Empty<string>())}" ?? "") : "",
                    pri_xy_str_11p != null && pri_xy_str_11p.Length > 0 ? ($"TPR:PPV;{string.Join(";", pri_xy_str_11p?.Select(a=> $"{a.x:G17}:{a.y:G17}").ToArray() ?? Array.Empty<string>())}" ?? "") : "",

                    string.Join(';',thresholds?.Select(a=> $"{a:G17}").ToArray() ?? Array.Empty<string>()),
                    string.Join(";", predictions?.Select(a=> string.Join("|", (a?.csv_values_array() ?? prediction.empty.csv_values_array()))).ToArray() ?? Array.Empty<string>())

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
