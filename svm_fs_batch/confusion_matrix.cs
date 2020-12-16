using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal class confusion_matrix
    {
        public const string module_name = nameof(confusion_matrix);

        // note: load does not load the rd/sd part of the cm file.  this has to be recalculated after loading the cm.

        internal static void save(string cm_filename, IList<confusion_matrix> cm_list)
        {
            save(cm_filename, cm_list, null, null);
        }

        internal static void save(string cm_sd_list_filename, IList<(confusion_matrix cm, score_data sd)> cm_sd_list)
        {
            save(cm_sd_list_filename, cm_sd_list.Select(a => a.cm).ToArray(), cm_sd_list.Select(a => a.sd).ToArray(), null);
        }

        internal static void save(string cm_sd_rd_list_filename, IList<(confusion_matrix cm, score_data sd, rank_data rd)> cm_sd_rd_list)
        {
            save(cm_sd_rd_list_filename, cm_sd_rd_list.Select(a => a.cm).ToArray(), cm_sd_rd_list.Select(a => a.sd).ToArray(), cm_sd_rd_list.Select(a => a.rd).ToArray());
        }


        internal static void save(string cm_sd_rd_list_filename, IList<confusion_matrix> cm_list, IList<score_data> sd_list, IList<rank_data> rd_list)
        {
            const string method_name = nameof(save);

            if (string.IsNullOrWhiteSpace(cm_sd_rd_list_filename)) throw new Exception();

            var lens = new int[] { cm_list?.Count??0, sd_list?.Count??0, rd_list?.Count??0 }.Where(a=>a>0).ToArray();

            if (lens.Length == 0 || lens.Distinct().Count() > 1) throw new Exception();

            var lines = new string[lens.Max() + 1];

            var csv_header_values = new List<string>();

            if (sd_list != null && sd_list.Count > 0) { csv_header_values.AddRange(score_data.csv_header_values); csv_header_values.Add($@"_"); }
            if (rd_list != null && rd_list.Count > 0) { csv_header_values.AddRange(rank_data.csv_header_values); csv_header_values.Add($@"_"); }
            if (cm_list != null && cm_list.Count > 0) { csv_header_values.AddRange(confusion_matrix.csv_header_values); csv_header_values.Add($@"_"); }

            lines[0] = string.Join(",", csv_header_values);


            Parallel.For(0,
                cm_list.Count,
                i =>
                {
                    var values = new List<string>();
                    if (sd_list != null && sd_list.Count > 0)
                    {
                        values.AddRange(sd_list[i].csv_values_array());
                        values.Add($@"_");
                    }

                    if (rd_list != null && rd_list.Count > 0)
                    {
                        values.AddRange(rd_list[i].csv_values_array());
                        values.Add($@"_");
                    }

                    if (cm_list != null && cm_list.Count > 0)
                    {
                        values.AddRange(cm_list[i].csv_values_array());
                        values.Add($@"_");
                    }
                    lines[i + 1] = string.Join(",", values);
                });

            io_proxy.WriteAllLines(cm_sd_rd_list_filename, lines, module_name, method_name);
        }

        internal static List<confusion_matrix> load(string filename, int column_offset = -1)
        {
            var lines = io_proxy.ReadAllLines(filename).ToList();
            var ret = load(lines, column_offset);//, filename);
            return ret;
        }

        internal static List<confusion_matrix> load(IList<string> lines, int column_offset = -1)//, string cm_fn = null)
        {
            var line_header = lines[0].Split(',').ToList();

            var has_header = false;

            if (column_offset == -1)
            {
                // find column position in csv of the header, and set column_offset accordingly
                for (var i = 0; i <= (line_header.Count - csv_header_values.Count); i++)
                {
                    if (line_header.Skip(i).Take(csv_header_values.Count).SequenceEqual(csv_header_values))
                    {
                        has_header = true;
                        column_offset = i;
                        break;
                    }
                }

                if (column_offset == -1) { throw new ArgumentOutOfRangeException(nameof(column_offset)); }
            }

            if (!has_header)
            {
                if (line_header.Skip(column_offset).Take(csv_header_values.Count).SequenceEqual(csv_header_values))
                {
                    has_header = true;
                }
            }

            //var cms = new List<confusion_matrix_data>();

            var cm_list = lines.Skip(has_header ? 1 : 0)
                .Where(a => !string.IsNullOrWhiteSpace(a))
                .AsParallel()
                .AsOrdered()
                .Select(line =>
                //for (var line_index = 0; line_index < lines.Count; line_index++)// in lines.Where(a => !string.IsNullOrWhiteSpace(a)))
                {
                    //var line = lines[line_index];

                    //if ((has_header && line_index == 0) || string.IsNullOrWhiteSpace(line))
                    //{
                    //    continue;
                    //}

                    var s_all = line.Split(',').ToList();

                    var x_double = s_all.AsParallel().AsOrdered().Select(a => double.TryParse(a, NumberStyles.Float, CultureInfo.InvariantCulture, out var out_double) ? out_double : (double?)null).ToList();
                    var x_int = s_all.AsParallel().AsOrdered().Select((a, j) => int.TryParse(a, NumberStyles.Integer, CultureInfo.InvariantCulture, out var out_int) ? out_int : (int?)null).ToList();
                    var x_bool = s_all.AsParallel().AsOrdered().Select((a, j) =>
                    {
                        if (x_int[j] == 0) return (bool?)false;
                        if (x_int[j] == 1) return (bool?)true;
                        return bool.TryParse(a, out var out_bool) ? out_bool : (bool?)null;
                    }).ToList();


                    //var key_value_list = line_header.Select((a, i) => (key: line_header[i], value_str: s_all.Count - 1 >= i ? s_all[i] : default, value_int: x_int.Count - 1 >= i ? x_int[i] : default, value_double: x_double.Count - 1 >= i ? x_double[i] : default)).ToList();
                    //var unknown_key_value_list = key_value_list.Where(a => !csv_header_values.Contains(a.key)).ToList();
                    //key_value_list = key_value_list.Where(a => csv_header_values.Contains(a.key)).ToList();


                    //if (line_index == 0 || string.IsNullOrWhiteSpace(line))
                    //{
                    //    cms.Add(new confusion_matrix_data()
                    //    {
                    //        line = line,
                    //        cm = (confusion_matrix)null,
                    //        key_value_list = key_value_list,
                    //        unknown_key_value_list = unknown_key_value_list
                    //    });
                    //    continue;
                    //}

                    var x_str = s_all.Skip(column_offset).ToList();

                    //if (x_str.Count != csv_header_values.Count) return null;//continue;
                    if (x_str.Count < csv_header_values.Count) return null;//continue;


                    var k = 0; // column_offset;

                    if (column_offset != 0)
                    {
                        x_double = x_double.Skip(column_offset).ToList();
                        x_int = x_int.Skip(column_offset).ToList();
                        x_bool = x_bool.Skip(column_offset).ToList();
                    }

                    var cm = new confusion_matrix()
                    {
                        selection_test_info = new selection_test_info()
                        {
                            y_is_group_selected = x_bool[k++] ?? default,
                            y_is_only_selection = x_bool[k++] ?? default,
                            y_is_last_winner = x_bool[k++] ?? default,
                            //y_num_groups_added_from_last_iteration = x_int[k++] ?? default,
                            //y_num_columns_added_from_last_iteration = x_int[k++] ?? default,
                            //y_num_groups_added_from_highest_score_iteration = x_int[k++] ?? default,
                            //y_num_columns_added_from_highest_score_iteration = x_int[k++] ?? default,
                            y_selection_direction = Enum.TryParse(x_str[k++], out program.direction out_selection_direction) ? out_selection_direction : default,

                            y_test_groups_count = x_int[k++] ?? default,
                            y_test_columns_count = x_int[k++] ?? default,
                            y_test_groups = x_str[k++].Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => int.TryParse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_int) ? out_int : -1).ToArray(),
                            y_test_columns = x_str[k++].Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => int.TryParse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_int) ? out_int : -1).ToArray(),

                            //y_previous_winner_groups_count = x_int[k++] ?? default,
                            //y_previous_winner_columns_count = x_int[k++] ?? default,
                            //y_previous_winner_groups = x_str[k++].Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => int.TryParse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_int) ? out_int : -1).ToArray(),
                            //y_previous_winner_columns = x_str[k++].Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => int.TryParse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_int) ? out_int : -1).ToArray(),

                            //y_best_winner_groups_count = x_int[k++] ?? default,
                            //y_best_winner_columns_count = x_int[k++] ?? default,
                            //y_best_winner_groups = x_str[k++].Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => int.TryParse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_int) ? out_int : -1).ToArray(),
                            //y_best_winner_columns = x_str[k++].Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => int.TryParse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_int) ? out_int : -1).ToArray(),

                        },

                        x_experiment_name = x_str[k++],
                        //x_id = x_int[k++],
                        x_iteration_index = x_int[k++],
                        //x_iteration_name = x_str[k++],
                        x_group_array_index = x_int[k++],
                        x_total_groups = x_int[k++],
                        x_calc_11p_thresholds = x_bool[k++],
                        x_key_file_tag = x_str[k++],
                        x_key_alphabet = x_str[k++],
                        x_key_stats = x_str[k++],
                        x_key_dimension = x_str[k++],
                        x_key_category = x_str[k++],
                        x_key_source = x_str[k++],
                        x_key_group = x_str[k++],
                        x_key_member = x_str[k++],
                        x_key_perspective = x_str[k++],
                        x_duration_grid_search = x_str[k++],
                        x_duration_training = x_str[k++],
                        x_duration_testing = x_str[k++],
                        x_scale_function = Enum.TryParse(x_str[k++], out scaling.scale_function out_scale_function) ? out_scale_function : default,
                        x_libsvm_cv = x_double[k++] ?? 0,
                        x_prediction_threshold = x_double[k++],
                        x_prediction_threshold_class = x_double[k++],
                        //x_old_column_count = x_int[k++] ?? 0,
                        //x_new_column_count = x_int[k++] ?? 0,
                        //x_old_group_count = x_int[k++] ?? 0,
                        //x_new_group_count = x_int[k++] ?? 0,
                        //x_columns_included = s[k++].Split(';').Select(a=>int.Parse(a,NumberStyles.Integer,NumberFormatInfo.InvariantInfo)).ToArray(),
                        //x_groups_included = s[k++].Split(';').Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray(),
                        x_inner_cv_folds = x_int[k++] ?? 0,
                        x_repetitions_index = x_int[k++] ?? 0,
                        x_repetitions_total = x_int[k++] ?? 0,
                        x_outer_cv_index = x_int[k++] ?? 0,
                        x_outer_cv_folds = x_int[k++] ?? 0,
                        x_outer_cv_folds_to_run = x_int[k++] ?? 0,
                        x_svm_type = Enum.TryParse(x_str[k++], out routines.libsvm_svm_type out_svm_type) ? out_svm_type : default,
                        x_svm_kernel = Enum.TryParse(x_str[k++], out routines.libsvm_kernel_type out_svm_kernel) ? out_svm_kernel : default,
                        grid_point = new grid_point() { cost = x_double[k++], gamma = x_double[k++], epsilon = x_double[k++], coef0 = x_double[k++], degree = x_double[k++], },
                        x_class_id = x_int[k++],
                        x_class_weight = x_double[k++],
                        x_class_name = x_str[k++],
                        x_class_size = x_double[k++] ?? 0,
                        x_class_training_size = x_double[k++] ?? 0,
                        x_class_testing_size = x_double[k++] ?? 0,
                        metrics = new metrics_box()
                        {
                            P = x_double[k++] ?? 0,
                            N = x_double[k++] ?? 0,
                            TP = x_double[k++] ?? 0,
                            FP = x_double[k++] ?? 0,
                            TN = x_double[k++] ?? 0,
                            FN = x_double[k++] ?? 0,
                            TPR = x_double[k++] ?? 0,
                            TNR = x_double[k++] ?? 0,
                            PPV = x_double[k++] ?? 0,
                            Precision = x_double[k++] ?? 0,
                            Prevalence = x_double[k++] ?? 0,
                            MCR = x_double[k++] ?? 0,
                            ER = x_double[k++] ?? 0,
                            NER = x_double[k++] ?? 0,
                            CNER = x_double[k++] ?? 0,
                            Kappa = x_double[k++] ?? 0,
                            Overlap = x_double[k++] ?? 0,
                            RND_ACC = x_double[k++] ?? 0,
                            Support = x_double[k++] ?? 0,
                            BaseRate = x_double[k++] ?? 0,
                            YoudenIndex = x_double[k++] ?? 0,
                            NPV = x_double[k++] ?? 0,
                            FNR = x_double[k++] ?? 0,
                            FPR = x_double[k++] ?? 0,
                            FDR = x_double[k++] ?? 0,
                            FOR = x_double[k++] ?? 0,
                            ACC = x_double[k++] ?? 0,
                            GMean = x_double[k++] ?? 0,
                            F1S = x_double[k++] ?? 0,
                            G1S = x_double[k++] ?? 0,
                            MCC = x_double[k++] ?? 0,
                            Informedness = x_double[k++] ?? 0,
                            Markedness = x_double[k++] ?? 0,
                            BalancedAccuracy = x_double[k++] ?? 0,
                            ROC_AUC_Approx_All = x_double[k++] ?? 0,
                            ROC_AUC_Approx_11p = x_double[k++] ?? 0,
                            ROC_AUC_All = x_double[k++] ?? 0,
                            PR_AUC_Approx_All = x_double[k++] ?? 0,
                            PR_AUC_Approx_11p = x_double[k++] ?? 0,
                            PRI_AUC_Approx_All = x_double[k++] ?? 0,
                            PRI_AUC_Approx_11p = x_double[k++] ?? 0,
                            AP_All = x_double[k++] ?? 0,
                            AP_11p = x_double[k++] ?? 0,
                            API_All = x_double[k++] ?? 0,
                            API_11p = x_double[k++] ?? 0,
                            Brier_Inverse_All = x_double[k++] ?? 0,
                            LRP = x_double[k++] ?? 0,
                            LRN = x_double[k++] ?? 0,
                            DOR = x_double[k++] ?? 0,
                            PrevalenceThreshold = x_double[k++] ?? 0,
                            CriticalSuccessIndex = x_double[k++] ?? 0,
                            F1B_00 = x_double[k++] ?? 0,
                            F1B_01 = x_double[k++] ?? 0,
                            F1B_02 = x_double[k++] ?? 0,
                            F1B_03 = x_double[k++] ?? 0,
                            F1B_04 = x_double[k++] ?? 0,
                            F1B_05 = x_double[k++] ?? 0,
                            F1B_06 = x_double[k++] ?? 0,
                            F1B_07 = x_double[k++] ?? 0,
                            F1B_08 = x_double[k++] ?? 0,
                            F1B_09 = x_double[k++] ?? 0,
                            F1B_10 = x_double[k++] ?? 0,
                        },
                        //metrics_ppf = new metrics_box()
                        //{
                        //    P = x_double[k++] ?? 0,
                        //    N = x_double[k++] ?? 0,
                        //    TP = x_double[k++] ?? 0,
                        //    FP = x_double[k++] ?? 0,
                        //    TN = x_double[k++] ?? 0,
                        //    FN = x_double[k++] ?? 0,
                        //    TPR = x_double[k++] ?? 0,
                        //    TNR = x_double[k++] ?? 0,
                        //    PPV = x_double[k++] ?? 0,
                        //    Precision = x_double[k++] ?? 0,
                        //    Prevalence = x_double[k++] ?? 0,
                        //    MCR = x_double[k++] ?? 0,
                        //    ER = x_double[k++] ?? 0,
                        //    NER = x_double[k++] ?? 0,
                        //    CNER = x_double[k++] ?? 0,
                        //    Kappa = x_double[k++] ?? 0,
                        //    Overlap = x_double[k++] ?? 0,
                        //    RND_ACC = x_double[k++] ?? 0,
                        //    Support = x_double[k++] ?? 0,
                        //    BaseRate = x_double[k++] ?? 0,
                        //    YoudenIndex = x_double[k++] ?? 0,
                        //    NPV = x_double[k++] ?? 0,
                        //    FNR = x_double[k++] ?? 0,
                        //    FPR = x_double[k++] ?? 0,
                        //    FDR = x_double[k++] ?? 0,
                        //    FOR = x_double[k++] ?? 0,
                        //    ACC = x_double[k++] ?? 0,
                        //    GMean = x_double[k++] ?? 0,
                        //    F1S = x_double[k++] ?? 0,
                        //    G1S = x_double[k++] ?? 0,
                        //    MCC = x_double[k++] ?? 0,
                        //    Informedness = x_double[k++] ?? 0,
                        //    Markedness = x_double[k++] ?? 0,
                        //    BalancedAccuracy = x_double[k++] ?? 0,
                        //    ROC_AUC_Approx_All = x_double[k++] ?? 0,
                        //    ROC_AUC_Approx_11p = x_double[k++] ?? 0,
                        //    ROC_AUC_All = x_double[k++] ?? 0,
                        //    PR_AUC_Approx_All = x_double[k++] ?? 0,
                        //    PR_AUC_Approx_11p = x_double[k++] ?? 0,
                        //    PRI_AUC_Approx_All = x_double[k++] ?? 0,
                        //    PRI_AUC_Approx_11p = x_double[k++] ?? 0,
                        //    AP_All = x_double[k++] ?? 0,
                        //    AP_11p = x_double[k++] ?? 0,
                        //    API_All = x_double[k++] ?? 0,
                        //    API_11p = x_double[k++] ?? 0,
                        //    Brier_Inverse_All = x_double[k++] ?? 0,
                        //    LRP = x_double[k++] ?? 0,
                        //    LRN = x_double[k++] ?? 0,
                        //    DOR = x_double[k++] ?? 0,
                        //    PrevalenceThreshold = x_double[k++] ?? 0,
                        //    CriticalSuccessIndex = x_double[k++] ?? 0,
                        //    F1B_00 = x_double[k++] ?? 0,
                        //    F1B_01 = x_double[k++] ?? 0,
                        //    F1B_02 = x_double[k++] ?? 0,
                        //    F1B_03 = x_double[k++] ?? 0,
                        //    F1B_04 = x_double[k++] ?? 0,
                        //    F1B_05 = x_double[k++] ?? 0,
                        //    F1B_06 = x_double[k++] ?? 0,
                        //    F1B_07 = x_double[k++] ?? 0,
                        //    F1B_08 = x_double[k++] ?? 0,
                        //    F1B_09 = x_double[k++] ?? 0,
                        //    F1B_10 = x_double[k++] ?? 0,
                        //},
                        //metrics_ppg = new metrics_box()
                        //{
                        //    P = x_double[k++] ?? 0,
                        //    N = x_double[k++] ?? 0,
                        //    TP = x_double[k++] ?? 0,
                        //    FP = x_double[k++] ?? 0,
                        //    TN = x_double[k++] ?? 0,
                        //    FN = x_double[k++] ?? 0,
                        //    TPR = x_double[k++] ?? 0,
                        //    TNR = x_double[k++] ?? 0,
                        //    PPV = x_double[k++] ?? 0,
                        //    Precision = x_double[k++] ?? 0,
                        //    Prevalence = x_double[k++] ?? 0,
                        //    MCR = x_double[k++] ?? 0,
                        //    ER = x_double[k++] ?? 0,
                        //    NER = x_double[k++] ?? 0,
                        //    CNER = x_double[k++] ?? 0,
                        //    Kappa = x_double[k++] ?? 0,
                        //    Overlap = x_double[k++] ?? 0,
                        //    RND_ACC = x_double[k++] ?? 0,
                        //    Support = x_double[k++] ?? 0,
                        //    BaseRate = x_double[k++] ?? 0,
                        //    YoudenIndex = x_double[k++] ?? 0,
                        //    NPV = x_double[k++] ?? 0,
                        //    FNR = x_double[k++] ?? 0,
                        //    FPR = x_double[k++] ?? 0,
                        //    FDR = x_double[k++] ?? 0,
                        //    FOR = x_double[k++] ?? 0,
                        //    ACC = x_double[k++] ?? 0,
                        //    GMean = x_double[k++] ?? 0,
                        //    F1S = x_double[k++] ?? 0,
                        //    G1S = x_double[k++] ?? 0,
                        //    MCC = x_double[k++] ?? 0,
                        //    Informedness = x_double[k++] ?? 0,
                        //    Markedness = x_double[k++] ?? 0,
                        //    BalancedAccuracy = x_double[k++] ?? 0,
                        //    ROC_AUC_Approx_All = x_double[k++] ?? 0,
                        //    ROC_AUC_Approx_11p = x_double[k++] ?? 0,
                        //    ROC_AUC_All = x_double[k++] ?? 0,
                        //    PR_AUC_Approx_All = x_double[k++] ?? 0,
                        //    PR_AUC_Approx_11p = x_double[k++] ?? 0,
                        //    PRI_AUC_Approx_All = x_double[k++] ?? 0,
                        //    PRI_AUC_Approx_11p = x_double[k++] ?? 0,
                        //    AP_All = x_double[k++] ?? 0,
                        //    AP_11p = x_double[k++] ?? 0,
                        //    API_All = x_double[k++] ?? 0,
                        //    API_11p = x_double[k++] ?? 0,
                        //    Brier_Inverse_All = x_double[k++] ?? 0,
                        //    LRP = x_double[k++] ?? 0,
                        //    LRN = x_double[k++] ?? 0,
                        //    DOR = x_double[k++] ?? 0,
                        //    PrevalenceThreshold = x_double[k++] ?? 0,
                        //    CriticalSuccessIndex = x_double[k++] ?? 0,
                        //    F1B_00 = x_double[k++] ?? 0,
                        //    F1B_01 = x_double[k++] ?? 0,
                        //    F1B_02 = x_double[k++] ?? 0,
                        //    F1B_03 = x_double[k++] ?? 0,
                        //    F1B_04 = x_double[k++] ?? 0,
                        //    F1B_05 = x_double[k++] ?? 0,
                        //    F1B_06 = x_double[k++] ?? 0,
                        //    F1B_07 = x_double[k++] ?? 0,
                        //    F1B_08 = x_double[k++] ?? 0,
                        //    F1B_09 = x_double[k++] ?? 0,
                        //    F1B_10 = x_double[k++] ?? 0,
                        //},
                        roc_xy_str_all = x_str.Count > k ? x_str[k++] : "",
                        roc_xy_str_11p = x_str.Count > k ? x_str[k++] : "",
                        pr_xy_str_all = x_str.Count > k ? x_str[k++] : "",
                        pr_xy_str_11p = x_str.Count > k ? x_str[k++] : "",
                        pri_xy_str_all = x_str.Count > k ? x_str[k++] : "",
                        pri_xy_str_11p = x_str.Count > k ? x_str[k++] : "",
                        thresholds = x_str.Count > k ? x_str[k++].Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => double.TryParse(a, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var th_out) ? th_out : -1).ToList() : null,
                        predictions = x_str.Count > k ? x_str[k++].Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => new prediction(a)).ToList() : null,
                    };

                    return cm;
                    //cms.Add(new confusion_matrix_data() {line = line, cm = cm, key_value_list = key_value_list, unknown_key_value_list = unknown_key_value_list,});
                })
                .Where(a => a != null)
                .ToList();

            //var cm_list = cms.Select(a => a.cm).ToList();
            return cm_list;
        }

        internal List<double> thresholds;
        internal List<prediction> predictions;

        internal string x_experiment_name;
        //internal int? x_id;
        internal int? x_iteration_index;
        //internal string x_iteration_name;
        internal int? x_group_array_index;
        internal int? x_total_groups;
        internal bool? x_calc_11p_thresholds;
        internal string x_key_file_tag;
        internal string x_key_alphabet;
        internal string x_key_stats;
        internal string x_key_dimension;
        internal string x_key_category;
        internal string x_key_source;
        internal string x_key_group;
        internal string x_key_member;
        internal string x_key_perspective;
        internal string x_duration_grid_search;
        internal string x_duration_training;
        internal string x_duration_testing;
        internal scaling.scale_function x_scale_function;
        internal double x_libsvm_cv;
        internal double? x_prediction_threshold = -1;
        internal double? x_prediction_threshold_class;
        //internal int x_old_column_count;
        //internal int x_new_column_count;
        //internal int x_old_group_count;
        //internal int x_new_group_count;
        //internal int[] x_columns_included;
        //internal int[] x_groups_included;
        internal int x_inner_cv_folds;
        internal int x_repetitions_index;
        internal int x_repetitions_total;
        internal int x_outer_cv_index;
        internal int x_outer_cv_folds;
        internal int x_outer_cv_folds_to_run;
        internal routines.libsvm_svm_type x_svm_type;
        internal routines.libsvm_kernel_type x_svm_kernel;
        internal grid_point grid_point;
        //internal double? x_cost;
        //internal double? x_gamma;
        //internal double? x_epsilon;
        //internal double? x_coef0;
        //internal double? x_degree;
        internal int? x_class_id;
        internal double? x_class_weight;
        internal string x_class_name;
        internal double x_class_size;
        internal double x_class_training_size;
        internal double x_class_testing_size;

        internal selection_test_info selection_test_info;

        internal metrics_box metrics;
        //internal metrics_box metrics_ppf;
        //internal metrics_box metrics_ppg;

        internal string roc_xy_str_all;
        internal string roc_xy_str_11p;
        internal string pr_xy_str_all;
        internal string pr_xy_str_11p;
        internal string pri_xy_str_all;
        internal string pri_xy_str_11p;



        internal void calculate_metrics(metrics_box metrics, bool calculate_auc, IList<prediction> prediction_list)
        {
            //var cm = this;

            if (calculate_auc && prediction_list != null && prediction_list.Count > 0 && prediction_list.Any(a => a.probability_estimates != null && a.probability_estimates.Count > 0))
            {

                var (p_roc_auc_approx_all, p_roc_auc_actual_all, p_pr_auc_approx_all, p_pri_auc_approx_all, p_ap_all, p_api_all, p_roc_xy_all, p_pr_xy_all, p_pri_xy_all) = perf.Calculate_ROC_PR_AUC(prediction_list, x_class_id.Value, perf.threshold_type.all_thresholds);
                var (p_roc_auc_approx_11p, p_roc_auc_actual_11p, p_pr_auc_approx_11p, p_pri_auc_approx_11p, p_ap_11p, p_api_11p, p_roc_xy_11p, p_pr_xy_11p, p_pri_xy_11p) = perf.Calculate_ROC_PR_AUC(prediction_list, x_class_id.Value, perf.threshold_type.eleven_points);

                var p_brier_score_all = perf.brier(prediction_list, x_class_id.Value);
                //var (p_brier_score_all, p_roc_auc_all, p_roc_auc2_all, p_pr_auc_all, p_pri_auc_all, p_ap_all, p_api_all, p_roc_xy_all, p_pr_xy_all, p_pri_xy_all) = performance_measure.Calculate_ROC_PR_AUC(prediction_list, class_id.Value, false);
                //var (p_brier_score_11p, p_roc_auc_11p, p_roc_auc2_11p, p_pr_auc_11p, p_pri_auc_11p, p_ap_11p, p_api_11p, p_roc_xy_11p, p_pr_xy_11p, p_pri_xy_11p) = performance_measure.Calculate_ROC_PR_AUC(prediction_list, class_id.Value, true);

                metrics.Brier_Inverse_All = 1 - p_brier_score_all;
                //Brier_11p = p_brier_score_11p;

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

                roc_xy_str_all = p_roc_xy_all != null ? $"FPR;TPR/{string.Join("/", p_roc_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";
                roc_xy_str_11p = p_roc_xy_11p != null ? $"FPR;TPR/{string.Join("/", p_roc_xy_11p.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";

                pr_xy_str_all = p_pr_xy_all != null ? $"TPR;PPV/{string.Join("/", p_pr_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";
                pr_xy_str_11p = p_pr_xy_11p != null ? $"TPR;PPV/{string.Join("/", p_pr_xy_11p.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";

                pri_xy_str_all = p_pri_xy_all != null ? $"TPR;PPV/{string.Join("/", p_pri_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";
                pri_xy_str_11p = p_pri_xy_11p != null ? $"TPR;PPV/{string.Join("/", p_pri_xy_11p.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";
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

            metrics.F1B_00 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, (double)0.0);
            metrics.F1B_01 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, (double)0.1);
            metrics.F1B_02 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, (double)0.2);
            metrics.F1B_03 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, (double)0.3);
            metrics.F1B_04 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, (double)0.4);
            metrics.F1B_05 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, (double)0.5);
            metrics.F1B_06 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, (double)0.6);
            metrics.F1B_07 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, (double)0.7);
            metrics.F1B_08 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, (double)0.8);
            metrics.F1B_09 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, (double)0.9);
            metrics.F1B_10 = metrics_box.fbeta2(metrics.PPV, metrics.TPR, (double)1.0);

            //calculate_ppf_ppg();
        }

        //internal void calculate_ppf_ppg()
        //{
        //    //metrics_ppf = new metrics_box(metrics, selection_test_info?.y_test_columns_count ?? 0);
        //    //metrics_ppg = new metrics_box(metrics, selection_test_info?.y_test_groups_count ?? 0);
        //}



        /*internal static confusion_matrix Average2(List<confusion_matrix> cm)
        {

            int? cid = cm.Select(a => a.class_id).Distinct().Count() == 1 ? cm.First().class_id : null;

            var result = new confusion_matrix()
            {
                class_id = cid,
                prediction_threshold = null,
                prediction_threshold_class = null,
                thresholds = cm.SelectMany(a => a.thresholds).Distinct().OrderByDescending(a => a).ToList(),

                duration_grid_search = String.Join(";", cm.Select(a => a.duration_grid_search).Distinct().ToList()),
                //duration_nm_search = String.Join(";", cm.Select(a => a.duration_nm_search).Distinct().ToList()),
                duration_training = String.Join(";", cm.Select(a => a.duration_training).Distinct().ToList()),
                duration_testing = String.Join(";", cm.Select(a => a.duration_testing).Distinct().ToList()),


                class_weight = cm.Select(a => a.class_weight).Average(),
                class_name = String.Join(";", cm.Select(a => a.class_name).Distinct().ToList()),

                experiment_name = String.Join(";", cm.Select(a => a.experiment_name).Distinct().ToList()),
                //experiment_id1 = String.Join(";", cm.Select(a => a.experiment_id1).Distinct().ToList()),
                //experiment_id2 = String.Join(";", cm.Select(a => a.experiment_id2).Distinct().ToList()),
                //experiment_id3 = String.Join(";", cm.Select(a => a.experiment_id3).Distinct().ToList()),

                //feature_selection_type = String.Join(";", cm.Select(a => a.feature_selection_type).Distinct().ToList()),

                //fs_perf_selection = String.Join(";", cm.Select(a => a.fs_perf_selection).Distinct().ToList()),


                features_included = String.Join(";", cm.Select(a => a.features_included).Distinct().ToList()),
                svm_kernel = String.Join(";", cm.Select(a => a.svm_kernel).Distinct().ToList()),


                libsvm_cv = cm.Select(a => a.libsvm_cv).Average(),
                //libsvm_cv_precision = cm.Select(a => a.libsvm_cv_precision).Average(),
                //libsvm_cv_accuracy = cm.Select(a => a.libsvm_cv_accuracy).Average(),
                //libsvm_cv_fscore = cm.Select(a => a.libsvm_cv_fscore).Average(),
                //libsvm_cv_ap = cm.Select(a => a.libsvm_cv_ap).Average(),
                //libsvm_cv_auc = cm.Select(a => a.libsvm_cv_auc).Average(),
                //libsvm_cv_bac = cm.Select(a => a.libsvm_cv_bac).Average(),
                //libsvm_cv_recall = cm.Select(a => a.libsvm_cv_recall).Average(),


                //math_operation = String.Join(";", cm.Select(a => a.math_operation).Distinct().ToList()),

                pr_xy_str_all = "",
                pr_xy_str_11p = "",
                pri_xy_str_all = "",
                pri_xy_str_11p = "",
                roc_xy_str_all = "",
                roc_xy_str_11p = "",

                scaling_method = String.Join(";", cm.Select(a => a.scaling_method).Distinct().ToList()),
                svm_type = String.Join(";", cm.Select(a => a.svm_type).Distinct().ToList()),
                //training_resampling_method = String.Join(";", cm.Select(a => a.training_resampling_method).Distinct().ToList()),
                //kernel_parameter_search_method = String.Join(";", cm.Select(a => a.kernel_parameter_search_method).Distinct().ToList()),



                P = cm.Select(a => a.P).Average(),
                N = cm.Select(a => a.N).Average(),

                TP = cm.Select(a => a.TP).Average(),
                FP = cm.Select(a => a.FP).Average(),
                TN = cm.Select(a => a.TN).Average(),
                FN = cm.Select(a => a.FN).Average(),
                TPR = cm.Select(a => a.TPR).Average(),
                TNR = cm.Select(a => a.TNR).Average(),
                PPV = cm.Select(a => a.PPV).Average(),

                Precision = cm.Select(a => a.Precision).Average(),
                Prevalence = cm.Select(a => a.Prevalence).Average(),
                MCR = cm.Select(a => a.MCR).Average(),
                ER = cm.Select(a => a.ER).Average(),
                NER = cm.Select(a => a.NER).Average(),
                CNER = cm.Select(a => a.CNER).Average(),
                Kappa = cm.Select(a => a.Kappa).Average(),
                Overlap = cm.Select(a => a.Overlap).Average(),
                RND_ACC = cm.Select(a => a.RND_ACC).Average(),
                Support = cm.Select(a => a.Support).Average(),
                BaseRate = cm.Select(a => a.BaseRate).Average(),
                YoudenIndex = cm.Select(a => a.YoudenIndex).Average(),


                NPV = cm.Select(a => a.NPV).Average(),
                FNR = cm.Select(a => a.FNR).Average(),
                FPR = cm.Select(a => a.FPR).Average(),
                FDR = cm.Select(a => a.FDR).Average(),
                FOR = cm.Select(a => a.FOR).Average(),
                ACC = cm.Select(a => a.ACC).Average(),
                GM = cm.Select(a => a.GM).Average(),
                F1S = cm.Select(a => a.F1S).Average(),
                G1S = cm.Select(a => a.G1S).Average(),
                MCC = cm.Select(a => a.MCC).Average(),
                Informedness = cm.Select(a => a.Informedness).Average(),
                Markedness = cm.Select(a => a.Markedness).Average(),
                BalancedAccuracy = cm.Select(a => a.BalancedAccuracy).Average(),
                ROC_AUC_Approx_All = cm.Select(a => a.ROC_AUC_Approx_All).Average(),
                ROC_AUC_Approx_11p = cm.Select(a => a.ROC_AUC_Approx_11p).Average(),
                PR_AUC_Approx_All = cm.Select(a => a.PR_AUC_Approx_All).Average(),
                PR_AUC_Approx_11p = cm.Select(a => a.PR_AUC_Approx_11p).Average(),
                PRI_AUC_Approx_All = cm.Select(a => a.PRI_AUC_Approx_All).Average(),
                PRI_AUC_Approx_11p = cm.Select(a => a.PRI_AUC_Approx_11p).Average(),
                ROC_AUC_All = cm.Select(a => a.ROC_AUC_All).Average(),

                AP_All = cm.Select(a => a.AP_All).Average(),
                AP_11p = cm.Select(a => a.AP_11p).Average(),
                API_All = cm.Select(a => a.API_All).Average(),
                API_11p = cm.Select(a => a.API_11p).Average(),
                Brier_All = cm.Select(a => a.Brier_All).Average(),
                F1B_00 = cm.Select(a => a.F1B_00).Average(),
                F1B_01 = cm.Select(a => a.F1B_01).Average(),
                F1B_02 = cm.Select(a => a.F1B_02).Average(),
                F1B_03 = cm.Select(a => a.F1B_03).Average(),
                F1B_04 = cm.Select(a => a.F1B_04).Average(),
                F1B_05 = cm.Select(a => a.F1B_05).Average(),
                F1B_06 = cm.Select(a => a.F1B_06).Average(),
                F1B_07 = cm.Select(a => a.F1B_07).Average(),
                F1B_08 = cm.Select(a => a.F1B_08).Average(),
                F1B_09 = cm.Select(a => a.F1B_09).Average(),
                F1B_10 = cm.Select(a => a.F1B_10).Average(),
                LRN = cm.Select(a => a.LRN).Average(),
                LRP = cm.Select(a => a.LRP).Average(),

                repetitions_index = cm.Select(a => a.repetitions_index).Average(),
                repetitions = cm.Select(a => a.repetitions).Average(),


                outer_cv_folds = cm.Select(a => a.outer_cv_folds).Average(),
                inner_cv_folds = cm.Select(a => a.inner_cv_folds).Average(),
                class_size = cm.Select(a => a.class_size).Average(),
                class_training_size = cm.Select(a => a.class_training_size).Average(),
                class_testing_size = cm.Select(a => a.class_testing_size).Average(),
                feature_count = (int)cm.Select(a => a.feature_count).Average(),
                group_count = (int)cm.Select(a => a.group_count).Average(),

                cost = cm.Select(a => a.cost).Average(),
                gamma = cm.Select(a => a.gamma).Average(),
                epsilon = cm.Select(a => a.epsilon).Average(),
                coef0 = cm.Select(a => a.coef0).Average(),
                degree = cm.Select(a => a.degree).Average(),


                outer_cv_index = cm.Select(a => a.outer_cv_index).Average(),


                //testing_size_pct = cm.Select(a => a.testing_size_pct).Average(),
                //training_size_pct = cm.Select(a => a.training_size_pct).Average(),
                //unused_size_pct = cm.Select(a => a.unused_size_pct).Average(),

            };


            result.calculate_metrics(false, null);
            result.calculate_ppf();

            return result;
        }*/

        /*internal static List<confusion_matrix> Average1(List<confusion_matrix> confusion_matrices)//, bool class_specific = true)
        {
            //var print_params = true;

            //if (print_params)
            //{
            //    var param_list = new List<(string key, string value)>() {(nameof(confusion_matrices), confusion_matrices.ToString()),};


            //    if (program.write_console_log) program.WriteLine($@"{nameof(Average1)}({String.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");
            //}

            var x = confusion_matrices.GroupBy(a =>
                (

                    a.svm_type,
                    a.svm_kernel,
                    //a.training_resampling_method,
                    a.repetitions,
                    a.outer_cv_folds,
                    a.inner_cv_folds,
                    //a.kernel_parameter_search_method,

                    a.experiment_name,
                    //a.experiment_id1,
                    //a.experiment_id2,
                    //a.experiment_id3,


                    a.class_id,
                    a.class_name,
                    a.feature_count,
                    a.group_count,
                    a.features_included,
                    //a.math_operation,
                    a.scaling_method,
                    //a.training_size_pct,
                    //a.unused_size_pct,
                    //a.testing_size_pct,
                    a.class_size,
                    a.class_training_size,
                    a.class_testing_size,
                    a.prediction_threshold_class,
                    a.prediction_threshold
                    )).Select(cm =>
                    {
                        var y = new confusion_matrix()
                        {
                            class_id = cm.Key.class_id,
                            prediction_threshold = cm.Key.prediction_threshold,
                            prediction_threshold_class = cm.Key.prediction_threshold_class,
                            thresholds = cm.SelectMany(a => a.thresholds).Distinct().OrderByDescending(a => a).ToList(),

                            duration_grid_search = String.Join(";", cm.Select(a => a.duration_grid_search).Distinct().ToList()),
                            //duration_nm_search = String.Join(";", cm.Select(a => a.duration_nm_search).Distinct().ToList()),
                            duration_training = String.Join(";", cm.Select(a => a.duration_training).Distinct().ToList()),
                            duration_testing = String.Join(";", cm.Select(a => a.duration_testing).Distinct().ToList()),


                            class_weight = cm.Select(a => a.class_weight).Average(),
                            class_name = String.Join(";", cm.Select(a => a.class_name).Distinct().ToList()),

                            experiment_name = String.Join(";", cm.Select(a => a.experiment_name).Distinct().ToList()),
                            //experiment_id1 = String.Join(";", cm.Select(a => a.experiment_id1).Distinct().ToList()),
                            //experiment_id2 = String.Join(";", cm.Select(a => a.experiment_id2).Distinct().ToList()),
                            //experiment_id3 = String.Join(";", cm.Select(a => a.experiment_id3).Distinct().ToList()),

                            ////fs_starting_point = String.Join(";", cm.Select(a => a.fs_starting_point).Distinct().ToList()),
                            //feature_selection_type = String.Join(";", cm.Select(a => a.feature_selection_type).Distinct().ToList()),
                            ////fs_algorithm_direction = String.Join(";", cm.Select(a => a.fs_algorithm_direction).Distinct().ToList()),
                            //fs_perf_selection = String.Join(";", cm.Select(a => a.fs_perf_selection).Distinct().ToList()),


                            features_included = String.Join(";", cm.Select(a => a.features_included).Distinct().ToList()),
                            svm_kernel = String.Join(";", cm.Select(a => a.svm_kernel).Distinct().ToList()),


                            libsvm_cv = cm.Select(a => a.libsvm_cv).Average(),
                            //libsvm_cv_precision = cm.Select(a => a.libsvm_cv_precision).Average(),
                            //libsvm_cv_accuracy = cm.Select(a => a.libsvm_cv_accuracy).Average(),
                            //libsvm_cv_fscore = cm.Select(a => a.libsvm_cv_fscore).Average(),
                            //libsvm_cv_ap = cm.Select(a => a.libsvm_cv_ap).Average(),
                            //libsvm_cv_auc = cm.Select(a => a.libsvm_cv_auc).Average(),
                            //libsvm_cv_bac = cm.Select(a => a.libsvm_cv_bac).Average(),
                            //libsvm_cv_recall = cm.Select(a => a.libsvm_cv_recall).Average(),


                            //math_operation = String.Join(";", cm.Select(a => a.math_operation).Distinct().ToList()),

                            pr_xy_str_all = "",
                            pr_xy_str_11p = "",
                            pri_xy_str_all = "",
                            pri_xy_str_11p = "",
                            roc_xy_str_all = "",
                            roc_xy_str_11p = "",

                            scaling_method = String.Join(";", cm.Select(a => a.scaling_method).Distinct().ToList()),
                            svm_type = String.Join(";", cm.Select(a => a.svm_type).Distinct().ToList()),
                            //training_resampling_method = String.Join(";", cm.Select(a => a.training_resampling_method).Distinct().ToList()),
                            //kernel_parameter_search_method = String.Join(";", cm.Select(a => a.kernel_parameter_search_method).Distinct().ToList()),



                            P = cm.Select(a => a.P).Average(),
                            N = cm.Select(a => a.N).Average(),

                            TP = cm.Select(a => a.TP).Average(),
                            FP = cm.Select(a => a.FP).Average(),
                            TN = cm.Select(a => a.TN).Average(),
                            FN = cm.Select(a => a.FN).Average(),
                            TPR = cm.Select(a => a.TPR).Average(),
                            TNR = cm.Select(a => a.TNR).Average(),
                            PPV = cm.Select(a => a.PPV).Average(),

                            Precision = cm.Select(a => a.Precision).Average(),
                            Prevalence = cm.Select(a => a.Prevalence).Average(),
                            MCR = cm.Select(a => a.MCR).Average(),
                            ER = cm.Select(a => a.ER).Average(),
                            NER = cm.Select(a => a.NER).Average(),
                            CNER = cm.Select(a => a.CNER).Average(),
                            Kappa = cm.Select(a => a.Kappa).Average(),
                            Overlap = cm.Select(a => a.Overlap).Average(),
                            RND_ACC = cm.Select(a => a.RND_ACC).Average(),
                            Support = cm.Select(a => a.Support).Average(),
                            BaseRate = cm.Select(a => a.BaseRate).Average(),
                            YoudenIndex = cm.Select(a => a.YoudenIndex).Average(),


                            NPV = cm.Select(a => a.NPV).Average(),
                            FNR = cm.Select(a => a.FNR).Average(),
                            FPR = cm.Select(a => a.FPR).Average(),
                            FDR = cm.Select(a => a.FDR).Average(),
                            FOR = cm.Select(a => a.FOR).Average(),
                            ACC = cm.Select(a => a.ACC).Average(),
                            GM = cm.Select(a => a.GM).Average(),
                            F1S = cm.Select(a => a.F1S).Average(),
                            G1S = cm.Select(a => a.G1S).Average(),
                            MCC = cm.Select(a => a.MCC).Average(),
                            Informedness = cm.Select(a => a.Informedness).Average(),
                            Markedness = cm.Select(a => a.Markedness).Average(),
                            BalancedAccuracy = cm.Select(a => a.BalancedAccuracy).Average(),
                            ROC_AUC_Approx_All = cm.Select(a => a.ROC_AUC_Approx_All).Average(),
                            ROC_AUC_Approx_11p = cm.Select(a => a.ROC_AUC_Approx_11p).Average(),
                            PR_AUC_Approx_All = cm.Select(a => a.PR_AUC_Approx_All).Average(),
                            PR_AUC_Approx_11p = cm.Select(a => a.PR_AUC_Approx_11p).Average(),
                            PRI_AUC_Approx_All = cm.Select(a => a.PRI_AUC_Approx_All).Average(),
                            PRI_AUC_Approx_11p = cm.Select(a => a.PRI_AUC_Approx_11p).Average(),
                            ROC_AUC_All = cm.Select(a => a.ROC_AUC_All).Average(),

                            AP_All = cm.Select(a => a.AP_All).Average(),
                            AP_11p = cm.Select(a => a.AP_11p).Average(),
                            API_All = cm.Select(a => a.API_All).Average(),
                            API_11p = cm.Select(a => a.API_11p).Average(),
                            Brier_All = cm.Select(a => a.Brier_All).Average(),
                            F1B_00 = cm.Select(a => a.F1B_00).Average(),
                            F1B_01 = cm.Select(a => a.F1B_01).Average(),
                            F1B_02 = cm.Select(a => a.F1B_02).Average(),
                            F1B_03 = cm.Select(a => a.F1B_03).Average(),
                            F1B_04 = cm.Select(a => a.F1B_04).Average(),
                            F1B_05 = cm.Select(a => a.F1B_05).Average(),
                            F1B_06 = cm.Select(a => a.F1B_06).Average(),
                            F1B_07 = cm.Select(a => a.F1B_07).Average(),
                            F1B_08 = cm.Select(a => a.F1B_08).Average(),
                            F1B_09 = cm.Select(a => a.F1B_09).Average(),
                            F1B_10 = cm.Select(a => a.F1B_10).Average(),
                            LRN = cm.Select(a => a.LRN).Average(),
                            LRP = cm.Select(a => a.LRP).Average(),

                            repetitions_index = cm.Select(a => a.repetitions_index).Average(),
                            repetitions = cm.Select(a => a.repetitions).Average(),


                            outer_cv_folds = cm.Select(a => a.outer_cv_folds).Average(),
                            inner_cv_folds = cm.Select(a => a.inner_cv_folds).Average(),
                            class_size = cm.Select(a => a.class_size).Average(),
                            class_training_size = cm.Select(a => a.class_training_size).Average(),
                            class_testing_size = cm.Select(a => a.class_testing_size).Average(),
                            feature_count = (int)cm.Select(a => a.feature_count).Average(),
                            group_count = (int)cm.Select(a => a.group_count).Average(),

                            cost = cm.Select(a => a.cost).Average(),
                            gamma = cm.Select(a => a.gamma).Average(),
                            epsilon = cm.Select(a => a.epsilon).Average(),
                            coef0 = cm.Select(a => a.coef0).Average(),
                            degree = cm.Select(a => a.degree).Average(),


                            outer_cv_index = cm.Select(a => a.outer_cv_index).Average(),


                            //testing_size_pct = cm.Select(a => a.testing_size_pct).Average(),
                            //training_size_pct = cm.Select(a => a.training_size_pct).Average(),
                            //unused_size_pct = cm.Select(a => a.unused_size_pct).Average(),


                        };

                        y.calculate_metrics(false, null);

                        return y;
                    }).ToList();

            return x;
        }*/



        internal static readonly List<string> csv_header_values = new List<string>()
            {

                nameof(svm_fs_batch.selection_test_info.y_is_group_selected),
                nameof(svm_fs_batch.selection_test_info.y_is_only_selection),
                nameof(svm_fs_batch.selection_test_info.y_is_last_winner),
                //nameof(svm_fs_batch.selection_test_info.y_num_groups_added_from_last_iteration),
                //nameof(svm_fs_batch.selection_test_info.y_num_columns_added_from_last_iteration),
                //nameof(svm_fs_batch.selection_test_info.y_num_groups_added_from_highest_score_iteration),
                //nameof(svm_fs_batch.selection_test_info.y_num_columns_added_from_highest_score_iteration),
                nameof(svm_fs_batch.selection_test_info.y_selection_direction),

                nameof(svm_fs_batch.selection_test_info.y_test_groups_count),
                nameof(svm_fs_batch.selection_test_info.y_test_columns_count),
                nameof(svm_fs_batch.selection_test_info.y_test_groups),
                nameof(svm_fs_batch.selection_test_info.y_test_columns),

                //nameof(svm_fs_batch.selection_test_info.y_previous_winner_groups_count),
                //nameof(svm_fs_batch.selection_test_info.y_previous_winner_columns_count),
                //nameof(svm_fs_batch.selection_test_info.y_previous_winner_groups),
                //nameof(svm_fs_batch.selection_test_info.y_previous_winner_columns),
                //nameof(svm_fs_batch.selection_test_info.y_best_winner_groups_count),
                //nameof(svm_fs_batch.selection_test_info.y_best_winner_columns_count),
                //nameof(svm_fs_batch.selection_test_info.y_best_winner_groups),
                //nameof(svm_fs_batch.selection_test_info.y_best_winner_columns),

                nameof(x_experiment_name),
                //nameof(x_id),
                nameof(x_iteration_index),
                //nameof(x_iteration_name),
                nameof(x_group_array_index),
                nameof(x_total_groups),
                nameof(x_calc_11p_thresholds),
                nameof(x_key_file_tag),
                nameof(x_key_alphabet),
                nameof(x_key_stats),
                nameof(x_key_dimension),
                nameof(x_key_category),
                nameof(x_key_source),
                nameof(x_key_group),
                nameof(x_key_member),
                nameof(x_key_perspective),
                nameof(x_duration_grid_search),
                nameof(x_duration_training),
                nameof(x_duration_testing),
                nameof(x_scale_function),
                nameof(x_libsvm_cv),
                nameof(x_prediction_threshold),
                nameof(x_prediction_threshold_class),
                //nameof(x_old_column_count),
                //nameof(x_new_column_count),
                //nameof(x_old_group_count),
                //nameof(x_new_group_count),
                //nameof(x_columns_included),
                //nameof(x_groups_included),
                nameof(x_inner_cv_folds),
                nameof(x_repetitions_index),
                nameof(x_repetitions_total),
                nameof(x_outer_cv_index),
                nameof(x_outer_cv_folds),
                nameof(x_outer_cv_folds_to_run),
                nameof(x_svm_type),
                nameof(x_svm_kernel),
                "x_"+nameof(svm_fs_batch.grid_point.cost),
                "x_"+nameof(svm_fs_batch.grid_point.gamma),
                "x_"+nameof(svm_fs_batch.grid_point.epsilon),
                "x_"+nameof(svm_fs_batch.grid_point.coef0),
                "x_"+nameof(svm_fs_batch.grid_point.degree),
                nameof(x_class_id),
                nameof(x_class_weight),
                nameof(x_class_name),
                nameof(x_class_size),
                nameof(x_class_training_size),
                nameof(x_class_testing_size),

                nameof(metrics_box.P),
                nameof(metrics_box.N),
                nameof(metrics_box.TP),
                nameof(metrics_box.FP),
                nameof(metrics_box.TN),
                nameof(metrics_box.FN),
                nameof(metrics_box.TPR),
                nameof(metrics_box.TNR),
                nameof(metrics_box.PPV),
                nameof(metrics_box.Precision),
                nameof(metrics_box.Prevalence),
                nameof(metrics_box.MCR),
                nameof(metrics_box.ER),
                nameof(metrics_box.NER),
                nameof(metrics_box.CNER),
                nameof(metrics_box.Kappa),
                nameof(metrics_box.Overlap),
                nameof(metrics_box.RND_ACC),
                nameof(metrics_box.Support),
                nameof(metrics_box.BaseRate),
                nameof(metrics_box.YoudenIndex),
                nameof(metrics_box.NPV),
                nameof(metrics_box.FNR),
                nameof(metrics_box.FPR),
                nameof(metrics_box.FDR),
                nameof(metrics_box.FOR),
                nameof(metrics_box.ACC),
                nameof(metrics_box.GMean),
                nameof(metrics_box.F1S),
                nameof(metrics_box.G1S),
                nameof(metrics_box.MCC),
                nameof(metrics_box.Informedness),
                nameof(metrics_box.Markedness),
                nameof(metrics_box.BalancedAccuracy),
                nameof(metrics_box.ROC_AUC_Approx_All),
                nameof(metrics_box.ROC_AUC_Approx_11p),
                nameof(metrics_box.ROC_AUC_All),
                nameof(metrics_box.PR_AUC_Approx_All),
                nameof(metrics_box.PR_AUC_Approx_11p),
                nameof(metrics_box.PRI_AUC_Approx_All),
                nameof(metrics_box.PRI_AUC_Approx_11p),
                nameof(metrics_box.AP_All),
                nameof(metrics_box.AP_11p),
                nameof(metrics_box.API_All),
                nameof(metrics_box.API_11p),
                nameof(metrics_box.Brier_Inverse_All),
                nameof(metrics_box.LRP),
                nameof(metrics_box.LRN),
                nameof(metrics_box.DOR),
                nameof(metrics_box.PrevalenceThreshold),
                nameof(metrics_box.CriticalSuccessIndex),
                nameof(metrics_box.F1B_00),
                nameof(metrics_box.F1B_01),
                nameof(metrics_box.F1B_02),
                nameof(metrics_box.F1B_03),
                nameof(metrics_box.F1B_04),
                nameof(metrics_box.F1B_05),
                nameof(metrics_box.F1B_06),
                nameof(metrics_box.F1B_07),
                nameof(metrics_box.F1B_08),
                nameof(metrics_box.F1B_09),
                nameof(metrics_box.F1B_10),

                //"ppf_" + nameof(metrics_box.P),
                //"ppf_" + nameof(metrics_box.N),
                //"ppf_" + nameof(metrics_box.TP),
                //"ppf_" + nameof(metrics_box.FP),
                //"ppf_" + nameof(metrics_box.TN),
                //"ppf_" + nameof(metrics_box.FN),
                //"ppf_" + nameof(metrics_box.TPR),
                //"ppf_" + nameof(metrics_box.TNR),
                //"ppf_" + nameof(metrics_box.PPV),
                //"ppf_" + nameof(metrics_box.Precision),
                //"ppf_" + nameof(metrics_box.Prevalence),
                //"ppf_" + nameof(metrics_box.MCR),
                //"ppf_" + nameof(metrics_box.ER),
                //"ppf_" + nameof(metrics_box.NER),
                //"ppf_" + nameof(metrics_box.CNER),
                //"ppf_" + nameof(metrics_box.Kappa),
                //"ppf_" + nameof(metrics_box.Overlap),
                //"ppf_" + nameof(metrics_box.RND_ACC),
                //"ppf_" + nameof(metrics_box.Support),
                //"ppf_" + nameof(metrics_box.BaseRate),
                //"ppf_" + nameof(metrics_box.YoudenIndex),
                //"ppf_" + nameof(metrics_box.NPV),
                //"ppf_" + nameof(metrics_box.FNR),
                //"ppf_" + nameof(metrics_box.FPR),
                //"ppf_" + nameof(metrics_box.FDR),
                //"ppf_" + nameof(metrics_box.FOR),
                //"ppf_" + nameof(metrics_box.ACC),
                //"ppf_" + nameof(metrics_box.GMean),
                //"ppf_" + nameof(metrics_box.F1S),
                //"ppf_" + nameof(metrics_box.G1S),
                //"ppf_" + nameof(metrics_box.MCC),
                //"ppf_" + nameof(metrics_box.Informedness),
                //"ppf_" + nameof(metrics_box.Markedness),
                //"ppf_" + nameof(metrics_box.BalancedAccuracy),
                //"ppf_" + nameof(metrics_box.ROC_AUC_Approx_All),
                //"ppf_" + nameof(metrics_box.ROC_AUC_Approx_11p),
                //"ppf_" + nameof(metrics_box.ROC_AUC_All),
                //"ppf_" + nameof(metrics_box.PR_AUC_Approx_All),
                //"ppf_" + nameof(metrics_box.PR_AUC_Approx_11p),
                //"ppf_" + nameof(metrics_box.PRI_AUC_Approx_All),
                //"ppf_" + nameof(metrics_box.PRI_AUC_Approx_11p),
                //"ppf_" + nameof(metrics_box.AP_All),
                //"ppf_" + nameof(metrics_box.AP_11p),
                //"ppf_" + nameof(metrics_box.API_All),
                //"ppf_" + nameof(metrics_box.API_11p),
                //"ppf_" + nameof(metrics_box.Brier_Inverse_All),
                //"ppf_" + nameof(metrics_box.LRP),
                //"ppf_" + nameof(metrics_box.LRN),
                //"ppf_" + nameof(metrics_box.DOR),
                //"ppf_" + nameof(metrics_box.PrevalenceThreshold),
                //"ppf_" + nameof(metrics_box.CriticalSuccessIndex),
                //"ppf_" + nameof(metrics_box.F1B_00),
                //"ppf_" + nameof(metrics_box.F1B_01),
                //"ppf_" + nameof(metrics_box.F1B_02),
                //"ppf_" + nameof(metrics_box.F1B_03),
                //"ppf_" + nameof(metrics_box.F1B_04),
                //"ppf_" + nameof(metrics_box.F1B_05),
                //"ppf_" + nameof(metrics_box.F1B_06),
                //"ppf_" + nameof(metrics_box.F1B_07),
                //"ppf_" + nameof(metrics_box.F1B_08),
                //"ppf_" + nameof(metrics_box.F1B_09),
                //"ppf_" + nameof(metrics_box.F1B_10),

                //"ppg_" + nameof(metrics_box.P),
                //"ppg_" + nameof(metrics_box.N),
                //"ppg_" + nameof(metrics_box.TP),
                //"ppg_" + nameof(metrics_box.FP),
                //"ppg_" + nameof(metrics_box.TN),
                //"ppg_" + nameof(metrics_box.FN),
                //"ppg_" + nameof(metrics_box.TPR),
                //"ppg_" + nameof(metrics_box.TNR),
                //"ppg_" + nameof(metrics_box.PPV),
                //"ppg_" + nameof(metrics_box.Precision),
                //"ppg_" + nameof(metrics_box.Prevalence),
                //"ppg_" + nameof(metrics_box.MCR),
                //"ppg_" + nameof(metrics_box.ER),
                //"ppg_" + nameof(metrics_box.NER),
                //"ppg_" + nameof(metrics_box.CNER),
                //"ppg_" + nameof(metrics_box.Kappa),
                //"ppg_" + nameof(metrics_box.Overlap),
                //"ppg_" + nameof(metrics_box.RND_ACC),
                //"ppg_" + nameof(metrics_box.Support),
                //"ppg_" + nameof(metrics_box.BaseRate),
                //"ppg_" + nameof(metrics_box.YoudenIndex),
                //"ppg_" + nameof(metrics_box.NPV),
                //"ppg_" + nameof(metrics_box.FNR),
                //"ppg_" + nameof(metrics_box.FPR),
                //"ppg_" + nameof(metrics_box.FDR),
                //"ppg_" + nameof(metrics_box.FOR),
                //"ppg_" + nameof(metrics_box.ACC),
                //"ppg_" + nameof(metrics_box.GMean),
                //"ppg_" + nameof(metrics_box.F1S),
                //"ppg_" + nameof(metrics_box.G1S),
                //"ppg_" + nameof(metrics_box.MCC),
                //"ppg_" + nameof(metrics_box.Informedness),
                //"ppg_" + nameof(metrics_box.Markedness),
                //"ppg_" + nameof(metrics_box.BalancedAccuracy),
                //"ppg_" + nameof(metrics_box.ROC_AUC_Approx_All),
                //"ppg_" + nameof(metrics_box.ROC_AUC_Approx_11p),
                //"ppg_" + nameof(metrics_box.ROC_AUC_All),
                //"ppg_" + nameof(metrics_box.PR_AUC_Approx_All),
                //"ppg_" + nameof(metrics_box.PR_AUC_Approx_11p),
                //"ppg_" + nameof(metrics_box.PRI_AUC_Approx_All),
                //"ppg_" + nameof(metrics_box.PRI_AUC_Approx_11p),
                //"ppg_" + nameof(metrics_box.AP_All),
                //"ppg_" + nameof(metrics_box.AP_11p),
                //"ppg_" + nameof(metrics_box.API_All),
                //"ppg_" + nameof(metrics_box.API_11p),
                //"ppg_" + nameof(metrics_box.Brier_Inverse_All),
                //"ppg_" + nameof(metrics_box.LRP),
                //"ppg_" + nameof(metrics_box.LRN),
                //"ppg_" + nameof(metrics_box.DOR),
                //"ppg_" + nameof(metrics_box.PrevalenceThreshold),
                //"ppg_" + nameof(metrics_box.CriticalSuccessIndex),
                //"ppg_" + nameof(metrics_box.F1B_00),
                //"ppg_" + nameof(metrics_box.F1B_01),
                //"ppg_" + nameof(metrics_box.F1B_02),
                //"ppg_" + nameof(metrics_box.F1B_03),
                //"ppg_" + nameof(metrics_box.F1B_04),
                //"ppg_" + nameof(metrics_box.F1B_05),
                //"ppg_" + nameof(metrics_box.F1B_06),
                //"ppg_" + nameof(metrics_box.F1B_07),
                //"ppg_" + nameof(metrics_box.F1B_08),
                //"ppg_" + nameof(metrics_box.F1B_09),
                //"ppg_" + nameof(metrics_box.F1B_10),


                nameof(roc_xy_str_all),
                nameof(roc_xy_str_11p),
                nameof(pr_xy_str_all),
                nameof(pr_xy_str_11p),
                nameof(pri_xy_str_all),
                nameof(pri_xy_str_11p),

                nameof(thresholds),
                nameof(predictions),

            };

        internal static readonly string csv_header = string.Join(",", csv_header_values);


        internal string[] csv_values_array()
        {
            //var st = selection_test_info.get_values();
            //var m = metrics.as_csv_values();
            //var m_ppf = metrics_ppf.as_csv_values();
            //var m_ppg = metrics_ppg.as_csv_values();

            return new string[]
            {
                $"{((selection_test_info?.y_is_group_selected ?? false)? 1 : 0)}",
                $"{((selection_test_info?.y_is_only_selection ?? false)? 1 : 0)}",
                $"{((selection_test_info?.y_is_last_winner ?? false)? 1 : 0)}",
                //$"{selection_test_info?.y_num_groups_added_from_last_iteration}",
                //$"{selection_test_info?.y_num_columns_added_from_last_iteration}",
                //$"{selection_test_info?.y_num_groups_added_from_highest_score_iteration}",
                //$"{selection_test_info?.y_num_columns_added_from_highest_score_iteration}",
                
                $"{selection_test_info?.y_selection_direction}",

                $"{selection_test_info?.y_test_groups_count}",
                $"{selection_test_info?.y_test_columns_count}",
                $"{string.Join(";",selection_test_info?.y_test_groups ?? Array.Empty<int>())}",
                $"{string.Join(";",selection_test_info?.y_test_columns ?? Array.Empty<int>())}",

                //$"{selection_test_info?.y_previous_winner_groups_count}",
                //$"{selection_test_info?.y_previous_winner_columns_count}",
                //$"{string.Join(";",selection_test_info?.y_previous_winner_groups  ?? Array.Empty<int>())}",
                //$"{string.Join(";",selection_test_info?.y_previous_winner_columns ?? Array.Empty<int>())}",

                //$"{selection_test_info?.y_best_winner_groups_count}",
                //$"{selection_test_info?.y_best_winner_columns_count}",
                //$"{string.Join(";",selection_test_info?.y_best_winner_groups ?? Array.Empty<int>())}",
                //$"{string.Join(";",selection_test_info?.y_best_winner_columns ?? Array.Empty<int>())}",

                x_experiment_name ?? "",
                    //x_id?.ToString(CultureInfo.InvariantCulture),
                    x_iteration_index?.ToString(CultureInfo.InvariantCulture),
                    //x_iteration_name ?? "",
                    x_group_array_index?.ToString(CultureInfo.InvariantCulture),
                    x_total_groups?.ToString(CultureInfo.InvariantCulture),
                    $"{((x_calc_11p_thresholds ?? false) ? 1 : 0)}",
                    x_key_file_tag  ?? "",
                    x_key_alphabet  ?? "",
                    x_key_stats  ?? "",
                    x_key_dimension ?? "" ,
                    x_key_category ?? "",
                    x_key_source ?? "",
                    x_key_group ?? "" ,
                    x_key_member  ?? "",
                    x_key_perspective  ?? "",
                    x_duration_grid_search ?? "",
                    x_duration_training ?? "",
                    x_duration_testing ?? "",
                    x_scale_function.ToString(),
                    x_libsvm_cv.ToString("G17", NumberFormatInfo.InvariantInfo),
                    x_prediction_threshold?.ToString("G17", NumberFormatInfo.InvariantInfo),
                    x_prediction_threshold_class?.ToString("G17", NumberFormatInfo.InvariantInfo),
                    //x_old_column_count.ToString(CultureInfo.InvariantCulture),
                    //x_new_column_count.ToString(CultureInfo.InvariantCulture),
                    //x_old_group_count.ToString(CultureInfo.InvariantCulture),
                    //x_new_group_count.ToString(CultureInfo.InvariantCulture),
                    //string.Join(";", x_columns_included ?? Array.Empty<int>()),
                    //string.Join(";", x_groups_included ?? Array.Empty<int>()),
                    x_inner_cv_folds.ToString(CultureInfo.InvariantCulture),
                    x_repetitions_index.ToString(CultureInfo.InvariantCulture),
                    x_repetitions_total.ToString(CultureInfo.InvariantCulture),
                    x_outer_cv_index.ToString(CultureInfo.InvariantCulture),
                    x_outer_cv_folds.ToString(CultureInfo.InvariantCulture),
                    x_outer_cv_folds_to_run.ToString(CultureInfo.InvariantCulture),
                    x_svm_type.ToString() ?? "",
                    x_svm_kernel.ToString() ?? "",
                    //grid_point = new grid_point() {cost = x_double[k++], gamma = x_double[k++], epsilon = x_double[k++], coef0 = x_double[k++], degree = x_double[k++],},
                    grid_point.cost?.ToString("G17", NumberFormatInfo.InvariantInfo).Replace(",",";", StringComparison.InvariantCulture) ?? "",
                    grid_point.gamma?.ToString("G17", NumberFormatInfo.InvariantInfo).Replace(",",";", StringComparison.InvariantCulture) ?? "",
                    grid_point.epsilon?.ToString("G17", NumberFormatInfo.InvariantInfo).Replace(",",";", StringComparison.InvariantCulture) ?? "",
                    grid_point.coef0?.ToString("G17", NumberFormatInfo.InvariantInfo).Replace(",",";", StringComparison.InvariantCulture) ?? "",
                    grid_point.degree?.ToString("G17", NumberFormatInfo.InvariantInfo).Replace(",",";", StringComparison.InvariantCulture) ?? "",
                    x_class_id?.ToString(CultureInfo.InvariantCulture),
                    x_class_weight?.ToString("G17", NumberFormatInfo.InvariantInfo).Replace(",",";", StringComparison.InvariantCulture) ?? "",
                    x_class_name ?? "",
                    x_class_size.ToString("G17", NumberFormatInfo.InvariantInfo),
                    x_class_training_size.ToString("G17", NumberFormatInfo.InvariantInfo),
                    x_class_testing_size.ToString("G17", NumberFormatInfo.InvariantInfo),

                    metrics.P.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.N.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.TP.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.FP.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.TN.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.FN.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.TPR.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.TNR.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.PPV.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.Precision.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.Prevalence.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.MCR.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.ER.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.NER.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.CNER.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.Kappa.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.Overlap.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.RND_ACC.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.Support.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.BaseRate.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.YoudenIndex.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.NPV.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.FNR.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.FPR.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.FDR.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.FOR.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.ACC.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.GMean.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.F1S.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.G1S.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.MCC.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.Informedness.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.Markedness.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.BalancedAccuracy.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.ROC_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.ROC_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.ROC_AUC_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.PR_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.PR_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.PRI_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.PRI_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.AP_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.AP_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.API_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.API_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.Brier_Inverse_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.LRP.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.LRN.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.DOR.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.PrevalenceThreshold.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.CriticalSuccessIndex.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.F1B_00.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.F1B_01.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.F1B_02.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.F1B_03.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.F1B_04.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.F1B_05.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.F1B_06.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.F1B_07.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.F1B_08.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.F1B_09.ToString("G17", NumberFormatInfo.InvariantInfo),
                    metrics.F1B_10.ToString("G17", NumberFormatInfo.InvariantInfo),

                    //metrics_ppf?.P.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.N.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.TP.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.FP.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.TN.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.FN.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.TPR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.TNR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.PPV.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.Precision.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.Prevalence.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.MCR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.ER.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.NER.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.CNER.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.Kappa.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.Overlap.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.RND_ACC.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.Support.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.BaseRate.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.YoudenIndex.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.NPV.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.FNR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.FPR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.FDR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.FOR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.ACC.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.GMean.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.F1S.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.G1S.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.MCC.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.Informedness.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.Markedness.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.BalancedAccuracy.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.ROC_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.ROC_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.ROC_AUC_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.PR_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.PR_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.PRI_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.PRI_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.AP_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.AP_11p.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.API_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.API_11p.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.Brier_Inverse_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.LRP.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.LRN.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.DOR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.PrevalenceThreshold.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.CriticalSuccessIndex.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.F1B_00.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.F1B_01.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.F1B_02.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.F1B_03.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.F1B_04.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.F1B_05.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.F1B_06.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.F1B_07.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.F1B_08.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.F1B_09.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppf?.F1B_10.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",

                    //metrics_ppg?.P.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.N.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.TP.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.FP.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.TN.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.FN.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.TPR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.TNR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.PPV.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.Precision.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.Prevalence.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.MCR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.ER.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.NER.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.CNER.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.Kappa.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.Overlap.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.RND_ACC.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.Support.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.BaseRate.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.YoudenIndex.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.NPV.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.FNR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.FPR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.FDR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.FOR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.ACC.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.GMean.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.F1S.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.G1S.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.MCC.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.Informedness.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.Markedness.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.BalancedAccuracy.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.ROC_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.ROC_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.ROC_AUC_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.PR_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.PR_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.PRI_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.PRI_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.AP_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.AP_11p.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.API_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.API_11p.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.Brier_Inverse_All.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.LRP.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.LRN.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.DOR.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.PrevalenceThreshold.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.CriticalSuccessIndex.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.F1B_00.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.F1B_01.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.F1B_02.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.F1B_03.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.F1B_04.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.F1B_05.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.F1B_06.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.F1B_07.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.F1B_08.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.F1B_09.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    //metrics_ppg?.F1B_10.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",



                    roc_xy_str_all ?? "",
                    roc_xy_str_11p ?? "",
                    pr_xy_str_all ?? "",
                    pr_xy_str_11p ?? "",
                    pri_xy_str_all ?? "",
                    pri_xy_str_11p ?? "",
                    string.Join(';',thresholds?.Select(a=> $"{a:G17}").ToArray() ?? Array.Empty<string>()),
                    string.Join(";", predictions?.Select(a=> a?.str() ?? "").ToArray() ?? Array.Empty<string>())
            }.Select(a => a?.Replace(",", ";", StringComparison.InvariantCultureIgnoreCase) ?? "").ToArray();
        }

        public string csv_values()
        {
            return string.Join(",", csv_values_array());
        }
    }
}
