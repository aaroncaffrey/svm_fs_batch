using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace svm_fs_batch
{
    internal static class perf
    {
        internal class prediction
        {
            internal int prediction_index;
            internal int actual_class;
            internal int default_predicted_class;
            internal bool probability_estimates_stated;
            internal List<(int class_id, double probability_estimate)> probability_estimates;
            internal string[] test_row_vector;
            internal List<(string comment_header, string comment_value)> comment;
        }

        internal class confusion_matrix
        {
            internal static List<( string line, confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)> load(string filename, int column_offset = -1)
            {
                var lines = io_proxy.ReadAllLines(filename).ToList();
                var ret = load(lines, column_offset, filename);
                return ret;
            }

        
            internal static List<(
                string line, 
                confusion_matrix cm, 
                List<(string key, string value_str, int? value_int, double? value_double)> key_value_list,
                List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)> 
                load(IList<string> lines, int column_offset = -1, string cm_fn = null)
            {
                
                
                var line_header = lines[0].Split(',').ToList();


                if (column_offset == -1)
                {
                    for (var i = 0; i <= (line_header.Count - csv_header.Count); i++)
                    {
                        if (line_header.Skip(i).SequenceEqual(csv_header))
                        {
                            column_offset = i; 
                            break;
                        }
                    }

                    if (column_offset == -1) { throw new Exception(); }
                }

                var cms = new List<(string line, confusion_matrix cm, List<(string key, string value_str, int? value_int, double? value_double)> key_value_list, List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list)>();

                for (var line_index = 0; line_index < lines.Count; line_index++)// in lines.Where(a => !string.IsNullOrWhiteSpace(a)))
                {
                    var line = lines[line_index];

                    var s_all = line.Split(',').ToList();
                    var key_value_list = line_header.Select((a, i) => (
                        key: line_header[i],
                        value_str: s_all.Count - 1 >= i ? s_all[i] : "", 
                        value_int:int.TryParse(s_all[i],NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var value_int) ? (int?)value_int : (int?)null,
                        value_double:double.TryParse(s_all[i],NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var value_double) ? (double?)value_double : (double?)null
                    )).ToList();

                    var unknown_key_value_list = key_value_list.Where(a => !csv_header.Contains(a.key)).ToList();
                    key_value_list = key_value_list.Where(a => csv_header.Contains(a.key)).ToList();

                    if (line_index == 0 || string.IsNullOrWhiteSpace(line))
                    {
                        cms.Add((line,(confusion_matrix)null, key_value_list, unknown_key_value_list));
                        continue;
                    }
                    
                    var s = s_all.Skip(column_offset).ToList();//line.Split(',').Skip(column_offset).ToList();

                    if (s.Count != csv_header.Count) continue;

                    var x_d = new double[s.Count];
                    var x_i = new int[s.Count];
                    var x_b = new bool[s.Count];

                    var k = 0;// column_offset;

                    var cm = new confusion_matrix()
                    {
                        x_experiment_name = s[k++],
                        x_id = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : (int?)null,

                        x_iteration_index = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : (int?)null,
                        x_group_index = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : (int?)null,
                        x_total_groups = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : (int?)null,
                        x_output_threshold_adjustment_performance = bool.TryParse(s[k++], out x_b[k]) ? x_b[k]: (bool?)null,

                        x_key_alphabet = s[k++],
                        x_key_dimension = s[k++],
                        x_key_category = s[k++],
                        x_key_source = s[k++],
                        x_key_group = s[k++],
                        x_key_member = s[k++],
                        x_key_perspective = s[k++],

                        x_duration_grid_search = s[k++],
                        x_duration_training = s[k++],
                        x_duration_testing = s[k++],
                        x_scale_function = Enum.TryParse(s[k++], out routines.scale_function scale_function) ? scale_function : (routines.scale_function)0,
                        x_libsvm_cv = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        x_prediction_threshold = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : (double?)null,
                        x_prediction_threshold_class = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : (double?)null,
                        x_old_feature_count = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : 0,
                        x_new_feature_count = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : 0,
                        x_old_group_count = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : 0,
                        x_new_group_count = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : 0,
                        x_features_included = s[k++],
                        x_inner_cv_folds = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : 0,
                        x_repetitions_index = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : 0,
                        x_repetitions_total = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : 0,
                        x_outer_cv_index = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : 0,
                        x_outer_cv_folds = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : 0,
                        x_svm_type = Enum.TryParse(s[k++], out routines.libsvm_svm_type svm_type) ? svm_type : (routines.libsvm_svm_type)0,
                        x_svm_kernel = Enum.TryParse(s[k++], out routines.libsvm_kernel_type svm_kernel) ? svm_kernel : (routines.libsvm_kernel_type)0,

                        x_cost = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : (double?)null,
                        x_gamma = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : (double?)null,
                        x_epsilon = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : (double?)null,
                        x_coef0 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : (double?)null,
                        x_degree = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : (double?)null,

                        class_id = int.TryParse(s[k++], NumberStyles.Integer, CultureInfo.InvariantCulture, out x_i[k]) ? x_i[k] : (int?)null,
                        x_class_name = s[k++],
                        x_class_size = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        x_class_training_size = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        x_class_testing_size = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,

                        P = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        N = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        TP = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        FP = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        TN = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        FN = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,

                        TPR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        TNR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPV = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        Precision = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        Prevalence = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        MCR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        ER = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        NER = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        CNER = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        Kappa = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        Overlap = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        RND_ACC = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        Support = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        BaseRate = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        Youden = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        NPV = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        FNR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        FPR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        FDR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        FOR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        ACC = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        GM = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        F1S = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        G1S = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        MCC = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        BM_ = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        MK_ = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        BAC = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        ROC_AUC_Approx_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        ROC_AUC_Approx_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        ROC_AUC_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PR_AUC_Approx_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PR_AUC_Approx_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PRI_AUC_Approx_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PRI_AUC_Approx_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        AP_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        AP_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        API_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        API_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        Brier_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        LRP = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        LRN = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,

                        F1B_00 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        F1B_01 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        F1B_02 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        F1B_03 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        F1B_04 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        F1B_05 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        F1B_06 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        F1B_07 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        F1B_08 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        F1B_09 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        F1B_10 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,

                        PPF_TP = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_FP = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_TN = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_FN = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_TPR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_TNR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_PPV = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_Precision = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_Prevalence = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_MCR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_ER = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_NER = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_CNER = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_Kappa = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_Overlap = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_RND_ACC = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_Support = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_BaseRate = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_Youden = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_NPV = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_FNR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_FPR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_FDR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_FOR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_ACC = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_GM = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_F1S = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_G1S = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_MCC = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_BM_ = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_MK_ = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_BAC = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_ROC_AUC_Approx_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_ROC_AUC_Approx_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_ROC_AUC_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_PR_AUC_Approx_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_PR_AUC_Approx_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_PRI_AUC_Approx_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_PRI_AUC_Approx_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_AP_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_AP_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_API_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_API_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_Brier_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_LRP = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_LRN = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_F1B_00 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_F1B_01 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_F1B_02 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_F1B_03 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_F1B_04 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_F1B_05 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_F1B_06 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_F1B_07 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_F1B_08 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_F1B_09 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPF_F1B_10 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,

                        PPG_TP = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_FP = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_TN = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_FN = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_TPR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_TNR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_PPV = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_Precision = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_Prevalence = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_MCR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_ER = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_NER = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_CNER = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_Kappa = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_Overlap = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_RND_ACC = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_Support = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_BaseRate = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_Youden = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_NPV = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_FNR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_FPR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_FDR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_FOR = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_ACC = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_GM = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_F1S = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_G1S = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_MCC = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_BM_ = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_MK_ = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_BAC = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_ROC_AUC_Approx_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_ROC_AUC_Approx_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_ROC_AUC_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_PR_AUC_Approx_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_PR_AUC_Approx_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_PRI_AUC_Approx_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_PRI_AUC_Approx_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_AP_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_AP_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_API_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_API_11p = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_Brier_All = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_LRP = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_LRN = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_F1B_00 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_F1B_01 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_F1B_02 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_F1B_03 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_F1B_04 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_F1B_05 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_F1B_06 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_F1B_07 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_F1B_08 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_F1B_09 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,
                        PPG_F1B_10 = double.TryParse(s[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out x_d[k]) ? x_d[k] : 0,

                        roc_xy_str_all = s[k++],
                        roc_xy_str_11p = s[k++],
                        pr_xy_str_all = s[k++],
                        pr_xy_str_11p = s[k++],
                        pri_xy_str_all = s[k++],
                        pri_xy_str_11p = s[k++],
                    };

                    cms.Add((line, cm, key_value_list, unknown_key_value_list));
                }

                return cms;
            }

            //internal string duration_nm_search;
            //internal double libsvm_cv_precision;
            //internal double libsvm_cv_recall;
            //internal double libsvm_cv_fscore;
            //internal double libsvm_cv_bac;
            //internal double libsvm_cv_auc;
            //internal double libsvm_cv_accuracy;
            //internal double libsvm_cv_ap;
            //internal string training_resampling_method;
            //internal double training_size_pct;
            //internal double unused_size_pct;
            //internal double testing_size_pct;
            //internal string kernel_parameter_search_method;

            internal int? x_id;

            internal int? x_iteration_index;
            internal int? x_group_index;
            internal int? x_total_groups;
            internal bool? x_output_threshold_adjustment_performance;

            internal string x_key_alphabet;
            internal string x_key_dimension;
            internal string x_key_category;
            internal string x_key_source;
            internal string x_key_group;
            internal string x_key_member;
            internal string x_key_perspective;



            internal string x_duration_grid_search;
            internal string x_duration_training;
            internal string x_duration_testing;
            internal string x_experiment_name;
            internal routines.scale_function x_scale_function;
            internal double x_libsvm_cv;
            internal double? x_prediction_threshold = -1;
            internal double? x_prediction_threshold_class;
            internal int x_old_feature_count;
            internal int x_new_feature_count;
            internal int x_old_group_count;
            internal int x_new_group_count;
            internal string x_features_included;
            internal int x_inner_cv_folds;
            internal int x_repetitions_index;
            internal int x_repetitions_total;
            internal int x_outer_cv_index;
            internal int x_outer_cv_folds;
            internal routines.libsvm_svm_type x_svm_type;
            internal routines.libsvm_kernel_type x_svm_kernel;
            internal double? x_cost;
            internal double? x_gamma;
            internal double? x_epsilon;
            internal double? x_coef0;
            internal double? x_degree;
            internal double? x_class_weight;
            internal string x_class_name;
            internal double x_class_size;
            internal double x_class_training_size;
            internal double x_class_testing_size;

            internal List<double> thresholds;
            internal string roc_xy_str_all;
            internal string roc_xy_str_11p;
            internal string pr_xy_str_all;
            internal string pr_xy_str_11p;
            internal string pri_xy_str_all;
            internal string pri_xy_str_11p;

            internal int? class_id;

            internal double P;
            internal double N;
            internal double TP;
            internal double FP;
            internal double TN;
            internal double FN;
            internal double TPR;
            internal double TNR;
            internal double PPV;
            internal double Precision;
            internal double Prevalence;
            internal double MCR;
            internal double ER;
            internal double NER;
            internal double CNER;
            internal double Kappa;
            internal double Overlap;
            internal double RND_ACC;
            internal double Support;
            internal double BaseRate;
            internal double Youden;
            internal double NPV;
            internal double FNR;
            internal double FPR;
            internal double FDR;
            internal double FOR;
            internal double ACC;
            internal double GM;
            internal double F1S;
            internal double G1S;
            internal double MCC;
            internal double BM_;
            internal double MK_;
            internal double BAC;
            internal double ROC_AUC_Approx_All;
            internal double ROC_AUC_Approx_11p;
            internal double ROC_AUC_All;
            internal double PR_AUC_Approx_All;
            internal double PR_AUC_Approx_11p;
            internal double PRI_AUC_Approx_All;
            internal double PRI_AUC_Approx_11p;
            internal double AP_All;
            internal double AP_11p;
            internal double API_All;
            internal double API_11p;
            internal double Brier_All;
            internal double LRP;
            internal double LRN;
            internal double F1B_00;
            internal double F1B_01;
            internal double F1B_02;
            internal double F1B_03;
            internal double F1B_04;
            internal double F1B_05;
            internal double F1B_06;
            internal double F1B_07;
            internal double F1B_08;
            internal double F1B_09;
            internal double F1B_10;
            internal double PPF_TP;
            internal double PPF_FP;
            internal double PPF_TN;
            internal double PPF_FN;
            internal double PPF_TPR;
            internal double PPF_TNR;
            internal double PPF_PPV;
            internal double PPF_Precision;
            internal double PPF_Prevalence;
            internal double PPF_MCR;
            internal double PPF_ER;
            internal double PPF_NER;
            internal double PPF_CNER;
            internal double PPF_Kappa;
            internal double PPF_Overlap;
            internal double PPF_RND_ACC;
            internal double PPF_Support;
            internal double PPF_BaseRate;
            internal double PPF_Youden;
            internal double PPF_NPV;
            internal double PPF_FNR;
            internal double PPF_FPR;
            internal double PPF_FDR;
            internal double PPF_FOR;
            internal double PPF_ACC;
            internal double PPF_GM;
            internal double PPF_F1S;
            internal double PPF_G1S;
            internal double PPF_MCC;
            internal double PPF_BM_;
            internal double PPF_MK_;
            internal double PPF_BAC;
            internal double PPF_ROC_AUC_Approx_All;
            internal double PPF_ROC_AUC_Approx_11p;
            internal double PPF_ROC_AUC_All;
            internal double PPF_PR_AUC_Approx_All;
            internal double PPF_PR_AUC_Approx_11p;
            internal double PPF_PRI_AUC_Approx_All;
            internal double PPF_PRI_AUC_Approx_11p;
            internal double PPF_AP_All;
            internal double PPF_AP_11p;
            internal double PPF_API_All;
            internal double PPF_API_11p;
            internal double PPF_Brier_All;
            internal double PPF_LRP;
            internal double PPF_LRN;
            internal double PPF_F1B_00;
            internal double PPF_F1B_01;
            internal double PPF_F1B_02;
            internal double PPF_F1B_03;
            internal double PPF_F1B_04;
            internal double PPF_F1B_05;
            internal double PPF_F1B_06;
            internal double PPF_F1B_07;
            internal double PPF_F1B_08;
            internal double PPF_F1B_09;
            internal double PPF_F1B_10;
            internal double PPG_TP;
            internal double PPG_FP;
            internal double PPG_TN;
            internal double PPG_FN;
            internal double PPG_TPR;
            internal double PPG_TNR;
            internal double PPG_PPV;
            internal double PPG_Precision;
            internal double PPG_Prevalence;
            internal double PPG_MCR;
            internal double PPG_ER;
            internal double PPG_NER;
            internal double PPG_CNER;
            internal double PPG_Kappa;
            internal double PPG_Overlap;
            internal double PPG_RND_ACC;
            internal double PPG_Support;
            internal double PPG_BaseRate;
            internal double PPG_Youden;
            internal double PPG_NPV;
            internal double PPG_FNR;
            internal double PPG_FPR;
            internal double PPG_FDR;
            internal double PPG_FOR;
            internal double PPG_ACC;
            internal double PPG_GM;
            internal double PPG_F1S;
            internal double PPG_G1S;
            internal double PPG_MCC;
            internal double PPG_BM_;
            internal double PPG_MK_;
            internal double PPG_BAC;
            internal double PPG_ROC_AUC_Approx_All;
            internal double PPG_ROC_AUC_Approx_11p;
            internal double PPG_ROC_AUC_All;
            internal double PPG_PR_AUC_Approx_All;
            internal double PPG_PR_AUC_Approx_11p;
            internal double PPG_PRI_AUC_Approx_All;
            internal double PPG_PRI_AUC_Approx_11p;
            internal double PPG_AP_All;
            internal double PPG_AP_11p;
            internal double PPG_API_All;
            internal double PPG_API_11p;
            internal double PPG_Brier_All;
            internal double PPG_LRP;
            internal double PPG_LRN;
            internal double PPG_F1B_00;
            internal double PPG_F1B_01;
            internal double PPG_F1B_02;
            internal double PPG_F1B_03;
            internal double PPG_F1B_04;
            internal double PPG_F1B_05;
            internal double PPG_F1B_06;
            internal double PPG_F1B_07;
            internal double PPG_F1B_08;
            internal double PPG_F1B_09;
            internal double PPG_F1B_10;

            internal double get_value_by_name(string name)
            {
                if (string.Equals(name, nameof(TP), StringComparison.InvariantCultureIgnoreCase)) return TP;
                if (string.Equals(name, nameof(FP), StringComparison.InvariantCultureIgnoreCase)) return FP;
                if (string.Equals(name, nameof(TN), StringComparison.InvariantCultureIgnoreCase)) return TN;
                if (string.Equals(name, nameof(FN), StringComparison.InvariantCultureIgnoreCase)) return FN;
                if (string.Equals(name, nameof(TPR), StringComparison.InvariantCultureIgnoreCase)) return TPR;
                if (string.Equals(name, nameof(TNR), StringComparison.InvariantCultureIgnoreCase)) return TNR;
                if (string.Equals(name, nameof(PPV), StringComparison.InvariantCultureIgnoreCase)) return PPV;
                if (string.Equals(name, nameof(Precision), StringComparison.InvariantCultureIgnoreCase)) return Precision;
                if (string.Equals(name, nameof(Prevalence), StringComparison.InvariantCultureIgnoreCase)) return Prevalence;
                if (string.Equals(name, nameof(MCR), StringComparison.InvariantCultureIgnoreCase)) return MCR;
                if (string.Equals(name, nameof(ER), StringComparison.InvariantCultureIgnoreCase)) return ER;
                if (string.Equals(name, nameof(NER), StringComparison.InvariantCultureIgnoreCase)) return NER;
                if (string.Equals(name, nameof(CNER), StringComparison.InvariantCultureIgnoreCase)) return CNER;
                if (string.Equals(name, nameof(Kappa), StringComparison.InvariantCultureIgnoreCase)) return Kappa;
                if (string.Equals(name, nameof(Overlap), StringComparison.InvariantCultureIgnoreCase)) return Overlap;
                if (string.Equals(name, nameof(RND_ACC), StringComparison.InvariantCultureIgnoreCase)) return RND_ACC;
                if (string.Equals(name, nameof(Support), StringComparison.InvariantCultureIgnoreCase)) return Support;
                if (string.Equals(name, nameof(BaseRate), StringComparison.InvariantCultureIgnoreCase)) return BaseRate;
                if (string.Equals(name, nameof(Youden), StringComparison.InvariantCultureIgnoreCase)) return Youden;
                if (string.Equals(name, nameof(NPV), StringComparison.InvariantCultureIgnoreCase)) return NPV;
                if (string.Equals(name, nameof(FNR), StringComparison.InvariantCultureIgnoreCase)) return FNR;
                if (string.Equals(name, nameof(FPR), StringComparison.InvariantCultureIgnoreCase)) return FPR;
                if (string.Equals(name, nameof(FDR), StringComparison.InvariantCultureIgnoreCase)) return FDR;
                if (string.Equals(name, nameof(FOR), StringComparison.InvariantCultureIgnoreCase)) return FOR;
                if (string.Equals(name, nameof(ACC), StringComparison.InvariantCultureIgnoreCase)) return ACC;
                if (string.Equals(name, nameof(GM), StringComparison.InvariantCultureIgnoreCase)) return GM;
                if (string.Equals(name, nameof(F1S), StringComparison.InvariantCultureIgnoreCase)) return F1S;
                if (string.Equals(name, nameof(G1S), StringComparison.InvariantCultureIgnoreCase)) return G1S;
                if (string.Equals(name, nameof(MCC), StringComparison.InvariantCultureIgnoreCase)) return MCC;
                if (string.Equals(name, nameof(BM_), StringComparison.InvariantCultureIgnoreCase)) return BM_;
                if (string.Equals(name, nameof(MK_), StringComparison.InvariantCultureIgnoreCase)) return MK_;
                if (string.Equals(name, nameof(BAC), StringComparison.InvariantCultureIgnoreCase)) return BAC;
                if (string.Equals(name, nameof(ROC_AUC_Approx_All), StringComparison.InvariantCultureIgnoreCase)) return ROC_AUC_Approx_All;
                if (string.Equals(name, nameof(ROC_AUC_Approx_11p), StringComparison.InvariantCultureIgnoreCase)) return ROC_AUC_Approx_11p;
                if (string.Equals(name, nameof(ROC_AUC_All), StringComparison.InvariantCultureIgnoreCase)) return ROC_AUC_All;
                if (string.Equals(name, nameof(PR_AUC_Approx_All), StringComparison.InvariantCultureIgnoreCase)) return PR_AUC_Approx_All;
                if (string.Equals(name, nameof(PR_AUC_Approx_11p), StringComparison.InvariantCultureIgnoreCase)) return PR_AUC_Approx_11p;
                if (string.Equals(name, nameof(PRI_AUC_Approx_All), StringComparison.InvariantCultureIgnoreCase)) return PRI_AUC_Approx_All;
                if (string.Equals(name, nameof(PRI_AUC_Approx_11p), StringComparison.InvariantCultureIgnoreCase)) return PRI_AUC_Approx_11p;
                if (string.Equals(name, nameof(AP_All), StringComparison.InvariantCultureIgnoreCase)) return AP_All;
                if (string.Equals(name, nameof(AP_11p), StringComparison.InvariantCultureIgnoreCase)) return AP_11p;
                if (string.Equals(name, nameof(API_All), StringComparison.InvariantCultureIgnoreCase)) return API_All;
                if (string.Equals(name, nameof(API_11p), StringComparison.InvariantCultureIgnoreCase)) return API_11p;
                if (string.Equals(name, nameof(Brier_All), StringComparison.InvariantCultureIgnoreCase)) return Brier_All;
                if (string.Equals(name, nameof(LRP), StringComparison.InvariantCultureIgnoreCase)) return LRP;
                if (string.Equals(name, nameof(LRN), StringComparison.InvariantCultureIgnoreCase)) return LRN;
                if (string.Equals(name, nameof(F1B_00), StringComparison.InvariantCultureIgnoreCase)) return F1B_00;
                if (string.Equals(name, nameof(F1B_01), StringComparison.InvariantCultureIgnoreCase)) return F1B_01;
                if (string.Equals(name, nameof(F1B_02), StringComparison.InvariantCultureIgnoreCase)) return F1B_02;
                if (string.Equals(name, nameof(F1B_03), StringComparison.InvariantCultureIgnoreCase)) return F1B_03;
                if (string.Equals(name, nameof(F1B_04), StringComparison.InvariantCultureIgnoreCase)) return F1B_04;
                if (string.Equals(name, nameof(F1B_05), StringComparison.InvariantCultureIgnoreCase)) return F1B_05;
                if (string.Equals(name, nameof(F1B_06), StringComparison.InvariantCultureIgnoreCase)) return F1B_06;
                if (string.Equals(name, nameof(F1B_07), StringComparison.InvariantCultureIgnoreCase)) return F1B_07;
                if (string.Equals(name, nameof(F1B_08), StringComparison.InvariantCultureIgnoreCase)) return F1B_08;
                if (string.Equals(name, nameof(F1B_09), StringComparison.InvariantCultureIgnoreCase)) return F1B_09;
                if (string.Equals(name, nameof(F1B_10), StringComparison.InvariantCultureIgnoreCase)) return F1B_10;
                return default;
            }

            internal List<double> get_specific_values(cross_validation_metrics cross_validation_metrics)
            {
                var metric_values = new List<double>();

                if (cross_validation_metrics == 0) throw new Exception();

                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.TP)) { metric_values.Add(this.TP); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.FP)) { metric_values.Add(this.FP); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.TN)) { metric_values.Add(this.TN); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.FN)) { metric_values.Add(this.FN); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.TPR)) { metric_values.Add(this.TPR); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.TNR)) { metric_values.Add(this.TNR); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.PPV)) { metric_values.Add(this.PPV); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.Precision)) { metric_values.Add(this.Precision); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.Prevalence)) { metric_values.Add(this.Prevalence); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.MCR)) { metric_values.Add(this.MCR); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.ER)) { metric_values.Add(this.ER); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.NER)) { metric_values.Add(this.NER); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.CNER)) { metric_values.Add(this.CNER); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.Kappa)) { metric_values.Add(this.Kappa); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.Overlap)) { metric_values.Add(this.Overlap); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.RND_ACC)) { metric_values.Add(this.RND_ACC); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.Support)) { metric_values.Add(this.Support); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.BaseRate)) { metric_values.Add(this.BaseRate); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.Youden)) { metric_values.Add(this.Youden); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.NPV)) { metric_values.Add(this.NPV); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.FNR)) { metric_values.Add(this.FNR); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.FPR)) { metric_values.Add(this.FPR); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.FDR)) { metric_values.Add(this.FDR); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.FOR)) { metric_values.Add(this.FOR); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.ACC)) { metric_values.Add(this.ACC); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.GM)) { metric_values.Add(this.GM); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.F1S)) { metric_values.Add(this.F1S); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.G1S)) { metric_values.Add(this.G1S); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.MCC)) { metric_values.Add(this.MCC); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.BM_)) { metric_values.Add(this.BM_); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.MK_)) { metric_values.Add(this.MK_); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.BAC)) { metric_values.Add(this.BAC); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.ROC_AUC_Approx_All)) { metric_values.Add(this.ROC_AUC_Approx_All); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.ROC_AUC_Approx_11p)) { metric_values.Add(this.ROC_AUC_Approx_11p); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.ROC_AUC_All)) { metric_values.Add(this.ROC_AUC_All); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.PR_AUC_Approx_All)) { metric_values.Add(this.PR_AUC_Approx_All); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.PR_AUC_Approx_11p)) { metric_values.Add(this.PR_AUC_Approx_11p); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.PRI_AUC_Approx_All)) { metric_values.Add(this.PRI_AUC_Approx_All); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.PRI_AUC_Approx_11p)) { metric_values.Add(this.PRI_AUC_Approx_11p); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.AP_All)) { metric_values.Add(this.AP_All); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.AP_11p)) { metric_values.Add(this.AP_11p); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.API_All)) { metric_values.Add(this.API_All); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.API_11p)) { metric_values.Add(this.API_11p); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.Brier_All)) { metric_values.Add(this.Brier_All); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.LRP)) { metric_values.Add(this.LRP); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.LRN)) { metric_values.Add(this.LRN); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.F1B_00)) { metric_values.Add(this.F1B_00); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.F1B_01)) { metric_values.Add(this.F1B_01); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.F1B_02)) { metric_values.Add(this.F1B_02); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.F1B_03)) { metric_values.Add(this.F1B_03); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.F1B_04)) { metric_values.Add(this.F1B_04); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.F1B_05)) { metric_values.Add(this.F1B_05); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.F1B_06)) { metric_values.Add(this.F1B_06); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.F1B_07)) { metric_values.Add(this.F1B_07); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.F1B_08)) { metric_values.Add(this.F1B_08); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.F1B_09)) { metric_values.Add(this.F1B_09); }
                if (cross_validation_metrics.HasFlag(perf.confusion_matrix.cross_validation_metrics.F1B_10)) { metric_values.Add(this.F1B_10); }

                //if (cross_validation_metrics.HasFlag(performance_measure.confusion_matrix.cross_validation_metrics.ROC_AUC2_11p)) { metric_values.Add(this.ROC_AUC_11p); }
                //if (cross_validation_metrics.HasFlag(performance_measure.confusion_matrix.cross_validation_metrics.Brier_11p)) { metric_values.Add(this.Brier_11p); }

                return metric_values;
            }

            internal (string name, double value)[] get_perf_value_strings()
            {
                var result = new (string name, double value)[] {

                    (nameof(TP), TP),
                    (nameof(FP), FP),
                    (nameof(TN), TN),
                    (nameof(FN), FN),
                    (nameof(TPR), TPR),
                    (nameof(TNR), TNR),
                    (nameof(PPV), PPV),
                    (nameof(Precision), Precision),
                    (nameof(Prevalence), Prevalence),
                    (nameof(MCR), MCR),
                    (nameof(ER), ER),
                    (nameof(NER), NER),
                    (nameof(CNER), CNER),
                    (nameof(Kappa), Kappa),
                    (nameof(Overlap), Overlap),
                    (nameof(RND_ACC), RND_ACC),
                    (nameof(Support), Support),
                    (nameof(BaseRate), BaseRate),
                    (nameof(Youden), Youden),
                    (nameof(NPV), NPV),
                    (nameof(FNR), FNR),
                    (nameof(FPR), FPR),
                    (nameof(FDR), FDR),
                    (nameof(FOR), FOR),
                    (nameof(ACC), ACC),
                    (nameof(GM), GM),
                    (nameof(F1S), F1S),
                    (nameof(G1S), G1S),
                    (nameof(MCC), MCC),
                    (nameof(BM_), BM_),
                    (nameof(MK_), MK_),
                    (nameof(BAC), BAC),
                    (nameof(ROC_AUC_Approx_All), ROC_AUC_Approx_All),
                    (nameof(ROC_AUC_Approx_11p), ROC_AUC_Approx_11p),
                    (nameof(ROC_AUC_All), ROC_AUC_All),
                    (nameof(PR_AUC_Approx_All), PR_AUC_Approx_All),
                    (nameof(PR_AUC_Approx_11p), PR_AUC_Approx_11p),
                    (nameof(PRI_AUC_Approx_All), PRI_AUC_Approx_All),
                    (nameof(PRI_AUC_Approx_11p), PRI_AUC_Approx_11p),
                    (nameof(AP_All), AP_All),
                    (nameof(AP_11p), AP_11p),
                    (nameof(API_All), API_All),
                    (nameof(API_11p), API_11p),
                    (nameof(Brier_All), Brier_All),
                    (nameof(LRP), LRP),
                    (nameof(LRN), LRN),
                    (nameof(F1B_00), F1B_00),
                    (nameof(F1B_01), F1B_01),
                    (nameof(F1B_02), F1B_02),
                    (nameof(F1B_03), F1B_03),
                    (nameof(F1B_04), F1B_04),
                    (nameof(F1B_05), F1B_05),
                    (nameof(F1B_06), F1B_06),
                    (nameof(F1B_07), F1B_07),
                    (nameof(F1B_08), F1B_08),
                    (nameof(F1B_09), F1B_09),
                    (nameof(F1B_10), F1B_10),
                };

                return result;
            }

            [Flags]
            internal enum cross_validation_metrics : ulong
            {
                None = 0UL,
                TP = 1UL << 01,
                FP = 1UL << 02,
                TN = 1UL << 03,
                FN = 1UL << 04,
                TPR = 1UL << 05,
                TNR = 1UL << 06,
                PPV = 1UL << 07,
                Precision = 1UL << 08,
                Prevalence = 1UL << 09,
                MCR = 1UL << 10,
                ER = 1UL << 11,
                NER = 1UL << 12,
                CNER = 1UL << 13,
                Kappa = 1UL << 14,
                Overlap = 1UL << 15,
                RND_ACC = 1UL << 16,
                Support = 1UL << 17,
                BaseRate = 1UL << 18,
                Youden = 1UL << 19,
                NPV = 1UL << 20,
                FNR = 1UL << 21,
                FPR = 1UL << 22,
                FDR = 1UL << 23,
                FOR = 1UL << 24,
                ACC = 1UL << 25,
                GM = 1UL << 26,
                F1S = 1UL << 27,
                G1S = 1UL << 28,
                MCC = 1UL << 29,
                BM_ = 1UL << 30,
                MK_ = 1UL << 31,
                BAC = 1UL << 32,
                ROC_AUC_Approx_All = 1UL << 33,
                ROC_AUC_Approx_11p = 1UL << 34,
                ROC_AUC_All = 1UL << 35,
                PR_AUC_Approx_All = 1UL << 36,
                PR_AUC_Approx_11p = 1UL << 37,
                PRI_AUC_Approx_All = 1UL << 38,
                PRI_AUC_Approx_11p = 1UL << 39,
                AP_All = 1UL << 40,
                AP_11p = 1UL << 41,
                API_All = 1UL << 42,
                API_11p = 1UL << 43,
                Brier_All = 1UL << 44,
                LRP = 1UL << 45,
                LRN = 1UL << 46,
                F1B_00 = 1UL << 47,
                F1B_01 = 1UL << 48,
                F1B_02 = 1UL << 49,
                F1B_03 = 1UL << 50,
                F1B_04 = 1UL << 51,
                F1B_05 = 1UL << 52,
                F1B_06 = 1UL << 53,
                F1B_07 = 1UL << 54,
                F1B_08 = 1UL << 55,
                F1B_09 = 1UL << 56,
                F1B_10 = 1UL << 57,

                //ROC_AUC_11p = 1UL << 36,
            }


            internal void calculate_ppf()
            {
                if (x_new_feature_count != 0)
                {
                    PPF_TP = TP / (double)x_new_feature_count;
                    PPF_FP = FP / (double)x_new_feature_count;
                    PPF_TN = TN / (double)x_new_feature_count;
                    PPF_FN = FN / (double)x_new_feature_count;
                    PPF_TPR = TPR / (double)x_new_feature_count;
                    PPF_TNR = TNR / (double)x_new_feature_count;
                    PPF_PPV = PPV / (double)x_new_feature_count;
                    PPF_Precision = Precision / (double)x_new_feature_count;
                    PPF_Prevalence = Prevalence / (double)x_new_feature_count;
                    PPF_MCR = MCR / (double)x_new_feature_count;
                    PPF_ER = ER / (double)x_new_feature_count;
                    PPF_NER = NER / (double)x_new_feature_count;
                    PPF_CNER = CNER / (double)x_new_feature_count;
                    PPF_Kappa = Kappa / (double)x_new_feature_count;
                    PPF_Overlap = Overlap / (double)x_new_feature_count;
                    PPF_RND_ACC = RND_ACC / (double)x_new_feature_count;
                    PPF_Support = Support / (double)x_new_feature_count;
                    PPF_BaseRate = BaseRate / (double)x_new_feature_count;
                    PPF_Youden = Youden / (double)x_new_feature_count;
                    PPF_NPV = NPV / (double)x_new_feature_count;
                    PPF_FNR = FNR / (double)x_new_feature_count;
                    PPF_FPR = FPR / (double)x_new_feature_count;
                    PPF_FDR = FDR / (double)x_new_feature_count;
                    PPF_FOR = FOR / (double)x_new_feature_count;
                    PPF_ACC = ACC / (double)x_new_feature_count;
                    PPF_GM = GM / (double)x_new_feature_count;
                    PPF_F1S = F1S / (double)x_new_feature_count;
                    PPF_G1S = G1S / (double)x_new_feature_count;
                    PPF_MCC = MCC / (double)x_new_feature_count;
                    PPF_BM_ = BM_ / (double)x_new_feature_count;
                    PPF_MK_ = MK_ / (double)x_new_feature_count;
                    PPF_BAC = BAC / (double)x_new_feature_count;
                    PPF_ROC_AUC_Approx_All = ROC_AUC_Approx_All / (double)x_new_feature_count;
                    PPF_ROC_AUC_Approx_11p = ROC_AUC_Approx_11p / (double)x_new_feature_count;
                    PPF_ROC_AUC_All = ROC_AUC_All / (double)x_new_feature_count;
                    PPF_PR_AUC_Approx_All = PR_AUC_Approx_All / (double)x_new_feature_count;
                    PPF_PR_AUC_Approx_11p = PR_AUC_Approx_11p / (double)x_new_feature_count;
                    PPF_PRI_AUC_Approx_All = PRI_AUC_Approx_All / (double)x_new_feature_count;
                    PPF_PRI_AUC_Approx_11p = PRI_AUC_Approx_11p / (double)x_new_feature_count;
                    PPF_AP_All = AP_All / (double)x_new_feature_count;
                    PPF_AP_11p = AP_11p / (double)x_new_feature_count;
                    PPF_API_All = API_All / (double)x_new_feature_count;
                    PPF_API_11p = API_11p / (double)x_new_feature_count;
                    PPF_Brier_All = Brier_All / (double)x_new_feature_count;
                    PPF_LRP = LRP / (double)x_new_feature_count;
                    PPF_LRN = LRN / (double)x_new_feature_count;
                    PPF_F1B_00 = F1B_00 / (double)x_new_feature_count;
                    PPF_F1B_01 = F1B_01 / (double)x_new_feature_count;
                    PPF_F1B_02 = F1B_02 / (double)x_new_feature_count;
                    PPF_F1B_03 = F1B_03 / (double)x_new_feature_count;
                    PPF_F1B_04 = F1B_04 / (double)x_new_feature_count;
                    PPF_F1B_05 = F1B_05 / (double)x_new_feature_count;
                    PPF_F1B_06 = F1B_06 / (double)x_new_feature_count;
                    PPF_F1B_07 = F1B_07 / (double)x_new_feature_count;
                    PPF_F1B_08 = F1B_08 / (double)x_new_feature_count;
                    PPF_F1B_09 = F1B_09 / (double)x_new_feature_count;
                    PPF_F1B_10 = F1B_10 / (double)x_new_feature_count;

                    //PPF_ROC_AUC_11p = ROC_AUC_11p / (double)x_new_feature_count;
                }

                if (x_new_group_count != 0)
                {
                    PPG_TP = TP / (double)x_new_group_count;
                    PPG_FP = FP / (double)x_new_group_count;
                    PPG_TN = TN / (double)x_new_group_count;
                    PPG_FN = FN / (double)x_new_group_count;
                    PPG_TPR = TPR / (double)x_new_group_count;
                    PPG_TNR = TNR / (double)x_new_group_count;
                    PPG_PPV = PPV / (double)x_new_group_count;
                    PPG_Precision = Precision / (double)x_new_group_count;
                    PPG_Prevalence = Prevalence / (double)x_new_group_count;
                    PPG_MCR = MCR / (double)x_new_group_count;
                    PPG_ER = ER / (double)x_new_group_count;
                    PPG_NER = NER / (double)x_new_group_count;
                    PPG_CNER = CNER / (double)x_new_group_count;
                    PPG_Kappa = Kappa / (double)x_new_group_count;
                    PPG_Overlap = Overlap / (double)x_new_group_count;
                    PPG_RND_ACC = RND_ACC / (double)x_new_group_count;
                    PPG_Support = Support / (double)x_new_group_count;
                    PPG_BaseRate = BaseRate / (double)x_new_group_count;
                    PPG_Youden = Youden / (double)x_new_group_count;
                    PPG_NPV = NPV / (double)x_new_group_count;
                    PPG_FNR = FNR / (double)x_new_group_count;
                    PPG_FPR = FPR / (double)x_new_group_count;
                    PPG_FDR = FDR / (double)x_new_group_count;
                    PPG_FOR = FOR / (double)x_new_group_count;
                    PPG_ACC = ACC / (double)x_new_group_count;
                    PPG_GM = GM / (double)x_new_group_count;
                    PPG_F1S = F1S / (double)x_new_group_count;
                    PPG_G1S = G1S / (double)x_new_group_count;
                    PPG_MCC = MCC / (double)x_new_group_count;
                    PPG_BM_ = BM_ / (double)x_new_group_count;
                    PPG_MK_ = MK_ / (double)x_new_group_count;
                    PPG_BAC = BAC / (double)x_new_group_count;
                    PPG_ROC_AUC_Approx_All = ROC_AUC_Approx_All / (double)x_new_group_count;
                    PPG_ROC_AUC_Approx_11p = ROC_AUC_Approx_11p / (double)x_new_group_count;
                    PPG_ROC_AUC_All = ROC_AUC_All / (double)x_new_group_count;
                    PPG_PR_AUC_Approx_All = PR_AUC_Approx_All / (double)x_new_group_count;
                    PPG_PR_AUC_Approx_11p = PR_AUC_Approx_11p / (double)x_new_group_count;
                    PPG_PRI_AUC_Approx_All = PRI_AUC_Approx_All / (double)x_new_group_count;
                    PPG_PRI_AUC_Approx_11p = PRI_AUC_Approx_11p / (double)x_new_group_count;
                    PPG_AP_All = AP_All / (double)x_new_group_count;
                    PPG_AP_11p = AP_11p / (double)x_new_group_count;
                    PPG_API_All = API_All / (double)x_new_group_count;
                    PPG_API_11p = API_11p / (double)x_new_group_count;
                    PPG_Brier_All = Brier_All / (double)x_new_group_count;
                    PPG_LRP = LRP / (double)x_new_group_count;
                    PPG_LRN = LRN / (double)x_new_group_count;
                    PPG_F1B_00 = F1B_00 / (double)x_new_group_count;
                    PPG_F1B_01 = F1B_01 / (double)x_new_group_count;
                    PPG_F1B_02 = F1B_02 / (double)x_new_group_count;
                    PPG_F1B_03 = F1B_03 / (double)x_new_group_count;
                    PPG_F1B_04 = F1B_04 / (double)x_new_group_count;
                    PPG_F1B_05 = F1B_05 / (double)x_new_group_count;
                    PPG_F1B_06 = F1B_06 / (double)x_new_group_count;
                    PPG_F1B_07 = F1B_07 / (double)x_new_group_count;
                    PPG_F1B_08 = F1B_08 / (double)x_new_group_count;
                    PPG_F1B_09 = F1B_09 / (double)x_new_group_count;
                    PPG_F1B_10 = F1B_10 / (double)x_new_group_count;

                    //PPG_ROC_AUC_11p = ROC_AUC_11p / (double)x_new_group_count;
                }

            }

            internal static confusion_matrix calculate_difference_value(confusion_matrix old_cm, confusion_matrix new_cm)
            {
                var group_change = new_cm.x_new_group_count - old_cm.x_old_group_count;
                var column_change = new_cm.x_new_feature_count - old_cm.x_old_feature_count;

                if (column_change == 0) return new confusion_matrix();

                var result = calculate_difference(old_cm, new_cm);

                //result.P = result.P / column_change;
                //result.N = result.N / column_change;

                result.TP = result.TP / column_change;
                result.FP = result.FP / column_change;
                result.TN = result.TN / column_change;
                result.FN = result.FN / column_change;
                
                result.TPR = result.TPR / column_change;
                result.TNR = result.TNR / column_change;
                result.PPV = result.PPV / column_change;
                result.Precision = result.Precision / column_change;
                result.Prevalence = result.Prevalence / column_change;
                result.MCR = result.MCR / column_change;
                result.ER = result.ER / column_change;
                result.NER = result.NER / column_change;
                result.CNER = result.CNER / column_change;
                result.Kappa = result.Kappa / column_change;
                result.Overlap = result.Overlap / column_change;
                result.RND_ACC = result.RND_ACC / column_change;
                result.Support = result.Support / column_change;
                result.BaseRate = result.BaseRate / column_change;
                result.Youden = result.Youden / column_change;
                result.NPV = result.NPV / column_change;
                result.FNR = result.FNR / column_change;
                result.FPR = result.FPR / column_change;
                result.FDR = result.FDR / column_change;
                result.FOR = result.FOR / column_change;
                result.ACC = result.ACC / column_change;
                result.GM = result.GM / column_change;
                result.F1S = result.F1S / column_change;
                result.G1S = result.G1S / column_change;
                result.MCC = result.MCC / column_change;
                result.BM_ = result.BM_ / column_change;
                result.MK_ = result.MK_ / column_change;
                result.BAC = result.BAC / column_change;
                result.ROC_AUC_Approx_All = result.ROC_AUC_Approx_All / column_change;
                result.ROC_AUC_Approx_11p = result.ROC_AUC_Approx_11p / column_change;
                result.ROC_AUC_All = result.ROC_AUC_All / column_change;
                result.PR_AUC_Approx_All = result.PR_AUC_Approx_All / column_change;
                result.PR_AUC_Approx_11p = result.PR_AUC_Approx_11p / column_change;
                result.PRI_AUC_Approx_All = result.PRI_AUC_Approx_All / column_change;
                result.PRI_AUC_Approx_11p = result.PRI_AUC_Approx_11p / column_change;
                result.AP_All = result.AP_All / column_change;
                result.AP_11p = result.AP_11p / column_change;
                result.API_All = result.API_All / column_change;
                result.API_11p = result.API_11p / column_change;
                result.Brier_All = result.Brier_All / column_change;
                result.LRP = result.LRP / column_change;
                result.LRN = result.LRN / column_change;
                result.F1B_00 = result.F1B_00 / column_change;
                result.F1B_01 = result.F1B_01 / column_change;
                result.F1B_02 = result.F1B_02 / column_change;
                result.F1B_03 = result.F1B_03 / column_change;
                result.F1B_04 = result.F1B_04 / column_change;
                result.F1B_05 = result.F1B_05 / column_change;
                result.F1B_06 = result.F1B_06 / column_change;
                result.F1B_07 = result.F1B_07 / column_change;
                result.F1B_08 = result.F1B_08 / column_change;
                result.F1B_09 = result.F1B_09 / column_change;
                result.F1B_10 = result.F1B_10 / column_change;
                result.PPF_TP = result.PPF_TP / column_change;
                result.PPF_FP = result.PPF_FP / column_change;
                result.PPF_TN = result.PPF_TN / column_change;
                result.PPF_FN = result.PPF_FN / column_change;
                result.PPF_TPR = result.PPF_TPR / column_change;
                result.PPF_TNR = result.PPF_TNR / column_change;
                result.PPF_PPV = result.PPF_PPV / column_change;
                result.PPF_Precision = result.PPF_Precision / column_change;
                result.PPF_Prevalence = result.PPF_Prevalence / column_change;
                result.PPF_MCR = result.PPF_MCR / column_change;
                result.PPF_ER = result.PPF_ER / column_change;
                result.PPF_NER = result.PPF_NER / column_change;
                result.PPF_CNER = result.PPF_CNER / column_change;
                result.PPF_Kappa = result.PPF_Kappa / column_change;
                result.PPF_Overlap = result.PPF_Overlap / column_change;
                result.PPF_RND_ACC = result.PPF_RND_ACC / column_change;
                result.PPF_Support = result.PPF_Support / column_change;
                result.PPF_BaseRate = result.PPF_BaseRate / column_change;
                result.PPF_Youden = result.PPF_Youden / column_change;
                result.PPF_NPV = result.PPF_NPV / column_change;
                result.PPF_FNR = result.PPF_FNR / column_change;
                result.PPF_FPR = result.PPF_FPR / column_change;
                result.PPF_FDR = result.PPF_FDR / column_change;
                result.PPF_FOR = result.PPF_FOR / column_change;
                result.PPF_ACC = result.PPF_ACC / column_change;
                result.PPF_GM = result.PPF_GM / column_change;
                result.PPF_F1S = result.PPF_F1S / column_change;
                result.PPF_G1S = result.PPF_G1S / column_change;
                result.PPF_MCC = result.PPF_MCC / column_change;
                result.PPF_BM_ = result.PPF_BM_ / column_change;
                result.PPF_MK_ = result.PPF_MK_ / column_change;
                result.PPF_BAC = result.PPF_BAC / column_change;
                result.PPF_ROC_AUC_Approx_All = result.PPF_ROC_AUC_Approx_All / column_change;
                result.PPF_ROC_AUC_Approx_11p = result.PPF_ROC_AUC_Approx_11p / column_change;
                result.PPF_ROC_AUC_All = result.PPF_ROC_AUC_All / column_change;
                result.PPF_PR_AUC_Approx_All = result.PPF_PR_AUC_Approx_All / column_change;
                result.PPF_PR_AUC_Approx_11p = result.PPF_PR_AUC_Approx_11p / column_change;
                result.PPF_PRI_AUC_Approx_All = result.PPF_PRI_AUC_Approx_All / column_change;
                result.PPF_PRI_AUC_Approx_11p = result.PPF_PRI_AUC_Approx_11p / column_change;
                result.PPF_AP_All = result.PPF_AP_All / column_change;
                result.PPF_AP_11p = result.PPF_AP_11p / column_change;
                result.PPF_API_All = result.PPF_API_All / column_change;
                result.PPF_API_11p = result.PPF_API_11p / column_change;
                result.PPF_Brier_All = result.PPF_Brier_All / column_change;
                result.PPF_LRP = result.PPF_LRP / column_change;
                result.PPF_LRN = result.PPF_LRN / column_change;
                result.PPF_F1B_00 = result.PPF_F1B_00 / column_change;
                result.PPF_F1B_01 = result.PPF_F1B_01 / column_change;
                result.PPF_F1B_02 = result.PPF_F1B_02 / column_change;
                result.PPF_F1B_03 = result.PPF_F1B_03 / column_change;
                result.PPF_F1B_04 = result.PPF_F1B_04 / column_change;
                result.PPF_F1B_05 = result.PPF_F1B_05 / column_change;
                result.PPF_F1B_06 = result.PPF_F1B_06 / column_change;
                result.PPF_F1B_07 = result.PPF_F1B_07 / column_change;
                result.PPF_F1B_08 = result.PPF_F1B_08 / column_change;
                result.PPF_F1B_09 = result.PPF_F1B_09 / column_change;
                result.PPF_F1B_10 = result.PPF_F1B_10 / column_change;
                result.PPG_TP = result.PPG_TP / column_change;
                result.PPG_FP = result.PPG_FP / column_change;
                result.PPG_TN = result.PPG_TN / column_change;
                result.PPG_FN = result.PPG_FN / column_change;
                result.PPG_TPR = result.PPG_TPR / column_change;
                result.PPG_TNR = result.PPG_TNR / column_change;
                result.PPG_PPV = result.PPG_PPV / column_change;
                result.PPG_Precision = result.PPG_Precision / column_change;
                result.PPG_Prevalence = result.PPG_Prevalence / column_change;
                result.PPG_MCR = result.PPG_MCR / column_change;
                result.PPG_ER = result.PPG_ER / column_change;
                result.PPG_NER = result.PPG_NER / column_change;
                result.PPG_CNER = result.PPG_CNER / column_change;
                result.PPG_Kappa = result.PPG_Kappa / column_change;
                result.PPG_Overlap = result.PPG_Overlap / column_change;
                result.PPG_RND_ACC = result.PPG_RND_ACC / column_change;
                result.PPG_Support = result.PPG_Support / column_change;
                result.PPG_BaseRate = result.PPG_BaseRate / column_change;
                result.PPG_Youden = result.PPG_Youden / column_change;
                result.PPG_NPV = result.PPG_NPV / column_change;
                result.PPG_FNR = result.PPG_FNR / column_change;
                result.PPG_FPR = result.PPG_FPR / column_change;
                result.PPG_FDR = result.PPG_FDR / column_change;
                result.PPG_FOR = result.PPG_FOR / column_change;
                result.PPG_ACC = result.PPG_ACC / column_change;
                result.PPG_GM = result.PPG_GM / column_change;
                result.PPG_F1S = result.PPG_F1S / column_change;
                result.PPG_G1S = result.PPG_G1S / column_change;
                result.PPG_MCC = result.PPG_MCC / column_change;
                result.PPG_BM_ = result.PPG_BM_ / column_change;
                result.PPG_MK_ = result.PPG_MK_ / column_change;
                result.PPG_BAC = result.PPG_BAC / column_change;
                result.PPG_ROC_AUC_Approx_All = result.PPG_ROC_AUC_Approx_All / column_change;
                result.PPG_ROC_AUC_Approx_11p = result.PPG_ROC_AUC_Approx_11p / column_change;
                result.PPG_ROC_AUC_All = result.PPG_ROC_AUC_All / column_change;
                result.PPG_PR_AUC_Approx_All = result.PPG_PR_AUC_Approx_All / column_change;
                result.PPG_PR_AUC_Approx_11p = result.PPG_PR_AUC_Approx_11p / column_change;
                result.PPG_PRI_AUC_Approx_All = result.PPG_PRI_AUC_Approx_All / column_change;
                result.PPG_PRI_AUC_Approx_11p = result.PPG_PRI_AUC_Approx_11p / column_change;
                result.PPG_AP_All = result.PPG_AP_All / column_change;
                result.PPG_AP_11p = result.PPG_AP_11p / column_change;
                result.PPG_API_All = result.PPG_API_All / column_change;
                result.PPG_API_11p = result.PPG_API_11p / column_change;
                result.PPG_Brier_All = result.PPG_Brier_All / column_change;
                result.PPG_LRP = result.PPG_LRP / column_change;
                result.PPG_LRN = result.PPG_LRN / column_change;
                result.PPG_F1B_00 = result.PPG_F1B_00 / column_change;
                result.PPG_F1B_01 = result.PPG_F1B_01 / column_change;
                result.PPG_F1B_02 = result.PPG_F1B_02 / column_change;
                result.PPG_F1B_03 = result.PPG_F1B_03 / column_change;
                result.PPG_F1B_04 = result.PPG_F1B_04 / column_change;
                result.PPG_F1B_05 = result.PPG_F1B_05 / column_change;
                result.PPG_F1B_06 = result.PPG_F1B_06 / column_change;
                result.PPG_F1B_07 = result.PPG_F1B_07 / column_change;
                result.PPG_F1B_08 = result.PPG_F1B_08 / column_change;
                result.PPG_F1B_09 = result.PPG_F1B_09 / column_change;
                result.PPG_F1B_10 = result.PPG_F1B_10 / column_change;

                return result;
            }

            internal static confusion_matrix calculate_difference(confusion_matrix old_cm, confusion_matrix new_cm)
            {
                var result = new confusion_matrix();

                //result.P = new_cm.P - old_cm.P;
                //result.N = new_cm.N - old_cm.N;

                result.TP = new_cm.TP - old_cm.TP;
                result.FP = new_cm.FP - old_cm.FP;
                result.TN = new_cm.TN - old_cm.TN;
                result.FN = new_cm.FN - old_cm.FN;

                result.TPR = new_cm.TPR - old_cm.TPR;
                result.TNR = new_cm.TNR - old_cm.TNR;
                result.PPV = new_cm.PPV - old_cm.PPV;
                result.Precision = new_cm.Precision - old_cm.Precision;
                result.Prevalence = new_cm.Prevalence - old_cm.Prevalence;
                result.MCR = new_cm.MCR - old_cm.MCR;
                result.ER = new_cm.ER - old_cm.ER;
                result.NER = new_cm.NER - old_cm.NER;
                result.CNER = new_cm.CNER - old_cm.CNER;
                result.Kappa = new_cm.Kappa - old_cm.Kappa;
                result.Overlap = new_cm.Overlap - old_cm.Overlap;
                result.RND_ACC = new_cm.RND_ACC - old_cm.RND_ACC;
                result.Support = new_cm.Support - old_cm.Support;
                result.BaseRate = new_cm.BaseRate - old_cm.BaseRate;
                result.Youden = new_cm.Youden - old_cm.Youden;
                result.NPV = new_cm.NPV - old_cm.NPV;
                result.FNR = new_cm.FNR - old_cm.FNR;
                result.FPR = new_cm.FPR - old_cm.FPR;
                result.FDR = new_cm.FDR - old_cm.FDR;
                result.FOR = new_cm.FOR - old_cm.FOR;
                result.ACC = new_cm.ACC - old_cm.ACC;
                result.GM = new_cm.GM - old_cm.GM;
                result.F1S = new_cm.F1S - old_cm.F1S;
                result.G1S = new_cm.G1S - old_cm.G1S;
                result.MCC = new_cm.MCC - old_cm.MCC;
                result.BM_ = new_cm.BM_ - old_cm.BM_;
                result.MK_ = new_cm.MK_ - old_cm.MK_;
                result.BAC = new_cm.BAC - old_cm.BAC;
                result.ROC_AUC_Approx_All = new_cm.ROC_AUC_Approx_All - old_cm.ROC_AUC_Approx_All;
                result.ROC_AUC_Approx_11p = new_cm.ROC_AUC_Approx_11p - old_cm.ROC_AUC_Approx_11p;
                result.ROC_AUC_All = new_cm.ROC_AUC_All - old_cm.ROC_AUC_All;
                result.PR_AUC_Approx_All = new_cm.PR_AUC_Approx_All - old_cm.PR_AUC_Approx_All;
                result.PR_AUC_Approx_11p = new_cm.PR_AUC_Approx_11p - old_cm.PR_AUC_Approx_11p;
                result.PRI_AUC_Approx_All = new_cm.PRI_AUC_Approx_All - old_cm.PRI_AUC_Approx_All;
                result.PRI_AUC_Approx_11p = new_cm.PRI_AUC_Approx_11p - old_cm.PRI_AUC_Approx_11p;
                result.AP_All = new_cm.AP_All - old_cm.AP_All;
                result.AP_11p = new_cm.AP_11p - old_cm.AP_11p;
                result.API_All = new_cm.API_All - old_cm.API_All;
                result.API_11p = new_cm.API_11p - old_cm.API_11p;
                result.Brier_All = new_cm.Brier_All - old_cm.Brier_All;
                result.LRP = new_cm.LRP - old_cm.LRP;
                result.LRN = new_cm.LRN - old_cm.LRN;
                result.F1B_00 = new_cm.F1B_00 - old_cm.F1B_00;
                result.F1B_01 = new_cm.F1B_01 - old_cm.F1B_01;
                result.F1B_02 = new_cm.F1B_02 - old_cm.F1B_02;
                result.F1B_03 = new_cm.F1B_03 - old_cm.F1B_03;
                result.F1B_04 = new_cm.F1B_04 - old_cm.F1B_04;
                result.F1B_05 = new_cm.F1B_05 - old_cm.F1B_05;
                result.F1B_06 = new_cm.F1B_06 - old_cm.F1B_06;
                result.F1B_07 = new_cm.F1B_07 - old_cm.F1B_07;
                result.F1B_08 = new_cm.F1B_08 - old_cm.F1B_08;
                result.F1B_09 = new_cm.F1B_09 - old_cm.F1B_09;
                result.F1B_10 = new_cm.F1B_10 - old_cm.F1B_10;
                result.PPF_TP = new_cm.PPF_TP - old_cm.PPF_TP;
                result.PPF_FP = new_cm.PPF_FP - old_cm.PPF_FP;
                result.PPF_TN = new_cm.PPF_TN - old_cm.PPF_TN;
                result.PPF_FN = new_cm.PPF_FN - old_cm.PPF_FN;
                result.PPF_TPR = new_cm.PPF_TPR - old_cm.PPF_TPR;
                result.PPF_TNR = new_cm.PPF_TNR - old_cm.PPF_TNR;
                result.PPF_PPV = new_cm.PPF_PPV - old_cm.PPF_PPV;
                result.PPF_Precision = new_cm.PPF_Precision - old_cm.PPF_Precision;
                result.PPF_Prevalence = new_cm.PPF_Prevalence - old_cm.PPF_Prevalence;
                result.PPF_MCR = new_cm.PPF_MCR - old_cm.PPF_MCR;
                result.PPF_ER = new_cm.PPF_ER - old_cm.PPF_ER;
                result.PPF_NER = new_cm.PPF_NER - old_cm.PPF_NER;
                result.PPF_CNER = new_cm.PPF_CNER - old_cm.PPF_CNER;
                result.PPF_Kappa = new_cm.PPF_Kappa - old_cm.PPF_Kappa;
                result.PPF_Overlap = new_cm.PPF_Overlap - old_cm.PPF_Overlap;
                result.PPF_RND_ACC = new_cm.PPF_RND_ACC - old_cm.PPF_RND_ACC;
                result.PPF_Support = new_cm.PPF_Support - old_cm.PPF_Support;
                result.PPF_BaseRate = new_cm.PPF_BaseRate - old_cm.PPF_BaseRate;
                result.PPF_Youden = new_cm.PPF_Youden - old_cm.PPF_Youden;
                result.PPF_NPV = new_cm.PPF_NPV - old_cm.PPF_NPV;
                result.PPF_FNR = new_cm.PPF_FNR - old_cm.PPF_FNR;
                result.PPF_FPR = new_cm.PPF_FPR - old_cm.PPF_FPR;
                result.PPF_FDR = new_cm.PPF_FDR - old_cm.PPF_FDR;
                result.PPF_FOR = new_cm.PPF_FOR - old_cm.PPF_FOR;
                result.PPF_ACC = new_cm.PPF_ACC - old_cm.PPF_ACC;
                result.PPF_GM = new_cm.PPF_GM - old_cm.PPF_GM;
                result.PPF_F1S = new_cm.PPF_F1S - old_cm.PPF_F1S;
                result.PPF_G1S = new_cm.PPF_G1S - old_cm.PPF_G1S;
                result.PPF_MCC = new_cm.PPF_MCC - old_cm.PPF_MCC;
                result.PPF_BM_ = new_cm.PPF_BM_ - old_cm.PPF_BM_;
                result.PPF_MK_ = new_cm.PPF_MK_ - old_cm.PPF_MK_;
                result.PPF_BAC = new_cm.PPF_BAC - old_cm.PPF_BAC;
                result.PPF_ROC_AUC_Approx_All = new_cm.PPF_ROC_AUC_Approx_All - old_cm.PPF_ROC_AUC_Approx_All;
                result.PPF_ROC_AUC_Approx_11p = new_cm.PPF_ROC_AUC_Approx_11p - old_cm.PPF_ROC_AUC_Approx_11p;
                result.PPF_ROC_AUC_All = new_cm.PPF_ROC_AUC_All - old_cm.PPF_ROC_AUC_All;
                result.PPF_PR_AUC_Approx_All = new_cm.PPF_PR_AUC_Approx_All - old_cm.PPF_PR_AUC_Approx_All;
                result.PPF_PR_AUC_Approx_11p = new_cm.PPF_PR_AUC_Approx_11p - old_cm.PPF_PR_AUC_Approx_11p;
                result.PPF_PRI_AUC_Approx_All = new_cm.PPF_PRI_AUC_Approx_All - old_cm.PPF_PRI_AUC_Approx_All;
                result.PPF_PRI_AUC_Approx_11p = new_cm.PPF_PRI_AUC_Approx_11p - old_cm.PPF_PRI_AUC_Approx_11p;
                result.PPF_AP_All = new_cm.PPF_AP_All - old_cm.PPF_AP_All;
                result.PPF_AP_11p = new_cm.PPF_AP_11p - old_cm.PPF_AP_11p;
                result.PPF_API_All = new_cm.PPF_API_All - old_cm.PPF_API_All;
                result.PPF_API_11p = new_cm.PPF_API_11p - old_cm.PPF_API_11p;
                result.PPF_Brier_All = new_cm.PPF_Brier_All - old_cm.PPF_Brier_All;
                result.PPF_LRP = new_cm.PPF_LRP - old_cm.PPF_LRP;
                result.PPF_LRN = new_cm.PPF_LRN - old_cm.PPF_LRN;
                result.PPF_F1B_00 = new_cm.PPF_F1B_00 - old_cm.PPF_F1B_00;
                result.PPF_F1B_01 = new_cm.PPF_F1B_01 - old_cm.PPF_F1B_01;
                result.PPF_F1B_02 = new_cm.PPF_F1B_02 - old_cm.PPF_F1B_02;
                result.PPF_F1B_03 = new_cm.PPF_F1B_03 - old_cm.PPF_F1B_03;
                result.PPF_F1B_04 = new_cm.PPF_F1B_04 - old_cm.PPF_F1B_04;
                result.PPF_F1B_05 = new_cm.PPF_F1B_05 - old_cm.PPF_F1B_05;
                result.PPF_F1B_06 = new_cm.PPF_F1B_06 - old_cm.PPF_F1B_06;
                result.PPF_F1B_07 = new_cm.PPF_F1B_07 - old_cm.PPF_F1B_07;
                result.PPF_F1B_08 = new_cm.PPF_F1B_08 - old_cm.PPF_F1B_08;
                result.PPF_F1B_09 = new_cm.PPF_F1B_09 - old_cm.PPF_F1B_09;
                result.PPF_F1B_10 = new_cm.PPF_F1B_10 - old_cm.PPF_F1B_10;
                result.PPG_TP = new_cm.PPG_TP - old_cm.PPG_TP;
                result.PPG_FP = new_cm.PPG_FP - old_cm.PPG_FP;
                result.PPG_TN = new_cm.PPG_TN - old_cm.PPG_TN;
                result.PPG_FN = new_cm.PPG_FN - old_cm.PPG_FN;
                result.PPG_TPR = new_cm.PPG_TPR - old_cm.PPG_TPR;
                result.PPG_TNR = new_cm.PPG_TNR - old_cm.PPG_TNR;
                result.PPG_PPV = new_cm.PPG_PPV - old_cm.PPG_PPV;
                result.PPG_Precision = new_cm.PPG_Precision - old_cm.PPG_Precision;
                result.PPG_Prevalence = new_cm.PPG_Prevalence - old_cm.PPG_Prevalence;
                result.PPG_MCR = new_cm.PPG_MCR - old_cm.PPG_MCR;
                result.PPG_ER = new_cm.PPG_ER - old_cm.PPG_ER;
                result.PPG_NER = new_cm.PPG_NER - old_cm.PPG_NER;
                result.PPG_CNER = new_cm.PPG_CNER - old_cm.PPG_CNER;
                result.PPG_Kappa = new_cm.PPG_Kappa - old_cm.PPG_Kappa;
                result.PPG_Overlap = new_cm.PPG_Overlap - old_cm.PPG_Overlap;
                result.PPG_RND_ACC = new_cm.PPG_RND_ACC - old_cm.PPG_RND_ACC;
                result.PPG_Support = new_cm.PPG_Support - old_cm.PPG_Support;
                result.PPG_BaseRate = new_cm.PPG_BaseRate - old_cm.PPG_BaseRate;
                result.PPG_Youden = new_cm.PPG_Youden - old_cm.PPG_Youden;
                result.PPG_NPV = new_cm.PPG_NPV - old_cm.PPG_NPV;
                result.PPG_FNR = new_cm.PPG_FNR - old_cm.PPG_FNR;
                result.PPG_FPR = new_cm.PPG_FPR - old_cm.PPG_FPR;
                result.PPG_FDR = new_cm.PPG_FDR - old_cm.PPG_FDR;
                result.PPG_FOR = new_cm.PPG_FOR - old_cm.PPG_FOR;
                result.PPG_ACC = new_cm.PPG_ACC - old_cm.PPG_ACC;
                result.PPG_GM = new_cm.PPG_GM - old_cm.PPG_GM;
                result.PPG_F1S = new_cm.PPG_F1S - old_cm.PPG_F1S;
                result.PPG_G1S = new_cm.PPG_G1S - old_cm.PPG_G1S;
                result.PPG_MCC = new_cm.PPG_MCC - old_cm.PPG_MCC;
                result.PPG_BM_ = new_cm.PPG_BM_ - old_cm.PPG_BM_;
                result.PPG_MK_ = new_cm.PPG_MK_ - old_cm.PPG_MK_;
                result.PPG_BAC = new_cm.PPG_BAC - old_cm.PPG_BAC;
                result.PPG_ROC_AUC_Approx_All = new_cm.PPG_ROC_AUC_Approx_All - old_cm.PPG_ROC_AUC_Approx_All;
                result.PPG_ROC_AUC_Approx_11p = new_cm.PPG_ROC_AUC_Approx_11p - old_cm.PPG_ROC_AUC_Approx_11p;
                result.PPG_ROC_AUC_All = new_cm.PPG_ROC_AUC_All - old_cm.PPG_ROC_AUC_All;
                result.PPG_PR_AUC_Approx_All = new_cm.PPG_PR_AUC_Approx_All - old_cm.PPG_PR_AUC_Approx_All;
                result.PPG_PR_AUC_Approx_11p = new_cm.PPG_PR_AUC_Approx_11p - old_cm.PPG_PR_AUC_Approx_11p;
                result.PPG_PRI_AUC_Approx_All = new_cm.PPG_PRI_AUC_Approx_All - old_cm.PPG_PRI_AUC_Approx_All;
                result.PPG_PRI_AUC_Approx_11p = new_cm.PPG_PRI_AUC_Approx_11p - old_cm.PPG_PRI_AUC_Approx_11p;
                result.PPG_AP_All = new_cm.PPG_AP_All - old_cm.PPG_AP_All;
                result.PPG_AP_11p = new_cm.PPG_AP_11p - old_cm.PPG_AP_11p;
                result.PPG_API_All = new_cm.PPG_API_All - old_cm.PPG_API_All;
                result.PPG_API_11p = new_cm.PPG_API_11p - old_cm.PPG_API_11p;
                result.PPG_Brier_All = new_cm.PPG_Brier_All - old_cm.PPG_Brier_All;
                result.PPG_LRP = new_cm.PPG_LRP - old_cm.PPG_LRP;
                result.PPG_LRN = new_cm.PPG_LRN - old_cm.PPG_LRN;
                result.PPG_F1B_00 = new_cm.PPG_F1B_00 - old_cm.PPG_F1B_00;
                result.PPG_F1B_01 = new_cm.PPG_F1B_01 - old_cm.PPG_F1B_01;
                result.PPG_F1B_02 = new_cm.PPG_F1B_02 - old_cm.PPG_F1B_02;
                result.PPG_F1B_03 = new_cm.PPG_F1B_03 - old_cm.PPG_F1B_03;
                result.PPG_F1B_04 = new_cm.PPG_F1B_04 - old_cm.PPG_F1B_04;
                result.PPG_F1B_05 = new_cm.PPG_F1B_05 - old_cm.PPG_F1B_05;
                result.PPG_F1B_06 = new_cm.PPG_F1B_06 - old_cm.PPG_F1B_06;
                result.PPG_F1B_07 = new_cm.PPG_F1B_07 - old_cm.PPG_F1B_07;
                result.PPG_F1B_08 = new_cm.PPG_F1B_08 - old_cm.PPG_F1B_08;
                result.PPG_F1B_09 = new_cm.PPG_F1B_09 - old_cm.PPG_F1B_09;
                result.PPG_F1B_10 = new_cm.PPG_F1B_10 - old_cm.PPG_F1B_10;


                return result;
            }

            internal void calculate_metrics(bool calculate_auc, List<prediction> prediction_list)
            {
                //var cm = this;

                if (calculate_auc && prediction_list != null && prediction_list.Count > 0 && prediction_list.Any(a => a.probability_estimates_stated))
                {

                    var (p_roc_auc_approx_all, p_roc_auc_actual_all, p_pr_auc_approx_all, p_pri_auc_approx_all, p_ap_all, p_api_all, p_roc_xy_all, p_pr_xy_all, p_pri_xy_all) = perf.Calculate_ROC_PR_AUC(prediction_list, class_id.Value, threshold_type.all_thresholds);
                    var (p_roc_auc_approx_11p, p_roc_auc_actual_11p, p_pr_auc_approx_11p, p_pri_auc_approx_11p, p_ap_11p, p_api_11p, p_roc_xy_11p, p_pr_xy_11p, p_pri_xy_11p) = perf.Calculate_ROC_PR_AUC(prediction_list, class_id.Value, threshold_type.eleven_points);

                    var p_brier_score_all = brier(prediction_list, class_id.Value);
                    //var (p_brier_score_all, p_roc_auc_all, p_roc_auc2_all, p_pr_auc_all, p_pri_auc_all, p_ap_all, p_api_all, p_roc_xy_all, p_pr_xy_all, p_pri_xy_all) = performance_measure.Calculate_ROC_PR_AUC(prediction_list, class_id.Value, false);
                    //var (p_brier_score_11p, p_roc_auc_11p, p_roc_auc2_11p, p_pr_auc_11p, p_pri_auc_11p, p_ap_11p, p_api_11p, p_roc_xy_11p, p_pr_xy_11p, p_pri_xy_11p) = performance_measure.Calculate_ROC_PR_AUC(prediction_list, class_id.Value, true);

                    Brier_All = p_brier_score_all;
                    //Brier_11p = p_brier_score_11p;

                    ROC_AUC_Approx_All = p_roc_auc_approx_all;
                    ROC_AUC_Approx_11p = p_roc_auc_approx_11p;

                    ROC_AUC_All = p_roc_auc_actual_all;
                    //ROC_AUC_11p = p_roc_auc_actual_11p;

                    PR_AUC_Approx_All = p_pr_auc_approx_all;
                    PR_AUC_Approx_11p = p_pr_auc_approx_11p;

                    PRI_AUC_Approx_All = p_pri_auc_approx_all;
                    PRI_AUC_Approx_11p = p_pri_auc_approx_11p;

                    AP_All = p_ap_all;
                    AP_11p = p_ap_11p;
                    API_All = p_api_all;
                    API_11p = p_api_11p;

                    // PR (x: a.TPR, y: a.PPV)
                    // ROC (x: a.FPR, y: a.TPR)

                    roc_xy_str_all = p_roc_xy_all != null ? $"FPR;TPR/{string.Join("/", p_roc_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";
                    roc_xy_str_11p = p_roc_xy_11p != null ? $"FPR;TPR/{string.Join("/", p_roc_xy_11p.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";

                    pr_xy_str_all = p_pr_xy_all != null ? $"TPR;PPV/{string.Join("/", p_pr_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";
                    pr_xy_str_11p = p_pr_xy_11p != null ? $"TPR;PPV/{string.Join("/", p_pr_xy_11p.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";

                    pri_xy_str_all = p_pri_xy_all != null ? $"TPR;PPV/{string.Join("/", p_pri_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";
                    pri_xy_str_11p = p_pri_xy_11p != null ? $"TPR;PPV/{string.Join("/", p_pri_xy_11p.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToList())}" : "";


                }

                Support = (N) == (double)0.0 ? (double)0.0 : (double)(TP + FP) / (double)(N);
                BaseRate = (N) == (double)0.0 ? (double)0.0 : (double)(TP + FN) / (double)(N);

                Prevalence = (P + N) == (double)0.0 ? (double)0.0 : (FN + TP) / (P + N);
                MCR = (P + N) == (double)0.0 ? (double)0.0 : (FP + FN) / (P + N);
                TPR = (TP + FN) == (double)0.0 ? (double)0.0 : (double)(TP) / (double)(TP + FN);
                TNR = (TN + FP) == (double)0.0 ? (double)0.0 : (double)(TN) / (double)(TN + FP);
                Precision = (TP + FP) == (double)0.0 ? (double)0.0 : (double)(TP) / (double)(TP + FP);

                Overlap = (TP + FP + FN) == (double)0.0 ? (double)0.0 : TP / (TP + FP + FN);


                NER = (P + N) == (double)0.0 ? (double)0.0 : (P > N ? P : N) / (P + N);
                CNER = (P + N) == (double)0.0 ? (double)0.0 : P / (P + N);


                PPV = (TPR * Prevalence + (1.0 - TNR) * (1.0 - Prevalence)) == (double)0.0 ? (double)0.0 : (TPR * Prevalence) / (TPR * Prevalence + (1.0 - TNR) * (1.0 - Prevalence));

                NPV = (TN + FN) == (double)0.0 ? (double)0.0 : (double)(TN) / (double)(TN + FN);
                FNR = (FN + TP) == (double)0.0 ? (double)0.0 : (double)(FN) / (double)(FN + TP);
                FPR = (FP + TN) == (double)0.0 ? (double)0.0 : (double)(FP) / (double)(FP + TN);
                FDR = (FP + TP) == (double)0.0 ? (double)0.0 : (double)(FP) / (double)(FP + TP);
                FOR = (FN + TN) == (double)0.0 ? (double)0.0 : (double)(FN) / (double)(FN + TN);

                ACC = (P + N) == (double)0.0 ? (double)0.0 : (double)(TP + TN) / (double)(P + N);

                ER = 1.0 - ACC;
                Youden = TPR + TNR - 1.0;


                //Kappa = (totalAccuracy - randomAccuracy) / (1 - randomAccuracy)
                //totalAccuracy = (TP + TN) / (TP + TN + FP + FN)
                //randomAccuracy = referenceLikelihood(F) * resultLikelihood(F) + referenceLikelihood(T) * resultLikelihood(T)
                //randomAccuracy = (ActualFalse * PredictedFalse + ActualTrue * PredictedTrue) / Total * Total
                //randomAccuracy = (TN + FP) * (TN + FN) + (FN + TP) * (FP + TP) / Total * Total

                RND_ACC = ((TN + FP) * (TN + FN) + (FN + TP) * (FP + TP)) / ((TP + TN + FP + FN) * (TP + TN + FP + FN));

                Kappa = (1 - RND_ACC) == (double)0.0 ? (double)0.0 : (ACC - RND_ACC) / (1 - RND_ACC);

                GM = Math.Sqrt(TPR * TNR);
                F1S = (PPV + TPR) == (double)0.0 ? (double)0.0 : (double)(2 * PPV * TPR) / (double)(PPV + TPR);
                G1S = (PPV + TPR) == (double)0.0 ? (double)0.0 : (double)Math.Sqrt((double)(PPV * TPR));
                MCC = ((double)Math.Sqrt((double)((TP + FP) * (TP + FN) * (TN + FP) * (TN + FN))) == (double)0.0) ? (double)0.0 : ((TP * TN) - (FP * FN)) / (double)Math.Sqrt((double)((TP + FP) * (TP + FN) * (TN + FP) * (TN + FN)));
                BM_ = (TPR + TNR) - (double)1.0;
                MK_ = (PPV + NPV) - (double)1.0;
                BAC = (double)(TPR + TNR) / (double)2.0;

                LRP = (1 - TNR) == (double)0.0 ? (double)0.0 : (TPR) / (1.0 - TNR);
                LRN = (TNR) == (double)0.0 ? (double)0.0 : (1.0 - TPR) / (TNR);


                F1B_00 = fbeta2(PPV, TPR, (double)0.0);
                F1B_01 = fbeta2(PPV, TPR, (double)0.1);
                F1B_02 = fbeta2(PPV, TPR, (double)0.2);
                F1B_03 = fbeta2(PPV, TPR, (double)0.3);
                F1B_04 = fbeta2(PPV, TPR, (double)0.4);
                F1B_05 = fbeta2(PPV, TPR, (double)0.5);
                F1B_06 = fbeta2(PPV, TPR, (double)0.6);
                F1B_07 = fbeta2(PPV, TPR, (double)0.7);
                F1B_08 = fbeta2(PPV, TPR, (double)0.8);
                F1B_09 = fbeta2(PPV, TPR, (double)0.9);
                F1B_10 = fbeta2(PPV, TPR, (double)1.0);

                calculate_ppf();
            }

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
                    Youden = cm.Select(a => a.Youden).Average(),


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
                    BM_ = cm.Select(a => a.BM_).Average(),
                    MK_ = cm.Select(a => a.MK_).Average(),
                    BAC = cm.Select(a => a.BAC).Average(),
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
                                Youden = cm.Select(a => a.Youden).Average(),


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
                                BM_ = cm.Select(a => a.BM_).Average(),
                                MK_ = cm.Select(a => a.MK_).Average(),
                                BAC = cm.Select(a => a.BAC).Average(),
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

            internal static List<string> csv_header = new List<string>()
            {
                nameof(x_experiment_name),


                nameof(x_id),

                nameof(x_iteration_index),
                nameof(x_group_index),
                nameof(x_total_groups),
                nameof(x_output_threshold_adjustment_performance),

                nameof(x_key_alphabet),
                nameof(x_key_dimension),
                nameof(x_key_category),
                nameof(x_key_source),
                nameof(x_key_group),
                nameof(x_key_member),
                nameof(x_key_perspective),


            //nameof(experiment_id1),
            //nameof(experiment_id2),
            //nameof(experiment_id3),

            ////nameof(fs_starting_point),
            //nameof(feature_selection_type),
            ////nameof(fs_algorithm_direction),
            //nameof(fs_perf_selection),
            //nameof(duration_nm_search),
            //nameof(math_operation),

            nameof(x_duration_grid_search),
                
                nameof(x_duration_training),
                nameof(x_duration_testing),
                
                nameof(x_scale_function),

                nameof(x_libsvm_cv),
                //nameof(libsvm_cv_accuracy),
                //nameof(libsvm_cv_bac),
                //nameof(libsvm_cv_recall),
                //nameof(libsvm_cv_fscore),
                //nameof(libsvm_cv_precision),
                //nameof(libsvm_cv_auc),
                //nameof(libsvm_cv_ap),

                nameof(x_prediction_threshold),
                nameof(x_prediction_threshold_class),
                nameof(x_old_feature_count),
                nameof(x_new_feature_count),
                nameof(x_old_group_count),
                nameof(x_new_group_count),
                nameof(x_features_included),
                //nameof(training_resampling_method),
                //nameof(training_size_pct),
                //nameof(unused_size_pct),
                //nameof(testing_size_pct),
                nameof(x_inner_cv_folds),
                nameof(x_repetitions_index),
                nameof(x_repetitions_total),
                nameof(x_outer_cv_index),
                nameof(x_outer_cv_folds),
                nameof(x_svm_type),
                nameof(x_svm_kernel),
                //nameof(kernel_parameter_search_method),
                nameof(x_cost),
                nameof(x_gamma),
                nameof(x_epsilon),
                nameof(x_coef0),
                nameof(x_degree),

                nameof(class_id),
                nameof(x_class_name),
                nameof(x_class_size),
                nameof(x_class_training_size),
                nameof(x_class_testing_size),
                nameof(P),
                nameof(N),
                nameof(TP),
                nameof(FP),
                nameof(TN),
                nameof(FN),
                nameof(TPR),
                nameof(TNR),
                nameof(PPV),
                nameof(Precision),
                nameof(Prevalence),
                nameof(MCR),
                nameof(ER),
                nameof(NER),
                nameof(CNER),
                nameof(Kappa),
                nameof(Overlap),
                nameof(RND_ACC),
                nameof(Support),
                nameof(BaseRate),
                nameof(Youden),
                nameof(NPV),
                nameof(FNR),
                nameof(FPR),
                nameof(FDR),
                nameof(FOR),
                nameof(ACC),
                nameof(GM),
                nameof(F1S),
                nameof(G1S),
                nameof(MCC),
                nameof(BM_),
                nameof(MK_),
                nameof(BAC),
                nameof(ROC_AUC_Approx_All),
                nameof(ROC_AUC_Approx_11p),
                nameof(ROC_AUC_All),
                //nameof(ROC_AUC_11p),
                nameof(PR_AUC_Approx_All),
                nameof(PR_AUC_Approx_11p),
                nameof(PRI_AUC_Approx_All),
                nameof(PRI_AUC_Approx_11p),
                nameof(AP_All),
                nameof(AP_11p),
                nameof(API_All),
                nameof(API_11p),
                nameof(Brier_All),
                nameof(LRP),
                nameof(LRN),
                nameof(F1B_00),
                nameof(F1B_01),
                nameof(F1B_02),
                nameof(F1B_03),
                nameof(F1B_04),
                nameof(F1B_05),
                nameof(F1B_06),
                nameof(F1B_07),
                nameof(F1B_08),
                nameof(F1B_09),
                nameof(F1B_10),

                nameof(PPF_TP),
                nameof(PPF_FP),
                nameof(PPF_TN),
                nameof(PPF_FN),
                nameof(PPF_TPR),
                nameof(PPF_TNR),
                nameof(PPF_PPV),
                nameof(PPF_Precision),
                nameof(PPF_Prevalence),
                nameof(PPF_MCR),
                nameof(PPF_ER),
                nameof(PPF_NER),
                nameof(PPF_CNER),
                nameof(PPF_Kappa),
                nameof(PPF_Overlap),
                nameof(PPF_RND_ACC),
                nameof(PPF_Support),
                nameof(PPF_BaseRate),
                nameof(PPF_Youden),
                nameof(PPF_NPV),
                nameof(PPF_FNR),
                nameof(PPF_FPR),
                nameof(PPF_FDR),
                nameof(PPF_FOR),
                nameof(PPF_ACC),
                nameof(PPF_GM),
                nameof(PPF_F1S),
                nameof(PPF_G1S),
                nameof(PPF_MCC),
                nameof(PPF_BM_),
                nameof(PPF_MK_),
                nameof(PPF_BAC),
                nameof(PPF_ROC_AUC_Approx_All),
                nameof(PPF_ROC_AUC_Approx_11p),
                nameof(PPF_ROC_AUC_All),
                //nameof(PPF_ROC_AUC_11p),
                nameof(PPF_PR_AUC_Approx_All),
                nameof(PPF_PR_AUC_Approx_11p),
                nameof(PPF_PRI_AUC_Approx_All),
                nameof(PPF_PRI_AUC_Approx_11p),
                nameof(PPF_AP_All),
                nameof(PPF_AP_11p),
                nameof(PPF_API_All),
                nameof(PPF_API_11p),
                nameof(PPF_Brier_All),
                nameof(PPF_LRP),
                nameof(PPF_LRN),
                nameof(PPF_F1B_00),
                nameof(PPF_F1B_01),
                nameof(PPF_F1B_02),
                nameof(PPF_F1B_03),
                nameof(PPF_F1B_04),
                nameof(PPF_F1B_05),
                nameof(PPF_F1B_06),
                nameof(PPF_F1B_07),
                nameof(PPF_F1B_08),
                nameof(PPF_F1B_09),
                nameof(PPF_F1B_10),

                nameof(PPG_TP),
                nameof(PPG_FP),
                nameof(PPG_TN),
                nameof(PPG_FN),
                nameof(PPG_TPR),
                nameof(PPG_TNR),
                nameof(PPG_PPV),
                nameof(PPG_Precision),
                nameof(PPG_Prevalence),
                nameof(PPG_MCR),
                nameof(PPG_ER),
                nameof(PPG_NER),
                nameof(PPG_CNER),
                nameof(PPG_Kappa),
                nameof(PPG_Overlap),
                nameof(PPG_RND_ACC),
                nameof(PPG_Support),
                nameof(PPG_BaseRate),
                nameof(PPG_Youden),
                nameof(PPG_NPV),
                nameof(PPG_FNR),
                nameof(PPG_FPR),
                nameof(PPG_FDR),
                nameof(PPG_FOR),
                nameof(PPG_ACC),
                nameof(PPG_GM),
                nameof(PPG_F1S),
                nameof(PPG_G1S),
                nameof(PPG_MCC),
                nameof(PPG_BM_),
                nameof(PPG_MK_),
                nameof(PPG_BAC),
                nameof(PPG_ROC_AUC_Approx_All),
                nameof(PPG_ROC_AUC_Approx_11p),
                nameof(PPG_ROC_AUC_All),
                //nameof(PPG_ROC_AUC_11p),
                nameof(PPG_PR_AUC_Approx_All),
                nameof(PPG_PR_AUC_Approx_11p),
                nameof(PPG_PRI_AUC_Approx_All),
                nameof(PPG_PRI_AUC_Approx_11p),
                nameof(PPG_AP_All),
                nameof(PPG_AP_11p),
                nameof(PPG_API_All),
                nameof(PPG_API_11p),
                nameof(PPG_Brier_All),
                nameof(PPG_LRP),
                nameof(PPG_LRN),
                nameof(PPG_F1B_00),
                nameof(PPG_F1B_01),
                nameof(PPG_F1B_02),
                nameof(PPG_F1B_03),
                nameof(PPG_F1B_04),
                nameof(PPG_F1B_05),
                nameof(PPG_F1B_06),
                nameof(PPG_F1B_07),
                nameof(PPG_F1B_08),
                nameof(PPG_F1B_09),
                nameof(PPG_F1B_10),

                nameof(roc_xy_str_all),
                nameof(roc_xy_str_11p),
                nameof(pr_xy_str_all),
                nameof(pr_xy_str_11p),
                nameof(pri_xy_str_all),
                nameof(pri_xy_str_11p),
            };


            internal string[] get_values()
            {
                var data = new string[]
                {
                    x_experiment_name,

                    x_id?.ToString(CultureInfo.InvariantCulture),

                    x_iteration_index?.ToString(CultureInfo.InvariantCulture),
                    x_group_index?.ToString(CultureInfo.InvariantCulture),
                    x_total_groups?.ToString(CultureInfo.InvariantCulture),
                    x_output_threshold_adjustment_performance?.ToString(CultureInfo.InvariantCulture),

                    x_key_alphabet ,
                    x_key_dimension ,
                    x_key_category,
                    x_key_source ,
                    x_key_group ,
                    x_key_member ,
                    x_key_perspective ,

                    x_duration_grid_search,
                    x_duration_training,
                    x_duration_testing,
                    x_scale_function.ToString(),
                    x_libsvm_cv.ToString("G17", CultureInfo.InvariantCulture),
                    x_prediction_threshold?.ToString("G17", CultureInfo.InvariantCulture),
                    x_prediction_threshold_class?.ToString("G17", CultureInfo.InvariantCulture),
                    x_old_feature_count.ToString(CultureInfo.InvariantCulture),
                    x_new_feature_count.ToString(CultureInfo.InvariantCulture),
                    x_old_group_count.ToString(CultureInfo.InvariantCulture),
                    x_new_group_count.ToString(CultureInfo.InvariantCulture),
                    x_features_included,
                    x_inner_cv_folds.ToString(CultureInfo.InvariantCulture),
                    x_repetitions_index.ToString(CultureInfo.InvariantCulture),
                    x_repetitions_total.ToString(CultureInfo.InvariantCulture),
                    x_outer_cv_index.ToString(CultureInfo.InvariantCulture),
                    x_outer_cv_folds.ToString(CultureInfo.InvariantCulture),
                    x_svm_type.ToString() ?? "",
                    x_svm_kernel.ToString() ?? "",
                    x_cost?.ToString("G17", CultureInfo.InvariantCulture).Replace(",",";", StringComparison.InvariantCulture) ?? "",
                    x_gamma?.ToString("G17", CultureInfo.InvariantCulture).Replace(",",";", StringComparison.InvariantCulture) ?? "",
                    x_epsilon?.ToString("G17", CultureInfo.InvariantCulture).Replace(",",";", StringComparison.InvariantCulture) ?? "",
                    x_coef0?.ToString("G17", CultureInfo.InvariantCulture).Replace(",",";", StringComparison.InvariantCulture) ?? "",
                    x_degree?.ToString("G17", CultureInfo.InvariantCulture).Replace(",",";", StringComparison.InvariantCulture) ?? "",

                    class_id?.ToString(CultureInfo.InvariantCulture),
                    x_class_name,
                    x_class_size.ToString("G17", CultureInfo.InvariantCulture),
                    x_class_training_size.ToString("G17", CultureInfo.InvariantCulture),
                    x_class_testing_size.ToString("G17", CultureInfo.InvariantCulture),

                    P.ToString("G17", CultureInfo.InvariantCulture),
                    N.ToString("G17", CultureInfo.InvariantCulture),
                    TP.ToString("G17", CultureInfo.InvariantCulture),
                    FP.ToString("G17", CultureInfo.InvariantCulture),
                    TN.ToString("G17", CultureInfo.InvariantCulture),
                    FN.ToString("G17", CultureInfo.InvariantCulture),

                    TPR.ToString("G17", CultureInfo.InvariantCulture),
                    TNR.ToString("G17", CultureInfo.InvariantCulture),
                    PPV.ToString("G17", CultureInfo.InvariantCulture),
                    Precision.ToString("G17", CultureInfo.InvariantCulture),
                    Prevalence.ToString("G17", CultureInfo.InvariantCulture),
                    MCR.ToString("G17", CultureInfo.InvariantCulture),
                    ER.ToString("G17", CultureInfo.InvariantCulture),
                    NER.ToString("G17", CultureInfo.InvariantCulture),
                    CNER.ToString("G17", CultureInfo.InvariantCulture),
                    Kappa.ToString("G17", CultureInfo.InvariantCulture),
                    Overlap.ToString("G17", CultureInfo.InvariantCulture),
                    RND_ACC.ToString("G17", CultureInfo.InvariantCulture),
                    Support.ToString("G17", CultureInfo.InvariantCulture),
                    BaseRate.ToString("G17", CultureInfo.InvariantCulture),
                    Youden.ToString("G17", CultureInfo.InvariantCulture),
                    NPV.ToString("G17", CultureInfo.InvariantCulture),
                    FNR.ToString("G17", CultureInfo.InvariantCulture),
                    FPR.ToString("G17", CultureInfo.InvariantCulture),
                    FDR.ToString("G17", CultureInfo.InvariantCulture),
                    FOR.ToString("G17", CultureInfo.InvariantCulture),
                    ACC.ToString("G17", CultureInfo.InvariantCulture),
                    GM.ToString("G17", CultureInfo.InvariantCulture),
                    F1S.ToString("G17", CultureInfo.InvariantCulture),
                    G1S.ToString("G17", CultureInfo.InvariantCulture),
                    MCC.ToString("G17", CultureInfo.InvariantCulture),
                    BM_.ToString("G17", CultureInfo.InvariantCulture),
                    MK_.ToString("G17", CultureInfo.InvariantCulture),
                    BAC.ToString("G17", CultureInfo.InvariantCulture),
                    ROC_AUC_Approx_All.ToString("G17", CultureInfo.InvariantCulture),
                    ROC_AUC_Approx_11p.ToString("G17", CultureInfo.InvariantCulture),
                    ROC_AUC_All.ToString("G17", CultureInfo.InvariantCulture),
                    PR_AUC_Approx_All.ToString("G17", CultureInfo.InvariantCulture),
                    PR_AUC_Approx_11p.ToString("G17", CultureInfo.InvariantCulture),
                    PRI_AUC_Approx_All.ToString("G17", CultureInfo.InvariantCulture),
                    PRI_AUC_Approx_11p.ToString("G17", CultureInfo.InvariantCulture),
                    AP_All.ToString("G17", CultureInfo.InvariantCulture),
                    AP_11p.ToString("G17", CultureInfo.InvariantCulture),
                    API_All.ToString("G17", CultureInfo.InvariantCulture),
                    API_11p.ToString("G17", CultureInfo.InvariantCulture),
                    Brier_All.ToString("G17", CultureInfo.InvariantCulture),
                    LRP.ToString("G17", CultureInfo.InvariantCulture),
                    LRN.ToString("G17", CultureInfo.InvariantCulture),

                    F1B_00.ToString("G17", CultureInfo.InvariantCulture),
                    F1B_01.ToString("G17", CultureInfo.InvariantCulture),
                    F1B_02.ToString("G17", CultureInfo.InvariantCulture),
                    F1B_03.ToString("G17", CultureInfo.InvariantCulture),
                    F1B_04.ToString("G17", CultureInfo.InvariantCulture),
                    F1B_05.ToString("G17", CultureInfo.InvariantCulture),
                    F1B_06.ToString("G17", CultureInfo.InvariantCulture),
                    F1B_07.ToString("G17", CultureInfo.InvariantCulture),
                    F1B_08.ToString("G17", CultureInfo.InvariantCulture),
                    F1B_09.ToString("G17", CultureInfo.InvariantCulture),
                    F1B_10.ToString("G17", CultureInfo.InvariantCulture),

                    PPF_TP.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_FP.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_TN.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_FN.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_TPR.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_TNR.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_PPV.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_Precision.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_Prevalence.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_MCR.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_ER.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_NER.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_CNER.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_Kappa.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_Overlap.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_RND_ACC.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_Support.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_BaseRate.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_Youden.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_NPV.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_FNR.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_FPR.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_FDR.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_FOR.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_ACC.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_GM.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_F1S.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_G1S.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_MCC.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_BM_.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_MK_.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_BAC.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_ROC_AUC_Approx_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_ROC_AUC_Approx_11p.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_ROC_AUC_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_PR_AUC_Approx_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_PR_AUC_Approx_11p.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_PRI_AUC_Approx_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_PRI_AUC_Approx_11p.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_AP_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_AP_11p.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_API_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_API_11p.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_Brier_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_LRP.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_LRN.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_F1B_00.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_F1B_01.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_F1B_02.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_F1B_03.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_F1B_04.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_F1B_05.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_F1B_06.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_F1B_07.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_F1B_08.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_F1B_09.ToString("G17", CultureInfo.InvariantCulture),
                    PPF_F1B_10.ToString("G17", CultureInfo.InvariantCulture),

                    PPG_TP.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_FP.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_TN.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_FN.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_TPR.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_TNR.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_PPV.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_Precision.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_Prevalence.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_MCR.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_ER.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_NER.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_CNER.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_Kappa.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_Overlap.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_RND_ACC.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_Support.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_BaseRate.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_Youden.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_NPV.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_FNR.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_FPR.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_FDR.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_FOR.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_ACC.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_GM.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_F1S.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_G1S.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_MCC.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_BM_.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_MK_.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_BAC.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_ROC_AUC_Approx_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_ROC_AUC_Approx_11p.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_ROC_AUC_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_PR_AUC_Approx_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_PR_AUC_Approx_11p.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_PRI_AUC_Approx_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_PRI_AUC_Approx_11p.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_AP_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_AP_11p.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_API_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_API_11p.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_Brier_All.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_LRP.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_LRN.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_F1B_00.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_F1B_01.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_F1B_02.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_F1B_03.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_F1B_04.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_F1B_05.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_F1B_06.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_F1B_07.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_F1B_08.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_F1B_09.ToString("G17", CultureInfo.InvariantCulture),
                    PPG_F1B_10.ToString("G17", CultureInfo.InvariantCulture),

                    roc_xy_str_all,
                    roc_xy_str_11p,
                    pr_xy_str_all,
                    pr_xy_str_11p,
                    pri_xy_str_all,
                    pri_xy_str_11p,
                };

                var data1 = data.Select(a => a?.ToString(CultureInfo.InvariantCulture).Replace(",", ";", StringComparison.InvariantCulture) ?? "").ToArray();

                return data1;
            }

            public override string ToString()
            {
                return String.Join(",", get_values().Select(a => a?.ToString(CultureInfo.InvariantCulture).Replace(",", ";", StringComparison.InvariantCulture) ?? "").ToList());
            }
        }

        //internal static double fbeta1(double PPV, double TPR, double fbeta)
        //{
        //    var fb = (PPV == 0.0 || TPR == 0.0) ? (double)0.0 : (double)(1 + (fbeta * fbeta)) * ((PPV * TPR) / (((fbeta * fbeta) * PPV) + TPR));

        //    return fb;
        //}

        internal static double fbeta2(double PPV, double TPR, double fbeta)
        {
            var fb = (PPV == 0.0 || TPR == 0.0) ? (double)0.0 : (double)(1 / (fbeta * (1 / PPV) + (1 - fbeta) * (1 / TPR)));

            return fb;
        }

        internal static List<confusion_matrix> count_prediction_error(
            List<prediction> prediction_list,
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


            var actual_class_id_list = prediction_list.Select(a => a.actual_class).Distinct().OrderBy(a => a).ToList();

            var confusion_matrix_list = new List<confusion_matrix>();

            for (var i = 0; i < actual_class_id_list.Count; i++)
            {
                var actual_class_id = actual_class_id_list[i];

                var confusion_matrix1 = new confusion_matrix()
                {
                    class_id = actual_class_id,
                    P = prediction_list.Count(b => actual_class_id == b.actual_class),
                    N = prediction_list.Count(b => actual_class_id != b.actual_class),
                    x_prediction_threshold = threshold,
                    x_prediction_threshold_class = threshold_class,
                    thresholds = prediction_list.Select(a => a.probability_estimates.FirstOrDefault(b => b.class_id == actual_class_id).probability_estimate).Distinct().OrderByDescending(a => a).ToList(),

                };
                confusion_matrix_list.Add(confusion_matrix1);

            }

            for (var prediction_list_index = 0; prediction_list_index < prediction_list.Count; prediction_list_index++)
            {
                var prediction = prediction_list[prediction_list_index];
                var actual_class = prediction.actual_class;
                var predicted_class = prediction.default_predicted_class;

                var actual_class_matrix = confusion_matrix_list.First(a => a.class_id == actual_class);
                var predicted_class_matrix = confusion_matrix_list.First(a => a.class_id == predicted_class);

                if (actual_class == predicted_class)
                {
                    actual_class_matrix.TP++;

                    for (var index = 0; index < confusion_matrix_list.Count; index++)
                    {
                        if (confusion_matrix_list[index].class_id != actual_class)
                        {
                            confusion_matrix_list[index].TN++;
                        }
                    }
                }

                else if (actual_class != predicted_class)
                {
                    actual_class_matrix.FN++;

                    predicted_class_matrix.FP++;
                }
            }

            foreach (var cm in confusion_matrix_list)
            {
                cm.calculate_metrics(calculate_auc, prediction_list);
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
                //cm.BM_ = (cm.TPR + cm.TNR) - (double) 1.0;
                //cm.MK_ = (cm.PPV + cm.NPV) - (double) 1.0;
                //cm.BAC = (double) (cm.TPR + cm.TNR) / (double) 2.0;

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
            }

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


        internal static List<prediction> load_prediction_file_regression_values(List<(string test_file, string test_comments_file, string prediction_file)> files)
        {

            // method untested

            var lines = files.Select((a, i) => (test_file_lines: io_proxy.ReadAllLines(a.test_file, nameof(perf), nameof(load_prediction_file_regression_values)).ToList(), test_comments_file_lines: io_proxy.ReadAllLines(a.test_comments_file, nameof(perf), nameof(load_prediction_file_regression_values)).ToList(), prediction_file_lines: io_proxy.ReadAllLines(a.prediction_file, nameof(perf), nameof(load_prediction_file_regression_values)).ToList())).ToList();

            // prediction file MAY have a header, but only if probability estimates are enabled

            //var test_has_headers = false;
            //var test_comments_has_headers = true;
            var prediction_has_headers = lines.Any(a => a.prediction_file_lines.FirstOrDefault().StartsWith("labels", StringComparison.InvariantCulture));


            if (prediction_has_headers && lines.Select(a => a.prediction_file_lines.FirstOrDefault()).Distinct().Count() != 1)
            {
                throw new Exception();
            }

            lines = lines.Select((a, i) => (a.test_file_lines, a.test_comments_file_lines.Skip(1).ToList(), a.prediction_file_lines.Skip(i > 0 && prediction_has_headers ? 1 : 0).ToList())).ToList();

            var test_file_lines = lines.SelectMany(a => a.test_file_lines).ToList();
            var test_comments_file_lines = lines.SelectMany(a => a.test_comments_file_lines).ToList();
            var prediction_file_lines = lines.SelectMany(a => a.prediction_file_lines).ToList();

            return load_prediction_file_regression_values_from_text(test_file_lines, test_comments_file_lines, prediction_file_lines);
        }

        internal static List<prediction> load_prediction_file_regression_values(string test_file, string test_comments_file, string prediction_file)
        {
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

            var test_file_lines = io_proxy.ReadAllLines(test_file, nameof(perf), nameof(load_prediction_file_regression_values)).ToList();

            var test_comments_file_lines = !string.IsNullOrWhiteSpace(test_comments_file) && io_proxy.Exists(test_comments_file, nameof(perf), nameof(load_prediction_file_regression_values)) ? io_proxy.ReadAllLines(test_comments_file, nameof(perf), nameof(load_prediction_file_regression_values)).ToList() : null;

            var prediction_file_lines = io_proxy.ReadAllLines(prediction_file, nameof(perf), nameof(load_prediction_file_regression_values)).ToList();

            return load_prediction_file_regression_values_from_text(test_file_lines, test_comments_file_lines, prediction_file_lines);
        }

        internal static List<prediction> load_prediction_file_regression_values_from_text(IList<string> test_file_lines, IList<string> test_comments_file_lines, IList<string> prediction_file_lines)
        {
            if (test_file_lines == null || test_file_lines.Count == 0)
            {
                throw new ArgumentNullException(nameof(test_file_lines));
            }

            if (prediction_file_lines == null || prediction_file_lines.Count == 0)
            {
                throw new ArgumentNullException(nameof(prediction_file_lines));
            }

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


            var test_file_data = test_file_lines.Where(a => !string.IsNullOrWhiteSpace(a) && int.TryParse(a.Split().First(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var line_cid)).Select(a => a.Trim().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries)).ToList();



            if (test_file_lines.Count == 0) return null;


            var prediction_file_data = prediction_file_lines.Where(a => !string.IsNullOrWhiteSpace(a) && int.TryParse(a.Split().First(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var line_cid)).Select(a => a.Trim().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries)).ToList();

            if (prediction_file_lines.Count == 0) return null;

            var probability_estimate_class_labels = new List<int>();

            if (prediction_file_lines.First().Trim().Split().First() == "labels")
            {
                probability_estimate_class_labels = prediction_file_lines.First().Trim().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries).Skip(1).Select(a => int.Parse(a, CultureInfo.InvariantCulture)).ToList();
            }

            if (test_comments_file_lines != null && test_file_data.Count != test_comments_file_lines.Count)
            {
                throw new Exception("Error: test file and test comments file have different instance length: " + test_file_data.Count + " " + test_comments_file_lines.Count);
            }

            if (test_file_data.Count != prediction_file_data.Count)
            {
                throw new Exception("Error: test file and prediction file have different instance length: " + test_file_data.Count + " " + prediction_file_data.Count);
            }


            var prediction_list = new List<prediction>();

            for (var prediction_index = 0; prediction_index < test_file_data.Count; prediction_index++)
            {
                var probability_estimates = (prediction_file_data[prediction_index].Length <= 1) ?
                    new List<(int, double)>() :
                    prediction_file_data[prediction_index].Skip(1).Select((a, i) => (class_id: probability_estimate_class_labels[i], probability_estimate: double.Parse(a, NumberStyles.Float, CultureInfo.InvariantCulture))).ToList();

                var probability_estimates_stated = probability_estimates != null && probability_estimates.Count > 0 && probability_estimate_class_labels != null && probability_estimate_class_labels.Count > 0;

                var prediction = new prediction()
                {
                    comment = (test_comments_file_lines != null && test_comments_file_lines.Count > 0) ? test_comments_file_lines[prediction_index].Split(',').Select((a, i) => (comment_header: (test_file_comments_header.Length - 1 > i ? test_file_comments_header[i] : ""), comment_value: a)).ToList() : new List<(string comment_header, string comment_value)>(),
                    prediction_index = prediction_index,
                    actual_class = int.Parse(test_file_data[prediction_index][0], CultureInfo.InvariantCulture),
                    default_predicted_class = int.Parse(prediction_file_data[prediction_index][0], CultureInfo.InvariantCulture),
                    probability_estimates_stated = probability_estimates_stated,
                    probability_estimates = probability_estimates,
                    test_row_vector = test_file_data[prediction_index].Skip(1).ToArray(),
                };

                prediction_list.Add(prediction);
            }

            return prediction_list;
        }

        //internal static (List<prediction> prediction_list, List<confusion_matrix> cm_list) load_prediction_file(List<(string test_file, string test_comments_file, string prediction_file)> files, bool output_threshold_adjustment_performance)
        //{


        //    var prediction_list = load_prediction_file_regression_values(test_file, test_comments_file, prediction_file);
        //    var cm_list = load_prediction_file(prediction_list, output_threshold_adjustment_performance);

        //    return (prediction_list, cm_list);

        //}

        internal static (List<prediction> prediction_list, List<confusion_matrix> cm_list) load_prediction_file(string test_file, string test_comments_file, string prediction_file, bool output_threshold_adjustment_performance)
        {

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(test_file),test_io_proxy.ToString()),
            //    (nameof(prediction_file),prediction_io_proxy.ToString()),
            //    (nameof(output_threshold_adjustment_performance),output_threshold_adjustment_performance.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(load_prediction_file)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var prediction_list = load_prediction_file_regression_values(test_file, test_comments_file, prediction_file);
            var cm_list = load_prediction_file(prediction_list, output_threshold_adjustment_performance);

            return (prediction_list, cm_list);
        }

        internal static (List<prediction> prediction_list, List<confusion_matrix> cm_list) load_prediction_file(IList<string> test_file_lines, IList<string> test_comments_file_lines, IList<string> prediction_file_lines, bool output_threshold_adjustment_performance)
        {

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(test_file),test_io_proxy.ToString()),
            //    (nameof(prediction_file),prediction_io_proxy.ToString()),
            //    (nameof(output_threshold_adjustment_performance),output_threshold_adjustment_performance.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(load_prediction_file)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var prediction_list = load_prediction_file_regression_values_from_text(test_file_lines, test_comments_file_lines, prediction_file_lines);
            var cm_list = load_prediction_file(prediction_list, output_threshold_adjustment_performance);

            return (prediction_list, cm_list);
        }

        internal static List<confusion_matrix> load_prediction_file(List<prediction> prediction_list, bool output_threshold_adjustment_performance)
        {

            //var param_list = new List<(string key, string value)>()
            //{
            //    (nameof(prediction_list),prediction_list.ToString()),
            //    (nameof(output_threshold_adjustment_performance),output_threshold_adjustment_performance.ToString()),
            //};

            //if (program.write_console_log) program.WriteLine($@"{nameof(load_prediction_file)}({string.Join(", ", param_list.Select(a => $"{a.key}=\"{a.value}\"").ToList())});");


            var class_id_list = prediction_list.Select(a => a.actual_class).Distinct().OrderBy(a => a).ToList();

            var default_confusion_matrix_list = count_prediction_error(prediction_list);

            var confusion_matrix_list = new List<confusion_matrix>();
            confusion_matrix_list.AddRange(default_confusion_matrix_list);

            if (class_id_list.Count >= 2 && output_threshold_adjustment_performance)
            {
                var positive_id = class_id_list.Max();
                var negative_id = class_id_list.Min();

                var thresholds = new List<double>();
                for (var i = 0.00m; i <= 1.00m; i += 0.05m)
                {
                    thresholds.Add((double)i);
                }



                var threshold_prediction_list = thresholds.Select(t => (positive_threshold: t, prediction_list: prediction_list.Select(p => new prediction()
                {
                    actual_class = p.actual_class,
                    default_predicted_class = p.probability_estimates.First(e => e.class_id == positive_id).probability_estimate >= t ? positive_id : negative_id,
                    probability_estimates = p.probability_estimates,
                    prediction_index = p.prediction_index,
                    test_row_vector = p.test_row_vector,
                    comment = p.comment,
                }).ToList())).ToList();

                var threshold_confusion_matrix_list = threshold_prediction_list.SelectMany(a => count_prediction_error(a.prediction_list, a.positive_threshold, positive_id, false)).ToList();

                for (var i = 0; i < threshold_confusion_matrix_list.Count; i++)
                {
                    var default_cm = default_confusion_matrix_list.First(a => a.class_id == threshold_confusion_matrix_list[i].class_id);

                    threshold_confusion_matrix_list[i].ROC_AUC_Approx_All = default_cm.ROC_AUC_Approx_All;
                    threshold_confusion_matrix_list[i].ROC_AUC_Approx_11p = default_cm.ROC_AUC_Approx_11p;
                    threshold_confusion_matrix_list[i].roc_xy_str_all = default_cm.roc_xy_str_all;
                    threshold_confusion_matrix_list[i].roc_xy_str_11p = default_cm.roc_xy_str_11p;

                    threshold_confusion_matrix_list[i].PR_AUC_Approx_All = default_cm.PR_AUC_Approx_All;
                    threshold_confusion_matrix_list[i].PR_AUC_Approx_11p = default_cm.PR_AUC_Approx_11p;
                    threshold_confusion_matrix_list[i].pr_xy_str_all = default_cm.pr_xy_str_all;
                    threshold_confusion_matrix_list[i].pr_xy_str_11p = default_cm.pr_xy_str_11p;
                    threshold_confusion_matrix_list[i].pri_xy_str_all = default_cm.pri_xy_str_all;
                    threshold_confusion_matrix_list[i].pri_xy_str_11p = default_cm.pri_xy_str_11p;
                }

                confusion_matrix_list.AddRange(threshold_confusion_matrix_list);
            }

            return confusion_matrix_list;
        }

        internal static double brier(List<prediction> prediction_list, int positive_id)
        {
            if (prediction_list.Any(a => !a.probability_estimates_stated)) return default;

            prediction_list = prediction_list.OrderByDescending(a => a.probability_estimates.First(b => b.class_id == positive_id).probability_estimate).ToList();

            // Calc Brier
            var brier_score = ((double)1 / (double)prediction_list.Count)
                              * (prediction_list.Sum(a => Math.Pow(a.probability_estimates.First(b => b.class_id == a.default_predicted_class).probability_estimate - (a.actual_class == a.default_predicted_class ? 1 : 0), 2)));

            return brier_score;
        }

        internal enum threshold_type
        {
            all_thresholds,
            eleven_points
        }

        internal static (/*double brier_score,*/ double roc_auc_approx, double roc_auc_actual, double pr_auc_approx, double pri_auc_approx, double ap, double api, List<(double x, double y)> roc_xy, List<(double x, double y)> pr_xy, List<(double x, double y)> pri_xy)
            Calculate_ROC_PR_AUC(List<prediction> prediction_list, int positive_id, threshold_type threshold_type = threshold_type.all_thresholds)
        {
            if (prediction_list.Any(a => !a.probability_estimates_stated)) return default;

            // Assume binary classifier - get negative class id
            var negative_id = prediction_list.First(a => a.actual_class != positive_id).actual_class;

            // Calc P
            var p = prediction_list.Count(a => a.actual_class == positive_id);

            // Calc N
            var n = prediction_list.Count(a => a.actual_class == negative_id);

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
            var threshold_prediction_list = thresholds.Select(t => (positive_threshold: t, prediction_list: prediction_list.Select(pl => new prediction()
            {
                comment = pl.comment,
                actual_class = pl.actual_class,
                default_predicted_class = pl.probability_estimates.FirstOrDefault(e => e.class_id == positive_id).probability_estimate >= t ? positive_id : negative_id,
                probability_estimates = pl.probability_estimates,
                prediction_index = pl.prediction_index,
                test_row_vector = pl.test_row_vector
            }).ToList())).ToList();

            // Calc confusion matrices at each threshold
            var threshold_confusion_matrix_list = threshold_prediction_list.SelectMany(a => count_prediction_error(a.prediction_list, a.positive_threshold, positive_id, false)).ToList();
            threshold_confusion_matrix_list = threshold_confusion_matrix_list.Where(a => a.class_id == positive_id).ToList();

            //// Calc Brier
            //var brier_score = ((double)1 / (double)prediction_list.Count)
            //                  * (prediction_list.Sum(a => Math.Pow(a.probability_estimates.First(b => b.class_id == a.default_predicted_class).probability_estimate - (a.actual_class == a.default_predicted_class ? 1 : 0), 2)));

            // Average Precision (Not Approximated)
            var ap = threshold_confusion_matrix_list.Select((a, i) =>
            {
                //var max_p = threshold_confusion_matrix_list.Where(b => b.TPR >= a.TPR).Max(b => b.PPV);
                var delta_tpr = Math.Abs(a.TPR - (i == 0 ? 0 : threshold_confusion_matrix_list[i - 1].TPR));
                var _ap = a.PPV * delta_tpr;

                if (double.IsNaN(_ap)) _ap = 0;
                //var _api = max_p * delta_tpr;
                return _ap;
            }).Sum();

            // Average Precision Interpolated (Not Approximated)
            var api = threshold_confusion_matrix_list.Select((a, i) =>
            {
                var max_ppv = threshold_confusion_matrix_list.Where(b => b.TPR >= a.TPR).Max(b => b.PPV);

                if (double.IsNaN(max_ppv)/* || max_ppv == 0*/) max_ppv = a.PPV; // = 0? =1? unknown: should it be a.PPV, 0, or 1 when there are no true results?

                var delta_tpr = Math.Abs(a.TPR - (i == 0 ? 0 : threshold_confusion_matrix_list[i - 1].TPR));
                //var _ap = a.PPV * delta_tpr;
                var _api = max_ppv * delta_tpr;

                if (double.IsNaN(_api)) _api = 0;

                return _api;
            }).Sum();

            // PR Curve Coordinates
            var pr_plot_coords = threshold_confusion_matrix_list.Select(a => (x: a.TPR, y: a.PPV)).ToList();

            if (pr_plot_coords.First().x != 0.0)
            {
                pr_plot_coords.Insert(0, ((double)0.0, pr_plot_coords.First().y));
            }

            if (pr_plot_coords.Last().x != 1.0 && threshold_confusion_matrix_list.Count > 0)
            {
                var m = threshold_confusion_matrix_list.First();
                pr_plot_coords.Add(((double)1.0, (double)m.P / ((double)m.P + (double)m.N)));
            }

            // PRI Curve Coordinates
            var pri_plot_coords = threshold_confusion_matrix_list.Select(a =>
            {
                var max_ppv = threshold_confusion_matrix_list.Where(b => b.TPR >= a.TPR).Max(b => b.PPV);
                if (double.IsNaN(max_ppv)) max_ppv = a.PPV;// 0;

                return (x: a.TPR, y: max_ppv);
            }).ToList();

            if (pri_plot_coords.First().x != 0.0)
            {
                pri_plot_coords.Insert(0, ((double)0.0, pri_plot_coords.First().y));
            }

            if (pri_plot_coords.Last().x != 1.0 && threshold_confusion_matrix_list.Count > 0)
            {
                var m = threshold_confusion_matrix_list.First();
                pri_plot_coords.Add(((double)1.0, (double)m.P / ((double)m.P + (double)m.N)));
            }

            // ROC Curve Coordinates
            var roc_plot_coords = threshold_confusion_matrix_list.Select(a => (x: a.FPR, y: a.TPR)).ToList();
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

            var total_neg_for_threshold = prediction_list.Select((a, i) => (actual_class: a.actual_class, total_neg_at_point: prediction_list.Where((b, j) => j <= i && b.actual_class == negative_id).Count())).ToList();
            var roc_auc_actual = ((double)1 / (double)(p * n)) * (double)prediction_list
                                  .Select((a, i) =>
                                  {
                                      if (a.actual_class != positive_id) return 0;
                                      var total_n_at_current_threshold = total_neg_for_threshold[i].total_neg_at_point;

                                      var n_more_than_current_n = total_neg_for_threshold.Count(b => b.actual_class == negative_id && b.total_neg_at_point > total_n_at_current_threshold);

                                      return n_more_than_current_n;
                                  }).Sum();

            return (/*brier_score: brier_score,*/ roc_auc_approx: roc_auc_approx, roc_auc_actual: roc_auc_actual, pr_auc_approx: pr_auc_approx, pri_auc_approx: pri_auc_approx, ap: ap, api: api, roc_xy: roc_plot_coords, pr_xy: pr_plot_coords, pri_xy: pri_plot_coords);
        }
    }
}
