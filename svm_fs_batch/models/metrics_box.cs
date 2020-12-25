using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace svm_fs_batch
{
    internal class metrics_box
    {
        public const string module_name = nameof(metrics_box);
        internal static readonly metrics_box empty = new metrics_box();

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
        internal double YoudenIndex;
        internal double NPV;
        internal double FNR;
        internal double FPR;
        internal double FDR;
        internal double FOR;
        internal double ACC;
        internal double GMean;
        internal double F1S;
        internal double G1S;
        internal double MCC;
        internal double Informedness;
        internal double Markedness;
        internal double BalancedAccuracy;
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
        internal double Brier_Inverse_All;
        internal double LRP;
        internal double LRN;
        internal double DOR;
        internal double PrevalenceThreshold;
        internal double CriticalSuccessIndex;
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

        public metrics_box()
        {

        }

        public metrics_box(metrics_box metrics, double? divisor = null)
        {
            if (divisor != null && divisor == 0)
            {
                return;
            }

            P = metrics.P;
            N = metrics.N;
            TP = metrics.TP;
            FP = metrics.FP;
            TN = metrics.TN;
            FN = metrics.FN;
            TPR = metrics.TPR;
            TNR = metrics.TNR;
            PPV = metrics.PPV;
            Precision = metrics.Precision;
            Prevalence = metrics.Prevalence;
            MCR = metrics.MCR;
            ER = metrics.ER;
            NER = metrics.NER;
            CNER = metrics.CNER;
            Kappa = metrics.Kappa;
            Overlap = metrics.Overlap;
            RND_ACC = metrics.RND_ACC;
            Support = metrics.Support;
            BaseRate = metrics.BaseRate;
            YoudenIndex = metrics.YoudenIndex;
            NPV = metrics.NPV;
            FNR = metrics.FNR;
            FPR = metrics.FPR;
            FDR = metrics.FDR;
            FOR = metrics.FOR;
            ACC = metrics.ACC;
            GMean = metrics.GMean;
            F1S = metrics.F1S;
            G1S = metrics.G1S;
            MCC = metrics.MCC;
            Informedness = metrics.Informedness;
            Markedness = metrics.Markedness;
            BalancedAccuracy = metrics.BalancedAccuracy;
            ROC_AUC_Approx_All = metrics.ROC_AUC_Approx_All;
            ROC_AUC_Approx_11p = metrics.ROC_AUC_Approx_11p;
            ROC_AUC_All = metrics.ROC_AUC_All;
            PR_AUC_Approx_All = metrics.PR_AUC_Approx_All;
            PR_AUC_Approx_11p = metrics.PR_AUC_Approx_11p;
            PRI_AUC_Approx_All = metrics.PRI_AUC_Approx_All;
            PRI_AUC_Approx_11p = metrics.PRI_AUC_Approx_11p;
            AP_All = metrics.AP_All;
            AP_11p = metrics.AP_11p;
            API_All = metrics.API_All;
            API_11p = metrics.API_11p;
            Brier_Inverse_All = metrics.Brier_Inverse_All;
            LRP = metrics.LRP;
            LRN = metrics.LRN;
            DOR = metrics.DOR;
            PrevalenceThreshold = metrics.PrevalenceThreshold;
            CriticalSuccessIndex = metrics.CriticalSuccessIndex;
            F1B_00 = metrics.F1B_00;
            F1B_01 = metrics.F1B_01;
            F1B_02 = metrics.F1B_02;
            F1B_03 = metrics.F1B_03;
            F1B_04 = metrics.F1B_04;
            F1B_05 = metrics.F1B_05;
            F1B_06 = metrics.F1B_06;
            F1B_07 = metrics.F1B_07;
            F1B_08 = metrics.F1B_08;
            F1B_09 = metrics.F1B_09;
            F1B_10 = metrics.F1B_10;

            if (divisor != null && divisor.Value != 0)
            {
                divide(divisor.Value);
            }
        }

        private void divide(double divisor)
        {
            if (divisor == 0) return;

            P /= divisor;
            N /= divisor;
            TP /= divisor;
            FP /= divisor;
            TN /= divisor;
            FN /= divisor;
            TPR /= divisor;
            TNR /= divisor;
            PPV /= divisor;
            Precision /= divisor;
            Prevalence /= divisor;
            MCR /= divisor;
            ER /= divisor;
            NER /= divisor;
            CNER /= divisor;
            Kappa /= divisor;
            Overlap /= divisor;
            RND_ACC /= divisor;
            Support /= divisor;
            BaseRate /= divisor;
            YoudenIndex /= divisor;
            NPV /= divisor;
            FNR /= divisor;
            FPR /= divisor;
            FDR /= divisor;
            FOR /= divisor;
            ACC /= divisor;
            GMean /= divisor;
            F1S /= divisor;
            G1S /= divisor;
            MCC /= divisor;
            Informedness /= divisor;
            Markedness /= divisor;
            BalancedAccuracy /= divisor;
            ROC_AUC_Approx_All /= divisor;
            ROC_AUC_Approx_11p /= divisor;
            ROC_AUC_All /= divisor;
            PR_AUC_Approx_All /= divisor;
            PR_AUC_Approx_11p /= divisor;
            PRI_AUC_Approx_All /= divisor;
            PRI_AUC_Approx_11p /= divisor;
            AP_All /= divisor;
            AP_11p /= divisor;
            API_All /= divisor;
            API_11p /= divisor;
            Brier_Inverse_All /= divisor;
            LRP /= divisor;
            LRN /= divisor;
            DOR /= divisor;
            PrevalenceThreshold /= divisor;
            CriticalSuccessIndex /= divisor;
            F1B_00 /= divisor;
            F1B_01 /= divisor;
            F1B_02 /= divisor;
            F1B_03 /= divisor;
            F1B_04 /= divisor;
            F1B_05 /= divisor;
            F1B_06 /= divisor;
            F1B_07 /= divisor;
            F1B_08 /= divisor;
            F1B_09 /= divisor;
            F1B_10 /= divisor;
        }

        internal metrics_box(metrics_box[] metrics_boxes)
        {
            if (metrics_boxes == null || metrics_boxes.Length == 0 || metrics_boxes.All(a => a == null)) return;
            
            P = metrics_boxes.Where(a => a != null).Select(a => a.P).DefaultIfEmpty(0).Average();
            N = metrics_boxes.Where(a => a != null).Select(a => a.N).DefaultIfEmpty(0).Average();
            TP = metrics_boxes.Where(a => a != null).Select(a => a.TP).DefaultIfEmpty(0).Average();
            FP = metrics_boxes.Where(a => a != null).Select(a => a.FP).DefaultIfEmpty(0).Average();
            TN = metrics_boxes.Where(a => a != null).Select(a => a.TN).DefaultIfEmpty(0).Average();
            FN = metrics_boxes.Where(a => a != null).Select(a => a.FN).DefaultIfEmpty(0).Average();
            TPR = metrics_boxes.Where(a => a != null).Select(a => a.TPR).DefaultIfEmpty(0).Average();
            TNR = metrics_boxes.Where(a => a != null).Select(a => a.TNR).DefaultIfEmpty(0).Average();
            PPV = metrics_boxes.Where(a => a != null).Select(a => a.PPV).DefaultIfEmpty(0).Average();
            Precision = metrics_boxes.Where(a => a != null).Select(a => a.Precision).DefaultIfEmpty(0).Average();
            Prevalence = metrics_boxes.Where(a => a != null).Select(a => a.Prevalence).DefaultIfEmpty(0).Average();
            MCR = metrics_boxes.Where(a => a != null).Select(a => a.MCR).DefaultIfEmpty(0).Average();
            ER = metrics_boxes.Where(a => a != null).Select(a => a.ER).DefaultIfEmpty(0).Average();
            NER = metrics_boxes.Where(a => a != null).Select(a => a.NER).DefaultIfEmpty(0).Average();
            CNER = metrics_boxes.Where(a => a != null).Select(a => a.CNER).DefaultIfEmpty(0).Average();
            Kappa = metrics_boxes.Where(a => a != null).Select(a => a.Kappa).DefaultIfEmpty(0).Average();
            Overlap = metrics_boxes.Where(a => a != null).Select(a => a.Overlap).DefaultIfEmpty(0).Average();
            RND_ACC = metrics_boxes.Where(a => a != null).Select(a => a.RND_ACC).DefaultIfEmpty(0).Average();
            Support = metrics_boxes.Where(a => a != null).Select(a => a.Support).DefaultIfEmpty(0).Average();
            BaseRate = metrics_boxes.Where(a => a != null).Select(a => a.BaseRate).DefaultIfEmpty(0).Average();
            YoudenIndex = metrics_boxes.Where(a => a != null).Select(a => a.YoudenIndex).DefaultIfEmpty(0).Average();
            NPV = metrics_boxes.Where(a => a != null).Select(a => a.NPV).DefaultIfEmpty(0).Average();
            FNR = metrics_boxes.Where(a => a != null).Select(a => a.FNR).DefaultIfEmpty(0).Average();
            FPR = metrics_boxes.Where(a => a != null).Select(a => a.FPR).DefaultIfEmpty(0).Average();
            FDR = metrics_boxes.Where(a => a != null).Select(a => a.FDR).DefaultIfEmpty(0).Average();
            FOR = metrics_boxes.Where(a => a != null).Select(a => a.FOR).DefaultIfEmpty(0).Average();
            ACC = metrics_boxes.Where(a => a != null).Select(a => a.ACC).DefaultIfEmpty(0).Average();
            GMean = metrics_boxes.Where(a => a != null).Select(a => a.GMean).DefaultIfEmpty(0).Average();
            F1S = metrics_boxes.Where(a => a != null).Select(a => a.F1S).DefaultIfEmpty(0).Average();
            G1S = metrics_boxes.Where(a => a != null).Select(a => a.G1S).DefaultIfEmpty(0).Average();
            MCC = metrics_boxes.Where(a => a != null).Select(a => a.MCC).DefaultIfEmpty(0).Average();
            Informedness = metrics_boxes.Where(a => a != null).Select(a => a.Informedness).DefaultIfEmpty(0).Average();
            Markedness = metrics_boxes.Where(a => a != null).Select(a => a.Markedness).DefaultIfEmpty(0).Average();
            BalancedAccuracy = metrics_boxes.Where(a => a != null).Select(a => a.BalancedAccuracy).DefaultIfEmpty(0).Average();
            ROC_AUC_Approx_All = metrics_boxes.Where(a => a != null).Select(a => a.ROC_AUC_Approx_All).DefaultIfEmpty(0).Average();
            ROC_AUC_Approx_11p = metrics_boxes.Where(a => a != null).Select(a => a.ROC_AUC_Approx_11p).DefaultIfEmpty(0).Average();
            ROC_AUC_All = metrics_boxes.Where(a => a != null).Select(a => a.ROC_AUC_All).DefaultIfEmpty(0).Average();
            PR_AUC_Approx_All = metrics_boxes.Where(a => a != null).Select(a => a.PR_AUC_Approx_All).DefaultIfEmpty(0).Average();
            PR_AUC_Approx_11p = metrics_boxes.Where(a => a != null).Select(a => a.PR_AUC_Approx_11p).DefaultIfEmpty(0).Average();
            PRI_AUC_Approx_All = metrics_boxes.Where(a => a != null).Select(a => a.PRI_AUC_Approx_All).DefaultIfEmpty(0).Average();
            PRI_AUC_Approx_11p = metrics_boxes.Where(a => a != null).Select(a => a.PRI_AUC_Approx_11p).DefaultIfEmpty(0).Average();
            AP_All = metrics_boxes.Where(a => a != null).Select(a => a.AP_All).DefaultIfEmpty(0).Average();
            AP_11p = metrics_boxes.Where(a => a != null).Select(a => a.AP_11p).DefaultIfEmpty(0).Average();
            API_All = metrics_boxes.Where(a => a != null).Select(a => a.API_All).DefaultIfEmpty(0).Average();
            API_11p = metrics_boxes.Where(a => a != null).Select(a => a.API_11p).DefaultIfEmpty(0).Average();
            Brier_Inverse_All = metrics_boxes.Where(a => a != null).Select(a => a.Brier_Inverse_All).DefaultIfEmpty(0).Average();
            LRP = metrics_boxes.Where(a => a != null).Select(a => a.LRP).DefaultIfEmpty(0).Average();
            LRN = metrics_boxes.Where(a => a != null).Select(a => a.LRN).DefaultIfEmpty(0).Average();
            DOR = metrics_boxes.Where(a => a != null).Select(a => a.DOR).DefaultIfEmpty(0).Average();
            PrevalenceThreshold = metrics_boxes.Where(a => a != null).Select(a => a.PrevalenceThreshold).DefaultIfEmpty(0).Average();
            CriticalSuccessIndex = metrics_boxes.Where(a => a != null).Select(a => a.CriticalSuccessIndex).DefaultIfEmpty(0).Average();
            F1B_00 = metrics_boxes.Where(a => a != null).Select(a => a.F1B_00).DefaultIfEmpty(0).Average();
            F1B_01 = metrics_boxes.Where(a => a != null).Select(a => a.F1B_01).DefaultIfEmpty(0).Average();
            F1B_02 = metrics_boxes.Where(a => a != null).Select(a => a.F1B_02).DefaultIfEmpty(0).Average();
            F1B_03 = metrics_boxes.Where(a => a != null).Select(a => a.F1B_03).DefaultIfEmpty(0).Average();
            F1B_04 = metrics_boxes.Where(a => a != null).Select(a => a.F1B_04).DefaultIfEmpty(0).Average();
            F1B_05 = metrics_boxes.Where(a => a != null).Select(a => a.F1B_05).DefaultIfEmpty(0).Average();
            F1B_06 = metrics_boxes.Where(a => a != null).Select(a => a.F1B_06).DefaultIfEmpty(0).Average();
            F1B_07 = metrics_boxes.Where(a => a != null).Select(a => a.F1B_07).DefaultIfEmpty(0).Average();
            F1B_08 = metrics_boxes.Where(a => a != null).Select(a => a.F1B_08).DefaultIfEmpty(0).Average();
            F1B_09 = metrics_boxes.Where(a => a != null).Select(a => a.F1B_09).DefaultIfEmpty(0).Average();
            F1B_10 = metrics_boxes.Where(a => a != null).Select(a => a.F1B_10).DefaultIfEmpty(0).Average();
        }

        internal static double fbeta2(double PPV, double TPR, double fbeta)
        {
            return (double.IsNaN(PPV) || double.IsNaN(TPR) || PPV == 0.0d || TPR == 0.0d) ? 0.0d : (double)(1 / (fbeta * (1 / PPV) + (1 - fbeta) * (1 / TPR)));
        }

        internal double[] get_values_by_names(string[] names)
        {
            return names.Select(a => get_value_by_name(a)).ToArray();
        }
        
        internal double get_value_by_name(string name)
        {
            var metrics = this;
            if (string.Equals(name, nameof(metrics.P), StringComparison.OrdinalIgnoreCase)) return metrics.P;
            if (string.Equals(name, nameof(metrics.N), StringComparison.OrdinalIgnoreCase)) return metrics.N;
            if (string.Equals(name, nameof(metrics.TP), StringComparison.OrdinalIgnoreCase)) return metrics.TP;
            if (string.Equals(name, nameof(metrics.FP), StringComparison.OrdinalIgnoreCase)) return metrics.FP;
            if (string.Equals(name, nameof(metrics.TN), StringComparison.OrdinalIgnoreCase)) return metrics.TN;
            if (string.Equals(name, nameof(metrics.FN), StringComparison.OrdinalIgnoreCase)) return metrics.FN;
            if (string.Equals(name, nameof(metrics.TPR), StringComparison.OrdinalIgnoreCase)) return metrics.TPR;
            if (string.Equals(name, nameof(metrics.TNR), StringComparison.OrdinalIgnoreCase)) return metrics.TNR;
            if (string.Equals(name, nameof(metrics.PPV), StringComparison.OrdinalIgnoreCase)) return metrics.PPV;
            if (string.Equals(name, nameof(metrics.Precision), StringComparison.OrdinalIgnoreCase)) return metrics.Precision;
            if (string.Equals(name, nameof(metrics.Prevalence), StringComparison.OrdinalIgnoreCase)) return metrics.Prevalence;
            if (string.Equals(name, nameof(metrics.MCR), StringComparison.OrdinalIgnoreCase)) return metrics.MCR;
            if (string.Equals(name, nameof(metrics.ER), StringComparison.OrdinalIgnoreCase)) return metrics.ER;
            if (string.Equals(name, nameof(metrics.NER), StringComparison.OrdinalIgnoreCase)) return metrics.NER;
            if (string.Equals(name, nameof(metrics.CNER), StringComparison.OrdinalIgnoreCase)) return metrics.CNER;
            if (string.Equals(name, nameof(metrics.Kappa), StringComparison.OrdinalIgnoreCase)) return metrics.Kappa;
            if (string.Equals(name, nameof(metrics.Overlap), StringComparison.OrdinalIgnoreCase)) return metrics.Overlap;
            if (string.Equals(name, nameof(metrics.RND_ACC), StringComparison.OrdinalIgnoreCase)) return metrics.RND_ACC;
            if (string.Equals(name, nameof(metrics.Support), StringComparison.OrdinalIgnoreCase)) return metrics.Support;
            if (string.Equals(name, nameof(metrics.BaseRate), StringComparison.OrdinalIgnoreCase)) return metrics.BaseRate;
            if (string.Equals(name, nameof(metrics.YoudenIndex), StringComparison.OrdinalIgnoreCase)) return metrics.YoudenIndex;
            if (string.Equals(name, nameof(metrics.NPV), StringComparison.OrdinalIgnoreCase)) return metrics.NPV;
            if (string.Equals(name, nameof(metrics.FNR), StringComparison.OrdinalIgnoreCase)) return metrics.FNR;
            if (string.Equals(name, nameof(metrics.FPR), StringComparison.OrdinalIgnoreCase)) return metrics.FPR;
            if (string.Equals(name, nameof(metrics.FDR), StringComparison.OrdinalIgnoreCase)) return metrics.FDR;
            if (string.Equals(name, nameof(metrics.FOR), StringComparison.OrdinalIgnoreCase)) return metrics.FOR;
            if (string.Equals(name, nameof(metrics.ACC), StringComparison.OrdinalIgnoreCase)) return metrics.ACC;
            if (string.Equals(name, nameof(metrics.GMean), StringComparison.OrdinalIgnoreCase)) return metrics.GMean;
            if (string.Equals(name, nameof(metrics.F1S), StringComparison.OrdinalIgnoreCase)) return metrics.F1S;
            if (string.Equals(name, nameof(metrics.G1S), StringComparison.OrdinalIgnoreCase)) return metrics.G1S;
            if (string.Equals(name, nameof(metrics.MCC), StringComparison.OrdinalIgnoreCase)) return metrics.MCC;
            if (string.Equals(name, nameof(metrics.Informedness), StringComparison.OrdinalIgnoreCase)) return metrics.Informedness;
            if (string.Equals(name, nameof(metrics.Markedness), StringComparison.OrdinalIgnoreCase)) return metrics.Markedness;
            if (string.Equals(name, nameof(metrics.BalancedAccuracy), StringComparison.OrdinalIgnoreCase)) return metrics.BalancedAccuracy;
            if (string.Equals(name, nameof(metrics.ROC_AUC_Approx_All), StringComparison.OrdinalIgnoreCase)) return metrics.ROC_AUC_Approx_All;
            if (string.Equals(name, nameof(metrics.ROC_AUC_Approx_11p), StringComparison.OrdinalIgnoreCase)) return metrics.ROC_AUC_Approx_11p;
            if (string.Equals(name, nameof(metrics.ROC_AUC_All), StringComparison.OrdinalIgnoreCase)) return metrics.ROC_AUC_All;
            if (string.Equals(name, nameof(metrics.PR_AUC_Approx_All), StringComparison.OrdinalIgnoreCase)) return metrics.PR_AUC_Approx_All;
            if (string.Equals(name, nameof(metrics.PR_AUC_Approx_11p), StringComparison.OrdinalIgnoreCase)) return metrics.PR_AUC_Approx_11p;
            if (string.Equals(name, nameof(metrics.PRI_AUC_Approx_All), StringComparison.OrdinalIgnoreCase)) return metrics.PRI_AUC_Approx_All;
            if (string.Equals(name, nameof(metrics.PRI_AUC_Approx_11p), StringComparison.OrdinalIgnoreCase)) return metrics.PRI_AUC_Approx_11p;
            if (string.Equals(name, nameof(metrics.AP_All), StringComparison.OrdinalIgnoreCase)) return metrics.AP_All;
            if (string.Equals(name, nameof(metrics.AP_11p), StringComparison.OrdinalIgnoreCase)) return metrics.AP_11p;
            if (string.Equals(name, nameof(metrics.API_All), StringComparison.OrdinalIgnoreCase)) return metrics.API_All;
            if (string.Equals(name, nameof(metrics.API_11p), StringComparison.OrdinalIgnoreCase)) return metrics.API_11p;
            if (string.Equals(name, nameof(metrics.Brier_Inverse_All), StringComparison.OrdinalIgnoreCase)) return metrics.Brier_Inverse_All;
            if (string.Equals(name, nameof(metrics.LRP), StringComparison.OrdinalIgnoreCase)) return metrics.LRP;
            if (string.Equals(name, nameof(metrics.LRN), StringComparison.OrdinalIgnoreCase)) return metrics.LRN;
            if (string.Equals(name, nameof(metrics.DOR), StringComparison.OrdinalIgnoreCase)) return metrics.DOR;
            if (string.Equals(name, nameof(metrics.PrevalenceThreshold), StringComparison.OrdinalIgnoreCase)) return metrics.PrevalenceThreshold;
            if (string.Equals(name, nameof(metrics.CriticalSuccessIndex), StringComparison.OrdinalIgnoreCase)) return metrics.CriticalSuccessIndex;
            if (string.Equals(name, nameof(metrics.F1B_00), StringComparison.OrdinalIgnoreCase)) return metrics.F1B_00;
            if (string.Equals(name, nameof(metrics.F1B_01), StringComparison.OrdinalIgnoreCase)) return metrics.F1B_01;
            if (string.Equals(name, nameof(metrics.F1B_02), StringComparison.OrdinalIgnoreCase)) return metrics.F1B_02;
            if (string.Equals(name, nameof(metrics.F1B_03), StringComparison.OrdinalIgnoreCase)) return metrics.F1B_03;
            if (string.Equals(name, nameof(metrics.F1B_04), StringComparison.OrdinalIgnoreCase)) return metrics.F1B_04;
            if (string.Equals(name, nameof(metrics.F1B_05), StringComparison.OrdinalIgnoreCase)) return metrics.F1B_05;
            if (string.Equals(name, nameof(metrics.F1B_06), StringComparison.OrdinalIgnoreCase)) return metrics.F1B_06;
            if (string.Equals(name, nameof(metrics.F1B_07), StringComparison.OrdinalIgnoreCase)) return metrics.F1B_07;
            if (string.Equals(name, nameof(metrics.F1B_08), StringComparison.OrdinalIgnoreCase)) return metrics.F1B_08;
            if (string.Equals(name, nameof(metrics.F1B_09), StringComparison.OrdinalIgnoreCase)) return metrics.F1B_09;
            if (string.Equals(name, nameof(metrics.F1B_10), StringComparison.OrdinalIgnoreCase)) return metrics.F1B_10;
            return default;
        }

        internal List<double> get_specific_values(cross_validation_metrics cross_validation_metrics)
        {
            var metric_values = new List<double>();

            if (cross_validation_metrics == 0) throw new Exception();

            if (cross_validation_metrics.HasFlag(cross_validation_metrics.TP)) { metric_values.Add(TP); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FP)) { metric_values.Add(FP); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.TN)) { metric_values.Add(TN); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FN)) { metric_values.Add(FN); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.TPR)) { metric_values.Add(TPR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.TNR)) { metric_values.Add(TNR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PPV)) { metric_values.Add(PPV); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Precision)) { metric_values.Add(Precision); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Prevalence)) { metric_values.Add(Prevalence); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.MCR)) { metric_values.Add(MCR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ER)) { metric_values.Add(ER); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.NER)) { metric_values.Add(NER); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.CNER)) { metric_values.Add(CNER); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Kappa)) { metric_values.Add(Kappa); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Overlap)) { metric_values.Add(Overlap); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.RND_ACC)) { metric_values.Add(RND_ACC); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Support)) { metric_values.Add(Support); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.BaseRate)) { metric_values.Add(BaseRate); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.YoudenIndex)) { metric_values.Add(YoudenIndex); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.NPV)) { metric_values.Add(NPV); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FNR)) { metric_values.Add(FNR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FPR)) { metric_values.Add(FPR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FDR)) { metric_values.Add(FDR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FOR)) { metric_values.Add(FOR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ACC)) { metric_values.Add(ACC); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.GM)) { metric_values.Add(GMean); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1S)) { metric_values.Add(F1S); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.G1S)) { metric_values.Add(G1S); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.MCC)) { metric_values.Add(MCC); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Informedness)) { metric_values.Add(Informedness); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Markedness)) { metric_values.Add(Markedness); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.BalancedAccuracy)) { metric_values.Add(BalancedAccuracy); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ROC_AUC_Approx_All)) { metric_values.Add(ROC_AUC_Approx_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ROC_AUC_Approx_11p)) { metric_values.Add(ROC_AUC_Approx_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ROC_AUC_All)) { metric_values.Add(ROC_AUC_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PR_AUC_Approx_All)) { metric_values.Add(PR_AUC_Approx_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PR_AUC_Approx_11p)) { metric_values.Add(PR_AUC_Approx_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PRI_AUC_Approx_All)) { metric_values.Add(PRI_AUC_Approx_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PRI_AUC_Approx_11p)) { metric_values.Add(PRI_AUC_Approx_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.AP_All)) { metric_values.Add(AP_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.AP_11p)) { metric_values.Add(AP_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.API_All)) { metric_values.Add(API_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.API_11p)) { metric_values.Add(API_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Brier_Inverse_All)) { metric_values.Add(Brier_Inverse_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.LRP)) { metric_values.Add(LRP); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.LRN)) { metric_values.Add(LRN); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.DOR)) { metric_values.Add(DOR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PrevalenceThreshold)) { metric_values.Add(PrevalenceThreshold); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.CriticalSuccessIndex)) { metric_values.Add(CriticalSuccessIndex); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_00)) { metric_values.Add(F1B_00); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_01)) { metric_values.Add(F1B_01); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_02)) { metric_values.Add(F1B_02); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_03)) { metric_values.Add(F1B_03); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_04)) { metric_values.Add(F1B_04); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_05)) { metric_values.Add(F1B_05); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_06)) { metric_values.Add(F1B_06); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_07)) { metric_values.Add(F1B_07); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_08)) { metric_values.Add(F1B_08); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_09)) { metric_values.Add(F1B_09); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_10)) { metric_values.Add(F1B_10); }

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
                    (nameof(YoudenIndex), YoudenIndex),
                    (nameof(NPV), NPV),
                    (nameof(FNR), FNR),
                    (nameof(FPR), FPR),
                    (nameof(FDR), FDR),
                    (nameof(FOR), FOR),
                    (nameof(ACC), ACC),
                    (nameof(GMean), GMean),
                    (nameof(F1S), F1S),
                    (nameof(G1S), G1S),
                    (nameof(MCC), MCC),
                    (nameof(Informedness), Informedness),
                    (nameof(Markedness), Markedness),
                    (nameof(BalancedAccuracy), BalancedAccuracy),
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
                    (nameof(Brier_Inverse_All), Brier_Inverse_All),
                    (nameof(LRP), LRP),
                    (nameof(LRN), LRN),
                    (nameof(DOR), DOR),
                    (nameof(PrevalenceThreshold), PrevalenceThreshold),
                    (nameof(CriticalSuccessIndex), CriticalSuccessIndex),
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
            YoudenIndex = 1UL << 19,
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
            Informedness = 1UL << 30,
            Markedness = 1UL << 31,
            BalancedAccuracy = 1UL << 32,
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
            Brier_Inverse_All = 1UL << 44,
            LRP = 1UL << 45,
            LRN = 1UL << 46,
            DOR = 1UL << 47,
            PrevalenceThreshold = 1UL << 48,
            CriticalSuccessIndex = 1UL << 49,

            F1B_00 = 1UL << 50,
            F1B_01 = 1UL << 51,
            F1B_02 = 1UL << 52,
            F1B_03 = 1UL << 53,
            F1B_04 = 1UL << 54,
            F1B_05 = 1UL << 55,
            F1B_06 = 1UL << 56,
            F1B_07 = 1UL << 57,
            F1B_08 = 1UL << 58,
            F1B_09 = 1UL << 59,
            F1B_10 = 1UL << 60,

            //ROC_AUC_11p = 1UL << 36,
        }


        public static readonly string[] csv_header_values_array = new string[]
            {
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
                nameof(YoudenIndex),
                nameof(NPV),
                nameof(FNR),
                nameof(FPR),
                nameof(FDR),
                nameof(FOR),
                nameof(ACC),
                nameof(GMean),
                nameof(F1S),
                nameof(G1S),
                nameof(MCC),
                nameof(Informedness),
                nameof(Markedness),
                nameof(BalancedAccuracy),
                nameof(ROC_AUC_Approx_All),
                nameof(ROC_AUC_Approx_11p),
                nameof(ROC_AUC_All),
                nameof(PR_AUC_Approx_All),
                nameof(PR_AUC_Approx_11p),
                nameof(PRI_AUC_Approx_All),
                nameof(PRI_AUC_Approx_11p),
                nameof(AP_All),
                nameof(AP_11p),
                nameof(API_All),
                nameof(API_11p),
                nameof(Brier_Inverse_All),
                nameof(LRP),
                nameof(LRN),
                nameof(DOR),
                nameof(PrevalenceThreshold),
                nameof(CriticalSuccessIndex),
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
            };

        public static readonly string csv_header_string = string.Join(",", csv_header_values_array);

        public string[] csv_values_array()
        {
            return new string[]
            {
                P.ToString("G17", NumberFormatInfo.InvariantInfo),
                N.ToString("G17", NumberFormatInfo.InvariantInfo),
                TP.ToString("G17", NumberFormatInfo.InvariantInfo),
                FP.ToString("G17", NumberFormatInfo.InvariantInfo),
                TN.ToString("G17", NumberFormatInfo.InvariantInfo),
                FN.ToString("G17", NumberFormatInfo.InvariantInfo),
                TPR.ToString("G17", NumberFormatInfo.InvariantInfo),
                TNR.ToString("G17", NumberFormatInfo.InvariantInfo),
                PPV.ToString("G17", NumberFormatInfo.InvariantInfo),
                Precision.ToString("G17", NumberFormatInfo.InvariantInfo),
                Prevalence.ToString("G17", NumberFormatInfo.InvariantInfo),
                MCR.ToString("G17", NumberFormatInfo.InvariantInfo),
                ER.ToString("G17", NumberFormatInfo.InvariantInfo),
                NER.ToString("G17", NumberFormatInfo.InvariantInfo),
                CNER.ToString("G17", NumberFormatInfo.InvariantInfo),
                Kappa.ToString("G17", NumberFormatInfo.InvariantInfo),
                Overlap.ToString("G17", NumberFormatInfo.InvariantInfo),
                RND_ACC.ToString("G17", NumberFormatInfo.InvariantInfo),
                Support.ToString("G17", NumberFormatInfo.InvariantInfo),
                BaseRate.ToString("G17", NumberFormatInfo.InvariantInfo),
                YoudenIndex.ToString("G17", NumberFormatInfo.InvariantInfo),
                NPV.ToString("G17", NumberFormatInfo.InvariantInfo),
                FNR.ToString("G17", NumberFormatInfo.InvariantInfo),
                FPR.ToString("G17", NumberFormatInfo.InvariantInfo),
                FDR.ToString("G17", NumberFormatInfo.InvariantInfo),
                FOR.ToString("G17", NumberFormatInfo.InvariantInfo),
                ACC.ToString("G17", NumberFormatInfo.InvariantInfo),
                GMean.ToString("G17", NumberFormatInfo.InvariantInfo),
                F1S.ToString("G17", NumberFormatInfo.InvariantInfo),
                G1S.ToString("G17", NumberFormatInfo.InvariantInfo),
                MCC.ToString("G17", NumberFormatInfo.InvariantInfo),
                Informedness.ToString("G17", NumberFormatInfo.InvariantInfo),
                Markedness.ToString("G17", NumberFormatInfo.InvariantInfo),
                BalancedAccuracy.ToString("G17", NumberFormatInfo.InvariantInfo),
                ROC_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                ROC_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                ROC_AUC_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                PR_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                PR_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                PRI_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                PRI_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                AP_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                AP_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                API_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                API_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                Brier_Inverse_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                LRP.ToString("G17", NumberFormatInfo.InvariantInfo),
                LRN.ToString("G17", NumberFormatInfo.InvariantInfo),
                DOR.ToString("G17", NumberFormatInfo.InvariantInfo),
                PrevalenceThreshold.ToString("G17", NumberFormatInfo.InvariantInfo),
                CriticalSuccessIndex.ToString("G17", NumberFormatInfo.InvariantInfo),
                F1B_00.ToString("G17", NumberFormatInfo.InvariantInfo),
                F1B_01.ToString("G17", NumberFormatInfo.InvariantInfo),
                F1B_02.ToString("G17", NumberFormatInfo.InvariantInfo),
                F1B_03.ToString("G17", NumberFormatInfo.InvariantInfo),
                F1B_04.ToString("G17", NumberFormatInfo.InvariantInfo),
                F1B_05.ToString("G17", NumberFormatInfo.InvariantInfo),
                F1B_06.ToString("G17", NumberFormatInfo.InvariantInfo),
                F1B_07.ToString("G17", NumberFormatInfo.InvariantInfo),
                F1B_08.ToString("G17", NumberFormatInfo.InvariantInfo),
                F1B_09.ToString("G17", NumberFormatInfo.InvariantInfo),
                F1B_10.ToString("G17", NumberFormatInfo.InvariantInfo),
            }.Select(a => a.Replace(",", ";", StringComparison.OrdinalIgnoreCase)).ToArray();
        }

        public string csv_values_string()
        {
            return string.Join(",", csv_values_array());
        }
    }
}
