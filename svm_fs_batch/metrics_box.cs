using System;
using System.Collections.Generic;
using System.Globalization;

namespace svm_fs_batch
{
    internal class metrics_box
    {
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

            if (divisor != null)
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

        internal static double fbeta2(double PPV, double TPR, double fbeta)
        {
            var fb = (PPV == 0.0 || TPR == 0.0) ? (double)0.0 : (double)(1 / (fbeta * (1 / PPV) + (1 - fbeta) * (1 / TPR)));

            return fb;
        }

        internal double get_value_by_name(string name)
        {
            var metrics = this;
            if (string.Equals(name, nameof(metrics.P), StringComparison.InvariantCultureIgnoreCase)) return metrics.P;
            if (string.Equals(name, nameof(metrics.N), StringComparison.InvariantCultureIgnoreCase)) return metrics.N;
            if (string.Equals(name, nameof(metrics.TP), StringComparison.InvariantCultureIgnoreCase)) return metrics.TP;
            if (string.Equals(name, nameof(metrics.FP), StringComparison.InvariantCultureIgnoreCase)) return metrics.FP;
            if (string.Equals(name, nameof(metrics.TN), StringComparison.InvariantCultureIgnoreCase)) return metrics.TN;
            if (string.Equals(name, nameof(metrics.FN), StringComparison.InvariantCultureIgnoreCase)) return metrics.FN;
            if (string.Equals(name, nameof(metrics.TPR), StringComparison.InvariantCultureIgnoreCase)) return metrics.TPR;
            if (string.Equals(name, nameof(metrics.TNR), StringComparison.InvariantCultureIgnoreCase)) return metrics.TNR;
            if (string.Equals(name, nameof(metrics.PPV), StringComparison.InvariantCultureIgnoreCase)) return metrics.PPV;
            if (string.Equals(name, nameof(metrics.Precision), StringComparison.InvariantCultureIgnoreCase)) return metrics.Precision;
            if (string.Equals(name, nameof(metrics.Prevalence), StringComparison.InvariantCultureIgnoreCase)) return metrics.Prevalence;
            if (string.Equals(name, nameof(metrics.MCR), StringComparison.InvariantCultureIgnoreCase)) return metrics.MCR;
            if (string.Equals(name, nameof(metrics.ER), StringComparison.InvariantCultureIgnoreCase)) return metrics.ER;
            if (string.Equals(name, nameof(metrics.NER), StringComparison.InvariantCultureIgnoreCase)) return metrics.NER;
            if (string.Equals(name, nameof(metrics.CNER), StringComparison.InvariantCultureIgnoreCase)) return metrics.CNER;
            if (string.Equals(name, nameof(metrics.Kappa), StringComparison.InvariantCultureIgnoreCase)) return metrics.Kappa;
            if (string.Equals(name, nameof(metrics.Overlap), StringComparison.InvariantCultureIgnoreCase)) return metrics.Overlap;
            if (string.Equals(name, nameof(metrics.RND_ACC), StringComparison.InvariantCultureIgnoreCase)) return metrics.RND_ACC;
            if (string.Equals(name, nameof(metrics.Support), StringComparison.InvariantCultureIgnoreCase)) return metrics.Support;
            if (string.Equals(name, nameof(metrics.BaseRate), StringComparison.InvariantCultureIgnoreCase)) return metrics.BaseRate;
            if (string.Equals(name, nameof(metrics.YoudenIndex), StringComparison.InvariantCultureIgnoreCase)) return metrics.YoudenIndex;
            if (string.Equals(name, nameof(metrics.NPV), StringComparison.InvariantCultureIgnoreCase)) return metrics.NPV;
            if (string.Equals(name, nameof(metrics.FNR), StringComparison.InvariantCultureIgnoreCase)) return metrics.FNR;
            if (string.Equals(name, nameof(metrics.FPR), StringComparison.InvariantCultureIgnoreCase)) return metrics.FPR;
            if (string.Equals(name, nameof(metrics.FDR), StringComparison.InvariantCultureIgnoreCase)) return metrics.FDR;
            if (string.Equals(name, nameof(metrics.FOR), StringComparison.InvariantCultureIgnoreCase)) return metrics.FOR;
            if (string.Equals(name, nameof(metrics.ACC), StringComparison.InvariantCultureIgnoreCase)) return metrics.ACC;
            if (string.Equals(name, nameof(metrics.GMean), StringComparison.InvariantCultureIgnoreCase)) return metrics.GMean;
            if (string.Equals(name, nameof(metrics.F1S), StringComparison.InvariantCultureIgnoreCase)) return metrics.F1S;
            if (string.Equals(name, nameof(metrics.G1S), StringComparison.InvariantCultureIgnoreCase)) return metrics.G1S;
            if (string.Equals(name, nameof(metrics.MCC), StringComparison.InvariantCultureIgnoreCase)) return metrics.MCC;
            if (string.Equals(name, nameof(metrics.Informedness), StringComparison.InvariantCultureIgnoreCase)) return metrics.Informedness;
            if (string.Equals(name, nameof(metrics.Markedness), StringComparison.InvariantCultureIgnoreCase)) return metrics.Markedness;
            if (string.Equals(name, nameof(metrics.BalancedAccuracy), StringComparison.InvariantCultureIgnoreCase)) return metrics.BalancedAccuracy;
            if (string.Equals(name, nameof(metrics.ROC_AUC_Approx_All), StringComparison.InvariantCultureIgnoreCase)) return metrics.ROC_AUC_Approx_All;
            if (string.Equals(name, nameof(metrics.ROC_AUC_Approx_11p), StringComparison.InvariantCultureIgnoreCase)) return metrics.ROC_AUC_Approx_11p;
            if (string.Equals(name, nameof(metrics.ROC_AUC_All), StringComparison.InvariantCultureIgnoreCase)) return metrics.ROC_AUC_All;
            if (string.Equals(name, nameof(metrics.PR_AUC_Approx_All), StringComparison.InvariantCultureIgnoreCase)) return metrics.PR_AUC_Approx_All;
            if (string.Equals(name, nameof(metrics.PR_AUC_Approx_11p), StringComparison.InvariantCultureIgnoreCase)) return metrics.PR_AUC_Approx_11p;
            if (string.Equals(name, nameof(metrics.PRI_AUC_Approx_All), StringComparison.InvariantCultureIgnoreCase)) return metrics.PRI_AUC_Approx_All;
            if (string.Equals(name, nameof(metrics.PRI_AUC_Approx_11p), StringComparison.InvariantCultureIgnoreCase)) return metrics.PRI_AUC_Approx_11p;
            if (string.Equals(name, nameof(metrics.AP_All), StringComparison.InvariantCultureIgnoreCase)) return metrics.AP_All;
            if (string.Equals(name, nameof(metrics.AP_11p), StringComparison.InvariantCultureIgnoreCase)) return metrics.AP_11p;
            if (string.Equals(name, nameof(metrics.API_All), StringComparison.InvariantCultureIgnoreCase)) return metrics.API_All;
            if (string.Equals(name, nameof(metrics.API_11p), StringComparison.InvariantCultureIgnoreCase)) return metrics.API_11p;
            if (string.Equals(name, nameof(metrics.Brier_Inverse_All), StringComparison.InvariantCultureIgnoreCase)) return metrics.Brier_Inverse_All;
            if (string.Equals(name, nameof(metrics.LRP), StringComparison.InvariantCultureIgnoreCase)) return metrics.LRP;
            if (string.Equals(name, nameof(metrics.LRN), StringComparison.InvariantCultureIgnoreCase)) return metrics.LRN;
            if (string.Equals(name, nameof(metrics.DOR), StringComparison.InvariantCultureIgnoreCase)) return metrics.DOR;
            if (string.Equals(name, nameof(metrics.PrevalenceThreshold), StringComparison.InvariantCultureIgnoreCase)) return metrics.PrevalenceThreshold;
            if (string.Equals(name, nameof(metrics.CriticalSuccessIndex), StringComparison.InvariantCultureIgnoreCase)) return metrics.CriticalSuccessIndex;
            if (string.Equals(name, nameof(metrics.F1B_00), StringComparison.InvariantCultureIgnoreCase)) return metrics.F1B_00;
            if (string.Equals(name, nameof(metrics.F1B_01), StringComparison.InvariantCultureIgnoreCase)) return metrics.F1B_01;
            if (string.Equals(name, nameof(metrics.F1B_02), StringComparison.InvariantCultureIgnoreCase)) return metrics.F1B_02;
            if (string.Equals(name, nameof(metrics.F1B_03), StringComparison.InvariantCultureIgnoreCase)) return metrics.F1B_03;
            if (string.Equals(name, nameof(metrics.F1B_04), StringComparison.InvariantCultureIgnoreCase)) return metrics.F1B_04;
            if (string.Equals(name, nameof(metrics.F1B_05), StringComparison.InvariantCultureIgnoreCase)) return metrics.F1B_05;
            if (string.Equals(name, nameof(metrics.F1B_06), StringComparison.InvariantCultureIgnoreCase)) return metrics.F1B_06;
            if (string.Equals(name, nameof(metrics.F1B_07), StringComparison.InvariantCultureIgnoreCase)) return metrics.F1B_07;
            if (string.Equals(name, nameof(metrics.F1B_08), StringComparison.InvariantCultureIgnoreCase)) return metrics.F1B_08;
            if (string.Equals(name, nameof(metrics.F1B_09), StringComparison.InvariantCultureIgnoreCase)) return metrics.F1B_09;
            if (string.Equals(name, nameof(metrics.F1B_10), StringComparison.InvariantCultureIgnoreCase)) return metrics.F1B_10;
            return default;
        }

        internal List<double> get_specific_values(cross_validation_metrics cross_validation_metrics)
        {
            var metric_values = new List<double>();

            if (cross_validation_metrics == 0) throw new Exception();

            var metrics = this;
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.TP)) { metric_values.Add(metrics.TP); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FP)) { metric_values.Add(metrics.FP); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.TN)) { metric_values.Add(metrics.TN); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FN)) { metric_values.Add(metrics.FN); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.TPR)) { metric_values.Add(metrics.TPR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.TNR)) { metric_values.Add(metrics.TNR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PPV)) { metric_values.Add(metrics.PPV); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Precision)) { metric_values.Add(metrics.Precision); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Prevalence)) { metric_values.Add(metrics.Prevalence); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.MCR)) { metric_values.Add(metrics.MCR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ER)) { metric_values.Add(metrics.ER); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.NER)) { metric_values.Add(metrics.NER); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.CNER)) { metric_values.Add(metrics.CNER); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Kappa)) { metric_values.Add(metrics.Kappa); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Overlap)) { metric_values.Add(metrics.Overlap); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.RND_ACC)) { metric_values.Add(metrics.RND_ACC); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Support)) { metric_values.Add(metrics.Support); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.BaseRate)) { metric_values.Add(metrics.BaseRate); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.YoudenIndex)) { metric_values.Add(metrics.YoudenIndex); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.NPV)) { metric_values.Add(metrics.NPV); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FNR)) { metric_values.Add(metrics.FNR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FPR)) { metric_values.Add(metrics.FPR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FDR)) { metric_values.Add(metrics.FDR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FOR)) { metric_values.Add(metrics.FOR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ACC)) { metric_values.Add(metrics.ACC); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.GM)) { metric_values.Add(metrics.GMean); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1S)) { metric_values.Add(metrics.F1S); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.G1S)) { metric_values.Add(metrics.G1S); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.MCC)) { metric_values.Add(metrics.MCC); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Informedness)) { metric_values.Add(metrics.Informedness); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Markedness)) { metric_values.Add(metrics.Markedness); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.BalancedAccuracy)) { metric_values.Add(metrics.BalancedAccuracy); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ROC_AUC_Approx_All)) { metric_values.Add(metrics.ROC_AUC_Approx_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ROC_AUC_Approx_11p)) { metric_values.Add(metrics.ROC_AUC_Approx_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ROC_AUC_All)) { metric_values.Add(metrics.ROC_AUC_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PR_AUC_Approx_All)) { metric_values.Add(metrics.PR_AUC_Approx_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PR_AUC_Approx_11p)) { metric_values.Add(metrics.PR_AUC_Approx_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PRI_AUC_Approx_All)) { metric_values.Add(metrics.PRI_AUC_Approx_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PRI_AUC_Approx_11p)) { metric_values.Add(metrics.PRI_AUC_Approx_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.AP_All)) { metric_values.Add(metrics.AP_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.AP_11p)) { metric_values.Add(metrics.AP_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.API_All)) { metric_values.Add(metrics.API_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.API_11p)) { metric_values.Add(metrics.API_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Brier_Inverse_All)) { metric_values.Add(metrics.Brier_Inverse_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.LRP)) { metric_values.Add(metrics.LRP); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.LRN)) { metric_values.Add(metrics.LRN); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.DOR)) { metric_values.Add(metrics.DOR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PrevalenceThreshold)) { metric_values.Add(metrics.PrevalenceThreshold); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.CriticalSuccessIndex)) { metric_values.Add(metrics.CriticalSuccessIndex); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_00)) { metric_values.Add(metrics.F1B_00); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_01)) { metric_values.Add(metrics.F1B_01); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_02)) { metric_values.Add(metrics.F1B_02); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_03)) { metric_values.Add(metrics.F1B_03); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_04)) { metric_values.Add(metrics.F1B_04); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_05)) { metric_values.Add(metrics.F1B_05); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_06)) { metric_values.Add(metrics.F1B_06); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_07)) { metric_values.Add(metrics.F1B_07); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_08)) { metric_values.Add(metrics.F1B_08); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_09)) { metric_values.Add(metrics.F1B_09); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_10)) { metric_values.Add(metrics.F1B_10); }

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

        public static readonly string csv_header = string.Join(",", csv_header_values);
        public static readonly string[] csv_header_values = new string[]
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

        public string csv_values()
        {
            return string.Join(",", csv_values_array());
        }

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
            };
        }
    }
}
