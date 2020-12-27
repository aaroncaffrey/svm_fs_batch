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

        internal double cm_P;
        internal double cm_N;

        internal double cm_P_TP;
        internal double cm_P_FN;
        internal double cm_N_TN;
        internal double cm_N_FP;

        internal double p_TPR;
        internal double p_TNR;
        internal double p_PPV;
        internal double p_Precision;
        internal double p_Prevalence;
        internal double p_MCR;
        internal double p_ER;
        internal double p_NER;
        internal double p_CNER;
        internal double p_Kappa;
        internal double p_Overlap;
        internal double p_RND_ACC;
        internal double p_Support;
        internal double p_BaseRate;
        internal double p_YoudenIndex;
        internal double p_NPV;
        internal double p_FNR;
        internal double p_FPR;
        internal double p_FDR;
        internal double p_FOR;
        internal double p_ACC;
        internal double p_GMean;
        internal double p_F1S;
        internal double p_G1S;
        internal double p_MCC;
        internal double p_Informedness;
        internal double p_Markedness;
        internal double p_BalancedAccuracy;
        internal double p_ROC_AUC_Approx_All;
        internal double p_ROC_AUC_Approx_11p;
        internal double p_ROC_AUC_All;
        internal double p_PR_AUC_Approx_All;
        internal double p_PR_AUC_Approx_11p;
        internal double p_PRI_AUC_Approx_All;
        internal double p_PRI_AUC_Approx_11p;
        internal double p_AP_All;
        internal double p_AP_11p;
        internal double p_API_All;
        internal double p_API_11p;
        internal double p_Brier_Inverse_All;
        internal double p_LRP;
        internal double p_LRN;
        internal double p_DOR;
        internal double p_PrevalenceThreshold;
        internal double p_CriticalSuccessIndex;
        internal double p_F1B_00;
        internal double p_F1B_01;
        internal double p_F1B_02;
        internal double p_F1B_03;
        internal double p_F1B_04;
        internal double p_F1B_05;
        internal double p_F1B_06;
        internal double p_F1B_07;
        internal double p_F1B_08;
        internal double p_F1B_09;
        internal double p_F1B_10;

        public metrics_box()
        {

        }

        public metrics_box(metrics_box metrics)
        {
            cm_P = metrics.cm_P;
            cm_N = metrics.cm_N;
            cm_P_TP = metrics.cm_P_TP;
            cm_P_FN = metrics.cm_P_FN;
            cm_N_TN = metrics.cm_N_TN;
            cm_N_FP = metrics.cm_N_FP;
            p_TPR = metrics.p_TPR;
            p_TNR = metrics.p_TNR;
            p_PPV = metrics.p_PPV;
            p_Precision = metrics.p_Precision;
            p_Prevalence = metrics.p_Prevalence;
            p_MCR = metrics.p_MCR;
            p_ER = metrics.p_ER;
            p_NER = metrics.p_NER;
            p_CNER = metrics.p_CNER;
            p_Kappa = metrics.p_Kappa;
            p_Overlap = metrics.p_Overlap;
            p_RND_ACC = metrics.p_RND_ACC;
            p_Support = metrics.p_Support;
            p_BaseRate = metrics.p_BaseRate;
            p_YoudenIndex = metrics.p_YoudenIndex;
            p_NPV = metrics.p_NPV;
            p_FNR = metrics.p_FNR;
            p_FPR = metrics.p_FPR;
            p_FDR = metrics.p_FDR;
            p_FOR = metrics.p_FOR;
            p_ACC = metrics.p_ACC;
            p_GMean = metrics.p_GMean;
            p_F1S = metrics.p_F1S;
            p_G1S = metrics.p_G1S;
            p_MCC = metrics.p_MCC;
            p_Informedness = metrics.p_Informedness;
            p_Markedness = metrics.p_Markedness;
            p_BalancedAccuracy = metrics.p_BalancedAccuracy;
            p_ROC_AUC_Approx_All = metrics.p_ROC_AUC_Approx_All;
            p_ROC_AUC_Approx_11p = metrics.p_ROC_AUC_Approx_11p;
            p_ROC_AUC_All = metrics.p_ROC_AUC_All;
            p_PR_AUC_Approx_All = metrics.p_PR_AUC_Approx_All;
            p_PR_AUC_Approx_11p = metrics.p_PR_AUC_Approx_11p;
            p_PRI_AUC_Approx_All = metrics.p_PRI_AUC_Approx_All;
            p_PRI_AUC_Approx_11p = metrics.p_PRI_AUC_Approx_11p;
            p_AP_All = metrics.p_AP_All;
            p_AP_11p = metrics.p_AP_11p;
            p_API_All = metrics.p_API_All;
            p_API_11p = metrics.p_API_11p;
            p_Brier_Inverse_All = metrics.p_Brier_Inverse_All;
            p_LRP = metrics.p_LRP;
            p_LRN = metrics.p_LRN;
            p_DOR = metrics.p_DOR;
            p_PrevalenceThreshold = metrics.p_PrevalenceThreshold;
            p_CriticalSuccessIndex = metrics.p_CriticalSuccessIndex;
            p_F1B_00 = metrics.p_F1B_00;
            p_F1B_01 = metrics.p_F1B_01;
            p_F1B_02 = metrics.p_F1B_02;
            p_F1B_03 = metrics.p_F1B_03;
            p_F1B_04 = metrics.p_F1B_04;
            p_F1B_05 = metrics.p_F1B_05;
            p_F1B_06 = metrics.p_F1B_06;
            p_F1B_07 = metrics.p_F1B_07;
            p_F1B_08 = metrics.p_F1B_08;
            p_F1B_09 = metrics.p_F1B_09;
            p_F1B_10 = metrics.p_F1B_10;
        }

        internal void set_cm(double? P = null, double? N = null, double? P_TP = null, double? P_FN = null, double? N_TN = null, double? N_FP = null)
        {
            // set the cm (P, N, TP, FN, TN, FP) inputs whilst allowing omission of calculable values from the non-omitted values

            if (P != null && P_TP == null && P_FN != null) P_TP = P - P_FN;
            if (P != null && P_TP != null && P_FN == null) P_FN = P - P_TP;
            if (P == null && P_TP != null && P_FN != null) P = P_TP + P_FN;
            if (P == null || P_TP == null || P_FN == null) throw new Exception();
            if (P != P_TP + P_FN) throw new Exception();

            if (N != null && N_TN == null && N_FP != null) N_TN = N - N_FP;
            if (N != null && N_TN != null && N_FP == null) N_FP = N - N_TN;
            if (N == null && N_TN != null && N_FP != null) N = N_TN + N_FP;
            if (N != N_TN + N_FP) throw new Exception();
            if (N == null || N_TN == null || N_FP == null) throw new Exception();

            cm_P = P.Value;
            cm_N = N.Value;

            cm_P_TP = P_TP.Value;
            cm_P_FN = P_FN.Value;
            cm_N_TN = N_TN.Value;
            cm_N_FP = N_FP.Value;

            calculate_metrics();
        }

        internal void set_random_perf()
        {
            cm_P_TP = cm_P / 2.0;
            cm_P_FN = cm_P / 2.0;

            cm_N_FP = cm_N / 2.0;
            cm_N_TN = cm_N / 2.0;

            calculate_metrics();
        }

        internal void apply_imbalance_correction1()
        {
            if (cm_P < cm_N)
            {
                var n_correct = (cm_N / cm_P);

                cm_P *= n_correct;
                cm_P_TP *= n_correct;
                cm_P_FN *= n_correct;

                cm_P = Math.Round(cm_P, 2);
                cm_P_TP = Math.Round(cm_P_TP, 2);
                cm_P_FN = Math.Round(cm_P_FN, 2);

                calculate_metrics();
            }
            else if (cm_N < cm_P)
            {
                var p_correct = (cm_P / cm_N);

                cm_N *= p_correct;
                cm_N_TN *= p_correct;
                cm_N_FP *= p_correct;

                cm_N = Math.Round(cm_N, 2);
                cm_N_TN = Math.Round(cm_N_TN, 2);
                cm_N_FP = Math.Round(cm_N_FP, 2);

                calculate_metrics();
            }
        }

        //internal void apply_imbalance_correction2()
        //{
        //    var correction = (cm_P <= cm_N) ? 1 + (1 - (cm_P / cm_N)) : 1 + (1 - (cm_N / cm_P));
        //
        //    p_TPR *= correction;
        //    p_TNR *= correction;
        //    p_PPV *= correction;
        //    p_Precision *= correction;
        //    p_Prevalence *= correction;
        //    p_MCR *= correction;
        //    p_ER *= correction;
        //    p_NER *= correction;
        //    p_CNER *= correction;
        //    p_Kappa *= correction;
        //    p_Overlap *= correction;
        //    p_RND_ACC *= correction;
        //    p_Support *= correction;
        //    p_BaseRate *= correction;
        //    p_YoudenIndex *= correction;
        //    p_NPV *= correction;
        //    p_FNR *= correction;
        //    p_FPR *= correction;
        //    p_FDR *= correction;
        //    p_FOR *= correction;
        //    p_ACC *= correction;
        //    p_GMean *= correction;
        //    p_F1S *= correction;
        //    p_G1S *= correction;
        //    p_MCC *= correction;
        //    p_Informedness *= correction;
        //    p_Markedness *= correction;
        //    p_BalancedAccuracy *= correction;
        //    p_ROC_AUC_Approx_All *= correction;
        //    p_ROC_AUC_Approx_11p *= correction;
        //    p_ROC_AUC_All *= correction;
        //    p_PR_AUC_Approx_All *= correction;
        //    p_PR_AUC_Approx_11p *= correction;
        //    p_PRI_AUC_Approx_All *= correction;
        //    p_PRI_AUC_Approx_11p *= correction;
        //    p_AP_All *= correction;
        //    p_AP_11p *= correction;
        //    p_API_All *= correction;
        //    p_API_11p *= correction;
        //    p_Brier_Inverse_All *= correction;
        //    p_LRP *= correction;
        //    p_LRN *= correction;
        //    p_DOR *= correction;
        //    p_PrevalenceThreshold *= correction;
        //    p_CriticalSuccessIndex *= correction;
        //    p_F1B_00 *= correction;
        //    p_F1B_01 *= correction;
        //    p_F1B_02 *= correction;
        //    p_F1B_03 *= correction;
        //    p_F1B_04 *= correction;
        //    p_F1B_05 *= correction;
        //    p_F1B_06 *= correction;
        //    p_F1B_07 *= correction;
        //    p_F1B_08 *= correction;
        //    p_F1B_09 *= correction;
        //    p_F1B_10 *= correction;
        //}

        internal void calculate_metrics()
        {
            const double zero = 0.0;
            const double one = 1.0;
            const double two = 2.0;

            p_Support = (cm_N) == zero ? zero : (double)(cm_P_TP + cm_N_FP) / (double)(cm_N);
            p_BaseRate = (cm_N) == zero ? zero : (double)(cm_P_TP + cm_P_FN) / (double)(cm_N);

            p_Prevalence = (cm_P + cm_N) == zero ? zero : (cm_P_FN + cm_P_TP) / (cm_P + cm_N);

            p_MCR = (cm_P + cm_N) == zero ? zero : (cm_N_FP + cm_P_FN) / (cm_P + cm_N);
            p_TPR = (cm_P_TP + cm_P_FN) == zero ? zero : (double)(cm_P_TP) / (double)(cm_P_TP + cm_P_FN);
            p_TNR = (cm_N_TN + cm_N_FP) == zero ? zero : (double)(cm_N_TN) / (double)(cm_N_TN + cm_N_FP);
            p_Precision = (cm_P_TP + cm_N_FP) == zero ? zero : (double)(cm_P_TP) / (double)(cm_P_TP + cm_N_FP);

            p_Overlap = (cm_P_TP + cm_N_FP + cm_P_FN) == zero ? zero : cm_P_TP / (cm_P_TP + cm_N_FP + cm_P_FN);

            // null error rate
            p_NER = (cm_P + cm_N) == zero ? zero : (cm_P > cm_N ? cm_P : cm_N) / (cm_P + cm_N);

            // class null error rate
            p_CNER = (cm_P + cm_N) == zero ? zero : cm_P / (cm_P + cm_N);

            // positive predictive value (differs from precision, can be equal)
            p_PPV = (p_TPR * p_Prevalence + (one - p_TNR) * (one - p_Prevalence)) == zero ? zero : (p_TPR * p_Prevalence) / (p_TPR * p_Prevalence + (one - p_TNR) * (one - p_Prevalence));

            // negative predictive value
            p_NPV = (cm_N_TN + cm_P_FN) == zero ? zero : (double)(cm_N_TN) / (double)(cm_N_TN + cm_P_FN);

            // false negative rate
            p_FNR = (cm_P_FN + cm_P_TP) == zero ? zero : (double)(cm_P_FN) / (double)(cm_P_FN + cm_P_TP);

            // false positive rate
            p_FPR = (cm_N_FP + cm_N_TN) == zero ? zero : (double)(cm_N_FP) / (double)(cm_N_FP + cm_N_TN);

            // false discovery rate
            p_FDR = (cm_N_FP + cm_P_TP) == zero ? zero : (double)(cm_N_FP) / (double)(cm_N_FP + cm_P_TP);

            // false omission rate
            p_FOR = (cm_P_FN + cm_N_TN) == zero ? zero : (double)(cm_P_FN) / (double)(cm_P_FN + cm_N_TN);

            // accuracy
            p_ACC = (cm_P + cm_N) == zero ? zero : (double)(cm_P_TP + cm_N_TN) / (double)(cm_P + cm_N);

            // test error rate (inaccuracy)
            p_ER = one - p_ACC;

            p_YoudenIndex = p_TPR + p_TNR - one;


            //Kappa = (totalAccuracy - randomAccuracy) / (1 - randomAccuracy)
            //totalAccuracy = (TP + TN) / (TP + TN + FP + FN)
            //randomAccuracy = referenceLikelihood(F) * resultLikelihood(F) + referenceLikelihood(T) * resultLikelihood(T)
            //randomAccuracy = (ActualFalse * PredictedFalse + ActualTrue * PredictedTrue) / Total * Total
            //randomAccuracy = (TN + FP) * (TN + FN) + (FN + TP) * (FP + TP) / Total * Total

            p_RND_ACC = ((cm_N_TN + cm_N_FP) * (cm_N_TN + cm_P_FN) + (cm_P_FN + cm_P_TP) * (cm_N_FP + cm_P_TP)) / ((cm_P_TP + cm_N_TN + cm_N_FP + cm_P_FN) * (cm_P_TP + cm_N_TN + cm_N_FP + cm_P_FN));

            p_Kappa = (one - p_RND_ACC) == zero ? zero : (p_ACC - p_RND_ACC) / (one - p_RND_ACC);

            // Geometric Mean score
            p_GMean = Math.Sqrt(p_TPR * p_TNR);

            // F1 score
            p_F1S = (p_PPV + p_TPR) == zero ? zero : (double)(2 * p_PPV * p_TPR) / (double)(p_PPV + p_TPR);

            // G1 score (same as Fowlkes–Mallows index?)
            p_G1S = (p_PPV + p_TPR) == zero ? zero : (double)Math.Sqrt((double)(p_PPV * p_TPR));

            // Matthews correlation coefficient (MCC)
            p_MCC = ((double)Math.Sqrt((double)((cm_P_TP + cm_N_FP) * (cm_P_TP + cm_P_FN) * (cm_N_TN + cm_N_FP) * (cm_N_TN + cm_P_FN))) == zero) ? zero : ((cm_P_TP * cm_N_TN) - (cm_N_FP * cm_P_FN)) / (double)Math.Sqrt((double)((cm_P_TP + cm_N_FP) * (cm_P_TP + cm_P_FN) * (cm_N_TN + cm_N_FP) * (cm_N_TN + cm_P_FN)));
            p_Informedness = (p_TPR + p_TNR) - one;
            p_Markedness = (p_PPV + p_NPV) - one;
            p_BalancedAccuracy = (p_TPR + p_TNR) / two;

            // likelihood ratio for positive results
            p_LRP = (one - p_TNR) == zero ? zero : (p_TPR) / (one - p_TNR);

            // likelihood ratio for negative results
            p_LRN = (p_TNR) == zero ? zero : (one - p_TPR) / (p_TNR);

            // Diagnostic odds ratio
            p_DOR = p_LRN == zero ? zero : p_LRP / p_LRN;

            // Prevalence Threshold
            p_PrevalenceThreshold = (p_TPR + p_TNR - one) == zero ? zero : (Math.Sqrt(p_TPR * (-p_TNR + one)) + p_TNR - one) / (p_TPR + p_TNR - one);

            // Threat Score / Critical Success Index
            p_CriticalSuccessIndex = (cm_P_TP + cm_P_FN + cm_N_FP) == zero ? zero : cm_P_TP / (cm_P_TP + cm_P_FN + cm_N_FP);

            // Fowlkes–Mallows index - (same as G1 score?):
            // var FM_Index = Math.Sqrt(PPV * TPR);

            p_F1B_00 = metrics_box.fbeta2(p_PPV, p_TPR, 0.0d);
            p_F1B_01 = metrics_box.fbeta2(p_PPV, p_TPR, 0.1d);
            p_F1B_02 = metrics_box.fbeta2(p_PPV, p_TPR, 0.2d);
            p_F1B_03 = metrics_box.fbeta2(p_PPV, p_TPR, 0.3d);
            p_F1B_04 = metrics_box.fbeta2(p_PPV, p_TPR, 0.4d);
            p_F1B_05 = metrics_box.fbeta2(p_PPV, p_TPR, 0.5d);
            p_F1B_06 = metrics_box.fbeta2(p_PPV, p_TPR, 0.6d);
            p_F1B_07 = metrics_box.fbeta2(p_PPV, p_TPR, 0.7d);
            p_F1B_08 = metrics_box.fbeta2(p_PPV, p_TPR, 0.8d);
            p_F1B_09 = metrics_box.fbeta2(p_PPV, p_TPR, 0.9d);
            p_F1B_10 = metrics_box.fbeta2(p_PPV, p_TPR, 1.0d);
        }

        private void divide(double divisor)
        {
            if (divisor == 0) return;

            cm_P /= divisor;
            cm_N /= divisor;
            cm_P_TP /= divisor;
            cm_P_FN /= divisor;
            cm_N_TN /= divisor;
            cm_N_FP /= divisor;
            p_TPR /= divisor;
            p_TNR /= divisor;
            p_PPV /= divisor;
            p_Precision /= divisor;
            p_Prevalence /= divisor;
            p_MCR /= divisor;
            p_ER /= divisor;
            p_NER /= divisor;
            p_CNER /= divisor;
            p_Kappa /= divisor;
            p_Overlap /= divisor;
            p_RND_ACC /= divisor;
            p_Support /= divisor;
            p_BaseRate /= divisor;
            p_YoudenIndex /= divisor;
            p_NPV /= divisor;
            p_FNR /= divisor;
            p_FPR /= divisor;
            p_FDR /= divisor;
            p_FOR /= divisor;
            p_ACC /= divisor;
            p_GMean /= divisor;
            p_F1S /= divisor;
            p_G1S /= divisor;
            p_MCC /= divisor;
            p_Informedness /= divisor;
            p_Markedness /= divisor;
            p_BalancedAccuracy /= divisor;
            p_ROC_AUC_Approx_All /= divisor;
            p_ROC_AUC_Approx_11p /= divisor;
            p_ROC_AUC_All /= divisor;
            p_PR_AUC_Approx_All /= divisor;
            p_PR_AUC_Approx_11p /= divisor;
            p_PRI_AUC_Approx_All /= divisor;
            p_PRI_AUC_Approx_11p /= divisor;
            p_AP_All /= divisor;
            p_AP_11p /= divisor;
            p_API_All /= divisor;
            p_API_11p /= divisor;
            p_Brier_Inverse_All /= divisor;
            p_LRP /= divisor;
            p_LRN /= divisor;
            p_DOR /= divisor;
            p_PrevalenceThreshold /= divisor;
            p_CriticalSuccessIndex /= divisor;
            p_F1B_00 /= divisor;
            p_F1B_01 /= divisor;
            p_F1B_02 /= divisor;
            p_F1B_03 /= divisor;
            p_F1B_04 /= divisor;
            p_F1B_05 /= divisor;
            p_F1B_06 /= divisor;
            p_F1B_07 /= divisor;
            p_F1B_08 /= divisor;
            p_F1B_09 /= divisor;
            p_F1B_10 /= divisor;
        }

        internal metrics_box(metrics_box[] metrics_boxes)
        {
            if (metrics_boxes == null || metrics_boxes.Length == 0 || metrics_boxes.All(a => a == null)) return;

            cm_P = metrics_boxes.Where(a => a != null).Select(a => a.cm_P).DefaultIfEmpty(0).Average();
            cm_N = metrics_boxes.Where(a => a != null).Select(a => a.cm_N).DefaultIfEmpty(0).Average();
            cm_P_TP = metrics_boxes.Where(a => a != null).Select(a => a.cm_P_TP).DefaultIfEmpty(0).Average();
            cm_P_FN = metrics_boxes.Where(a => a != null).Select(a => a.cm_P_FN).DefaultIfEmpty(0).Average();
            cm_N_TN = metrics_boxes.Where(a => a != null).Select(a => a.cm_N_TN).DefaultIfEmpty(0).Average();
            cm_N_FP = metrics_boxes.Where(a => a != null).Select(a => a.cm_N_FP).DefaultIfEmpty(0).Average();
            p_TPR = metrics_boxes.Where(a => a != null).Select(a => a.p_TPR).DefaultIfEmpty(0).Average();
            p_TNR = metrics_boxes.Where(a => a != null).Select(a => a.p_TNR).DefaultIfEmpty(0).Average();
            p_PPV = metrics_boxes.Where(a => a != null).Select(a => a.p_PPV).DefaultIfEmpty(0).Average();
            p_Precision = metrics_boxes.Where(a => a != null).Select(a => a.p_Precision).DefaultIfEmpty(0).Average();
            p_Prevalence = metrics_boxes.Where(a => a != null).Select(a => a.p_Prevalence).DefaultIfEmpty(0).Average();
            p_MCR = metrics_boxes.Where(a => a != null).Select(a => a.p_MCR).DefaultIfEmpty(0).Average();
            p_ER = metrics_boxes.Where(a => a != null).Select(a => a.p_ER).DefaultIfEmpty(0).Average();
            p_NER = metrics_boxes.Where(a => a != null).Select(a => a.p_NER).DefaultIfEmpty(0).Average();
            p_CNER = metrics_boxes.Where(a => a != null).Select(a => a.p_CNER).DefaultIfEmpty(0).Average();
            p_Kappa = metrics_boxes.Where(a => a != null).Select(a => a.p_Kappa).DefaultIfEmpty(0).Average();
            p_Overlap = metrics_boxes.Where(a => a != null).Select(a => a.p_Overlap).DefaultIfEmpty(0).Average();
            p_RND_ACC = metrics_boxes.Where(a => a != null).Select(a => a.p_RND_ACC).DefaultIfEmpty(0).Average();
            p_Support = metrics_boxes.Where(a => a != null).Select(a => a.p_Support).DefaultIfEmpty(0).Average();
            p_BaseRate = metrics_boxes.Where(a => a != null).Select(a => a.p_BaseRate).DefaultIfEmpty(0).Average();
            p_YoudenIndex = metrics_boxes.Where(a => a != null).Select(a => a.p_YoudenIndex).DefaultIfEmpty(0).Average();
            p_NPV = metrics_boxes.Where(a => a != null).Select(a => a.p_NPV).DefaultIfEmpty(0).Average();
            p_FNR = metrics_boxes.Where(a => a != null).Select(a => a.p_FNR).DefaultIfEmpty(0).Average();
            p_FPR = metrics_boxes.Where(a => a != null).Select(a => a.p_FPR).DefaultIfEmpty(0).Average();
            p_FDR = metrics_boxes.Where(a => a != null).Select(a => a.p_FDR).DefaultIfEmpty(0).Average();
            p_FOR = metrics_boxes.Where(a => a != null).Select(a => a.p_FOR).DefaultIfEmpty(0).Average();
            p_ACC = metrics_boxes.Where(a => a != null).Select(a => a.p_ACC).DefaultIfEmpty(0).Average();
            p_GMean = metrics_boxes.Where(a => a != null).Select(a => a.p_GMean).DefaultIfEmpty(0).Average();
            p_F1S = metrics_boxes.Where(a => a != null).Select(a => a.p_F1S).DefaultIfEmpty(0).Average();
            p_G1S = metrics_boxes.Where(a => a != null).Select(a => a.p_G1S).DefaultIfEmpty(0).Average();
            p_MCC = metrics_boxes.Where(a => a != null).Select(a => a.p_MCC).DefaultIfEmpty(0).Average();
            p_Informedness = metrics_boxes.Where(a => a != null).Select(a => a.p_Informedness).DefaultIfEmpty(0).Average();
            p_Markedness = metrics_boxes.Where(a => a != null).Select(a => a.p_Markedness).DefaultIfEmpty(0).Average();
            p_BalancedAccuracy = metrics_boxes.Where(a => a != null).Select(a => a.p_BalancedAccuracy).DefaultIfEmpty(0).Average();
            p_ROC_AUC_Approx_All = metrics_boxes.Where(a => a != null).Select(a => a.p_ROC_AUC_Approx_All).DefaultIfEmpty(0).Average();
            p_ROC_AUC_Approx_11p = metrics_boxes.Where(a => a != null).Select(a => a.p_ROC_AUC_Approx_11p).DefaultIfEmpty(0).Average();
            p_ROC_AUC_All = metrics_boxes.Where(a => a != null).Select(a => a.p_ROC_AUC_All).DefaultIfEmpty(0).Average();
            p_PR_AUC_Approx_All = metrics_boxes.Where(a => a != null).Select(a => a.p_PR_AUC_Approx_All).DefaultIfEmpty(0).Average();
            p_PR_AUC_Approx_11p = metrics_boxes.Where(a => a != null).Select(a => a.p_PR_AUC_Approx_11p).DefaultIfEmpty(0).Average();
            p_PRI_AUC_Approx_All = metrics_boxes.Where(a => a != null).Select(a => a.p_PRI_AUC_Approx_All).DefaultIfEmpty(0).Average();
            p_PRI_AUC_Approx_11p = metrics_boxes.Where(a => a != null).Select(a => a.p_PRI_AUC_Approx_11p).DefaultIfEmpty(0).Average();
            p_AP_All = metrics_boxes.Where(a => a != null).Select(a => a.p_AP_All).DefaultIfEmpty(0).Average();
            p_AP_11p = metrics_boxes.Where(a => a != null).Select(a => a.p_AP_11p).DefaultIfEmpty(0).Average();
            p_API_All = metrics_boxes.Where(a => a != null).Select(a => a.p_API_All).DefaultIfEmpty(0).Average();
            p_API_11p = metrics_boxes.Where(a => a != null).Select(a => a.p_API_11p).DefaultIfEmpty(0).Average();
            p_Brier_Inverse_All = metrics_boxes.Where(a => a != null).Select(a => a.p_Brier_Inverse_All).DefaultIfEmpty(0).Average();
            p_LRP = metrics_boxes.Where(a => a != null).Select(a => a.p_LRP).DefaultIfEmpty(0).Average();
            p_LRN = metrics_boxes.Where(a => a != null).Select(a => a.p_LRN).DefaultIfEmpty(0).Average();
            p_DOR = metrics_boxes.Where(a => a != null).Select(a => a.p_DOR).DefaultIfEmpty(0).Average();
            p_PrevalenceThreshold = metrics_boxes.Where(a => a != null).Select(a => a.p_PrevalenceThreshold).DefaultIfEmpty(0).Average();
            p_CriticalSuccessIndex = metrics_boxes.Where(a => a != null).Select(a => a.p_CriticalSuccessIndex).DefaultIfEmpty(0).Average();
            p_F1B_00 = metrics_boxes.Where(a => a != null).Select(a => a.p_F1B_00).DefaultIfEmpty(0).Average();
            p_F1B_01 = metrics_boxes.Where(a => a != null).Select(a => a.p_F1B_01).DefaultIfEmpty(0).Average();
            p_F1B_02 = metrics_boxes.Where(a => a != null).Select(a => a.p_F1B_02).DefaultIfEmpty(0).Average();
            p_F1B_03 = metrics_boxes.Where(a => a != null).Select(a => a.p_F1B_03).DefaultIfEmpty(0).Average();
            p_F1B_04 = metrics_boxes.Where(a => a != null).Select(a => a.p_F1B_04).DefaultIfEmpty(0).Average();
            p_F1B_05 = metrics_boxes.Where(a => a != null).Select(a => a.p_F1B_05).DefaultIfEmpty(0).Average();
            p_F1B_06 = metrics_boxes.Where(a => a != null).Select(a => a.p_F1B_06).DefaultIfEmpty(0).Average();
            p_F1B_07 = metrics_boxes.Where(a => a != null).Select(a => a.p_F1B_07).DefaultIfEmpty(0).Average();
            p_F1B_08 = metrics_boxes.Where(a => a != null).Select(a => a.p_F1B_08).DefaultIfEmpty(0).Average();
            p_F1B_09 = metrics_boxes.Where(a => a != null).Select(a => a.p_F1B_09).DefaultIfEmpty(0).Average();
            p_F1B_10 = metrics_boxes.Where(a => a != null).Select(a => a.p_F1B_10).DefaultIfEmpty(0).Average();
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
            if (string.Equals(name, nameof(metrics.cm_P), StringComparison.OrdinalIgnoreCase)) return metrics.cm_P;
            if (string.Equals(name, nameof(metrics.cm_N), StringComparison.OrdinalIgnoreCase)) return metrics.cm_N;
            if (string.Equals(name, nameof(metrics.cm_P_TP), StringComparison.OrdinalIgnoreCase)) return metrics.cm_P_TP;
            if (string.Equals(name, nameof(metrics.cm_P_FN), StringComparison.OrdinalIgnoreCase)) return metrics.cm_P_FN;
            if (string.Equals(name, nameof(metrics.cm_N_TN), StringComparison.OrdinalIgnoreCase)) return metrics.cm_N_TN;
            if (string.Equals(name, nameof(metrics.cm_N_FP), StringComparison.OrdinalIgnoreCase)) return metrics.cm_N_FP;
            if (string.Equals(name, nameof(metrics.p_TPR), StringComparison.OrdinalIgnoreCase)) return metrics.p_TPR;
            if (string.Equals(name, nameof(metrics.p_TNR), StringComparison.OrdinalIgnoreCase)) return metrics.p_TNR;
            if (string.Equals(name, nameof(metrics.p_PPV), StringComparison.OrdinalIgnoreCase)) return metrics.p_PPV;
            if (string.Equals(name, nameof(metrics.p_Precision), StringComparison.OrdinalIgnoreCase)) return metrics.p_Precision;
            if (string.Equals(name, nameof(metrics.p_Prevalence), StringComparison.OrdinalIgnoreCase)) return metrics.p_Prevalence;
            if (string.Equals(name, nameof(metrics.p_MCR), StringComparison.OrdinalIgnoreCase)) return metrics.p_MCR;
            if (string.Equals(name, nameof(metrics.p_ER), StringComparison.OrdinalIgnoreCase)) return metrics.p_ER;
            if (string.Equals(name, nameof(metrics.p_NER), StringComparison.OrdinalIgnoreCase)) return metrics.p_NER;
            if (string.Equals(name, nameof(metrics.p_CNER), StringComparison.OrdinalIgnoreCase)) return metrics.p_CNER;
            if (string.Equals(name, nameof(metrics.p_Kappa), StringComparison.OrdinalIgnoreCase)) return metrics.p_Kappa;
            if (string.Equals(name, nameof(metrics.p_Overlap), StringComparison.OrdinalIgnoreCase)) return metrics.p_Overlap;
            if (string.Equals(name, nameof(metrics.p_RND_ACC), StringComparison.OrdinalIgnoreCase)) return metrics.p_RND_ACC;
            if (string.Equals(name, nameof(metrics.p_Support), StringComparison.OrdinalIgnoreCase)) return metrics.p_Support;
            if (string.Equals(name, nameof(metrics.p_BaseRate), StringComparison.OrdinalIgnoreCase)) return metrics.p_BaseRate;
            if (string.Equals(name, nameof(metrics.p_YoudenIndex), StringComparison.OrdinalIgnoreCase)) return metrics.p_YoudenIndex;
            if (string.Equals(name, nameof(metrics.p_NPV), StringComparison.OrdinalIgnoreCase)) return metrics.p_NPV;
            if (string.Equals(name, nameof(metrics.p_FNR), StringComparison.OrdinalIgnoreCase)) return metrics.p_FNR;
            if (string.Equals(name, nameof(metrics.p_FPR), StringComparison.OrdinalIgnoreCase)) return metrics.p_FPR;
            if (string.Equals(name, nameof(metrics.p_FDR), StringComparison.OrdinalIgnoreCase)) return metrics.p_FDR;
            if (string.Equals(name, nameof(metrics.p_FOR), StringComparison.OrdinalIgnoreCase)) return metrics.p_FOR;
            if (string.Equals(name, nameof(metrics.p_ACC), StringComparison.OrdinalIgnoreCase)) return metrics.p_ACC;
            if (string.Equals(name, nameof(metrics.p_GMean), StringComparison.OrdinalIgnoreCase)) return metrics.p_GMean;
            if (string.Equals(name, nameof(metrics.p_F1S), StringComparison.OrdinalIgnoreCase)) return metrics.p_F1S;
            if (string.Equals(name, nameof(metrics.p_G1S), StringComparison.OrdinalIgnoreCase)) return metrics.p_G1S;
            if (string.Equals(name, nameof(metrics.p_MCC), StringComparison.OrdinalIgnoreCase)) return metrics.p_MCC;
            if (string.Equals(name, nameof(metrics.p_Informedness), StringComparison.OrdinalIgnoreCase)) return metrics.p_Informedness;
            if (string.Equals(name, nameof(metrics.p_Markedness), StringComparison.OrdinalIgnoreCase)) return metrics.p_Markedness;
            if (string.Equals(name, nameof(metrics.p_BalancedAccuracy), StringComparison.OrdinalIgnoreCase)) return metrics.p_BalancedAccuracy;
            if (string.Equals(name, nameof(metrics.p_ROC_AUC_Approx_All), StringComparison.OrdinalIgnoreCase)) return metrics.p_ROC_AUC_Approx_All;
            if (string.Equals(name, nameof(metrics.p_ROC_AUC_Approx_11p), StringComparison.OrdinalIgnoreCase)) return metrics.p_ROC_AUC_Approx_11p;
            if (string.Equals(name, nameof(metrics.p_ROC_AUC_All), StringComparison.OrdinalIgnoreCase)) return metrics.p_ROC_AUC_All;
            if (string.Equals(name, nameof(metrics.p_PR_AUC_Approx_All), StringComparison.OrdinalIgnoreCase)) return metrics.p_PR_AUC_Approx_All;
            if (string.Equals(name, nameof(metrics.p_PR_AUC_Approx_11p), StringComparison.OrdinalIgnoreCase)) return metrics.p_PR_AUC_Approx_11p;
            if (string.Equals(name, nameof(metrics.p_PRI_AUC_Approx_All), StringComparison.OrdinalIgnoreCase)) return metrics.p_PRI_AUC_Approx_All;
            if (string.Equals(name, nameof(metrics.p_PRI_AUC_Approx_11p), StringComparison.OrdinalIgnoreCase)) return metrics.p_PRI_AUC_Approx_11p;
            if (string.Equals(name, nameof(metrics.p_AP_All), StringComparison.OrdinalIgnoreCase)) return metrics.p_AP_All;
            if (string.Equals(name, nameof(metrics.p_AP_11p), StringComparison.OrdinalIgnoreCase)) return metrics.p_AP_11p;
            if (string.Equals(name, nameof(metrics.p_API_All), StringComparison.OrdinalIgnoreCase)) return metrics.p_API_All;
            if (string.Equals(name, nameof(metrics.p_API_11p), StringComparison.OrdinalIgnoreCase)) return metrics.p_API_11p;
            if (string.Equals(name, nameof(metrics.p_Brier_Inverse_All), StringComparison.OrdinalIgnoreCase)) return metrics.p_Brier_Inverse_All;
            if (string.Equals(name, nameof(metrics.p_LRP), StringComparison.OrdinalIgnoreCase)) return metrics.p_LRP;
            if (string.Equals(name, nameof(metrics.p_LRN), StringComparison.OrdinalIgnoreCase)) return metrics.p_LRN;
            if (string.Equals(name, nameof(metrics.p_DOR), StringComparison.OrdinalIgnoreCase)) return metrics.p_DOR;
            if (string.Equals(name, nameof(metrics.p_PrevalenceThreshold), StringComparison.OrdinalIgnoreCase)) return metrics.p_PrevalenceThreshold;
            if (string.Equals(name, nameof(metrics.p_CriticalSuccessIndex), StringComparison.OrdinalIgnoreCase)) return metrics.p_CriticalSuccessIndex;
            if (string.Equals(name, nameof(metrics.p_F1B_00), StringComparison.OrdinalIgnoreCase)) return metrics.p_F1B_00;
            if (string.Equals(name, nameof(metrics.p_F1B_01), StringComparison.OrdinalIgnoreCase)) return metrics.p_F1B_01;
            if (string.Equals(name, nameof(metrics.p_F1B_02), StringComparison.OrdinalIgnoreCase)) return metrics.p_F1B_02;
            if (string.Equals(name, nameof(metrics.p_F1B_03), StringComparison.OrdinalIgnoreCase)) return metrics.p_F1B_03;
            if (string.Equals(name, nameof(metrics.p_F1B_04), StringComparison.OrdinalIgnoreCase)) return metrics.p_F1B_04;
            if (string.Equals(name, nameof(metrics.p_F1B_05), StringComparison.OrdinalIgnoreCase)) return metrics.p_F1B_05;
            if (string.Equals(name, nameof(metrics.p_F1B_06), StringComparison.OrdinalIgnoreCase)) return metrics.p_F1B_06;
            if (string.Equals(name, nameof(metrics.p_F1B_07), StringComparison.OrdinalIgnoreCase)) return metrics.p_F1B_07;
            if (string.Equals(name, nameof(metrics.p_F1B_08), StringComparison.OrdinalIgnoreCase)) return metrics.p_F1B_08;
            if (string.Equals(name, nameof(metrics.p_F1B_09), StringComparison.OrdinalIgnoreCase)) return metrics.p_F1B_09;
            if (string.Equals(name, nameof(metrics.p_F1B_10), StringComparison.OrdinalIgnoreCase)) return metrics.p_F1B_10;
            return default;
        }

        internal List<double> get_specific_values(cross_validation_metrics cross_validation_metrics)
        {
            var metric_values = new List<double>();

            if (cross_validation_metrics == 0) throw new Exception();

            if (cross_validation_metrics.HasFlag(cross_validation_metrics.TP)) { metric_values.Add(cm_P_TP); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FN)) { metric_values.Add(cm_P_FN); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.TN)) { metric_values.Add(cm_N_TN); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FP)) { metric_values.Add(cm_N_FP); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.TPR)) { metric_values.Add(p_TPR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.TNR)) { metric_values.Add(p_TNR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PPV)) { metric_values.Add(p_PPV); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Precision)) { metric_values.Add(p_Precision); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Prevalence)) { metric_values.Add(p_Prevalence); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.MCR)) { metric_values.Add(p_MCR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ER)) { metric_values.Add(p_ER); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.NER)) { metric_values.Add(p_NER); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.CNER)) { metric_values.Add(p_CNER); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Kappa)) { metric_values.Add(p_Kappa); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Overlap)) { metric_values.Add(p_Overlap); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.RND_ACC)) { metric_values.Add(p_RND_ACC); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Support)) { metric_values.Add(p_Support); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.BaseRate)) { metric_values.Add(p_BaseRate); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.YoudenIndex)) { metric_values.Add(p_YoudenIndex); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.NPV)) { metric_values.Add(p_NPV); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FNR)) { metric_values.Add(p_FNR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FPR)) { metric_values.Add(p_FPR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FDR)) { metric_values.Add(p_FDR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.FOR)) { metric_values.Add(p_FOR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ACC)) { metric_values.Add(p_ACC); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.GM)) { metric_values.Add(p_GMean); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1S)) { metric_values.Add(p_F1S); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.G1S)) { metric_values.Add(p_G1S); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.MCC)) { metric_values.Add(p_MCC); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Informedness)) { metric_values.Add(p_Informedness); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Markedness)) { metric_values.Add(p_Markedness); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.BalancedAccuracy)) { metric_values.Add(p_BalancedAccuracy); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ROC_AUC_Approx_All)) { metric_values.Add(p_ROC_AUC_Approx_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ROC_AUC_Approx_11p)) { metric_values.Add(p_ROC_AUC_Approx_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.ROC_AUC_All)) { metric_values.Add(p_ROC_AUC_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PR_AUC_Approx_All)) { metric_values.Add(p_PR_AUC_Approx_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PR_AUC_Approx_11p)) { metric_values.Add(p_PR_AUC_Approx_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PRI_AUC_Approx_All)) { metric_values.Add(p_PRI_AUC_Approx_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PRI_AUC_Approx_11p)) { metric_values.Add(p_PRI_AUC_Approx_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.AP_All)) { metric_values.Add(p_AP_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.AP_11p)) { metric_values.Add(p_AP_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.API_All)) { metric_values.Add(p_API_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.API_11p)) { metric_values.Add(p_API_11p); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.Brier_Inverse_All)) { metric_values.Add(p_Brier_Inverse_All); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.LRP)) { metric_values.Add(p_LRP); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.LRN)) { metric_values.Add(p_LRN); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.DOR)) { metric_values.Add(p_DOR); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.PrevalenceThreshold)) { metric_values.Add(p_PrevalenceThreshold); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.CriticalSuccessIndex)) { metric_values.Add(p_CriticalSuccessIndex); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_00)) { metric_values.Add(p_F1B_00); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_01)) { metric_values.Add(p_F1B_01); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_02)) { metric_values.Add(p_F1B_02); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_03)) { metric_values.Add(p_F1B_03); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_04)) { metric_values.Add(p_F1B_04); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_05)) { metric_values.Add(p_F1B_05); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_06)) { metric_values.Add(p_F1B_06); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_07)) { metric_values.Add(p_F1B_07); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_08)) { metric_values.Add(p_F1B_08); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_09)) { metric_values.Add(p_F1B_09); }
            if (cross_validation_metrics.HasFlag(cross_validation_metrics.F1B_10)) { metric_values.Add(p_F1B_10); }

            return metric_values;
        }


        internal (string name, double value)[] get_perf_value_strings()
        {
            var result = new (string name, double value)[] {

                    (nameof(cm_P_TP), cm_P_TP),
                    (nameof(cm_P_FN), cm_P_FN),
                    (nameof(cm_N_TN), cm_N_TN),
                    (nameof(cm_N_FP), cm_N_FP),
                    (nameof(p_TPR), p_TPR),
                    (nameof(p_TNR), p_TNR),
                    (nameof(p_PPV), p_PPV),
                    (nameof(p_Precision), p_Precision),
                    (nameof(p_Prevalence), p_Prevalence),
                    (nameof(p_MCR), p_MCR),
                    (nameof(p_ER), p_ER),
                    (nameof(p_NER), p_NER),
                    (nameof(p_CNER), p_CNER),
                    (nameof(p_Kappa), p_Kappa),
                    (nameof(p_Overlap), p_Overlap),
                    (nameof(p_RND_ACC), p_RND_ACC),
                    (nameof(p_Support), p_Support),
                    (nameof(p_BaseRate), p_BaseRate),
                    (nameof(p_YoudenIndex), p_YoudenIndex),
                    (nameof(p_NPV), p_NPV),
                    (nameof(p_FNR), p_FNR),
                    (nameof(p_FPR), p_FPR),
                    (nameof(p_FDR), p_FDR),
                    (nameof(p_FOR), p_FOR),
                    (nameof(p_ACC), p_ACC),
                    (nameof(p_GMean), p_GMean),
                    (nameof(p_F1S), p_F1S),
                    (nameof(p_G1S), p_G1S),
                    (nameof(p_MCC), p_MCC),
                    (nameof(p_Informedness), p_Informedness),
                    (nameof(p_Markedness), p_Markedness),
                    (nameof(p_BalancedAccuracy), p_BalancedAccuracy),
                    (nameof(p_ROC_AUC_Approx_All), p_ROC_AUC_Approx_All),
                    (nameof(p_ROC_AUC_Approx_11p), p_ROC_AUC_Approx_11p),
                    (nameof(p_ROC_AUC_All), p_ROC_AUC_All),
                    (nameof(p_PR_AUC_Approx_All), p_PR_AUC_Approx_All),
                    (nameof(p_PR_AUC_Approx_11p), p_PR_AUC_Approx_11p),
                    (nameof(p_PRI_AUC_Approx_All), p_PRI_AUC_Approx_All),
                    (nameof(p_PRI_AUC_Approx_11p), p_PRI_AUC_Approx_11p),
                    (nameof(p_AP_All), p_AP_All),
                    (nameof(p_AP_11p), p_AP_11p),
                    (nameof(p_API_All), p_API_All),
                    (nameof(p_API_11p), p_API_11p),
                    (nameof(p_Brier_Inverse_All), p_Brier_Inverse_All),
                    (nameof(p_LRP), p_LRP),
                    (nameof(p_LRN), p_LRN),
                    (nameof(p_DOR), p_DOR),
                    (nameof(p_PrevalenceThreshold), p_PrevalenceThreshold),
                    (nameof(p_CriticalSuccessIndex), p_CriticalSuccessIndex),
                    (nameof(p_F1B_00), p_F1B_00),
                    (nameof(p_F1B_01), p_F1B_01),
                    (nameof(p_F1B_02), p_F1B_02),
                    (nameof(p_F1B_03), p_F1B_03),
                    (nameof(p_F1B_04), p_F1B_04),
                    (nameof(p_F1B_05), p_F1B_05),
                    (nameof(p_F1B_06), p_F1B_06),
                    (nameof(p_F1B_07), p_F1B_07),
                    (nameof(p_F1B_08), p_F1B_08),
                    (nameof(p_F1B_09), p_F1B_09),
                    (nameof(p_F1B_10), p_F1B_10),
                };

            return result;
        }

        [Flags]
        internal enum cross_validation_metrics : ulong
        {
            None = 0UL,
            TP = 1UL << 01,
            FN = 1UL << 02,
            TN = 1UL << 03,
            FP = 1UL << 04,
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
                nameof(cm_P),
                nameof(cm_N),
                nameof(cm_P_TP),
                nameof(cm_P_FN),
                nameof(cm_N_TN),
                nameof(cm_N_FP),
                nameof(p_TPR),
                nameof(p_TNR),
                nameof(p_PPV),
                nameof(p_Precision),
                nameof(p_Prevalence),
                nameof(p_MCR),
                nameof(p_ER),
                nameof(p_NER),
                nameof(p_CNER),
                nameof(p_Kappa),
                nameof(p_Overlap),
                nameof(p_RND_ACC),
                nameof(p_Support),
                nameof(p_BaseRate),
                nameof(p_YoudenIndex),
                nameof(p_NPV),
                nameof(p_FNR),
                nameof(p_FPR),
                nameof(p_FDR),
                nameof(p_FOR),
                nameof(p_ACC),
                nameof(p_GMean),
                nameof(p_F1S),
                nameof(p_G1S),
                nameof(p_MCC),
                nameof(p_Informedness),
                nameof(p_Markedness),
                nameof(p_BalancedAccuracy),
                nameof(p_ROC_AUC_Approx_All),
                nameof(p_ROC_AUC_Approx_11p),
                nameof(p_ROC_AUC_All),
                nameof(p_PR_AUC_Approx_All),
                nameof(p_PR_AUC_Approx_11p),
                nameof(p_PRI_AUC_Approx_All),
                nameof(p_PRI_AUC_Approx_11p),
                nameof(p_AP_All),
                nameof(p_AP_11p),
                nameof(p_API_All),
                nameof(p_API_11p),
                nameof(p_Brier_Inverse_All),
                nameof(p_LRP),
                nameof(p_LRN),
                nameof(p_DOR),
                nameof(p_PrevalenceThreshold),
                nameof(p_CriticalSuccessIndex),
                nameof(p_F1B_00),
                nameof(p_F1B_01),
                nameof(p_F1B_02),
                nameof(p_F1B_03),
                nameof(p_F1B_04),
                nameof(p_F1B_05),
                nameof(p_F1B_06),
                nameof(p_F1B_07),
                nameof(p_F1B_08),
                nameof(p_F1B_09),
                nameof(p_F1B_10),
            };

        public static readonly string csv_header_string = string.Join(",", csv_header_values_array);

        public string[] csv_values_array()
        {
            return new string[]
            {
                cm_P.ToString("G17", NumberFormatInfo.InvariantInfo),
                cm_N.ToString("G17", NumberFormatInfo.InvariantInfo),
                cm_P_TP.ToString("G17", NumberFormatInfo.InvariantInfo),
                cm_P_FN.ToString("G17", NumberFormatInfo.InvariantInfo),
                cm_N_TN.ToString("G17", NumberFormatInfo.InvariantInfo),
                cm_N_FP.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_TPR.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_TNR.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_PPV.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_Precision.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_Prevalence.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_MCR.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_ER.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_NER.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_CNER.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_Kappa.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_Overlap.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_RND_ACC.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_Support.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_BaseRate.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_YoudenIndex.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_NPV.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_FNR.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_FPR.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_FDR.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_FOR.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_ACC.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_GMean.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_F1S.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_G1S.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_MCC.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_Informedness.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_Markedness.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_BalancedAccuracy.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_ROC_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_ROC_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_ROC_AUC_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_PR_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_PR_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_PRI_AUC_Approx_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_PRI_AUC_Approx_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_AP_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_AP_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_API_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_API_11p.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_Brier_Inverse_All.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_LRP.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_LRN.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_DOR.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_PrevalenceThreshold.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_CriticalSuccessIndex.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_F1B_00.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_F1B_01.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_F1B_02.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_F1B_03.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_F1B_04.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_F1B_05.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_F1B_06.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_F1B_07.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_F1B_08.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_F1B_09.ToString("G17", NumberFormatInfo.InvariantInfo),
                p_F1B_10.ToString("G17", NumberFormatInfo.InvariantInfo),
            }.Select(a => a.Replace(",", ";", StringComparison.OrdinalIgnoreCase)).ToArray();
        }

        public string csv_values_string()
        {
            return string.Join(",", csv_values_array());
        }
    }
}
