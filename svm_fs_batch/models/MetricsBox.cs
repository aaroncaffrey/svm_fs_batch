﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace SvmFsBatch
{
    internal class MetricsBox
    {
        public const string ModuleName = nameof(MetricsBox);
        internal static readonly MetricsBox Empty = new MetricsBox();


        public static readonly string[] CsvHeaderValuesArray =
        {
            nameof(CmP),
            nameof(CmN),
            nameof(CmPTp),
            nameof(CmPFn),
            nameof(CmNTn),
            nameof(CmNFp),
            nameof(PTpr),
            nameof(PTnr),
            nameof(PPpv),
            nameof(PPrecision),
            nameof(PPrevalence),
            nameof(PMcr),
            nameof(PEr),
            nameof(PNer),
            nameof(PCner),
            nameof(PKappa),
            nameof(POverlap),
            nameof(PRndAcc),
            nameof(PSupport),
            nameof(PBaseRate),
            nameof(PYoudenIndex),
            nameof(PNpv),
            nameof(PFnr),
            nameof(PFpr),
            nameof(PFdr),
            nameof(PFor),
            nameof(PAcc),
            nameof(PGMean),
            nameof(PF1S),
            nameof(PG1S),
            nameof(PMcc),
            nameof(PInformedness),
            nameof(PMarkedness),
            nameof(PBalancedAccuracy),
            nameof(PRocAucApproxAll),
            nameof(PRocAucApproxElevenPoint),
            nameof(PRocAucAll),
            nameof(PPrAucApproxAll),
            nameof(PPrAucApproxElevenPoint),
            nameof(PPriAucApproxAll),
            nameof(PPriAucApproxElevenPoint),
            nameof(PApAll),
            nameof(PApElevenPoint),
            nameof(PApiAll),
            nameof(PApiElevenPoint),
            nameof(PBrierInverseAll),
            nameof(PLrp),
            nameof(PLrn),
            nameof(PDor),
            nameof(PPrevalenceThreshold),
            nameof(PCriticalSuccessIndex),
            nameof(PF1B00),
            nameof(PF1B01),
            nameof(PF1B02),
            nameof(PF1B03),
            nameof(PF1B04),
            nameof(PF1B05),
            nameof(PF1B06),
            nameof(PF1B07),
            nameof(PF1B08),
            nameof(PF1B09),
            nameof(PF1B10)
        };

        public static readonly string CsvHeaderValuesString = string.Join(",", CsvHeaderValuesArray);
        internal double CmN;
        internal double CmNFp;
        internal double CmNTn;

        internal double CmP;
        internal double CmPFn;

        internal double CmPTp;
        internal double PAcc;
        internal double PApAll;
        internal double PApElevenPoint;
        internal double PApiAll;
        internal double PApiElevenPoint;
        internal double PBalancedAccuracy;
        internal double PBaseRate;
        internal double PBrierInverseAll;
        internal double PCner;
        internal double PCriticalSuccessIndex;
        internal double PDor;
        internal double PEr;
        internal double PF1B00;
        internal double PF1B01;
        internal double PF1B02;
        internal double PF1B03;
        internal double PF1B04;
        internal double PF1B05;
        internal double PF1B06;
        internal double PF1B07;
        internal double PF1B08;
        internal double PF1B09;
        internal double PF1B10;
        internal double PF1S;
        internal double PFdr;
        internal double PFnr;
        internal double PFor;
        internal double PFpr;
        internal double PG1S;
        internal double PGMean;
        internal double PInformedness;
        internal double PKappa;
        internal double PLrn;
        internal double PLrp;
        internal double PMarkedness;
        internal double PMcc;
        internal double PMcr;
        internal double PNer;
        internal double PNpv;
        internal double POverlap;
        internal double PPpv;
        internal double PPrAucApproxAll;
        internal double PPrAucApproxElevenPoint;
        internal double PPrecision;
        internal double PPrevalence;
        internal double PPrevalenceThreshold;
        internal double PPriAucApproxAll;
        internal double PPriAucApproxElevenPoint;
        internal double PRndAcc;
        internal double PRocAucAll;
        internal double PRocAucApproxAll;
        internal double PRocAucApproxElevenPoint;
        internal double PSupport;
        internal double PTnr;

        internal double PTpr;
        internal double PYoudenIndex;

        public MetricsBox()
        {
        }

        public MetricsBox(MetricsBox metrics)
        {
            CmP = metrics.CmP;
            CmN = metrics.CmN;
            CmPTp = metrics.CmPTp;
            CmPFn = metrics.CmPFn;
            CmNTn = metrics.CmNTn;
            CmNFp = metrics.CmNFp;
            PTpr = metrics.PTpr;
            PTnr = metrics.PTnr;
            PPpv = metrics.PPpv;
            PPrecision = metrics.PPrecision;
            PPrevalence = metrics.PPrevalence;
            PMcr = metrics.PMcr;
            PEr = metrics.PEr;
            PNer = metrics.PNer;
            PCner = metrics.PCner;
            PKappa = metrics.PKappa;
            POverlap = metrics.POverlap;
            PRndAcc = metrics.PRndAcc;
            PSupport = metrics.PSupport;
            PBaseRate = metrics.PBaseRate;
            PYoudenIndex = metrics.PYoudenIndex;
            PNpv = metrics.PNpv;
            PFnr = metrics.PFnr;
            PFpr = metrics.PFpr;
            PFdr = metrics.PFdr;
            PFor = metrics.PFor;
            PAcc = metrics.PAcc;
            PGMean = metrics.PGMean;
            PF1S = metrics.PF1S;
            PG1S = metrics.PG1S;
            PMcc = metrics.PMcc;
            PInformedness = metrics.PInformedness;
            PMarkedness = metrics.PMarkedness;
            PBalancedAccuracy = metrics.PBalancedAccuracy;
            PRocAucApproxAll = metrics.PRocAucApproxAll;
            PRocAucApproxElevenPoint = metrics.PRocAucApproxElevenPoint;
            PRocAucAll = metrics.PRocAucAll;
            PPrAucApproxAll = metrics.PPrAucApproxAll;
            PPrAucApproxElevenPoint = metrics.PPrAucApproxElevenPoint;
            PPriAucApproxAll = metrics.PPriAucApproxAll;
            PPriAucApproxElevenPoint = metrics.PPriAucApproxElevenPoint;
            PApAll = metrics.PApAll;
            PApElevenPoint = metrics.PApElevenPoint;
            PApiAll = metrics.PApiAll;
            PApiElevenPoint = metrics.PApiElevenPoint;
            PBrierInverseAll = metrics.PBrierInverseAll;
            PLrp = metrics.PLrp;
            PLrn = metrics.PLrn;
            PDor = metrics.PDor;
            PPrevalenceThreshold = metrics.PPrevalenceThreshold;
            PCriticalSuccessIndex = metrics.PCriticalSuccessIndex;
            PF1B00 = metrics.PF1B00;
            PF1B01 = metrics.PF1B01;
            PF1B02 = metrics.PF1B02;
            PF1B03 = metrics.PF1B03;
            PF1B04 = metrics.PF1B04;
            PF1B05 = metrics.PF1B05;
            PF1B06 = metrics.PF1B06;
            PF1B07 = metrics.PF1B07;
            PF1B08 = metrics.PF1B08;
            PF1B09 = metrics.PF1B09;
            PF1B10 = metrics.PF1B10;
        }

        internal MetricsBox(MetricsBox[] metricsBoxes)
        {
            if (metricsBoxes == null || metricsBoxes.Length == 0 || metricsBoxes.All(a => a == null)) return;

            CmP = metricsBoxes.Where(a => a != null).Select(a => a.CmP).DefaultIfEmpty(0).Average();
            CmN = metricsBoxes.Where(a => a != null).Select(a => a.CmN).DefaultIfEmpty(0).Average();
            CmPTp = metricsBoxes.Where(a => a != null).Select(a => a.CmPTp).DefaultIfEmpty(0).Average();
            CmPFn = metricsBoxes.Where(a => a != null).Select(a => a.CmPFn).DefaultIfEmpty(0).Average();
            CmNTn = metricsBoxes.Where(a => a != null).Select(a => a.CmNTn).DefaultIfEmpty(0).Average();
            CmNFp = metricsBoxes.Where(a => a != null).Select(a => a.CmNFp).DefaultIfEmpty(0).Average();
            PTpr = metricsBoxes.Where(a => a != null).Select(a => a.PTpr).DefaultIfEmpty(0).Average();
            PTnr = metricsBoxes.Where(a => a != null).Select(a => a.PTnr).DefaultIfEmpty(0).Average();
            PPpv = metricsBoxes.Where(a => a != null).Select(a => a.PPpv).DefaultIfEmpty(0).Average();
            PPrecision = metricsBoxes.Where(a => a != null).Select(a => a.PPrecision).DefaultIfEmpty(0).Average();
            PPrevalence = metricsBoxes.Where(a => a != null).Select(a => a.PPrevalence).DefaultIfEmpty(0).Average();
            PMcr = metricsBoxes.Where(a => a != null).Select(a => a.PMcr).DefaultIfEmpty(0).Average();
            PEr = metricsBoxes.Where(a => a != null).Select(a => a.PEr).DefaultIfEmpty(0).Average();
            PNer = metricsBoxes.Where(a => a != null).Select(a => a.PNer).DefaultIfEmpty(0).Average();
            PCner = metricsBoxes.Where(a => a != null).Select(a => a.PCner).DefaultIfEmpty(0).Average();
            PKappa = metricsBoxes.Where(a => a != null).Select(a => a.PKappa).DefaultIfEmpty(0).Average();
            POverlap = metricsBoxes.Where(a => a != null).Select(a => a.POverlap).DefaultIfEmpty(0).Average();
            PRndAcc = metricsBoxes.Where(a => a != null).Select(a => a.PRndAcc).DefaultIfEmpty(0).Average();
            PSupport = metricsBoxes.Where(a => a != null).Select(a => a.PSupport).DefaultIfEmpty(0).Average();
            PBaseRate = metricsBoxes.Where(a => a != null).Select(a => a.PBaseRate).DefaultIfEmpty(0).Average();
            PYoudenIndex = metricsBoxes.Where(a => a != null).Select(a => a.PYoudenIndex).DefaultIfEmpty(0).Average();
            PNpv = metricsBoxes.Where(a => a != null).Select(a => a.PNpv).DefaultIfEmpty(0).Average();
            PFnr = metricsBoxes.Where(a => a != null).Select(a => a.PFnr).DefaultIfEmpty(0).Average();
            PFpr = metricsBoxes.Where(a => a != null).Select(a => a.PFpr).DefaultIfEmpty(0).Average();
            PFdr = metricsBoxes.Where(a => a != null).Select(a => a.PFdr).DefaultIfEmpty(0).Average();
            PFor = metricsBoxes.Where(a => a != null).Select(a => a.PFor).DefaultIfEmpty(0).Average();
            PAcc = metricsBoxes.Where(a => a != null).Select(a => a.PAcc).DefaultIfEmpty(0).Average();
            PGMean = metricsBoxes.Where(a => a != null).Select(a => a.PGMean).DefaultIfEmpty(0).Average();
            PF1S = metricsBoxes.Where(a => a != null).Select(a => a.PF1S).DefaultIfEmpty(0).Average();
            PG1S = metricsBoxes.Where(a => a != null).Select(a => a.PG1S).DefaultIfEmpty(0).Average();
            PMcc = metricsBoxes.Where(a => a != null).Select(a => a.PMcc).DefaultIfEmpty(0).Average();
            PInformedness = metricsBoxes.Where(a => a != null).Select(a => a.PInformedness).DefaultIfEmpty(0).Average();
            PMarkedness = metricsBoxes.Where(a => a != null).Select(a => a.PMarkedness).DefaultIfEmpty(0).Average();
            PBalancedAccuracy = metricsBoxes.Where(a => a != null).Select(a => a.PBalancedAccuracy).DefaultIfEmpty(0).Average();
            PRocAucApproxAll = metricsBoxes.Where(a => a != null).Select(a => a.PRocAucApproxAll).DefaultIfEmpty(0).Average();
            PRocAucApproxElevenPoint = metricsBoxes.Where(a => a != null).Select(a => a.PRocAucApproxElevenPoint).DefaultIfEmpty(0).Average();
            PRocAucAll = metricsBoxes.Where(a => a != null).Select(a => a.PRocAucAll).DefaultIfEmpty(0).Average();
            PPrAucApproxAll = metricsBoxes.Where(a => a != null).Select(a => a.PPrAucApproxAll).DefaultIfEmpty(0).Average();
            PPrAucApproxElevenPoint = metricsBoxes.Where(a => a != null).Select(a => a.PPrAucApproxElevenPoint).DefaultIfEmpty(0).Average();
            PPriAucApproxAll = metricsBoxes.Where(a => a != null).Select(a => a.PPriAucApproxAll).DefaultIfEmpty(0).Average();
            PPriAucApproxElevenPoint = metricsBoxes.Where(a => a != null).Select(a => a.PPriAucApproxElevenPoint).DefaultIfEmpty(0).Average();
            PApAll = metricsBoxes.Where(a => a != null).Select(a => a.PApAll).DefaultIfEmpty(0).Average();
            PApElevenPoint = metricsBoxes.Where(a => a != null).Select(a => a.PApElevenPoint).DefaultIfEmpty(0).Average();
            PApiAll = metricsBoxes.Where(a => a != null).Select(a => a.PApiAll).DefaultIfEmpty(0).Average();
            PApiElevenPoint = metricsBoxes.Where(a => a != null).Select(a => a.PApiElevenPoint).DefaultIfEmpty(0).Average();
            PBrierInverseAll = metricsBoxes.Where(a => a != null).Select(a => a.PBrierInverseAll).DefaultIfEmpty(0).Average();
            PLrp = metricsBoxes.Where(a => a != null).Select(a => a.PLrp).DefaultIfEmpty(0).Average();
            PLrn = metricsBoxes.Where(a => a != null).Select(a => a.PLrn).DefaultIfEmpty(0).Average();
            PDor = metricsBoxes.Where(a => a != null).Select(a => a.PDor).DefaultIfEmpty(0).Average();
            PPrevalenceThreshold = metricsBoxes.Where(a => a != null).Select(a => a.PPrevalenceThreshold).DefaultIfEmpty(0).Average();
            PCriticalSuccessIndex = metricsBoxes.Where(a => a != null).Select(a => a.PCriticalSuccessIndex).DefaultIfEmpty(0).Average();
            PF1B00 = metricsBoxes.Where(a => a != null).Select(a => a.PF1B00).DefaultIfEmpty(0).Average();
            PF1B01 = metricsBoxes.Where(a => a != null).Select(a => a.PF1B01).DefaultIfEmpty(0).Average();
            PF1B02 = metricsBoxes.Where(a => a != null).Select(a => a.PF1B02).DefaultIfEmpty(0).Average();
            PF1B03 = metricsBoxes.Where(a => a != null).Select(a => a.PF1B03).DefaultIfEmpty(0).Average();
            PF1B04 = metricsBoxes.Where(a => a != null).Select(a => a.PF1B04).DefaultIfEmpty(0).Average();
            PF1B05 = metricsBoxes.Where(a => a != null).Select(a => a.PF1B05).DefaultIfEmpty(0).Average();
            PF1B06 = metricsBoxes.Where(a => a != null).Select(a => a.PF1B06).DefaultIfEmpty(0).Average();
            PF1B07 = metricsBoxes.Where(a => a != null).Select(a => a.PF1B07).DefaultIfEmpty(0).Average();
            PF1B08 = metricsBoxes.Where(a => a != null).Select(a => a.PF1B08).DefaultIfEmpty(0).Average();
            PF1B09 = metricsBoxes.Where(a => a != null).Select(a => a.PF1B09).DefaultIfEmpty(0).Average();
            PF1B10 = metricsBoxes.Where(a => a != null).Select(a => a.PF1B10).DefaultIfEmpty(0).Average();
        }

        internal void SetCm(double? p = null, double? n = null, double? pTp = null, double? pFn = null, double? nTn = null, double? nFp = null)
        {
            // set the cm (P, N, TP, FN, TN, FP) inputs whilst allowing omission of calculable values from the non-omitted values

            if (p != null && pTp == null && pFn != null) pTp = p - pFn;
            if (p != null && pTp != null && pFn == null) pFn = p - pTp;
            if (p == null && pTp != null && pFn != null) p = pTp + pFn;
            if (p == null || pTp == null || pFn == null) throw new Exception();
            if (p != pTp + pFn) throw new Exception();

            if (n != null && nTn == null && nFp != null) nTn = n - nFp;
            if (n != null && nTn != null && nFp == null) nFp = n - nTn;
            if (n == null && nTn != null && nFp != null) n = nTn + nFp;
            if (n != nTn + nFp) throw new Exception();
            if (n == null || nTn == null || nFp == null) throw new Exception();

            CmP = p.Value;
            CmN = n.Value;

            CmPTp = pTp.Value;
            CmPFn = pFn.Value;
            CmNTn = nTn.Value;
            CmNFp = nFp.Value;

            CalculateMetrics();
        }

        internal void SetRandomPerformance()
        {
            CmPTp = CmP / 2.0;
            CmPFn = CmP / 2.0;

            CmNFp = CmN / 2.0;
            CmNTn = CmN / 2.0;

            CalculateMetrics();
        }

        internal void ApplyImbalanceCorrection1()
        {
            if (CmP < CmN)
            {
                var nCorrect = CmN / CmP;

                CmP *= nCorrect;
                CmPTp *= nCorrect;
                CmPFn *= nCorrect;

                CmP = Math.Round(CmP, 2);
                CmPTp = Math.Round(CmPTp, 2);
                CmPFn = Math.Round(CmPFn, 2);

                CalculateMetrics();
            }
            else if (CmN < CmP)
            {
                var pCorrect = CmP / CmN;

                CmN *= pCorrect;
                CmNTn *= pCorrect;
                CmNFp *= pCorrect;

                CmN = Math.Round(CmN, 2);
                CmNTn = Math.Round(CmNTn, 2);
                CmNFp = Math.Round(CmNFp, 2);

                CalculateMetrics();
            }
        }

        //internal void ApplyImbalanceCorrection2()
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
        //    p_ROC_AUC_Approx_ElevenPoint *= correction;
        //    p_ROC_AUC_All *= correction;
        //    p_PR_AUC_Approx_All *= correction;
        //    p_PR_AUC_Approx_ElevenPoint *= correction;
        //    p_PRI_AUC_Approx_All *= correction;
        //    p_PRI_AUC_Approx_ElevenPoint *= correction;
        //    p_AP_All *= correction;
        //    p_AP_ElevenPoint *= correction;
        //    p_API_All *= correction;
        //    p_API_ElevenPoint *= correction;
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

        internal void CalculateMetrics()
        {
            const double zero = 0.0;
            const double one = 1.0;
            const double two = 2.0;

            PSupport = CmN == zero
                ? zero
                : (CmPTp + CmNFp) / CmN;
            PBaseRate = CmN == zero
                ? zero
                : (CmPTp + CmPFn) / CmN;

            PPrevalence = CmP + CmN == zero
                ? zero
                : (CmPFn + CmPTp) / (CmP + CmN);

            PMcr = CmP + CmN == zero
                ? zero
                : (CmNFp + CmPFn) / (CmP + CmN);
            PTpr = CmPTp + CmPFn == zero
                ? zero
                : CmPTp / (CmPTp + CmPFn);
            PTnr = CmNTn + CmNFp == zero
                ? zero
                : CmNTn / (CmNTn + CmNFp);
            PPrecision = CmPTp + CmNFp == zero
                ? zero
                : CmPTp / (CmPTp + CmNFp);

            POverlap = CmPTp + CmNFp + CmPFn == zero
                ? zero
                : CmPTp / (CmPTp + CmNFp + CmPFn);

            // null error rate
            PNer = CmP + CmN == zero
                ? zero
                : (CmP > CmN
                    ? CmP
                    : CmN) / (CmP + CmN);

            // class null error rate
            PCner = CmP + CmN == zero
                ? zero
                : CmP / (CmP + CmN);

            // positive predictive value (differs from precision, can be equal)
            PPpv = PTpr * PPrevalence + (one - PTnr) * (one - PPrevalence) == zero
                ? zero
                : PTpr * PPrevalence / (PTpr * PPrevalence + (one - PTnr) * (one - PPrevalence));

            // negative predictive value
            PNpv = CmNTn + CmPFn == zero
                ? zero
                : CmNTn / (CmNTn + CmPFn);

            // false negative rate
            PFnr = CmPFn + CmPTp == zero
                ? zero
                : CmPFn / (CmPFn + CmPTp);

            // false positive rate
            PFpr = CmNFp + CmNTn == zero
                ? zero
                : CmNFp / (CmNFp + CmNTn);

            // false discovery rate
            PFdr = CmNFp + CmPTp == zero
                ? zero
                : CmNFp / (CmNFp + CmPTp);

            // false omission rate
            PFor = CmPFn + CmNTn == zero
                ? zero
                : CmPFn / (CmPFn + CmNTn);

            // accuracy
            PAcc = CmP + CmN == zero
                ? zero
                : (CmPTp + CmNTn) / (CmP + CmN);

            // test error rate (inaccuracy)
            PEr = one - PAcc;

            PYoudenIndex = PTpr + PTnr - one;


            //Kappa = (totalAccuracy - randomAccuracy) / (1 - randomAccuracy)
            //totalAccuracy = (TP + TN) / (TP + TN + FP + FN)
            //randomAccuracy = referenceLikelihood(F) * resultLikelihood(F) + referenceLikelihood(T) * resultLikelihood(T)
            //randomAccuracy = (ActualFalse * PredictedFalse + ActualTrue * PredictedTrue) / Total * Total
            //randomAccuracy = (TN + FP) * (TN + FN) + (FN + TP) * (FP + TP) / Total * Total

            PRndAcc = ((CmNTn + CmNFp) * (CmNTn + CmPFn) + (CmPFn + CmPTp) * (CmNFp + CmPTp)) / ((CmPTp + CmNTn + CmNFp + CmPFn) * (CmPTp + CmNTn + CmNFp + CmPFn));

            PKappa = one - PRndAcc == zero
                ? zero
                : (PAcc - PRndAcc) / (one - PRndAcc);

            // Geometric Mean score
            PGMean = Math.Sqrt(PTpr * PTnr);

            // F1 score
            PF1S = PPpv + PTpr == zero
                ? zero
                : 2 * PPpv * PTpr / (PPpv + PTpr);

            // G1 score (same as Fowlkes–Mallows index?)
            PG1S = PPpv + PTpr == zero
                ? zero
                : Math.Sqrt(PPpv * PTpr);

            // Matthews correlation coefficient (MCC)
            PMcc = Math.Sqrt((CmPTp + CmNFp) * (CmPTp + CmPFn) * (CmNTn + CmNFp) * (CmNTn + CmPFn)) == zero
                ? zero
                : (CmPTp * CmNTn - CmNFp * CmPFn) / Math.Sqrt((CmPTp + CmNFp) * (CmPTp + CmPFn) * (CmNTn + CmNFp) * (CmNTn + CmPFn));
            PInformedness = PTpr + PTnr - one;
            PMarkedness = PPpv + PNpv - one;
            PBalancedAccuracy = (PTpr + PTnr) / two;

            // likelihood ratio for positive results
            PLrp = one - PTnr == zero
                ? zero
                : PTpr / (one - PTnr);

            // likelihood ratio for negative results
            PLrn = PTnr == zero
                ? zero
                : (one - PTpr) / PTnr;

            // Diagnostic odds ratio
            PDor = PLrn == zero
                ? zero
                : PLrp / PLrn;

            // Prevalence Threshold
            PPrevalenceThreshold = PTpr + PTnr - one == zero
                ? zero
                : (Math.Sqrt(PTpr * (-PTnr + one)) + PTnr - one) / (PTpr + PTnr - one);

            // Threat Score / Critical Success Index
            PCriticalSuccessIndex = CmPTp + CmPFn + CmNFp == zero
                ? zero
                : CmPTp / (CmPTp + CmPFn + CmNFp);

            // Fowlkes–Mallows index - (same as G1 score?):
            // var FM_Index = Math.Sqrt(PPV * TPR);

            PF1B00 = Fbeta2(PPpv, PTpr, 0.0d);
            PF1B01 = Fbeta2(PPpv, PTpr, 0.1d);
            PF1B02 = Fbeta2(PPpv, PTpr, 0.2d);
            PF1B03 = Fbeta2(PPpv, PTpr, 0.3d);
            PF1B04 = Fbeta2(PPpv, PTpr, 0.4d);
            PF1B05 = Fbeta2(PPpv, PTpr, 0.5d);
            PF1B06 = Fbeta2(PPpv, PTpr, 0.6d);
            PF1B07 = Fbeta2(PPpv, PTpr, 0.7d);
            PF1B08 = Fbeta2(PPpv, PTpr, 0.8d);
            PF1B09 = Fbeta2(PPpv, PTpr, 0.9d);
            PF1B10 = Fbeta2(PPpv, PTpr, 1.0d);
        }

        private void Divide(double divisor)
        {
            if (divisor == 0) return;

            CmP /= divisor;
            CmN /= divisor;
            CmPTp /= divisor;
            CmPFn /= divisor;
            CmNTn /= divisor;
            CmNFp /= divisor;
            PTpr /= divisor;
            PTnr /= divisor;
            PPpv /= divisor;
            PPrecision /= divisor;
            PPrevalence /= divisor;
            PMcr /= divisor;
            PEr /= divisor;
            PNer /= divisor;
            PCner /= divisor;
            PKappa /= divisor;
            POverlap /= divisor;
            PRndAcc /= divisor;
            PSupport /= divisor;
            PBaseRate /= divisor;
            PYoudenIndex /= divisor;
            PNpv /= divisor;
            PFnr /= divisor;
            PFpr /= divisor;
            PFdr /= divisor;
            PFor /= divisor;
            PAcc /= divisor;
            PGMean /= divisor;
            PF1S /= divisor;
            PG1S /= divisor;
            PMcc /= divisor;
            PInformedness /= divisor;
            PMarkedness /= divisor;
            PBalancedAccuracy /= divisor;
            PRocAucApproxAll /= divisor;
            PRocAucApproxElevenPoint /= divisor;
            PRocAucAll /= divisor;
            PPrAucApproxAll /= divisor;
            PPrAucApproxElevenPoint /= divisor;
            PPriAucApproxAll /= divisor;
            PPriAucApproxElevenPoint /= divisor;
            PApAll /= divisor;
            PApElevenPoint /= divisor;
            PApiAll /= divisor;
            PApiElevenPoint /= divisor;
            PBrierInverseAll /= divisor;
            PLrp /= divisor;
            PLrn /= divisor;
            PDor /= divisor;
            PPrevalenceThreshold /= divisor;
            PCriticalSuccessIndex /= divisor;
            PF1B00 /= divisor;
            PF1B01 /= divisor;
            PF1B02 /= divisor;
            PF1B03 /= divisor;
            PF1B04 /= divisor;
            PF1B05 /= divisor;
            PF1B06 /= divisor;
            PF1B07 /= divisor;
            PF1B08 /= divisor;
            PF1B09 /= divisor;
            PF1B10 /= divisor;
        }

        internal static double Fbeta2(double ppv, double tpr, double fbeta)
        {
            return double.IsNaN(ppv) || double.IsNaN(tpr) || ppv == 0.0d || tpr == 0.0d
                ? 0.0d
                : 1 / (fbeta * (1 / ppv) + (1 - fbeta) * (1 / tpr));
        }

        internal double[] GetValuesByNames(string[] names)
        {
            return names.Select(a => GetValueByName(a)).ToArray();
        }

        internal double GetValueByName(string name)
        {
            var metrics = this;
            if (string.Equals(name, nameof(metrics.CmP), StringComparison.OrdinalIgnoreCase)) return metrics.CmP;
            if (string.Equals(name, nameof(metrics.CmN), StringComparison.OrdinalIgnoreCase)) return metrics.CmN;
            if (string.Equals(name, nameof(metrics.CmPTp), StringComparison.OrdinalIgnoreCase)) return metrics.CmPTp;
            if (string.Equals(name, nameof(metrics.CmPFn), StringComparison.OrdinalIgnoreCase)) return metrics.CmPFn;
            if (string.Equals(name, nameof(metrics.CmNTn), StringComparison.OrdinalIgnoreCase)) return metrics.CmNTn;
            if (string.Equals(name, nameof(metrics.CmNFp), StringComparison.OrdinalIgnoreCase)) return metrics.CmNFp;
            if (string.Equals(name, nameof(metrics.PTpr), StringComparison.OrdinalIgnoreCase)) return metrics.PTpr;
            if (string.Equals(name, nameof(metrics.PTnr), StringComparison.OrdinalIgnoreCase)) return metrics.PTnr;
            if (string.Equals(name, nameof(metrics.PPpv), StringComparison.OrdinalIgnoreCase)) return metrics.PPpv;
            if (string.Equals(name, nameof(metrics.PPrecision), StringComparison.OrdinalIgnoreCase)) return metrics.PPrecision;
            if (string.Equals(name, nameof(metrics.PPrevalence), StringComparison.OrdinalIgnoreCase)) return metrics.PPrevalence;
            if (string.Equals(name, nameof(metrics.PMcr), StringComparison.OrdinalIgnoreCase)) return metrics.PMcr;
            if (string.Equals(name, nameof(metrics.PEr), StringComparison.OrdinalIgnoreCase)) return metrics.PEr;
            if (string.Equals(name, nameof(metrics.PNer), StringComparison.OrdinalIgnoreCase)) return metrics.PNer;
            if (string.Equals(name, nameof(metrics.PCner), StringComparison.OrdinalIgnoreCase)) return metrics.PCner;
            if (string.Equals(name, nameof(metrics.PKappa), StringComparison.OrdinalIgnoreCase)) return metrics.PKappa;
            if (string.Equals(name, nameof(metrics.POverlap), StringComparison.OrdinalIgnoreCase)) return metrics.POverlap;
            if (string.Equals(name, nameof(metrics.PRndAcc), StringComparison.OrdinalIgnoreCase)) return metrics.PRndAcc;
            if (string.Equals(name, nameof(metrics.PSupport), StringComparison.OrdinalIgnoreCase)) return metrics.PSupport;
            if (string.Equals(name, nameof(metrics.PBaseRate), StringComparison.OrdinalIgnoreCase)) return metrics.PBaseRate;
            if (string.Equals(name, nameof(metrics.PYoudenIndex), StringComparison.OrdinalIgnoreCase)) return metrics.PYoudenIndex;
            if (string.Equals(name, nameof(metrics.PNpv), StringComparison.OrdinalIgnoreCase)) return metrics.PNpv;
            if (string.Equals(name, nameof(metrics.PFnr), StringComparison.OrdinalIgnoreCase)) return metrics.PFnr;
            if (string.Equals(name, nameof(metrics.PFpr), StringComparison.OrdinalIgnoreCase)) return metrics.PFpr;
            if (string.Equals(name, nameof(metrics.PFdr), StringComparison.OrdinalIgnoreCase)) return metrics.PFdr;
            if (string.Equals(name, nameof(metrics.PFor), StringComparison.OrdinalIgnoreCase)) return metrics.PFor;
            if (string.Equals(name, nameof(metrics.PAcc), StringComparison.OrdinalIgnoreCase)) return metrics.PAcc;
            if (string.Equals(name, nameof(metrics.PGMean), StringComparison.OrdinalIgnoreCase)) return metrics.PGMean;
            if (string.Equals(name, nameof(metrics.PF1S), StringComparison.OrdinalIgnoreCase)) return metrics.PF1S;
            if (string.Equals(name, nameof(metrics.PG1S), StringComparison.OrdinalIgnoreCase)) return metrics.PG1S;
            if (string.Equals(name, nameof(metrics.PMcc), StringComparison.OrdinalIgnoreCase)) return metrics.PMcc;
            if (string.Equals(name, nameof(metrics.PInformedness), StringComparison.OrdinalIgnoreCase)) return metrics.PInformedness;
            if (string.Equals(name, nameof(metrics.PMarkedness), StringComparison.OrdinalIgnoreCase)) return metrics.PMarkedness;
            if (string.Equals(name, nameof(metrics.PBalancedAccuracy), StringComparison.OrdinalIgnoreCase)) return metrics.PBalancedAccuracy;
            if (string.Equals(name, nameof(metrics.PRocAucApproxAll), StringComparison.OrdinalIgnoreCase)) return metrics.PRocAucApproxAll;
            if (string.Equals(name, nameof(metrics.PRocAucApproxElevenPoint), StringComparison.OrdinalIgnoreCase)) return metrics.PRocAucApproxElevenPoint;
            if (string.Equals(name, nameof(metrics.PRocAucAll), StringComparison.OrdinalIgnoreCase)) return metrics.PRocAucAll;
            if (string.Equals(name, nameof(metrics.PPrAucApproxAll), StringComparison.OrdinalIgnoreCase)) return metrics.PPrAucApproxAll;
            if (string.Equals(name, nameof(metrics.PPrAucApproxElevenPoint), StringComparison.OrdinalIgnoreCase)) return metrics.PPrAucApproxElevenPoint;
            if (string.Equals(name, nameof(metrics.PPriAucApproxAll), StringComparison.OrdinalIgnoreCase)) return metrics.PPriAucApproxAll;
            if (string.Equals(name, nameof(metrics.PPriAucApproxElevenPoint), StringComparison.OrdinalIgnoreCase)) return metrics.PPriAucApproxElevenPoint;
            if (string.Equals(name, nameof(metrics.PApAll), StringComparison.OrdinalIgnoreCase)) return metrics.PApAll;
            if (string.Equals(name, nameof(metrics.PApElevenPoint), StringComparison.OrdinalIgnoreCase)) return metrics.PApElevenPoint;
            if (string.Equals(name, nameof(metrics.PApiAll), StringComparison.OrdinalIgnoreCase)) return metrics.PApiAll;
            if (string.Equals(name, nameof(metrics.PApiElevenPoint), StringComparison.OrdinalIgnoreCase)) return metrics.PApiElevenPoint;
            if (string.Equals(name, nameof(metrics.PBrierInverseAll), StringComparison.OrdinalIgnoreCase)) return metrics.PBrierInverseAll;
            if (string.Equals(name, nameof(metrics.PLrp), StringComparison.OrdinalIgnoreCase)) return metrics.PLrp;
            if (string.Equals(name, nameof(metrics.PLrn), StringComparison.OrdinalIgnoreCase)) return metrics.PLrn;
            if (string.Equals(name, nameof(metrics.PDor), StringComparison.OrdinalIgnoreCase)) return metrics.PDor;
            if (string.Equals(name, nameof(metrics.PPrevalenceThreshold), StringComparison.OrdinalIgnoreCase)) return metrics.PPrevalenceThreshold;
            if (string.Equals(name, nameof(metrics.PCriticalSuccessIndex), StringComparison.OrdinalIgnoreCase)) return metrics.PCriticalSuccessIndex;
            if (string.Equals(name, nameof(metrics.PF1B00), StringComparison.OrdinalIgnoreCase)) return metrics.PF1B00;
            if (string.Equals(name, nameof(metrics.PF1B01), StringComparison.OrdinalIgnoreCase)) return metrics.PF1B01;
            if (string.Equals(name, nameof(metrics.PF1B02), StringComparison.OrdinalIgnoreCase)) return metrics.PF1B02;
            if (string.Equals(name, nameof(metrics.PF1B03), StringComparison.OrdinalIgnoreCase)) return metrics.PF1B03;
            if (string.Equals(name, nameof(metrics.PF1B04), StringComparison.OrdinalIgnoreCase)) return metrics.PF1B04;
            if (string.Equals(name, nameof(metrics.PF1B05), StringComparison.OrdinalIgnoreCase)) return metrics.PF1B05;
            if (string.Equals(name, nameof(metrics.PF1B06), StringComparison.OrdinalIgnoreCase)) return metrics.PF1B06;
            if (string.Equals(name, nameof(metrics.PF1B07), StringComparison.OrdinalIgnoreCase)) return metrics.PF1B07;
            if (string.Equals(name, nameof(metrics.PF1B08), StringComparison.OrdinalIgnoreCase)) return metrics.PF1B08;
            if (string.Equals(name, nameof(metrics.PF1B09), StringComparison.OrdinalIgnoreCase)) return metrics.PF1B09;
            if (string.Equals(name, nameof(metrics.PF1B10), StringComparison.OrdinalIgnoreCase)) return metrics.PF1B10;
            return    default;
        }

        internal List<double> GetSpecificValues(CrossValidationMetrics crossValidationMetrics)
        {
            var metricValues = new List<double>();

            if (crossValidationMetrics == 0) throw new Exception();

            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Tp)) metricValues.Add(CmPTp);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Fn)) metricValues.Add(CmPFn);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Tn)) metricValues.Add(CmNTn);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Fp)) metricValues.Add(CmNFp);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Tpr)) metricValues.Add(PTpr);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Tnr)) metricValues.Add(PTnr);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Ppv)) metricValues.Add(PPpv);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Precision)) metricValues.Add(PPrecision);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Prevalence)) metricValues.Add(PPrevalence);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Mcr)) metricValues.Add(PMcr);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Er)) metricValues.Add(PEr);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Ner)) metricValues.Add(PNer);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Cner)) metricValues.Add(PCner);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Kappa)) metricValues.Add(PKappa);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Overlap)) metricValues.Add(POverlap);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.RndAcc)) metricValues.Add(PRndAcc);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Support)) metricValues.Add(PSupport);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.BaseRate)) metricValues.Add(PBaseRate);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.YoudenIndex)) metricValues.Add(PYoudenIndex);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Npv)) metricValues.Add(PNpv);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Fnr)) metricValues.Add(PFnr);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Fpr)) metricValues.Add(PFpr);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Fdr)) metricValues.Add(PFdr);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.For)) metricValues.Add(PFor);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Acc)) metricValues.Add(PAcc);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Gm)) metricValues.Add(PGMean);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.F1S)) metricValues.Add(PF1S);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.G1S)) metricValues.Add(PG1S);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Mcc)) metricValues.Add(PMcc);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Informedness)) metricValues.Add(PInformedness);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Markedness)) metricValues.Add(PMarkedness);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.BalancedAccuracy)) metricValues.Add(PBalancedAccuracy);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.RocAucApproxAll)) metricValues.Add(PRocAucApproxAll);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.RocAucApproxElevenPoint)) metricValues.Add(PRocAucApproxElevenPoint);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.RocAucAll)) metricValues.Add(PRocAucAll);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.PrAucApproxAll)) metricValues.Add(PPrAucApproxAll);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.PrAucApproxElevenPoint)) metricValues.Add(PPrAucApproxElevenPoint);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.PriAucApproxAll)) metricValues.Add(PPriAucApproxAll);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.PriAucApproxElevenPoint)) metricValues.Add(PPriAucApproxElevenPoint);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.ApAll)) metricValues.Add(PApAll);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.ApElevenPoint)) metricValues.Add(PApElevenPoint);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.ApiAll)) metricValues.Add(PApiAll);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.ApiElevenPoint)) metricValues.Add(PApiElevenPoint);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.BrierInverseAll)) metricValues.Add(PBrierInverseAll);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Lrp)) metricValues.Add(PLrp);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Lrn)) metricValues.Add(PLrn);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.Dor)) metricValues.Add(PDor);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.PrevalenceThreshold)) metricValues.Add(PPrevalenceThreshold);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.CriticalSuccessIndex)) metricValues.Add(PCriticalSuccessIndex);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.F1B00)) metricValues.Add(PF1B00);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.F1B01)) metricValues.Add(PF1B01);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.F1B02)) metricValues.Add(PF1B02);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.F1B03)) metricValues.Add(PF1B03);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.F1B04)) metricValues.Add(PF1B04);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.F1B05)) metricValues.Add(PF1B05);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.F1B06)) metricValues.Add(PF1B06);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.F1B07)) metricValues.Add(PF1B07);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.F1B08)) metricValues.Add(PF1B08);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.F1B09)) metricValues.Add(PF1B09);
            if (crossValidationMetrics.HasFlag(CrossValidationMetrics.F1B10)) metricValues.Add(PF1B10);

            return metricValues;
        }


        internal (string name, double value)[] GetPerformanceValueStrings()
        {
            var result = new (string name, double value)[]
            {
                (nameof(CmPTp), CmPTp),
                (nameof(CmPFn), CmPFn),
                (nameof(CmNTn), CmNTn),
                (nameof(CmNFp), CmNFp),
                (nameof(PTpr), PTpr),
                (nameof(PTnr), PTnr),
                (nameof(PPpv), PPpv),
                (nameof(PPrecision), PPrecision),
                (nameof(PPrevalence), PPrevalence),
                (nameof(PMcr), PMcr),
                (nameof(PEr), PEr),
                (nameof(PNer), PNer),
                (nameof(PCner), PCner),
                (nameof(PKappa), PKappa),
                (nameof(POverlap), POverlap),
                (nameof(PRndAcc), PRndAcc),
                (nameof(PSupport), PSupport),
                (nameof(PBaseRate), PBaseRate),
                (nameof(PYoudenIndex), PYoudenIndex),
                (nameof(PNpv), PNpv),
                (nameof(PFnr), PFnr),
                (nameof(PFpr), PFpr),
                (nameof(PFdr), PFdr),
                (nameof(PFor), PFor),
                (nameof(PAcc), PAcc),
                (nameof(PGMean), PGMean),
                (nameof(PF1S), PF1S),
                (nameof(PG1S), PG1S),
                (nameof(PMcc), PMcc),
                (nameof(PInformedness), PInformedness),
                (nameof(PMarkedness), PMarkedness),
                (nameof(PBalancedAccuracy), PBalancedAccuracy),
                (nameof(PRocAucApproxAll), PRocAucApproxAll),
                (nameof(PRocAucApproxElevenPoint), PRocAucApproxElevenPoint),
                (nameof(PRocAucAll), PRocAucAll),
                (nameof(PPrAucApproxAll), PPrAucApproxAll),
                (nameof(PPrAucApproxElevenPoint), PPrAucApproxElevenPoint),
                (nameof(PPriAucApproxAll), PPriAucApproxAll),
                (nameof(PPriAucApproxElevenPoint), PPriAucApproxElevenPoint),
                (nameof(PApAll), PApAll),
                (nameof(PApElevenPoint), PApElevenPoint),
                (nameof(PApiAll), PApiAll),
                (nameof(PApiElevenPoint), PApiElevenPoint),
                (nameof(PBrierInverseAll), PBrierInverseAll),
                (nameof(PLrp), PLrp),
                (nameof(PLrn), PLrn),
                (nameof(PDor), PDor),
                (nameof(PPrevalenceThreshold), PPrevalenceThreshold),
                (nameof(PCriticalSuccessIndex), PCriticalSuccessIndex),
                (nameof(PF1B00), PF1B00),
                (nameof(PF1B01), PF1B01),
                (nameof(PF1B02), PF1B02),
                (nameof(PF1B03), PF1B03),
                (nameof(PF1B04), PF1B04),
                (nameof(PF1B05), PF1B05),
                (nameof(PF1B06), PF1B06),
                (nameof(PF1B07), PF1B07),
                (nameof(PF1B08), PF1B08),
                (nameof(PF1B09), PF1B09),
                (nameof(PF1B10), PF1B10)
            };

            return result;
        }

        public string[] CsvValuesArray()
        {
            return new[]
            {
                CmP.ToString("G17", NumberFormatInfo.InvariantInfo),
                CmN.ToString("G17", NumberFormatInfo.InvariantInfo),
                CmPTp.ToString("G17", NumberFormatInfo.InvariantInfo),
                CmPFn.ToString("G17", NumberFormatInfo.InvariantInfo),
                CmNTn.ToString("G17", NumberFormatInfo.InvariantInfo),
                CmNFp.ToString("G17", NumberFormatInfo.InvariantInfo),
                PTpr.ToString("G17", NumberFormatInfo.InvariantInfo),
                PTnr.ToString("G17", NumberFormatInfo.InvariantInfo),
                PPpv.ToString("G17", NumberFormatInfo.InvariantInfo),
                PPrecision.ToString("G17", NumberFormatInfo.InvariantInfo),
                PPrevalence.ToString("G17", NumberFormatInfo.InvariantInfo),
                PMcr.ToString("G17", NumberFormatInfo.InvariantInfo),
                PEr.ToString("G17", NumberFormatInfo.InvariantInfo),
                PNer.ToString("G17", NumberFormatInfo.InvariantInfo),
                PCner.ToString("G17", NumberFormatInfo.InvariantInfo),
                PKappa.ToString("G17", NumberFormatInfo.InvariantInfo),
                POverlap.ToString("G17", NumberFormatInfo.InvariantInfo),
                PRndAcc.ToString("G17", NumberFormatInfo.InvariantInfo),
                PSupport.ToString("G17", NumberFormatInfo.InvariantInfo),
                PBaseRate.ToString("G17", NumberFormatInfo.InvariantInfo),
                PYoudenIndex.ToString("G17", NumberFormatInfo.InvariantInfo),
                PNpv.ToString("G17", NumberFormatInfo.InvariantInfo),
                PFnr.ToString("G17", NumberFormatInfo.InvariantInfo),
                PFpr.ToString("G17", NumberFormatInfo.InvariantInfo),
                PFdr.ToString("G17", NumberFormatInfo.InvariantInfo),
                PFor.ToString("G17", NumberFormatInfo.InvariantInfo),
                PAcc.ToString("G17", NumberFormatInfo.InvariantInfo),
                PGMean.ToString("G17", NumberFormatInfo.InvariantInfo),
                PF1S.ToString("G17", NumberFormatInfo.InvariantInfo),
                PG1S.ToString("G17", NumberFormatInfo.InvariantInfo),
                PMcc.ToString("G17", NumberFormatInfo.InvariantInfo),
                PInformedness.ToString("G17", NumberFormatInfo.InvariantInfo),
                PMarkedness.ToString("G17", NumberFormatInfo.InvariantInfo),
                PBalancedAccuracy.ToString("G17", NumberFormatInfo.InvariantInfo),
                PRocAucApproxAll.ToString("G17", NumberFormatInfo.InvariantInfo),
                PRocAucApproxElevenPoint.ToString("G17", NumberFormatInfo.InvariantInfo),
                PRocAucAll.ToString("G17", NumberFormatInfo.InvariantInfo),
                PPrAucApproxAll.ToString("G17", NumberFormatInfo.InvariantInfo),
                PPrAucApproxElevenPoint.ToString("G17", NumberFormatInfo.InvariantInfo),
                PPriAucApproxAll.ToString("G17", NumberFormatInfo.InvariantInfo),
                PPriAucApproxElevenPoint.ToString("G17", NumberFormatInfo.InvariantInfo),
                PApAll.ToString("G17", NumberFormatInfo.InvariantInfo),
                PApElevenPoint.ToString("G17", NumberFormatInfo.InvariantInfo),
                PApiAll.ToString("G17", NumberFormatInfo.InvariantInfo),
                PApiElevenPoint.ToString("G17", NumberFormatInfo.InvariantInfo),
                PBrierInverseAll.ToString("G17", NumberFormatInfo.InvariantInfo),
                PLrp.ToString("G17", NumberFormatInfo.InvariantInfo),
                PLrn.ToString("G17", NumberFormatInfo.InvariantInfo),
                PDor.ToString("G17", NumberFormatInfo.InvariantInfo),
                PPrevalenceThreshold.ToString("G17", NumberFormatInfo.InvariantInfo),
                PCriticalSuccessIndex.ToString("G17", NumberFormatInfo.InvariantInfo),
                PF1B00.ToString("G17", NumberFormatInfo.InvariantInfo),
                PF1B01.ToString("G17", NumberFormatInfo.InvariantInfo),
                PF1B02.ToString("G17", NumberFormatInfo.InvariantInfo),
                PF1B03.ToString("G17", NumberFormatInfo.InvariantInfo),
                PF1B04.ToString("G17", NumberFormatInfo.InvariantInfo),
                PF1B05.ToString("G17", NumberFormatInfo.InvariantInfo),
                PF1B06.ToString("G17", NumberFormatInfo.InvariantInfo),
                PF1B07.ToString("G17", NumberFormatInfo.InvariantInfo),
                PF1B08.ToString("G17", NumberFormatInfo.InvariantInfo),
                PF1B09.ToString("G17", NumberFormatInfo.InvariantInfo),
                PF1B10.ToString("G17", NumberFormatInfo.InvariantInfo)
            }.Select(a => a.Replace(",", ";", StringComparison.OrdinalIgnoreCase)).ToArray();
        }

        public string CsvValuesString()
        {
            return string.Join(",", CsvValuesArray());
        }

        [Flags] internal enum CrossValidationMetrics : ulong
        {
            None = 0UL, Tp = 1UL << 01, Fn = 1UL << 02,
            Tn = 1UL << 03, Fp = 1UL << 04, Tpr = 1UL << 05,
            Tnr = 1UL << 06, Ppv = 1UL << 07, Precision = 1UL << 08,
            Prevalence = 1UL << 09, Mcr = 1UL << 10, Er = 1UL << 11,
            Ner = 1UL << 12, Cner = 1UL << 13, Kappa = 1UL << 14,
            Overlap = 1UL << 15, RndAcc = 1UL << 16, Support = 1UL << 17,
            BaseRate = 1UL << 18, YoudenIndex = 1UL << 19, Npv = 1UL << 20,
            Fnr = 1UL << 21, Fpr = 1UL << 22, Fdr = 1UL << 23,
            For = 1UL << 24, Acc = 1UL << 25, Gm = 1UL << 26,
            F1S = 1UL << 27, G1S = 1UL << 28, Mcc = 1UL << 29,
            Informedness = 1UL << 30, Markedness = 1UL << 31, BalancedAccuracy = 1UL << 32,
            RocAucApproxAll = 1UL << 33, RocAucApproxElevenPoint = 1UL << 34, RocAucAll = 1UL << 35,
            PrAucApproxAll = 1UL << 36, PrAucApproxElevenPoint = 1UL << 37, PriAucApproxAll = 1UL << 38,
            PriAucApproxElevenPoint = 1UL << 39, ApAll = 1UL << 40, ApElevenPoint = 1UL << 41,
            ApiAll = 1UL << 42, ApiElevenPoint = 1UL << 43, BrierInverseAll = 1UL << 44,
            Lrp = 1UL << 45, Lrn = 1UL << 46, Dor = 1UL << 47,
            PrevalenceThreshold = 1UL << 48, CriticalSuccessIndex = 1UL << 49, F1B00 = 1UL << 50,
            F1B01 = 1UL << 51, F1B02 = 1UL << 52, F1B03 = 1UL << 53,
            F1B04 = 1UL << 54, F1B05 = 1UL << 55, F1B06 = 1UL << 56,
            F1B07 = 1UL << 57, F1B08 = 1UL << 58, F1B09 = 1UL << 59,
            F1B10 = 1UL << 60

            //ROC_AUC_ElevenPoint = 1UL << 36,
        }
    }
}