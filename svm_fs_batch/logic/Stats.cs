﻿using System;
using System.Linq;

namespace SvmFsBatch
{
    internal class gkStats
    {
        internal static readonly string[] CsvHeaderValuesArray =
        {
            nameof(AbsSum),
            nameof(Count),
            nameof(CountOdd),
            nameof(CountEven),
            nameof(CountDistinctValues),
            nameof(CountNonZeroValues),
            nameof(CountZeroValues),
            nameof(DevStandard),
            nameof(InterquartileRange),
            nameof(Kurtosis),
            nameof(MadMeanArithmetic),
            nameof(MadMeanGeometricCorrected),
            nameof(MadMeanHarmonicCorrected),
            nameof(MadMedianQ1),
            nameof(MadMedianQ2),
            nameof(MadMedianQ3),
            nameof(MadMidRange),
            nameof(MadMode),
            nameof(Max),
            nameof(MeanArithmetic),
            nameof(MeanGeometricCorrected),
            nameof(MeanHarmonicCorrected),
            nameof(MedianQ1),
            nameof(MedianQ2),
            nameof(MedianQ3),
            nameof(MidRange),
            nameof(Min),
            nameof(Modality),
            nameof(Mode),
            nameof(Range),
            nameof(RootMeanSquare),
            nameof(Skewness),
            nameof(SrSos),
            nameof(Sum),
            nameof(Variance)
        };

        internal static readonly string CsvHeaderValuesString = string.Join(",", CsvHeaderValuesArray);
        internal double AbsSum;
        internal uint Count;
        internal uint CountDistinctValues;
        internal uint CountEven;
        internal uint CountNonZeroValues;
        internal uint CountOdd;
        internal uint CountZeroValues;
        internal double DevStandard;
        internal double InterquartileRange;
        internal double Kurtosis;
        internal double MadMeanArithmetic;
        internal double MadMeanGeometricCorrected;
        internal double MadMeanHarmonicCorrected;
        internal double MadMedianQ1;
        internal double MadMedianQ2;
        internal double MadMedianQ3;
        internal double MadMidRange;
        internal double MadMode;
        internal double Max;
        internal double MeanArithmetic;
        internal double MeanGeometricCorrected;
        internal double MeanHarmonicCorrected;
        internal double MedianQ1;
        internal double MedianQ2;
        internal double MedianQ3;
        internal double MidRange;
        internal double Min;
        internal double Modality;
        internal double Mode;
        internal double Range;
        internal double RootMeanSquare;
        internal double Skewness;
        internal double SrSos;
        internal double Sum;
        internal double Variance;


        internal gkStats(double[] data, bool presorted = false)
        {
            if (data.Any(a => double.IsInfinity(a) || double.IsNaN(a))) throw new ArgumentOutOfRangeException(nameof(data), "");

            data = presorted
                ? data
                : data.OrderBy(a => a).ToArray();

            Count = (uint) data.Length;
            CountEven = (uint) data.Count(a => a % 2 == 0);
            CountOdd = Count - CountEven;
            CountDistinctValues = (uint) data.Distinct().Count();
            CountNonZeroValues = (uint) data.Count(a => a != 0);
            CountZeroValues = (uint) data.Length - CountNonZeroValues;
            Sum = data.Sum();
            AbsSum = data.Sum(Math.Abs);
            RootMeanSquare = Rms(data);
            SrSos = SqrtSumOfSqrs(data);
            MeanArithmetic = Count != 0
                ? Sum / Count
                : 0d;
            var hm = HarmonicMean(data);
            MeanHarmonicCorrected = hm.corrected;
            //mean_harmonic_nonzeros = hm.nonzeros;
            var gm = GeometricMean(data);
            MeanGeometricCorrected = gm.corrected;
            //mean_geometric_nonzeros = gm.nonzeros;
            var stat = Shape(data);
            Variance = stat.variance;
            DevStandard = stat.stdev;
            Kurtosis = stat.kurtosis;
            Skewness = stat.skewness;
            Min = data[0];
            Max = data[^1];
            Range = Max - Min;
            MidRange = (Max + Min) / 2.0;
            MedianQ1 = Percentile(data, 25);
            MedianQ2 = Percentile(data, 50);
            MedianQ3 = Percentile(data, 75);
            InterquartileRange = Math.Abs(MedianQ3 - MedianQ1);

            var sortedDataGroups = data.GroupBy(x => x).ToArray();
            var sortedDataMaxGroupCount = sortedDataGroups.Max(g => g.Count());
            var modes = sortedDataGroups.Where(a => a.Count() == sortedDataMaxGroupCount).ToList();
            Mode = modes.Select(a => a.Key).DefaultIfEmpty(0).Average();
            Modality = modes.Count == Count
                ? 0d
                : modes.Count;

            MadMeanArithmetic = Mad(data, MeanArithmetic);
            MadMeanGeometricCorrected = Mad(data, MeanGeometricCorrected);
            MadMeanHarmonicCorrected = Mad(data, MeanHarmonicCorrected);
            MadMedianQ1 = Mad(data, MedianQ1);
            MadMedianQ2 = Mad(data, MedianQ2);
            MadMedianQ3 = Mad(data, MedianQ3);
            MadMode = Mad(data, Mode);
            MadMidRange = Mad(data, MidRange);

            FixDouble();
        }

        internal string[] CsvValuesArray()
        {
            return new[]
            {
                $@"{AbsSum:G17}",
                $@"{Count}",
                $@"{CountOdd}",
                $@"{CountEven}",
                $@"{CountDistinctValues}",
                $@"{CountNonZeroValues}",
                $@"{CountZeroValues}",
                $@"{DevStandard:G17}",
                $@"{InterquartileRange:G17}",
                $@"{Kurtosis:G17}",
                $@"{MadMeanArithmetic:G17}",
                $@"{MadMeanGeometricCorrected:G17}",
                $@"{MadMeanHarmonicCorrected:G17}",
                $@"{MadMedianQ1:G17}",
                $@"{MadMedianQ2:G17}",
                $@"{MadMedianQ3:G17}",
                $@"{MadMidRange:G17}",
                $@"{MadMode:G17}",
                $@"{Max:G17}",
                $@"{MeanArithmetic:G17}",
                $@"{MeanGeometricCorrected:G17}",
                $@"{MeanHarmonicCorrected:G17}",
                $@"{MedianQ1:G17}",
                $@"{MedianQ2:G17}",
                $@"{MedianQ3:G17}",
                $@"{MidRange:G17}",
                $@"{Min:G17}",
                $@"{Modality:G17}",
                $@"{Mode:G17}",
                $@"{Range:G17}",
                $@"{RootMeanSquare:G17}",
                $@"{Skewness:G17}",
                $@"{SrSos:G17}",
                $@"{Sum:G17}",
                $@"{Variance:G17}"
            };
        }

        internal string CsvValuesString()
        {
            return string.Join(",", CsvValuesArray());
        }

        internal static (double zeros, double nonzeros, double corrected) GeometricMean(double[] values)
        {
            if (values == null || values.Length == 0 || values.All(a => a == 0)) return (0.0, 0.0, 0.0);

            var useLog = false;
            var valuesNonzero = values.Where(a => a != 0).ToArray();
            var numZeros = values.Length - valuesNonzero.Length;
            var sum = useLog
                ? 0.0
                : 1.0;

            if (!useLog)
                for (var i = 0; i < valuesNonzero.Length; i++)
                {
                    sum *= valuesNonzero[i];

                    if (double.IsInfinity(sum) || double.IsNaN(sum) || sum >= double.MaxValue)
                    {
                        useLog = true;
                        sum = 0.0;
                        break;
                    }
                }

            if (useLog)
                for (var i = 0; i < valuesNonzero.Length; i++)
                    sum += Math.Log(valuesNonzero[i]);

            var resultNonzeros = useLog
                ? Math.Exp(sum / valuesNonzero.Length)
                : Math.Pow(sum, 1.0 / valuesNonzero.Length);
            var correction = (values.Length - (double) numZeros) / values.Length;
            var resultCorrected = resultNonzeros * correction;
            var resultZeros = numZeros == 0
                ? resultNonzeros
                : 0.0;

            return (resultZeros, resultNonzeros, resultCorrected);
        }

        internal static (double zeros, double nonzeros, double corrected) HarmonicMean(double[] values)
        {
            if (values == null || values.Length == 0 || values.All(a => a == 0)) return (0.0, 0.0, 0.0);

            //  hm = values.Length / values.Sum(i => 1.0 / i);
            var valuesNonzero = values.Where(a => a != 0).ToArray();
            var numZeros = values.Length - valuesNonzero.Length;
            var resultNonzeros = valuesNonzero.Length / valuesNonzero.Sum(i => 1.0 / i); // same as (1 / values_nonzero.Select(i => 1.0 / i).Average());
            var correction = (values.Length - (double) numZeros) / values.Length;
            var resultCorrected = resultNonzeros * correction;
            var resultZeros = numZeros == 0
                ? resultNonzeros
                : 0.0;

            return (resultZeros, resultNonzeros, resultCorrected);
        }

        internal static double SampleVariance(double[] samples)
        {
            if (samples == null || samples.Length <= 1) return 0;

            var variance = 0.0;
            var t = samples[0];
            for (var i = 1; i < samples.Length; i++)
            {
                t += samples[i];
                var diff = (i + 1) * samples[i] - t;
                variance += diff * diff / ((i + 1.0) * i);
            }

            return variance / (samples.Length - 1);
        }

        internal static (double variance, double stdev) SampleStandardDeviation(double[] samples)
        {
            if (samples == null || samples.Length <= 1) return (0, 0);

            var variance = SampleVariance(samples);
            var stdev = Math.Sqrt(variance);

            return (variance, stdev);
        }

        internal static double Rms(double[] data)
        {
            if (data == null || data.Length == 0) return double.NaN;

            double mean = 0;
            ulong m = 0;
            for (var i = 0; i < data.Length; i++) mean += (data[i] * data[i] - mean) / ++m;

            return Math.Sqrt(mean);
        }

        internal static double SqrtSumOfSqrs(double[] list)
        {
            return list == null || list.Length == 0
                ? 0d
                : Math.Sqrt(list.Sum(a => Math.Abs(a) * Math.Abs(a)));
        }

        internal static (double skewness, double kurtosis, double mean, double variance, double stdev) Shape(double[] data)
        {
            if (data == null || data.Length == 0 || data.All(a => a == 0)) return (0.0, 0.0, 0.0, 0.0, 0.0);

            var mean = 0.0;
            var error = 0.0;
            var skewness = 0.0;
            var kurtosis = 0.0;
            long n = 0;

            for (var index = 0; index < data.Length; index++)
            {
                var delta = data[index] - mean;
                var scaleDelta = delta / ++n;
                var scaleDeltaSqr = scaleDelta * scaleDelta;
                var tmpDelta = delta * (n - 1);

                mean += scaleDelta;

                kurtosis += tmpDelta * scaleDelta * scaleDeltaSqr * (n * n - 3 * n + 3) + 6 * scaleDeltaSqr * error - 4 * scaleDelta * skewness;

                skewness += tmpDelta * scaleDeltaSqr * (n - 2) - 3 * scaleDelta * error;
                error += tmpDelta * scaleDelta;
            }

            var variance = n > 1
                ? error / (n - 1)
                : 0;
            var stdev = variance != 0
                ? Math.Sqrt(variance)
                : 0;
            skewness = variance != 0 && n > 2
                ? (double) n / ((n - 1) * (n - 2)) * (skewness / (variance * stdev))
                : 0;
            kurtosis = variance != 0 && n > 3
                ? ((double) n * n - 1) / ((n - 2) * (n - 3)) * (n * kurtosis / (error * error) - 3 + 6.0 / (n + 1))
                : 0;

            return (skewness, kurtosis, mean, variance, stdev);
        }

        public void FixDouble()
        {
            FixDouble(ref Sum);
            FixDouble(ref MeanArithmetic);
            FixDouble(ref MeanGeometricCorrected);
            FixDouble(ref MeanHarmonicCorrected);
            FixDouble(ref Min);
            FixDouble(ref Max);
            FixDouble(ref Range);
            FixDouble(ref MidRange);
            FixDouble(ref Variance);
            FixDouble(ref DevStandard);
            FixDouble(ref RootMeanSquare);
            FixDouble(ref SrSos);
            FixDouble(ref Skewness);
            FixDouble(ref Kurtosis);
            FixDouble(ref InterquartileRange);
            FixDouble(ref MedianQ1);
            FixDouble(ref MedianQ2);
            FixDouble(ref MedianQ3);
            FixDouble(ref Modality);
            FixDouble(ref Mode);
            FixDouble(ref MadMeanArithmetic);
            FixDouble(ref MadMeanHarmonicCorrected);
            FixDouble(ref MadMeanGeometricCorrected);
            FixDouble(ref MadMedianQ1);
            FixDouble(ref MadMedianQ2);
            FixDouble(ref MadMedianQ3);
            FixDouble(ref MadMode);
            FixDouble(ref MadMidRange);
        }

        public static void FixDouble(ref double value)
        {
            const double cDoubleMax = 1.79769e+308;
            const double cDoubleMin = -cDoubleMax;
            const double doubleZero = 0.0;

            if (value == 0 || value >= cDoubleMin && value <= cDoubleMax) return;

            if (double.IsPositiveInfinity(value) || value >= cDoubleMax || value >= double.MaxValue) value = cDoubleMax;
            else if (double.IsNegativeInfinity(value) || value <= cDoubleMin || value <= double.MinValue) value = cDoubleMin;
            else if (double.IsNaN(value)) value = doubleZero;
        }

        internal static double Mad(double[] values, double? centre)
        {
            if (values == null || values.Length == 0) return 0;

            if (centre == null) centre = values.Average();

            var mad = values.Sum(a => Math.Abs(centre.Value - a)) / values.Length;

            return mad;
        }

        internal static double Percentile(double[] sortedData, double p)
        {
            if (sortedData == null || sortedData.Length == 0) return 0;

            if (sortedData.Length == 1) return sortedData[0];

            if (p >= 100.0) return sortedData[^1];

            var position = (sortedData.Length + 1) * p / 100.0;
            double leftNumber;
            double rightNumber;

            var n = p / 100.0 * (sortedData.Length - 1) + 1.0;

            if (position >= 1)
            {
                leftNumber = sortedData[(int) Math.Floor(n) - 1];
                rightNumber = sortedData[(int) Math.Floor(n)];
            }
            else
            {
                leftNumber = sortedData[0];
                rightNumber = sortedData[1];
            }

            if (leftNumber == rightNumber) return leftNumber;

            var part = n - Math.Floor(n);
            return leftNumber + part * (rightNumber - leftNumber);
        }
    }
}