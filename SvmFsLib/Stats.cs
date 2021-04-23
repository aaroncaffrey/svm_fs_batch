using System;
using System.Linq;

namespace SvmFsLib
{
    public class Stats
    {
        public const string ModuleName = nameof(Stats);

        public Stats()
        {

        }

        public static readonly string[] CsvHeaderValuesArray =
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

        public static readonly string CsvHeaderValuesString = string.Join(",", CsvHeaderValuesArray);
        public double AbsSum;
        public uint Count;
        public uint CountDistinctValues;
        public uint CountEven;
        public uint CountNonZeroValues;
        public uint CountOdd;
        public uint CountZeroValues;
        public double DevStandard;
        public double InterquartileRange;
        public double Kurtosis;
        public double MadMeanArithmetic;
        public double MadMeanGeometricCorrected;
        public double MadMeanHarmonicCorrected;
        public double MadMedianQ1;
        public double MadMedianQ2;
        public double MadMedianQ3;
        public double MadMidRange;
        public double MadMode;
        public double Max;
        public double MeanArithmetic;
        public double MeanGeometricCorrected;
        public double MeanHarmonicCorrected;
        public double MedianQ1;
        public double MedianQ2;
        public double MedianQ3;
        public double MidRange;
        public double Min;
        public double Modality;
        public double Mode;
        public double Range;
        public double RootMeanSquare;
        public double Skewness;
        public double SrSos;
        public double Sum;
        public double Variance;


        public Stats(double[] data, bool presorted = false)
        {
            Logging.LogCall(ModuleName);

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

        public string[] CsvValuesArray()
        {
            Logging.LogCall(ModuleName);
            Logging.LogExit(ModuleName); return new[]
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

        public string CsvValuesString()
        {
            Logging.LogCall(ModuleName);

            Logging.LogExit(ModuleName); return string.Join(",", CsvValuesArray());
        }

        public static (double zeros, double nonzeros, double corrected) GeometricMean(double[] values)
        {
            Logging.LogCall(ModuleName);

            if (values == null || values.Length == 0 || values.All(a => a == 0)) {Logging.LogExit(ModuleName); return (0.0, 0.0, 0.0); }

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

            Logging.LogExit(ModuleName); return (resultZeros, resultNonzeros, resultCorrected);
        }

        public static (double zeros, double nonzeros, double corrected) HarmonicMean(double[] values)
        {
            Logging.LogCall(ModuleName);

            if (values == null || values.Length == 0 || values.All(a => a == 0)) {Logging.LogExit(ModuleName); return (0.0, 0.0, 0.0); }

            //  hm = values.Length / values.Sum(i => 1.0 / i);
            var valuesNonzero = values.Where(a => a != 0).ToArray();
            var numZeros = values.Length - valuesNonzero.Length;
            var resultNonzeros = valuesNonzero.Length / valuesNonzero.Sum(i => 1.0 / i); // same as (1 / values_nonzero.Select(i => 1.0 / i).Average());
            var correction = (values.Length - (double) numZeros) / values.Length;
            var resultCorrected = resultNonzeros * correction;
            var resultZeros = numZeros == 0
                ? resultNonzeros
                : 0.0;

            Logging.LogExit(ModuleName); return (resultZeros, resultNonzeros, resultCorrected);
        }

        public static double SampleVariance(double[] samples)
        {
            Logging.LogCall(ModuleName);

            if (samples == null || samples.Length <= 1) {Logging.LogExit(ModuleName); return 0; }

            var variance = 0.0;
            var t = samples[0];
            for (var i = 1; i < samples.Length; i++)
            {
                t += samples[i];
                var diff = (i + 1) * samples[i] - t;
                variance += diff * diff / ((i + 1.0) * i);
            }

            Logging.LogExit(ModuleName); return variance / (samples.Length - 1);
        }

        public static (double variance, double stdev) SampleStandardDeviation(double[] samples)
        {
            Logging.LogCall(ModuleName);

            if (samples == null || samples.Length <= 1) {Logging.LogExit(ModuleName); return (0, 0); }

            var variance = SampleVariance(samples);
            var stdev = Math.Sqrt(variance);

            Logging.LogExit(ModuleName); return (variance, stdev);
        }

        public static double Rms(double[] data)
        {
            Logging.LogCall(ModuleName);


            if (data == null || data.Length == 0) {Logging.LogExit(ModuleName); return double.NaN; }

            double mean = 0;
            ulong m = 0;
            for (var i = 0; i < data.Length; i++) mean += (data[i] * data[i] - mean) / ++m;

            Logging.LogExit(ModuleName); return Math.Sqrt(mean);
        }

        public static double SqrtSumOfSqrs(double[] list)
        {
            Logging.LogCall(ModuleName);


            Logging.LogExit(ModuleName); return list == null || list.Length == 0
                ? 0d
                : Math.Sqrt(list.Sum(a => Math.Abs(a) * Math.Abs(a)));
        }

        public static (double skewness, double kurtosis, double mean, double variance, double stdev) Shape(double[] data)
        {
            Logging.LogCall(ModuleName);

            if (data == null || data.Length == 0 || data.All(a => a == 0)) {Logging.LogExit(ModuleName); return (0.0, 0.0, 0.0, 0.0, 0.0); }

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

            Logging.LogExit(ModuleName); return (skewness, kurtosis, mean, variance, stdev);
        }

        public void FixDouble()
        {
            Logging.LogCall(ModuleName);

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

            Logging.LogExit(ModuleName);
        }

        public static void FixDouble(ref double value)
        {
            Logging.LogCall(ModuleName);

            const double cDoubleMax = 1.79769e+308;
            const double cDoubleMin = -cDoubleMax;
            const double doubleZero = 0.0;

            if (value == 0 || value >= cDoubleMin && value <= cDoubleMax) { Logging.LogExit(ModuleName); return; }

            if (double.IsPositiveInfinity(value) || value >= cDoubleMax || value >= double.MaxValue) value = cDoubleMax;
            else if (double.IsNegativeInfinity(value) || value <= cDoubleMin || value <= double.MinValue) value = cDoubleMin;
            else if (double.IsNaN(value)) value = doubleZero;

            Logging.LogExit(ModuleName);
        }

        public static double Mad(double[] values, double? centre)
        {
            Logging.LogCall(ModuleName);

            if (values == null || values.Length == 0) {Logging.LogExit(ModuleName); return 0; }

            if (centre == null) centre = values.Average();

            var mad = values.Sum(a => Math.Abs(centre.Value - a)) / values.Length;

            Logging.LogExit(ModuleName); return mad;
        }

        public static double Percentile(double[] sortedData, double p)
        {
            Logging.LogCall(ModuleName);

            if (sortedData == null || sortedData.Length == 0) {Logging.LogExit(ModuleName); return 0; }

                                        if (sortedData.Length == 1) {Logging.LogExit(ModuleName); return sortedData[0]; }

                                        if (p >= 100.0) {Logging.LogExit(ModuleName); return sortedData[^1]; }

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

            if (leftNumber == rightNumber) {Logging.LogExit(ModuleName); return leftNumber; }

                                        var part = n - Math.Floor(n);
            Logging.LogExit(ModuleName); return leftNumber + part * (rightNumber - leftNumber);
        }
    }
}