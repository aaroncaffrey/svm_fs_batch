using System;
using System.Linq;

namespace SvmFsBatch
{
    internal class Scaling
    {
        public const string ModuleName = nameof(Scaling);
        internal double AbsSum;
        internal double Average;
        internal double ColumnMax;
        internal double ColumnMin;

        internal double NonZero;
        internal double RescaleScaleMax = +1;

        internal double RescaleScaleMin = -1;
        internal double Srsos;
        internal double Stdev;

        public Scaling(int[] yCol) : this(yCol.Select(a => (double) a).ToArray())
        {
        }

        public Scaling(double[] yCol)
        {
            if (yCol == null || yCol.Length == 0) return;

            NonZero = yCol.Count(y => y != 0);
            AbsSum = yCol.Sum(Math.Abs);
            Srsos = SqrtSumOfSqrs(yCol);
            Average = yCol.Average();
            Stdev = StandardDeviationSample(yCol);
            ColumnMin = yCol.Min();
            ColumnMax = yCol.Max();
        }

        internal static double SqrtSumOfSqrs(double[] list)
        {
            return list == null || list.Length == 0
                ? 0d
                : Math.Sqrt(list.Sum(a => Math.Abs(a) * Math.Abs(a)));
        }

        internal static double StandardDeviationSample(double[] values)
        {
            if (values.Length < 2) return 0;

            var mean = values.Average();

            return Math.Sqrt(values.Sum(x => Math.Pow(x - mean, 2)) / (values.Length - 1));
        }

        internal double[] Scale(double[] values, ScaleFunction scaleFunction)
        {
            return values == null || values.Length == 0
                ? Array.Empty<double>()
                : values.Select(a => Scale(a, scaleFunction)).ToArray();
        }

        internal double Scale(double value, ScaleFunction scaleFunction)
        {
            /*double non_zero, double abs_sum, double srsos, double column_min, double column_max, double average, double stdev,*/

            var sp = this;

            if (sp == null) return value;

            switch (scaleFunction)
            {
                case ScaleFunction.None: return value;

                case ScaleFunction.Rescale:


                    var x = (RescaleScaleMax - RescaleScaleMin) * (value - sp.ColumnMin);
                    var y = sp.ColumnMax - sp.ColumnMin;
                    var z = RescaleScaleMin;

                    if (y == 0) return 0;

                    var rescale = x / y + z;

                    return rescale;

                case ScaleFunction.Normalisation:

                    if (sp.ColumnMax - sp.ColumnMin == 0) return 0;

                    var meanNorm = (value - sp.Average) / (sp.ColumnMax - sp.ColumnMin);

                    return meanNorm;

                case ScaleFunction.Standardisation:

                    if (sp.Stdev == 0) return 0;

                    var standardisation = (value - sp.Average) / sp.Stdev;

                    return standardisation;

                case ScaleFunction.L0Norm:

                    if (sp.NonZero == 0) return 0;

                    return value / sp.NonZero;

                case ScaleFunction.L1Norm:

                    if (sp.AbsSum == 0) return 0;

                    return value / sp.AbsSum;

                case ScaleFunction.L2Norm:

                    if (sp.Srsos == 0) return 0;

                    return value / sp.Srsos;

                default: throw new ArgumentOutOfRangeException(nameof(scaleFunction)); //return 0;
            }
        }

        internal enum ScaleFunction
        {
            None, Rescale, Normalisation,
            Standardisation, L0Norm, L1Norm,
            L2Norm
        }
    }
}