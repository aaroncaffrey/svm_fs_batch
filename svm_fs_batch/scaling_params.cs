using System;
using System.Linq;

namespace svm_fs_batch
{
    internal class scaling
    {
        internal enum scale_function : int { none, rescale, normalisation, standardisation, L0_norm, L1_norm, L2_norm, }


        internal double non_zero;
        internal double abs_sum;
        internal double srsos;
        internal double average;
        internal double stdev;
        internal double column_min;
        internal double column_max;

        public scaling(double[] y_col)
        {
            if (y_col == null || y_col.Length == 0) return;

            this.non_zero = y_col.Count(y => y != 0);
            this.abs_sum = y_col.Sum(Math.Abs);
            this.srsos = routines.sqrt_sumofsqrs(y_col);
            this.average = y_col.Average();
            this.stdev = routines.standard_deviation_sample(y_col);
            this.column_min = y_col.Min();
            this.column_max = y_col.Max();
        }


        internal double scale(double value, scaling.scale_function scale_function)
        {
            /*double non_zero, double abs_sum, double srsos, double column_min, double column_max, double average, double stdev,*/

            var sp = this;

            if (sp == null) return value;

            switch (scale_function)
            {
                case scaling.scale_function.none: return value;

                case scaling.scale_function.rescale:
                    var scale_min = -1;
                    var scale_max = +1;

                    var x = (scale_max - scale_min) * (value - sp.column_min);
                    var y = (sp.column_max - sp.column_min);
                    var z = scale_min;

                    if (y == 0) return 0;

                    var rescale = (x / y) + z;

                    return rescale;

                case scaling.scale_function.normalisation:

                    if (sp.column_max - sp.column_min == 0) return 0;

                    var mean_norm = (value - sp.average) / (sp.column_max - sp.column_min);

                    return mean_norm;

                case scaling.scale_function.standardisation:

                    if (sp.stdev == 0) return 0;

                    var standardisation = (value - sp.average) / sp.stdev;

                    return standardisation;

                case scaling.scale_function.L0_norm:

                    if (sp.non_zero == 0) return 0;

                    return value / sp.non_zero;

                case scaling.scale_function.L1_norm:

                    if (sp.abs_sum == 0) return 0;

                    return value / sp.abs_sum;

                case scaling.scale_function.L2_norm:

                    if (sp.srsos == 0) return 0;

                    return value / sp.srsos;

                default: throw new ArgumentOutOfRangeException(nameof(scale_function)); //return 0;
            }
        }
    }
}