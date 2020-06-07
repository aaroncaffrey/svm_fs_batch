using System;
using System.Collections.Generic;
using System.Linq;

namespace svm_fs_batch
{
    internal static class routines
    {
        internal enum scale_function : int
        {
            none,
            rescale,
            normalisation,
            standardisation,
            L0_norm,
            L1_norm,
            L2_norm,
        }

        internal enum libsvm_kernel_type : int
        {
            //@default = rbf,
            linear = 0,
            polynomial = 1,
            rbf = 2,
            sigmoid = 3,
            precomputed = 4,
        }

        internal enum libsvm_svm_type : int
        {
            //@default = c_svc,
            c_svc = 0,
            nu_svc = 1,
            one_class_svm = 2,
            epsilon_svr = 3,
            nu_svr = 4,
        }

        //[ThreadStatic] private static Random _local;

        //internal static Random this_threads_random => _local ?? (_local = new Random(unchecked(Environment.TickCount * 31 + Thread.CurrentThread.ManagedThreadId)));

        internal static void shuffle<T>(this IList<T> list, Random random)// = null)
        {
            //if (random == null) random = this_threads_random;

            //var k_list = new List<int>();

            for (var n = list.Count - 1; n >= 0; n--)
            {
                var k = random.Next(0, list.Count - 1);
                //k_list.Add(k);

                var value = list[k];
                list[k] = list[n];
                list[n] = value;
            }

            // if (program.write_console_log) program.WriteLine(string.Join(",",k_list));
        }

        internal static List<(int randomisation_cv_index, int outer_cv_index, List<int> indexes)> folds(int examples, int outer_cv_folds, int randomisation_cv_folds, int? size_limit = null)
        {
            //var module_name = nameof(svm_ctl);
            //var method_name = nameof(folds);

            /*int first_index, int last_index, int num_items,*/

            var fold_sizes = new int[outer_cv_folds];

            if (examples > 0 && outer_cv_folds > 0)
            {
                var n = examples / outer_cv_folds;

                for (var i = 0; i < outer_cv_folds; i++) { fold_sizes[i] = n; }

                for (var i = 0; i < (examples % outer_cv_folds); i++) { fold_sizes[i]++; }
            }

            if (size_limit != null) { fold_sizes = fold_sizes.Select(a => a > size_limit ? size_limit.Value : a).ToArray(); }

            var rand = new Random(1);

            var indexes_pool = Enumerable.Range(0, examples).ToList();

            var x = new List<(int randomisation, int fold, List<int> indexes)>();

            var rdm = randomisation_cv_folds == 0 ? 1 : randomisation_cv_folds;

            for (var r = 0; r < rdm; r++)
            {
                if (randomisation_cv_folds != 0) indexes_pool.shuffle(rand);

                var y = fold_sizes.Select((fold_size, fold_index) => (randomisation_cv_index: r, outer_cv_index: fold_index, indexes: indexes_pool.Skip(fold_sizes.Where((b, j) => fold_index < j).Sum()).Take(fold_size).OrderBy(b => b).ToList())).ToList();

                x.AddRange(y);
            }

            return x;
        }


        internal static double standard_deviation_population(List<double> values)
        {
            if (values.Count == 0) return 0;

            var mean = values.Average();

            return Math.Sqrt(values.Sum(x => Math.Pow(x - mean, 2)) / (values.Count));
        }

        internal static double standard_deviation_sample(List<double> values)
        {
            if (values.Count < 2) return 0;

            var mean = values.Average();

            return Math.Sqrt(values.Sum(x => Math.Pow(x - mean, 2)) / (values.Count - 1));
        }

        internal static double sqrt_sumofsqrs(List<double> list) { return Math.Sqrt(list.Sum(a => Math.Abs(a) * Math.Abs(a))); }

        internal static double scale(double value, /* List<double> list,*/ double non_zero, double abs_sum, double srsos, double column_min, double column_max, double average, double stdev, routines.scale_function scale_function)
        {
            switch (scale_function)
            {
                case routines.scale_function.none: return value;

                case routines.scale_function.rescale:
                    var scale_min = -1;
                    var scale_max = +1;

                    var x = (scale_max - scale_min) * (value - column_min);
                    var y = (column_max - column_min);
                    var z = scale_min;

                    if (y == 0) return 0;

                    var rescale = (x / y) + z;

                    return rescale;

                case routines.scale_function.normalisation:

                    if (column_max - column_min == 0) return 0;

                    var mean_norm = (value - average) / (column_max - column_min);

                    return mean_norm;

                case routines.scale_function.standardisation:

                    if (stdev == 0) return 0;

                    var standardisation = (value - average) / stdev;

                    return standardisation;

                case routines.scale_function.L0_norm:

                    if (non_zero == 0) return 0;

                    return value / non_zero;

                case routines.scale_function.L1_norm:

                    if (abs_sum == 0) return 0;

                    return value / abs_sum;

                case routines.scale_function.L2_norm:

                    if (srsos == 0) return 0;

                    return value / srsos;

                default: throw new ArgumentOutOfRangeException(nameof(scale_function)); //return 0;
            }
        }

        internal static int for_loop_instance_id(List<(int current, int max)> points)
        {
            //var jid = (i * max_j * max_k) + (j * max_k) + k;
            //var job_id = (i * max_j * max_k * max_l) + (j * max_k * max_l) + (k * max_l) + l;
            var v = 0;

            for (var i = 0; i < points.Count - 1; i++)
            {
                var t = points[i].current;
                for (var j = i + 1; j < points.Count; j++) { t *= points[j].max; }

                v += t;
            }

            v += points.Last().current;
            return v;
        }

    }
}