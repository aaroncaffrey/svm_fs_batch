using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

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

        internal static List<(int repetitions_index, int outer_cv_index, List<int> indexes)> folds(int examples, int repetitions, int outer_cv_folds, int outer_cv_folds_to_run = 0, int fold_size_limit = 0)
        {
            // folds: returns a list of folds (including the indexes at each fold)... number folds = repetitions (!<1) * outer_cv_folds_to_run (!<1)

            // outer_cv_folds_to_run is the total number of outer_cv_folds to actually run/process... whereas, outer_cv_folds describes the data partitioning (i.e. 5 would be 5 tests of 80% train & 20% test, where as outer_cv_folds_to_run would reduce the 5 tests to given number).
            if (examples <= 0) throw new Exception();
            if (repetitions <= 0) throw new Exception();
            if (outer_cv_folds <= 0) throw new Exception();
            if (outer_cv_folds_to_run < 0 || outer_cv_folds_to_run > outer_cv_folds) throw new Exception();
            if (fold_size_limit < 0) throw new Exception();

            //var module_name = nameof(svm_ctl);
            //var method_name = nameof(folds);
            /*int first_index, int last_index, int num_items,*/
            
            if (outer_cv_folds_to_run == 0) outer_cv_folds_to_run = outer_cv_folds;
            var fold_sizes = new int[outer_cv_folds_to_run];

            if (examples > 0 && outer_cv_folds_to_run > 0)
            {
                var n = examples / outer_cv_folds;

                for (var i = 0; i < outer_cv_folds_to_run; i++) { fold_sizes[i] = n; }

                for (var i = 0; i < (examples % outer_cv_folds); i++) { fold_sizes[i]++; }
            }

            if (fold_size_limit > 0) { fold_sizes = fold_sizes.Select(a => a > fold_size_limit ? fold_size_limit : a).ToArray(); }

            // use same seed to ensure all calls to fold() are deterministic
            var rand = new Random(1);

            var indexes_pool = Enumerable.Range(0, examples).ToList();

            var x = new List<(int randomisation, int fold, List<int> indexes)>();

            // if repetitions is =0, then no shuffle, data will be in default order
            // if repetitions is >0, will be shuffled
            // (currently the option to specify 0 is disabled above)

            var rdm = repetitions == 0 ? 1 : repetitions;

            for (var r = 0; r < rdm; r++)
            {
                if (repetitions != 0) indexes_pool.shuffle(rand);

                var y = fold_sizes.Select((fold_size, fold_index) => (repetitions_index: r, outer_cv_index: fold_index, indexes: indexes_pool.Skip(fold_sizes.Where((b, j) => fold_index < j).Sum()).Take(fold_size).OrderBy(b => b).ToList())).ToList();

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


        internal static void wait_any<T>(IList<Task<T>> tasks, int max_tasks = -1)
        {
            wait_any(tasks.ToArray<Task>(), max_tasks);
        }

        internal static void wait_any(IList<Task> tasks, int max_tasks = -1)
        {
            if (max_tasks == -1)
            {
                max_tasks = Environment.ProcessorCount * 4;
            }

            Task[] incomplete_tasks = null;

            do
            {
                incomplete_tasks = tasks.Where(a => !a.IsCompleted).ToArray<Task>();

                if (incomplete_tasks.Length > 0 && incomplete_tasks.Length > max_tasks) { Task.WaitAny(incomplete_tasks); }

            } while (incomplete_tasks.Length >= max_tasks);
        }
    }
}