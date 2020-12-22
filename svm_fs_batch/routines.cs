using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace svm_fs_batch
{
    internal static class routines
    {
        public const string module_name = nameof(routines);

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

        internal static (
            (int class_id, int class_size, (int repetitions_index, int outer_cv_index, int[] indexes)[] folds)[] class_folds,
            (int class_id, int class_size, (int repetitions_index, int outer_cv_index, int[] indexes)[] folds)[] down_sampled_training_class_folds
            ) folds(CancellationTokenSource cts, IList<(int class_id, int class_size)> class_sizes, int repetitions, int outer_cv_folds)//, int outer_cv_folds_to_run = 0, int fold_size_limit = 0)
        {
            if (cts.IsCancellationRequested) return default;

            var class_folds = class_sizes.AsParallel().AsOrdered().WithCancellation(cts.Token).Select(a => (class_id: a.class_id, class_size: a.class_size, folds: routines.folds(a.class_size, repetitions, outer_cv_folds/*, outer_cv_folds_to_run, fold_size_limit*/))).ToArray();

            var down_sampled_training_class_folds = class_folds.Select(a => (class_id: a.class_id, class_size: a.class_size, folds: a.folds?.Select(b =>
                    {
                        var min_num_items_in_fold =
                            class_folds.Min(c => c.folds?
                                .Where(e => e.repetitions_index == b.repetitions_index && e.outer_cv_index == b.outer_cv_index)
                                .Min(e => e.indexes?.Length ?? 0) ?? 0
                            );

                        return (repetitions_index: b.repetitions_index, outer_cv_index: b.outer_cv_index, indexes: b.indexes?.Take(min_num_items_in_fold).ToArray());
                    })
                    .ToArray()))
                .ToArray();

            return (class_folds, down_sampled_training_class_folds);
        }

        internal static (int repetitions_index, int outer_cv_index, int[] indexes)[] folds(int num_class_samples, int repetitions, int outer_cv_folds)//, int outer_cv_folds_to_run = 0, int fold_size_limit = 0)
        {
            // folds: returns a list of folds (including the indexes at each fold)... number folds = repetitions (!<1) * outer_cv_folds_to_run (!<1)

            // outer_cv_folds_to_run is the total number of outer_cv_folds to actually run/process... whereas, outer_cv_folds describes the data partitioning (i.e. 5 would be 5 tests of 80% train & 20% test, where as outer_cv_folds_to_run would reduce the 5 tests to given number).
            if (num_class_samples <= 0) throw new Exception();
            if (repetitions <= 0) throw new Exception();
            if (outer_cv_folds <= 0) throw new Exception();
            //if (outer_cv_folds_to_run < 0 || outer_cv_folds_to_run > outer_cv_folds) throw new Exception();
            //if (fold_size_limit < 0) throw new Exception();

            //const string method_name = nameof(folds);
            /*int first_index, int last_index, int num_items,*/
            
            //if (outer_cv_folds_to_run == 0) outer_cv_folds_to_run = outer_cv_folds;
            var fold_sizes = new int[outer_cv_folds/*_to_run*/];

            if (num_class_samples > 0 && outer_cv_folds/*_to_run*/ > 0)
            {
                var n = num_class_samples / outer_cv_folds;

                for (var i = 0; i < outer_cv_folds/*_to_run*/; i++) { fold_sizes[i] = n; }

                for (var i = 0; i < (num_class_samples % outer_cv_folds) && i < outer_cv_folds/*_to_run*/; i++) { fold_sizes[i]++; }
            }

            //if (fold_size_limit > 0) { fold_sizes = fold_sizes.Select(a => a > fold_size_limit ? fold_size_limit : a).ToArray(); }

            // use same seed to ensure all calls to fold() are deterministic
            var rand = new Random(1);

            var indexes_pool = Enumerable.Range(0, num_class_samples).ToList();

            var fold_indexes = new List<(int randomisation, int outer_cv_index, int[] indexes)>();

            for (var repetitions_index = 0; repetitions_index < repetitions; repetitions_index++)
            {
                indexes_pool.shuffle(rand);
                
                var outer_cv_fold_indexes = fold_sizes
                    .Select((fold_size, outer_cv_index) => (repetitions_index: repetitions_index, outer_cv_index: outer_cv_index, indexes: indexes_pool
                        .Skip(fold_sizes.Where((b, j) => outer_cv_index > j /* skip previous folds*/).Sum())
                        .Take(fold_size /* take only current fold */)
                        .OrderBy(b => b /* order indexes in the current fold numerically */)
                        .ToArray()))
                    .Where(c => c.indexes != null && c.indexes.Length > 0)
                    .ToArray();

                fold_indexes.AddRange(outer_cv_fold_indexes);
            }

            return fold_indexes.ToArray();
        }


        internal static double standard_deviation_population(IList<double> values)
        {
            if (values == null || values.Count < 2) return 0;

            var mean = values.Average();

            return Math.Sqrt(values.Sum(x => Math.Pow(x - mean, 2)) / (values.Count));
        }

        internal static double standard_deviation_sample(IList<double> values)
        {
            if (values == null || values.Count < 2) return 0;

            var mean = values.Average();

            return Math.Sqrt(values.Sum(x => Math.Pow(x - mean, 2)) / (values.Count - 1));
        }


        //public static double sample_variance(double[] samples)
        //{
        //    if (samples == null || samples.Length <= 1)
        //    {
        //        return 0;
        //    }
        //
        //    var variance = 0.0;
        //    var t = samples[0];
        //    for (int i = 1; i < samples.Length; i++)
        //    {
        //        t += samples[i];
        //        var diff = ((i + 1) * samples[i]) - t;
        //        variance += (diff * diff) / ((i + 1.0) * i);
        //    }
        //
        //    return variance / (samples.Length - 1);
        //}
        //
        //public static (double variance, double stdev) sample_standard_deviation(double[] samples)
        //{
        //    if (samples == null || samples.Length <= 1)
        //    {
        //        return (0, 0);
        //    }
        //
        //    var variance = sample_variance(samples);
        //    var stdev = Math.Sqrt(variance);
        //
        //    return (variance, stdev);
        //}

        //internal static double sqrt_sumofsqrs(IList<double> list) { return list == null || list.Count == 0 ? 0 : Math.Sqrt(list.Sum(a => Math.Abs(a) * Math.Abs(a))); }

      

        //internal static int for_loop_instance_id(List<(int current, int max)> points)
        //{
        //    //var jid = (i * max_j * max_k) + (j * max_k) + k;
        //    //var job_id = (i * max_j * max_k * max_l) + (j * max_k * max_l) + (k * max_l) + l;
        //    var v = 0;

        //    for (var i = 0; i < points.Count - 1; i++)
        //    {
        //        var t = points[i].current;
        //        for (var j = i + 1; j < points.Count; j++) { t *= points[j].max; }

        //        v += t;
        //    }

        //    v += points.Last().current;
        //    return v;
        //}


        //internal static void wait_any<T>(IList<Task<T>> tasks, int max_tasks = -1)
        //{
        //    wait_any(tasks.ToArray<Task>(), max_tasks);
        //}

        //internal static void wait_any(IList<Task> tasks, int max_tasks = -1)
        //{
        //    if (max_tasks == -1)
        //    {
        //        max_tasks = Environment.ProcessorCount * 4;
        //    }

        //    Task[] incomplete_tasks = null;

        //    do
        //    {
        //        incomplete_tasks = tasks.Where(a => !a.IsCompleted).ToArray<Task>();

        //        if (incomplete_tasks.Length > 0 && incomplete_tasks.Length > max_tasks) { Task.WaitAny(incomplete_tasks); }

        //    } while (incomplete_tasks.Length >= max_tasks);
        //}
    }
}