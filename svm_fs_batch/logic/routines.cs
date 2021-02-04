using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace SvmFsBatch
{
    internal static class Routines
    {
        public const string ModuleName = nameof(Routines);

        //[ThreadStatic] private static Random _local;

        //internal static Random this_threads_random => _local ?? (_local = new Random(unchecked(Environment.TickCount * 31 + Thread.CurrentThread.ManagedThreadId)));

        //internal static int Getinstance_id(int whole_array_index_first, int whole_array_index_last, int whole_array_step_size, int partition_array_index_first, int partition_array_index_last)
        //{
        //    var instance_id = -1;
        //
        //    //for (var whole_array_index = program_args.whole_array_index_first; whole_array_index <= program_args.partition_array_index_first; whole_array_index += program_args.whole_array_step_size) { instance_id++; }
        //    for (var whole_array_index = (whole_array_index_first <= whole_array_index_last ? whole_array_index_first : whole_array_index_last); !is_in_range(partition_array_index_first, partition_array_index_last, whole_array_index); whole_array_index += whole_array_step_size) instance_id++;
        //
        //    return ct.IsCancellationRequested ? default :instance_id;
        //}

        public static int ForIterations(int first, int last, int step)
        {
            return step > 0 && last >= first || step < 0 && first >= last
                ? (last - first) / step + 1
                : 0;
        }

        internal static bool IsInRange(int rangeFirst, int rangeLast, int value)
        {
            return value >= rangeFirst && value <= rangeLast || value >= rangeLast && value <= rangeFirst;
        }

        public static List<(int from, int to, int step)> FindRanges(List<int> ids)
        {
            if (ids == null || ids.Count == 0) return new List<(int from, int to, int step)>();

            ids = ids.OrderBy(a => a).Distinct().ToList();

            if (ids.Count == 1) return new List<(int from, int to, int step)> {(ids[0], ids[0], 0)};

            var ranges = new List<(int from, int to, int step)>();

            var step = ids[1] - ids[0];
            var stepStartIndex = 0;

            for (var i = 1; i < ids.Count; i++)
            {
                // if step has changed, save last
                if (ids[i] - ids[i - 1] != step)
                {
                    ranges.Add((ids[stepStartIndex], ids[stepStartIndex] + step * (i - stepStartIndex - 1), step));

                    step = ids[i] - ids[i - 1];
                    stepStartIndex = i - 1;
                    i--;
                    continue;
                }

                if (i == ids.Count - 1)
                {
                    step = stepStartIndex == i
                        ? 1
                        : step;
                    ranges.Add((ids[stepStartIndex], ids[stepStartIndex] + step * (i - stepStartIndex), step));
                }
            }

            return ranges;
        }

        public static string FindRangesStr(List<int> ids)
        {
            var ranges = FindRanges(ids);

            return string.Join("_",
                ranges.Select(range => $@"{range.from}" + (range.from != range.to
                    ? $@"-{range.to}" + (range.step != -1 && range.step != 0 && range.step != +1
                        ? $@";{range.step}"
                        : @"")
                    : @"")).ToList());
        }

        public static int[] Range(int start, int end, int step)
        {
            var ix = ForIterations(start, end, step);

            var ret = new int[ix];

            var stepSum = start;

            for (var i = 0; i < ix; i++)
            {
                ret[i] = stepSum;

                stepSum += step;
            }

            return ret;
        }

        //internal static void shuffle<T>(List<T> list, Random random)
        //{
        //    for (var n = list.Count - 1; n >= 0; n--)
        //    {
        //        var k = random.Next(0, list.Count - 1);
        //
        //        var value = list[k];
        //        list[k] = list[n];
        //        list[n] = value;
        //    }
        //}

        //internal static (string asStr, int? asInt, double? asDouble, bool? asBool)[] x_types(CancellationToken ct,
        //    string[] values, bool asParallel = false)
        //{
        //    var xType = asParallel
        //        ? values
        //            .AsParallel()
        //            .AsOrdered()
        //            .WithCancellation(ct)
        //            .Select(asStr =>
        //            {
        //                var asDouble =
        //                    double.TryParse(asStr, NumberStyles.Float, NumberFormatInfo.InvariantInfo,
        //                        out var outDouble)
        //                        ? outDouble
        //                        : (double?) null;
        //                var asInt =
        //                    int.TryParse(asStr, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var outInt)
        //                        ? outInt
        //                        : (int?) null;
        //                var asBool = asInt == 1 && asDouble == 1 ? true :
        //                    asInt == 0 && asDouble == 0 ? false : (bool?) null;
        //                if (asBool == null && bool.TryParse(asStr, out var outBool)) asBool = outBool;

        //                return ct.IsCancellationRequested ? default :(asStr: asStr, asInt: asInt, asDouble: asDouble, asBool: asBool);
        //            })
        //            .ToArray()
        //        : values
        //            .Select(asStr =>
        //            {
        //                var asDouble =
        //                    double.TryParse(asStr, NumberStyles.Float, NumberFormatInfo.InvariantInfo,
        //                        out var outDouble)
        //                        ? outDouble
        //                        : (double?) null;
        //                var asInt =
        //                    int.TryParse(asStr, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var outInt)
        //                        ? outInt
        //                        : (int?) null;
        //                var asBool = asInt == 1 && asDouble == 1 ? true :
        //                    asInt == 0 && asDouble == 0 ? false : (bool?) null;
        //                if (asBool == null && bool.TryParse(asStr, out var outBool)) asBool = outBool;

        //                return ct.IsCancellationRequested ? default :(asStr: asStr, asInt: asInt, asDouble: asDouble, asBool: asBool);
        //            })
        //            .ToArray();

        //    return ct.IsCancellationRequested ? default :xType;
        //}

        internal static void Shuffle(this int[] values, Random random)
        {
            var maxIndex = values.Length - 1;

            for (var n = maxIndex; n >= 0; n--)
            {
                var k = random.Next(0, maxIndex);

                var value = values[k];
                values[k] = values[n];
                values[n] = value;
            }
        }

        internal static ( (int ClassId, int class_size, (int repetitions_index, int outer_cv_index, int[] class_sample_indexes)[] folds)[] class_folds, (int ClassId, int class_size, (int repetitions_index, int outer_cv_index, int[] class_sample_indexes)[] folds)[] down_sampled_training_class_folds ) Folds((int ClassId, int class_size)[] classSizes, int repetitions, int outerCvFolds, bool asParallel = false, CancellationToken ct = default) //, int outer_cv_folds_to_run = 0, int fold_size_limit = 0)
        {
            if (ct.IsCancellationRequested) return default;

            var classFolds = asParallel
                ? classSizes.AsParallel().AsOrdered().WithCancellation(ct).Select(a => (a.ClassId, a.class_size, folds: Folds(a.class_size, repetitions, outerCvFolds,ct))).ToArray()
                : classSizes.Select(a => (a.ClassId, a.class_size, folds: Folds(a.class_size, repetitions, outerCvFolds,ct))).ToArray();

            var downSampledTrainingClassFolds = classFolds.Select(a => (a.ClassId, a.class_size, folds: a.folds?.Select(b =>
            {
                var minNumItemsInFold = classFolds.Min(c => c.folds?.Where(e => e.repetitions_index == b.repetitions_index && e.outer_cv_index == b.outer_cv_index).Min(e => e.indexes?.Length ?? 0) ?? 0);

                return ct.IsCancellationRequested ? default :(b.repetitions_index, b.outer_cv_index, indexes: b.indexes?.Take(minNumItemsInFold).ToArray());
            }).ToArray())).ToArray();

            return ct.IsCancellationRequested ? default :(classFolds, downSampledTrainingClassFolds);
        }

        internal static (int repetitions_index, int outer_cv_index, int[] indexes)[] Folds(int numClassSamples, int repetitions, int outerCvFolds, CancellationToken ct) //, int outer_cv_folds_to_run = 0, int fold_size_limit = 0)
        {
            // folds: returns a list of folds (including the indexes at each fold)... number folds = repetitions (!<1) * outer_cv_folds_to_run (!<1)

            // outer_cv_folds_to_run is the total number of outer_cv_folds to actually run/process... whereas, outer_cv_folds describes the data partitioning (i.e. 5 would be 5 tests of 80% train & 20% test, where as outer_cv_folds_to_run would reduce the 5 tests to given number).
            if (numClassSamples <= 0) throw new Exception();
            if (repetitions <= 0) throw new Exception();
            if (outerCvFolds <= 0) throw new Exception();
            //if (outer_cv_folds_to_run < 0 || outer_cv_folds_to_run > outer_cv_folds) throw new Exception();
            //if (fold_size_limit < 0) throw new Exception();

            //const string _MethodName = nameof(folds);
            /*int first_index, int last_index, int num_items,*/

            //if (outer_cv_folds_to_run == 0) outer_cv_folds_to_run = outer_cv_folds;
            var foldSizes = new int[outerCvFolds /*_to_run*/];

            if (numClassSamples > 0 && outerCvFolds /*_to_run*/ > 0)
            {
                var n = numClassSamples / outerCvFolds;

                for (var i = 0; i < outerCvFolds /*_to_run*/; i++) foldSizes[i] = n;

                for (var i = 0; i < numClassSamples % outerCvFolds && i < outerCvFolds /*_to_run*/; i++) foldSizes[i]++;
            }

            //if (fold_size_limit > 0) { fold_sizes = fold_sizes.Select(a => a > fold_size_limit ? fold_size_limit : a).ToArray(); }

            // use same seed to ensure all calls to fold() are deterministic
            var rand = new Random(1);

            var indexesPool = Enumerable.Range(0, numClassSamples).ToArray();

            var foldIndexes = new List<(int randomisation, int outer_cv_index, int[] indexes)>();

            for (var repetitionsIndex = 0; repetitionsIndex < repetitions; repetitionsIndex++)
            {
                indexesPool.Shuffle(rand);

                var outerCvFoldIndexes = foldSizes.Select((foldSize, outerCvIndex) => (repetitions_index: repetitionsIndex, outer_cv_index: outerCvIndex, indexes: indexesPool.Skip(foldSizes.Where((b, j) => outerCvIndex > j /* skip previous folds*/).Sum()).Take(foldSize /* take only current fold */).OrderBy(b => b /* order indexes in the current fold numerically */).ToArray())).Where(c => c.indexes != null && c.indexes.Length > 0).ToArray();

                foldIndexes.AddRange(outerCvFoldIndexes);
            }

            return ct.IsCancellationRequested ? default :foldIndexes.ToArray();
        }


        internal static (double num_complete_pct, TimeSpan time_taken, TimeSpan time_remaining) GetETA(int numComplete, int numTotal, DateTime startTime)
        {
            var numIncomplete = numTotal - numComplete;

            var numCompletePct = numTotal > 0
                ? numComplete / (double) numTotal * 100
                : 0;

            var timeTakenTicks = DateTime.UtcNow.Subtract(startTime).Ticks;
            var timeTaken = TimeSpan.FromTicks(timeTakenTicks);

            var estTimeEach = numComplete > 0
                ? timeTakenTicks / (double) numComplete
                : 0;
            var estTimeRemain = estTimeEach * numIncomplete;

            var timeRemaining = TimeSpan.FromTicks((long) estTimeRemain);

            return (numCompletePct, timeTaken, timeRemaining);
        }

        internal static void PrintETA(int numComplete, int numTotal, DateTime startTime, string callerModuleName = @"", [CallerMemberName] string callerMethodName = @"")
        {
            var x = GetETA(numComplete, numTotal, startTime);

            Logging.WriteLine($@"ETA: tasks complete: {numComplete}/{numTotal} ({x.num_complete_pct:0.00}%) [time taken: {x.time_taken:dd\:hh\:mm\:ss\.fff}, time remaining: {(numComplete > 0 ? $@"{x.time_remaining:dd\:hh\:mm\:ss\.fff}" : @"...")}]", callerModuleName, callerMethodName);
        }

        internal static double StandardDeviationPopulation(double[] values)
        {
            if (values == null || values.Length < 2) return 0;

            var mean = values.Average();

            return Math.Sqrt(values.Sum(x => Math.Pow(x - mean, 2)) / values.Length);
        }

        internal static double StandardDeviationSample(double[] values)
        {
            if (values == null || values.Length < 2) return 0;

            var mean = values.Average();

            return Math.Sqrt(values.Sum(x => Math.Pow(x - mean, 2)) / (values.Length - 1));
        }

        internal enum LibsvmKernelType
        {
            //@default = Rbf,
            Linear = 0, Polynomial = 1, Rbf = 2,
            Sigmoid = 3, Precomputed = 4
        }

        internal enum LibsvmSvmType
        {
            //@default = CSvc,
            CSvc = 0, NuSvc = 1, OneClassSvm = 2,
            EpsilonSvr = 3, NuSvr = 4
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
        //    return ct.IsCancellationRequested ? default :variance / (samples.Length - 1);
        //}
        //
        //public static (double variance, double stdev) sample_standard_deviation(double[] samples)
        //{
        //    if (samples == null || samples.Length <= 1)
        //    {
        //        return ct.IsCancellationRequested ? default :(0, 0);
        //    }
        //
        //    var variance = sample_variance(samples);
        //    var stdev = Math.Sqrt(variance);
        //
        //    return ct.IsCancellationRequested ? default :(variance, stdev);
        //}

        //internal static double sqrt_sumofsqrs(double[] list) { return ct.IsCancellationRequested ? default :list == null || list.Count == 0 ? 0 : Math.Sqrt(list.Sum(a => Math.Abs(a) * Math.Abs(a))); }


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
        //    return ct.IsCancellationRequested ? default :v;
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