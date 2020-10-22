using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace svm_fs_batch
{
    internal class grid_cache_data
    {
        internal routines.libsvm_svm_type svm_type;
        internal routines.libsvm_kernel_type svm_kernel;
        internal int repetitions;
        internal int repetitions_index;
        internal int outer_cv_folds;
        //internal int outer_cv_folds_to_run;
        internal int outer_cv_index;
        internal int inner_cv_folds;
        internal bool probability_estimates;
        internal bool shrinking_heuristics;
        internal (double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point;
        internal double rate;

        public string[] get_keys()
        {
            return new string[] {
                    nameof(svm_type),
                    nameof(svm_kernel),
                    nameof(repetitions),
                    nameof(repetitions_index),
                    nameof(outer_cv_folds),
                    //nameof(outer_cv_folds_to_run),
                    nameof(outer_cv_index),
                    nameof(inner_cv_folds),
                    nameof(probability_estimates),
                    nameof(shrinking_heuristics),
                    nameof(point.cost),
                    nameof(point.gamma),
                    nameof(point.epsilon),
                    nameof(point.coef0),
                    nameof(point.degree),
                    nameof(rate)
                };
        }

        public string[] get_values()
        {
            return new string[]
            {
                   $@"{svm_type}",
                   $@"{svm_kernel}",
                   $@"{repetitions}",
                   $@"{repetitions_index}",
                   $@"{outer_cv_folds}",
                   //$@"{outer_cv_folds_to_run}",
                   $@"{outer_cv_index}",
                   $@"{inner_cv_folds}",
                   $@"{probability_estimates}",
                   $@"{shrinking_heuristics}",
                   $@"{point.cost:G17}",
                   $@"{point.gamma:G17}",
                   $@"{point.epsilon:G17}",
                   $@"{point.coef0:G17}",
                   $@"{point.degree:G17}",
                   $@"{rate:G17}",
            };
        }

        public grid_cache_data()
        {

        }

        public grid_cache_data(IList<string> line)
        {
            var k = -1;

            svm_type = (routines.libsvm_svm_type)Enum.Parse(typeof(routines.libsvm_svm_type), line[++k]);
            svm_kernel = (routines.libsvm_kernel_type)Enum.Parse(typeof(routines.libsvm_kernel_type), line[++k]);
            repetitions = Int32.Parse(line[++k], CultureInfo.InvariantCulture);
            repetitions_index = Int32.Parse(line[++k], CultureInfo.InvariantCulture);
            outer_cv_folds = Int32.Parse(line[++k], CultureInfo.InvariantCulture);
            //outer_cv_folds_to_run = Int32.Parse(line[++k], CultureInfo.InvariantCulture);
            outer_cv_index = Int32.Parse(line[++k], CultureInfo.InvariantCulture);
            inner_cv_folds = Int32.Parse(line[++k], CultureInfo.InvariantCulture);
            probability_estimates = Boolean.Parse(line[++k]);
            shrinking_heuristics = Boolean.Parse(line[++k]);
            point = (
                cost: Double.TryParse(line[++k], NumberStyles.Float, CultureInfo.InvariantCulture, out var p_cost) ? p_cost : (double?)null,
                gamma: Double.TryParse(line[++k], NumberStyles.Float, CultureInfo.InvariantCulture, out var p_gamma) ? p_gamma : (double?)null,
                epsilon: Double.TryParse(line[++k], NumberStyles.Float, CultureInfo.InvariantCulture, out var p_epsilon) ? p_epsilon : (double?)null,
                coef0: Double.TryParse(line[++k], NumberStyles.Float, CultureInfo.InvariantCulture, out var p_coef0) ? p_coef0 : (double?)null,
                degree: Double.TryParse(line[++k], NumberStyles.Float, CultureInfo.InvariantCulture, out var p_degree) ? p_degree : (double?)null
                );

            rate = Double.TryParse(line[++k], NumberStyles.Float, CultureInfo.InvariantCulture, out var p_rate) ? p_rate : 0d;
        }

        internal static List<grid_cache_data> read_cache_file(string cache_train_grid_csv)
        {
            var module_name = nameof(grid_cache_data);
            var method_name = nameof(read_cache_file);

            var cache = new List<grid_cache_data>();

            if (io_proxy.is_file_available(cache_train_grid_csv, module_name, method_name))
            {
                cache = io_proxy.ReadAllLines(cache_train_grid_csv, module_name, method_name).Skip(1).Select(a =>
                {
                    try
                    {
                        var line = a.Split(',').ToList();

                        return new grid_cache_data(line);
                    }
                    catch (Exception e)
                    {
                        io_proxy.log_exception(e, "", module_name, method_name);
                        return default;
                    }
                }).ToList();
            }

            cache = cache.Where(a => a.rate > 0).ToList();

            return cache;
        }

        internal static void write_cache_file(string cache_train_grid_csv, IList<grid_cache_data> grid_cache_data_list)
        {
            var module_name = nameof(grid_cache_data);
            var method_name = nameof(write_cache_file);

            var lines = new List<string>();

            var header = String.Join(",", new grid_cache_data().get_keys());
            lines.Add(header);

            lines.AddRange(grid_cache_data_list.Select(a => String.Join(",", a.get_values())).ToList());

            io_proxy.WriteAllLines(cache_train_grid_csv, lines, module_name, method_name);
        }
    }
}
