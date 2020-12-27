﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;

namespace svm_fs_batch
{
    internal class grid_cache_data
    {
        public const string module_name = nameof(grid_cache_data);

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
        internal grid_point grid_point;
        //internal double rate;

        

        public static readonly string[] csv_header_values_array = new string[] {
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
                    nameof(svm_fs_batch.grid_point.cost),
                    nameof(svm_fs_batch.grid_point.gamma),
                    nameof(svm_fs_batch.grid_point.epsilon),
                    nameof(svm_fs_batch.grid_point.coef0),
                    nameof(svm_fs_batch.grid_point.degree),
                    nameof(svm_fs_batch.grid_point.cv_rate),
                };

        public static readonly string csv_header_string = string.Join(",", csv_header_values_array);

        public string csv_values_string()
        {
            return string.Join(",", csv_values_array());
        }

        public string[] csv_values_array()
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
                   $@"{grid_point.cost:G17}",
                   $@"{grid_point.gamma:G17}",
                   $@"{grid_point.epsilon:G17}",
                   $@"{grid_point.coef0:G17}",
                   $@"{grid_point.degree:G17}",
                   $@"{grid_point.cv_rate:G17}",
            }.Select(a => a.Replace(",", ";", StringComparison.OrdinalIgnoreCase)).ToArray(); 
        }

        public grid_cache_data()
        {

        }

        public grid_cache_data(string[] line)
        {
            var k = -1;

            svm_type = (routines.libsvm_svm_type)Enum.Parse(typeof(routines.libsvm_svm_type), line[++k]);
            svm_kernel = (routines.libsvm_kernel_type)Enum.Parse(typeof(routines.libsvm_kernel_type), line[++k]);
            repetitions = int.Parse(line[++k], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            repetitions_index = int.Parse(line[++k], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            outer_cv_folds = int.Parse(line[++k], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            //outer_cv_folds_to_run = inr.Parse(line[++k], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            outer_cv_index = int.Parse(line[++k], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            inner_cv_folds = int.Parse(line[++k], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            probability_estimates = bool.Parse(line[++k]);
            shrinking_heuristics = bool.Parse(line[++k]);
            grid_point = new grid_point() {
                cost= double.TryParse(line[++k], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var p_cost) ? p_cost : (double?)null,
                gamma= double.TryParse(line[++k], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var p_gamma) ? p_gamma : (double?)null,
                epsilon= double.TryParse(line[++k], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var p_epsilon) ? p_epsilon : (double?)null,
                coef0= double.TryParse(line[++k], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var p_coef0) ? p_coef0 : (double?)null,
                degree= double.TryParse(line[++k], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var p_degree) ? p_degree : (double?)null,
                cv_rate = double.TryParse(line[++k], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var p_rate) ? p_rate : 0d,
            };
        }

        internal static grid_cache_data[] read_cache_file(CancellationTokenSource cts, string cache_train_grid_csv)
        {
            const string method_name = nameof(read_cache_file);

            if (cts.IsCancellationRequested) return default;

            var cache = Array.Empty<grid_cache_data>();

            if (io_proxy.is_file_available(cts, cache_train_grid_csv, false, module_name, method_name))
            {
                cache = io_proxy.ReadAllLines(cts, cache_train_grid_csv, module_name, method_name).Skip(1 /* skip header line */).Select(a =>
                {
                    try
                    {
                        var line = a.Split(',');

                        return new grid_cache_data(line);
                    }
                    catch (Exception e)
                    {
                        io_proxy.log_exception(e, "", module_name, method_name);
                        return default;
                    }
                }).ToArray();
            }

            cache = cache.Where(a => a.grid_point.cv_rate > 0).ToArray();

            return cache;
        }

        internal static void write_cache_file(CancellationTokenSource cts, string cache_train_grid_csv, grid_cache_data[] grid_cache_data_list)
        {
            const string method_name = nameof(write_cache_file);

            if (cts.IsCancellationRequested) return;

            var lines = new string[grid_cache_data_list.Length + 1];
            lines[0]=csv_header_string;

            for (var i = 0; i < grid_cache_data_list.Length; i++)
            {
                lines[i + 1] = grid_cache_data_list[i].csv_values_string();
            }

            io_proxy.WriteAllLines(cts, cache_train_grid_csv, lines, module_name, method_name);
        }
    }
}
