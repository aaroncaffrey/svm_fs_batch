using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;

namespace svm_fs_batch
{
    internal static class grid
    {
        public const string module_name = nameof(grid);

        internal static grid_point get_best_rate(List<grid_point> grid_search_results)
        {
            
            //const string method_name = nameof(get_best_rate);

            // libsvm grid.py: if ((rate > best_rate) || (rate == best_rate && g == best_g && c < best_c))

            var grid_point_best = new grid_point() {cv_rate = -1};
            
            foreach (var result in grid_search_results)
            {
                var is_rate_better = ((grid_point_best.cv_rate <= 0) || (result.cv_rate > -1 && result.cv_rate > grid_point_best.cv_rate));

                var is_rate_same = (result.cv_rate > -1 && grid_point_best.cv_rate > -1 && result.cv_rate == grid_point_best.cv_rate);

                if (!is_rate_better && is_rate_same)
                {
                    var is_new_cost_lower = result.cost != null && grid_point_best.cost != null && Math.Abs(result.cost.Value) < Math.Abs(grid_point_best.cost.Value);
                    var is_new_gamma_lower = result.gamma != null && grid_point_best.gamma != null && Math.Abs(result.gamma.Value) < Math.Abs(grid_point_best.gamma.Value);
                    var is_new_epsilon_lower = result.epsilon != null && grid_point_best.epsilon != null && Math.Abs(result.epsilon.Value) < Math.Abs(grid_point_best.epsilon.Value);
                    var is_new_coef0_lower = result.coef0 != null && grid_point_best.coef0 != null && Math.Abs(result.coef0.Value) < Math.Abs(grid_point_best.coef0.Value);
                    var is_new_degree_lower = result.degree != null && grid_point_best.degree != null && Math.Abs(result.degree.Value) < Math.Abs(grid_point_best.degree.Value);
                    var new_score = (is_new_cost_lower ? 1 : 0) + (is_new_gamma_lower ? 1 : 0) + (is_new_epsilon_lower ? 1 : 0) + (is_new_coef0_lower ? 1 : 0) + (is_new_degree_lower ? 1 : 0);

                    var is_old_cost_lower = result.cost != null && grid_point_best.cost != null && Math.Abs(result.cost.Value) > Math.Abs(grid_point_best.cost.Value);
                    var is_old_gamma_lower = result.gamma != null && grid_point_best.gamma != null && Math.Abs(result.gamma.Value) > Math.Abs(grid_point_best.gamma.Value);
                    var is_old_epsilon_lower = result.epsilon != null && grid_point_best.epsilon != null && Math.Abs(result.epsilon.Value) > Math.Abs(grid_point_best.epsilon.Value);
                    var is_old_coef0_lower = result.coef0 != null && grid_point_best.coef0 != null && Math.Abs(result.coef0.Value) > Math.Abs(grid_point_best.coef0.Value);
                    var is_old_degree_lower = result.degree != null && grid_point_best.degree != null && Math.Abs(result.degree.Value) > Math.Abs(grid_point_best.degree.Value);
                    var old_score = (is_old_cost_lower ? 1 : 0) + (is_old_gamma_lower ? 1 : 0) + (is_old_epsilon_lower ? 1 : 0) + (is_old_coef0_lower ? 1 : 0) + (is_old_degree_lower ? 1 : 0);

                    is_rate_same = new_score >= old_score;
                }

                if (is_rate_better || is_rate_same)
                {
                    grid_point_best = new grid_point(result);
                }
            }

            return grid_point_best;
        }


        internal static grid_point grid_parameter_search(
            CancellationTokenSource cts,
                string libsvm_train_exe,
                string cache_train_grid_csv,
                string training_file,
                string train_stdout_file,
                string train_stderr_file,

                (int class_id, double weight)[] class_weights = null,

                routines.libsvm_svm_type svm_type = routines.libsvm_svm_type.c_svc,
                routines.libsvm_kernel_type svm_kernel = routines.libsvm_kernel_type.rbf,

                int repetitions = -1,
                int repetitions_index = -1,

                int outer_cv_folds = -1,
                int outer_cv_index = -1,

                int inner_cv_folds = 5,

                bool probability_estimates = false,
                bool shrinking_heuristics = true,
                bool quiet_mode = true,
                int memory_limit_mb = 1024,
                TimeSpan? point_max_time = null,

                double? cost_exp_begin = -5,
                double? cost_exp_end = 15,
                double? cost_exp_step = 2,

                double? gamma_exp_begin = 3,
                double? gamma_exp_end = -15,
                double? gamma_exp_step = -2,

                double? epsilon_exp_begin = null,//8,
                double? epsilon_exp_end = null,//-1,
                double? epsilon_exp_step = null,//1,

                double? coef0_exp_begin = null,
                double? coef0_exp_end = null,
                double? coef0_exp_step = null,

                double? degree_exp_begin = null,
                double? degree_exp_end = null,
                double? degree_exp_step = null

            )
        {
            
            //const string method_name = nameof(grid_parameter_search);

            var cache_list = grid_cache_data.read_cache_file(cts, cache_train_grid_csv);

            if (inner_cv_folds <= 1) throw new Exception();

            if (svm_kernel == routines.libsvm_kernel_type.precomputed)
            {
                throw new Exception();
            }


            if (cost_exp_step == 0)
            {
                if (cost_exp_end - cost_exp_begin != 0) cost_exp_step = (cost_exp_end - cost_exp_begin) / 10;
                else cost_exp_step = null;
            }

            if (gamma_exp_step == 0)
            {

                if (gamma_exp_end - gamma_exp_begin != 0) gamma_exp_step = (gamma_exp_end - gamma_exp_begin) / 10;
                else gamma_exp_step = null;
            }

            if (epsilon_exp_step == 0)
            {

                if (epsilon_exp_end - epsilon_exp_begin != 0) epsilon_exp_step = (epsilon_exp_end - epsilon_exp_begin) / 10;
                else epsilon_exp_step = null;
            }


            if (coef0_exp_step == 0)
            {
                if (coef0_exp_end - coef0_exp_begin != 0) coef0_exp_step = (coef0_exp_end - coef0_exp_begin) / 10;
                else coef0_exp_step = null;
            }

            if (degree_exp_step == 0)
            {
                if (degree_exp_end - degree_exp_begin != 0) degree_exp_step = (degree_exp_end - degree_exp_begin) / 10;
                else degree_exp_step = null;
            }

            var cost_exp_list = new List<double?>();
            var gamma_exp_list = new List<double?>();
            var epsilon_exp_list = new List<double?>();
            var coef0_exp_list = new List<double?>();
            var degree_exp_list = new List<double?>();


            // always search for cost, unless not specified
            if (cost_exp_begin != null && cost_exp_end != null && cost_exp_step != null)
            {
                for (var c_exp = cost_exp_begin; (c_exp <= cost_exp_end && c_exp >= cost_exp_begin) || (c_exp >= cost_exp_end && c_exp <= cost_exp_begin); c_exp += cost_exp_step)
                {
                    var cost = Math.Pow(2.0, c_exp.Value);
                    cost_exp_list.Add(cost); //(c_exp, c));
                }
            }

            // search gamma only if svm_kernel isn't linear
            if (svm_kernel != routines.libsvm_kernel_type.linear && gamma_exp_begin != null && gamma_exp_end != null && gamma_exp_step != null)
            {
                for (var g_exp = gamma_exp_begin; (g_exp <= gamma_exp_end && g_exp >= gamma_exp_begin) || (g_exp >= gamma_exp_end && g_exp <= gamma_exp_begin); g_exp += gamma_exp_step)
                {
                    var gamma = Math.Pow(2.0, g_exp.Value);
                    gamma_exp_list.Add(gamma);
                }
            }


            // search epsilon only if svm type is svr
            if ((svm_type == routines.libsvm_svm_type.epsilon_svr || svm_type == routines.libsvm_svm_type.nu_svr) && epsilon_exp_begin != null && epsilon_exp_end != null && epsilon_exp_step != null)
            {
                for (var p_exp = epsilon_exp_begin; (p_exp <= epsilon_exp_end && p_exp >= epsilon_exp_begin) || (p_exp >= epsilon_exp_end && p_exp <= epsilon_exp_begin); p_exp += epsilon_exp_step)
                {
                    var epsilon = Math.Pow(2.0, p_exp.Value);
                    epsilon_exp_list.Add(epsilon);
                }
            }

            // search for coef0 only for sigmoid and polynomial
            if ((svm_kernel == routines.libsvm_kernel_type.sigmoid || svm_kernel == routines.libsvm_kernel_type.polynomial) && coef0_exp_begin != null && coef0_exp_end != null && coef0_exp_step != null)
            {
                for (var r_exp = coef0_exp_begin; (r_exp <= coef0_exp_end && r_exp >= coef0_exp_begin) || (r_exp >= coef0_exp_end && r_exp <= coef0_exp_begin); r_exp += coef0_exp_step)
                {
                    var coef0 = Math.Pow(2.0, r_exp.Value);
                    coef0_exp_list.Add(coef0);
                }
            }

            // search for degree only for polynomial
            if (svm_kernel == routines.libsvm_kernel_type.polynomial && degree_exp_begin != null && degree_exp_end != null && degree_exp_step != null)
            {
                for (var d_exp = degree_exp_begin; (d_exp <= degree_exp_end && d_exp >= degree_exp_begin) || (d_exp >= degree_exp_end && d_exp <= degree_exp_begin); d_exp += degree_exp_step)
                {
                    var degree = Math.Pow(2.0, d_exp.Value);
                    degree_exp_list.Add(degree);
                }
            }



            if (cost_exp_list == null || cost_exp_list.Count == 0) cost_exp_list = new List<double?>() { null };
            if (gamma_exp_list == null || gamma_exp_list.Count == 0) gamma_exp_list = new List<double?>() { null };
            if (epsilon_exp_list == null || epsilon_exp_list.Count == 0) epsilon_exp_list = new List<double?>() { null };
            if (coef0_exp_list == null || coef0_exp_list.Count == 0) coef0_exp_list = new List<double?>() { null };
            if (degree_exp_list == null || degree_exp_list.Count == 0) degree_exp_list = new List<double?>() { null };

            var cost_exp_list_len = cost_exp_list.Count;
            var gamma_exp_list_len = gamma_exp_list.Count;
            var epsilon_exp_list_len = epsilon_exp_list.Count;
            var coef0_exp_list_len = coef0_exp_list.Count;
            var degree_exp_list_len = degree_exp_list.Count;


            var search_grid_points = new (double? cost, double? gamma, double? epsilon, double? coef0, double? degree)[cost_exp_list_len * gamma_exp_list_len * epsilon_exp_list_len * coef0_exp_list_len * degree_exp_list_len];
            var k = -1;

            for (var cost_index = 0; cost_index < cost_exp_list_len; cost_index++)
            {
                for (var gamma_index = 0; gamma_index < gamma_exp_list_len; gamma_index++)
                {
                    for (var epsilon_index = 0; epsilon_index < epsilon_exp_list_len; epsilon_index++)
                    {
                        for (var coef0_index = 0; coef0_index < coef0_exp_list_len; coef0_index++)
                        {
                            for (var degree_index = 0; degree_index < degree_exp_list_len; degree_index++)
                            {
                                //search_grid_points.Add((cost_exp_list[cost_index], gamma_exp_list[gamma_index], epsilon_exp_list[epsilon_index], coef0_exp_list[coef0_index], degree_exp_list[degree_index]));
                                search_grid_points[++k] = (cost_exp_list[cost_index], gamma_exp_list[gamma_index], epsilon_exp_list[epsilon_index], coef0_exp_list[coef0_index], degree_exp_list[degree_index]);
                            }
                        }
                    }
                }
            }

            search_grid_points = search_grid_points.Distinct().OrderByDescending(a => a.cost).ThenByDescending(a => a.gamma).ThenByDescending(a => a.epsilon).ThenByDescending(a => a.coef0).ThenByDescending(a => a.degree).ToArray();

            var cached_search_grid_points = search_grid_points.Where(a => cache_list.Any(b => svm_type == b.svm_type && svm_kernel == b.svm_kernel && inner_cv_folds == b.inner_cv_folds && probability_estimates == b.probability_estimates && shrinking_heuristics == b.shrinking_heuristics &&
                                                                              a.cost == b.grid_point.cost &&
                                                                              a.gamma == b.grid_point.gamma &&
                                                                              a.epsilon == b.grid_point.epsilon &&
                                                                              a.coef0 == b.grid_point.coef0 &&
                                                                              a.degree == b.grid_point.degree
                                                                             )).ToArray();
            /*
            var cached_search = cache.Where(b => cached_search_grid_points.Any(a => svm_type == b.svm_type && svm_kernel == b.svm_kernel && inner_cv_folds == b.inner_cv_folds && probability_estimates == b.probability_estimates && shrinking_heuristics == b.shrinking_heuristics &&
                                                                             a.cost == b.point.cost &&
                                                                             a.gamma == b.point.gamma &&
                                                                             a.epsilon == b.point.epsilon &&
                                                                             a.coef0 == b.point.coef0 &&
                                                                             a.degree == b.point.degree
            )).ToList();
            */

            search_grid_points = search_grid_points.Except(cached_search_grid_points).ToArray();

            //var tasks = new List<Task<((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double cv_rate)>>();



            var results = search_grid_points
                .AsParallel()
                .AsOrdered()
                .WithCancellation(cts.Token)
                .Select((point, index) =>
            {
                //var point = search_grid_points[index];
                

                var model_index = index;

                var model_filename = $@"{training_file}_{(model_index + 1)}.model";

                var train_result = libsvm.train(
                    cts,
                    libsvm_train_exe,
                    training_file,
                    model_filename,
                    train_stdout_file,
                    train_stderr_file,
                    point.cost,
                    point.gamma,
                    point.epsilon,
                    point.coef0,
                    point.degree,
                    class_weights,
                    svm_type,
                    svm_kernel,
                    inner_cv_folds,
                    probability_estimates,
                    shrinking_heuristics,
                    point_max_time,
                    quiet_mode,
                    memory_limit_mb);

                //if (!string.IsNullOrWhiteSpace(train_result.cmd_line)) io_proxy.WriteLine(train_result.cmd_line, nameof(svm_wkr), nameof(grid_parameter_search));
                //if (!string.IsNullOrWhiteSpace(train_result.stdout)) io_proxy.WriteLine(train_result.stdout, nameof(svm_wkr), nameof(grid_parameter_search));
                //if (!string.IsNullOrWhiteSpace(train_result.stderr)) io_proxy.WriteLine(train_result.stderr, nameof(svm_wkr), nameof(grid_parameter_search));

                var train_result_lines = train_result.stdout?.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList();

                var cv_rate = libsvm_cv_perf(train_result_lines);

                var grid_point = new grid_point()
                {
                    cost = point.cost,
                    gamma = point.gamma,
                    epsilon = point.epsilon,
                    coef0 = point.coef0,
                    degree = point.degree,
                    cv_rate = cv_rate
                };

                return grid_point;//(point, cv_rate);

            }).ToList();

            foreach (var cache_item in cache_list)
            {
                results.Add(cache_item.grid_point);
                //((
                //    cache_item.grid_point.cost,
                //    cache_item.grid_point.gamma,
                //    cache_item.grid_point.epsilon,
                //    cache_item.grid_point.coef0,
                //    cache_item.grid_point.degree),
                //    cache_item.grid_point.cv_rate??0
                //    ));
            }

            results = results
                .Distinct()
                .OrderByDescending(a => a.cost)
                .ThenByDescending(a => a.gamma)
                .ThenByDescending(a => a.epsilon)
                .ThenByDescending(a => a.coef0)
                .ThenByDescending(a => a.degree)
                .ToList();


            if (search_grid_points.Length > 0)
            {
                var results_cache_format = results.Select(a =>
                    new grid_cache_data()
                    {
                        svm_type = svm_type,
                        svm_kernel = svm_kernel,
                        repetitions = repetitions,
                        repetitions_index = repetitions_index,
                        outer_cv_folds = outer_cv_folds,
                        outer_cv_index = outer_cv_index,
                        inner_cv_folds = inner_cv_folds,
                        probability_estimates = probability_estimates,
                        shrinking_heuristics = shrinking_heuristics,
                        grid_point = new grid_point(a)
                    }
                ).ToList();

                grid_cache_data.write_cache_file(cts, cache_train_grid_csv, results_cache_format);

                //svm_type, svm_kernel, repetitions, repetitions_index, outer_cv_folds, outer_cv_index, inner_cv_folds, probability_estimates, shrinking_heuristics, results);
            }

            var best_grid_point = get_best_rate(results);

            //io_proxy.WriteLine("Grid search complete.", nameof(grid), nameof(grid_parameter_search));
            return best_grid_point;
        }



        internal static double libsvm_cv_perf(List<string> libsvm_result_lines)
        {
            
            //const string method_name = nameof(libsvm_cv_perf);

            if (libsvm_result_lines == null || libsvm_result_lines.Count == 0) return -1;

            var v_libsvm_default_cross_validation_index = libsvm_result_lines.FindIndex(a => a.StartsWith("Cross Validation Accuracy = ", StringComparison.Ordinal));

            var v_libsvm_default_cross_validation_str = v_libsvm_default_cross_validation_index < 0 ? "" : libsvm_result_lines[v_libsvm_default_cross_validation_index].Split()[4];

            if (v_libsvm_default_cross_validation_index >= 0 && !string.IsNullOrWhiteSpace(v_libsvm_default_cross_validation_str))
            {
                return v_libsvm_default_cross_validation_str.Last() == '%' ?
                    double.Parse(v_libsvm_default_cross_validation_str[0..^1], NumberStyles.Float, NumberFormatInfo.InvariantInfo) / (double)100
                    : double.Parse(v_libsvm_default_cross_validation_str, NumberStyles.Float, NumberFormatInfo.InvariantInfo);
            }

            return -1;
        }
    }
}
