using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace svm_fs_batch
{
    internal static class grid
    {
        //internal class best_rate_container
        //{
        //private static readonly object _file_lock = new object();
        //private readonly object _rate_lock = new object();


        internal static List<(routines.libsvm_svm_type svm_type, routines.libsvm_kernel_type svm_kernel, int repetitions, int repetitions_index, int outer_cv_folds, int outer_cv_index, int inner_cv_folds, bool probability_estimates, bool shrinking_heuristics, (double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double rate)> read_cache(string cache_train_grid_csv)
        {
            var module_name = nameof(grid);
            var method_name = nameof(read_cache);

            var cache = new List<(
                routines.libsvm_svm_type svm_type,
                routines.libsvm_kernel_type svm_kernel,
                int repetitions, int repetitions_index, int outer_cv_folds, int outer_cv_index, int inner_cv_folds, bool probability_estimates, bool shrinking_heuristics, (double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double rate)>();

            //cache_train_grid_csv = (cache_train_grid_csv);

            //*!string.IsNullOrWhiteSpace(cache_train_grid_csv) && io_proxy.Exists(cache_train_grid_csv) && new FileInfo(cache_train_grid_csv).Length > 0 && */

            if (io_proxy.is_file_available(cache_train_grid_csv, module_name, method_name))
            {
                //var grid = new List<(double cost, double gamma, double epsilon, double coef0, double degree, double rate)>();

                cache = io_proxy.ReadAllLines(cache_train_grid_csv, module_name, method_name).Skip(1).Select(a =>
                {
                    try
                    {
                        var line = a.Split(',').ToList();

                        var k = 0;
                        return
                        (
                            svm_type: (routines.libsvm_svm_type)Enum.Parse(typeof(routines.libsvm_svm_type), line[k++]),
                            svm_kernel: (routines.libsvm_kernel_type)Enum.Parse(typeof(routines.libsvm_kernel_type), line[k++]),
                            repetitions: int.Parse(line[k++], CultureInfo.InvariantCulture),
                            repetitions_index: int.Parse(line[k++], CultureInfo.InvariantCulture),
                            outer_cv_folds: int.Parse(line[k++], CultureInfo.InvariantCulture),
                            outer_cv_index: int.Parse(line[k++], CultureInfo.InvariantCulture),
                            inner_cv_folds: int.Parse(line[k++], CultureInfo.InvariantCulture),
                            probability_estimates: bool.Parse(line[k++]),
                            shrinking_heuristics: bool.Parse(line[k++]),
                            point: (cost: double.TryParse(line[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out var p_cost) ? p_cost : (double?)null,
                            gamma: double.TryParse(line[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out var p_gamma) ? p_gamma : (double?)null,
                            epsilon: double.TryParse(line[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out var p_epsilon) ? p_epsilon : (double?)null,
                            coef0: double.TryParse(line[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out var p_coef0) ? p_coef0 : (double?)null,
                            degree: double.TryParse(line[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out var p_degree) ? p_degree : (double?)null),
                            rate: double.TryParse(line[k++], NumberStyles.Float, CultureInfo.InvariantCulture, out var p_rate) ? p_rate : 0d
                        );
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

        internal static void write_cache_file(string cache_file, routines.libsvm_svm_type svm_type, routines.libsvm_kernel_type svm_kernel, int repetitions, int repetitions_index, int outer_cv_folds, int outer_cv_index, int inner_cv_folds, bool probability_estimates, bool shrinking_heuristics, List<((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double rate)> res)
        {
            var module_name = nameof(grid);
            var method_name = nameof(write_cache_file);

            var lines = new List<string>();
            lines.Add(string.Join(",", new string[] { nameof(svm_type), nameof(svm_kernel), nameof(repetitions), nameof(repetitions_index), nameof(outer_cv_folds), nameof(outer_cv_index), nameof(inner_cv_folds), nameof(probability_estimates), nameof(shrinking_heuristics), "cost", "gamma", "epsilon", "coef0", "degree", "rate", }));
            lines.AddRange(res.Select(a => string.Join(",", new string[] { svm_type.ToString(), svm_kernel.ToString(), repetitions.ToString(CultureInfo.InvariantCulture), repetitions_index.ToString(CultureInfo.InvariantCulture), outer_cv_folds.ToString(CultureInfo.InvariantCulture), outer_cv_index.ToString(CultureInfo.InvariantCulture), inner_cv_folds.ToString(CultureInfo.InvariantCulture), probability_estimates.ToString(CultureInfo.InvariantCulture), shrinking_heuristics.ToString(CultureInfo.InvariantCulture),

                a.point.cost?.ToString("G17", CultureInfo.InvariantCulture),
                a.point.gamma?.ToString("G17", CultureInfo.InvariantCulture),
                a.point.epsilon?.ToString("G17", CultureInfo.InvariantCulture),
                a.point.coef0?.ToString("G17", CultureInfo.InvariantCulture),
                a.point.degree?.ToString("G17", CultureInfo.InvariantCulture),
                a.rate.ToString("G17", CultureInfo.InvariantCulture) })).ToList());

            io_proxy.WriteAllLines((cache_file), lines, module_name, method_name);
        }


        internal static ((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double? rate) get_best_rate(List<((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double rate)> res)
        {
            //var module_name = nameof(grid);
            //var method_name = nameof(get_best_rate);

            // libsvm grid.py: if ((rate > best_rate) || (rate == best_rate && g == best_g && c < best_c))

            double best_rate = -1;
            double? best_cost = null;
            double? best_gamma = null;
            double? best_epsilon = null;
            double? best_coef0 = null;
            double? best_degree = null;

            foreach (var r in res)
            {
                //io_proxy.WriteLine(r.ToString(), nameof(grid), nameof(get_best_rate));

                var is_rate_better = ((best_rate <= 0) || (r.rate > -1 && r.rate > best_rate));

                var is_rate_same = (r.rate > -1 && best_rate > -1 && r.rate == best_rate);

                if (!is_rate_better && is_rate_same)
                {
                    var is_new_cost_lower = r.point.cost != null && best_cost != null && Math.Abs(r.point.cost.Value) < Math.Abs(best_cost.Value);
                    var is_new_gamma_lower = r.point.gamma != null && best_gamma != null && Math.Abs(r.point.gamma.Value) < Math.Abs(best_gamma.Value);
                    var is_new_epsilon_lower = r.point.epsilon != null && best_epsilon != null && Math.Abs(r.point.epsilon.Value) < Math.Abs(best_epsilon.Value);
                    var is_new_coef0_lower = r.point.coef0 != null && best_coef0 != null && Math.Abs(r.point.coef0.Value) < Math.Abs(best_coef0.Value);
                    var is_new_degree_lower = r.point.degree != null && best_degree != null && Math.Abs(r.point.degree.Value) < Math.Abs(best_degree.Value);
                    var new_score = (is_new_cost_lower ? 1 : 0) + (is_new_gamma_lower ? 1 : 0) + (is_new_epsilon_lower ? 1 : 0) + (is_new_coef0_lower ? 1 : 0) + (is_new_degree_lower ? 1 : 0);

                    var is_old_cost_lower = r.point.cost != null && best_cost != null && Math.Abs(r.point.cost.Value) > Math.Abs(best_cost.Value);
                    var is_old_gamma_lower = r.point.gamma != null && best_gamma != null && Math.Abs(r.point.gamma.Value) > Math.Abs(best_gamma.Value);
                    var is_old_epsilon_lower = r.point.epsilon != null && best_epsilon != null && Math.Abs(r.point.epsilon.Value) > Math.Abs(best_epsilon.Value);
                    var is_old_coef0_lower = r.point.coef0 != null && best_coef0 != null && Math.Abs(r.point.coef0.Value) > Math.Abs(best_coef0.Value);
                    var is_old_degree_lower = r.point.degree != null && best_degree != null && Math.Abs(r.point.degree.Value) > Math.Abs(best_degree.Value);
                    var old_score = (is_old_cost_lower ? 1 : 0) + (is_old_gamma_lower ? 1 : 0) + (is_old_epsilon_lower ? 1 : 0) + (is_old_coef0_lower ? 1 : 0) + (is_old_degree_lower ? 1 : 0);

                    is_rate_same = new_score >= old_score;
                }


                if (is_rate_better || is_rate_same)
                {
                    best_rate = r.rate;
                    best_cost = r.point.cost;
                    best_gamma = r.point.gamma;
                    best_epsilon = r.point.epsilon;
                    best_coef0 = r.point.coef0;
                    best_degree = r.point.degree;
                }
            }

            var ret = ((best_cost, best_gamma, best_epsilon, best_coef0, best_degree), best_rate);

            //io_proxy.WriteLine("r: " + ret, nameof(grid), nameof(get_best_rate));

            return ret;
        }


        internal static ((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double? cv_rate) grid_parameter_search(
            string libsvm_train_exe,
            string cache_train_grid_csv,
                string training_file,
                string train_stdout_file,
                string train_stderr_file,


                List<(int class_id, double weight)> class_weights = null,

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
            //var module_name = nameof(grid);
            //var method_name = nameof(grid_parameter_search);

            var cache = read_cache(cache_train_grid_csv);

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

            var search_grid_points = new List<(double? cost, double? gamma, double? epsilon, double? coef0, double? degree)>();

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

            for (var cost_index = 0; cost_index < cost_exp_list.Count; cost_index++)
            {
                for (var gamma_index = 0; gamma_index < gamma_exp_list.Count; gamma_index++)
                {
                    for (var epsilon_index = 0; epsilon_index < epsilon_exp_list.Count; epsilon_index++)
                    {
                        for (var coef0_index = 0; coef0_index < coef0_exp_list.Count; coef0_index++)
                        {
                            for (var degree_index = 0; degree_index < degree_exp_list.Count; degree_index++)
                            {
                                search_grid_points.Add((cost_exp_list[cost_index], gamma_exp_list[gamma_index], epsilon_exp_list[epsilon_index], coef0_exp_list[coef0_index], degree_exp_list[degree_index]));
                            }
                        }
                    }
                }
            }

            search_grid_points = search_grid_points.Distinct().OrderByDescending(a => a.cost).ThenByDescending(a => a.gamma).ThenByDescending(a => a.epsilon).ThenByDescending(a => a.coef0).ThenByDescending(a => a.degree).ToList();

            var cached_search_grid_points = search_grid_points.Where(a => cache.Any(b => svm_type == b.svm_type && svm_kernel == b.svm_kernel && inner_cv_folds == b.inner_cv_folds && probability_estimates == b.probability_estimates && shrinking_heuristics == b.shrinking_heuristics &&
                                                                              a.cost == b.point.cost &&
                                                                              a.gamma == b.point.gamma &&
                                                                              a.epsilon == b.point.epsilon &&
                                                                              a.coef0 == b.point.coef0 &&
                                                                              a.degree == b.point.degree
                                                                             )).ToList();

            var cached_search = cache.Where(b => cached_search_grid_points.Any(a => svm_type == b.svm_type && svm_kernel == b.svm_kernel && inner_cv_folds == b.inner_cv_folds && probability_estimates == b.probability_estimates && shrinking_heuristics == b.shrinking_heuristics &&
                                                                             a.cost == b.point.cost &&
                                                                             a.gamma == b.point.gamma &&
                                                                             a.epsilon == b.point.epsilon &&
                                                                             a.coef0 == b.point.coef0 &&
                                                                             a.degree == b.point.degree
            )).ToList();

            search_grid_points = search_grid_points.Except(cached_search_grid_points).ToList();

            //var tasks = new List<Task<((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double cv_rate)>>();


          
            var results = Enumerable.Range(0, search_grid_points.Count).AsParallel().AsOrdered().Select(index =>
            {
                var point = search_grid_points[index];

                var model_index = index;

                var model_filename = $@"{training_file}_{(model_index + 1)}.model";

                var train_result = libsvm.train(
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

                return (point, cv_rate);

            }).ToList();

            //var results = new List<((double? cost, double? gamma, double? epsilon, double? coef0, double? degree) point, double? cv_rate)>();

            foreach (var c in cache)
            {
                results.Add((c.point, c.rate));
            }

            //if (tasks != null && tasks.Count > 0)
            //{
            //    try
            //    {
            //        Task.WaitAll(tasks.ToArray<Task>());
            //    }
            //    catch (Exception)
            //    {
            //
            //    }
            //
            //    var tr = tasks.Where(a => a.IsCompletedSuccessfully).Select(a => a.Result).ToList();
            //
            //    foreach (var r in tr)
            //    {
            //        results.Add(r);
            //    }
            //}

            results = results.Distinct().OrderByDescending(a => a.point.cost).ThenByDescending(a => a.point.gamma).ThenByDescending(a => a.point.epsilon).ThenByDescending(a => a.point.coef0).ThenByDescending(a => a.point.degree).ToList();


            if (search_grid_points.Count > 0)
            {
                write_cache_file(cache_train_grid_csv, svm_type, svm_kernel, repetitions, repetitions_index, outer_cv_folds, outer_cv_index, inner_cv_folds, probability_estimates, shrinking_heuristics, results);
            }

            var xr = get_best_rate(results);

            //io_proxy.WriteLine("Grid search complete.", nameof(grid), nameof(grid_parameter_search));
            return xr;
        }



        internal static double libsvm_cv_perf(List<string> libsvm_result_lines)
        {
            //var module_name = nameof(grid);
            //var method_name = nameof(libsvm_cv_perf);

            if (libsvm_result_lines == null || libsvm_result_lines.Count == 0) return -1;

            var v_libsvm_default_cross_validation_index = libsvm_result_lines.FindIndex(a => a.StartsWith("Cross Validation Accuracy = ", StringComparison.InvariantCulture));

            var v_libsvm_default_cross_validation_str = v_libsvm_default_cross_validation_index < 0 ? "" : libsvm_result_lines[v_libsvm_default_cross_validation_index].Split()[4];

            if (v_libsvm_default_cross_validation_index >= 0 && !string.IsNullOrWhiteSpace(v_libsvm_default_cross_validation_str))
            {
                return v_libsvm_default_cross_validation_str.Last() == '%' ?
                    double.Parse(v_libsvm_default_cross_validation_str.Substring(0, v_libsvm_default_cross_validation_str.Length - 1), NumberStyles.Float, CultureInfo.InvariantCulture) / (double)100
                    : double.Parse(v_libsvm_default_cross_validation_str, NumberStyles.Float, CultureInfo.InvariantCulture);
            }

            return -1;
        }

    }
}
