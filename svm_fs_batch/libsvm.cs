using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal static class libsvm
    {
        public const string module_name = nameof(libsvm);

        private static readonly Random random = new Random();

        internal static (string cmd_line, string stdout, string stderr) train(string libsvm_train_exe_file, string train_file, string model_out_file, string stdout_file = null, string stderr_file = null, double? cost = null, double? gamma = null, double? epsilon = null, double? coef0 = null, double? degree = null, (int class_id, double weight)[] class_weights = null, routines.libsvm_svm_type svm_type = routines.libsvm_svm_type.c_svc, routines.libsvm_kernel_type svm_kernel = routines.libsvm_kernel_type.rbf, int? inner_cv_folds = null, bool probability_estimates = false, bool shrinking_heuristics = true, TimeSpan? process_max_time = null, bool quiet_mode = true, int memory_limit_mb = 1024, bool log = true)
        {
            List<(string key, string value)> get_params()//string libsvm_train_exe_file, string train_file, string model_out_file, string stdout_file = null, string stderr_file = null, double? cost = null, double? gamma = null, double? epsilon = null, double? coef0 = null, double? degree = null, List<(int class_id, double weight)> class_weights = null, routines.libsvm_svm_type svm_type = routines.libsvm_svm_type.c_svc, routines.libsvm_kernel_type svm_kernel = routines.libsvm_kernel_type.rbf, int? inner_cv_folds = null, bool probability_estimates = false, bool shrinking_heuristics = true, TimeSpan? process_max_time = null, bool quiet_mode = true, int memory_limit_mb = 1024, bool log = false)
            {
                try
                {
                    return new List<(string key, string value)>()
                {
                    (nameof(libsvm_train_exe_file), libsvm_train_exe_file), (nameof(train_file), train_file), (nameof(model_out_file), model_out_file), (nameof(stdout_file), stdout_file), (nameof(stderr_file), stderr_file), (nameof(cost), cost?.ToString() ?? ""), (nameof(gamma), gamma?.ToString() ?? ""), (nameof(epsilon), epsilon?.ToString() ?? ""), (nameof(coef0), coef0?.ToString() ?? ""), (nameof(degree), degree?.ToString() ?? ""), (nameof(class_weights), class_weights != null ? string.Join(";", class_weights.Select(a => $@"{a.class_id}={a.weight}").ToList()) : ""), (nameof(svm_type), svm_type.ToString()), (nameof(svm_kernel), svm_kernel.ToString()), (nameof(inner_cv_folds), inner_cv_folds?.ToString() ?? ""), (nameof(probability_estimates), probability_estimates.ToString()), (nameof(shrinking_heuristics), shrinking_heuristics.ToString()), (nameof(process_max_time), process_max_time?.ToString() ?? ""), (nameof(quiet_mode), quiet_mode.ToString()), (nameof(memory_limit_mb), memory_limit_mb.ToString()), (nameof(log), log.ToString())
                };
                }
                catch (Exception) { }

                return new List<(string key, string value)>();
            }

            string get_params_str()//string libsvm_train_exe_file, string train_file, string model_out_file, string stdout_file = null, string stderr_file = null, double? cost = null, double? gamma = null, double? epsilon = null, double? coef0 = null, double? degree = null, List<(int class_id, double weight)> class_weights = null, routines.libsvm_svm_type svm_type = routines.libsvm_svm_type.c_svc, routines.libsvm_kernel_type svm_kernel = routines.libsvm_kernel_type.rbf, int? inner_cv_folds = null, bool probability_estimates = false, bool shrinking_heuristics = true, TimeSpan? process_max_time = null, bool quiet_mode = true, int memory_limit_mb = 1024, bool log = false)
            {
                //try { return string.Join(", ", get_params(libsvm_train_exe_file, train_file, model_out_file, stdout_file, stderr_file, cost, gamma, epsilon, coef0, degree, class_weights, svm_type, svm_kernel, inner_cv_folds, probability_estimates, shrinking_heuristics, process_max_time, quiet_mode, memory_limit_mb, log).Select(a => $@"{a.key}=""{a.value}""").ToList()); } catch (Exception) { }
                try { return string.Join(", ", get_params().Select(a => $@"{a.key}=""{a.value}""").ToList()); } catch (Exception) { }

                return "";
            }


            //libsvm_train_exe_file = (libsvm_train_exe_file);
            //train_file = (train_file);
            //model_out_file = (model_out_file);
            //stdout_file = (stdout_file);
            //stderr_file = (stderr_file);

            //var quiet_mode = true;
            //var memory_limit_mb = 1024;

            var libsvm_params = new List<string>();


            if (quiet_mode) { libsvm_params.Add("-q"); }

            if (memory_limit_mb != 100) { libsvm_params.Add($@"-m {memory_limit_mb}"); }

            if (probability_estimates) { libsvm_params.Add($@"-b {(probability_estimates ? "1" : "0")}"); }

            if (svm_type != routines.libsvm_svm_type.c_svc) { libsvm_params.Add($@"-s {(int)svm_type}"); }


            if (svm_kernel != routines.libsvm_kernel_type.rbf) { libsvm_params.Add($@"-t {(int)svm_kernel}"); }


            if (inner_cv_folds != null && inner_cv_folds >= 2) { libsvm_params.Add($@"-v {inner_cv_folds}"); }

            if (cost != null) { libsvm_params.Add($@"-c {cost.Value}"); }

            if (gamma != null && svm_kernel != routines.libsvm_kernel_type.linear) { libsvm_params.Add($@"-g {gamma.Value}"); }

            if (epsilon != null && (svm_type == routines.libsvm_svm_type.epsilon_svr || svm_type == routines.libsvm_svm_type.nu_svr)) { libsvm_params.Add($@"-p {epsilon.Value}"); }

            if (coef0 != null && (svm_kernel == routines.libsvm_kernel_type.sigmoid || svm_kernel == routines.libsvm_kernel_type.polynomial)) { libsvm_params.Add($@"-r {coef0.Value}"); }

            if (degree != null && svm_kernel == routines.libsvm_kernel_type.polynomial) { libsvm_params.Add($@"-d {degree.Value}"); }

            if (class_weights != null && class_weights.Length > 0)
            {
                class_weights = class_weights.OrderBy(a => a.class_id).ToArray();

                for (var class_weight_index = 0; class_weight_index < class_weights.Length; class_weight_index++)
                {
                    var class_weight = class_weights[class_weight_index];
                    libsvm_params.Add($@"-w{class_weight.class_id} {class_weight.weight}");
                }
            }

            if (!shrinking_heuristics) { libsvm_params.Add($@"-h {(shrinking_heuristics ? "1" : "0")}"); }

            libsvm_params = libsvm_params.OrderBy(a => a).ToList();

            var train_file_param = train_file;
            var model_file_param = model_out_file;

            if (!String.IsNullOrWhiteSpace(train_file)) { libsvm_params.Add($@"{train_file_param}"); }

            if (!String.IsNullOrWhiteSpace(model_out_file) && (inner_cv_folds == null || inner_cv_folds <= 1)) { libsvm_params.Add($@"{model_file_param}"); }

            var wd = Path.GetDirectoryName(train_file);

            var args = string.Join(" ", libsvm_params);

            var start = new ProcessStartInfo()
            {
                FileName = libsvm_train_exe_file,
                Arguments = args,
                UseShellExecute = false,
                CreateNoWindow = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true,
                //WorkingDirectory = wd ?? "" // Path.GetDirectoryName(exe_file) ?? ""
            };

            var cmd_line = string.Join(" ", libsvm_train_exe_file, args);

            //var priority_boost_enabled = false;
            //var priority_class = ProcessPriorityClass.AboveNormal;
            //if (inner_cv_folds == null || inner_cv_folds < 2) { priority_class = ProcessPriorityClass.High; }

            var retry_index = -1;
            var retry = false;
            do
            {
                retry_index++;

                try
                {
                    using var process = Process.Start(start);

                    if (process == null)
                    {
                        retry = true;
                        try { Task.Delay(new TimeSpan(0, 0, 0, 30 + random.Next(0, 61))).Wait(); } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(train)); }

                        continue;
                    }

                    if (log) { io_proxy.WriteLine($"Spawned process {Path.GetFileName(start.FileName)}: {process.Id}", nameof(libsvm), nameof(train)); }

                    //try { process.PriorityBoostEnabled = priority_boost_enabled; } catch (Exception e) { io_proxy.log_exception(e, "", nameof(svm_ctl), nameof(train)); }
                    //try { process.PriorityClass = priority_class; } catch (Exception e) { io_proxy.log_exception(e, "", nameof(svm_ctl), nameof(train)); }

                    //var stdout = process.StandardOutput.ReadToEndAsync();
                    //var stderr = process.StandardError.ReadToEndAsync();

                    var exited = process.WaitForExit((int)Math.Ceiling(new TimeSpan(0, 45, 0).TotalMilliseconds));

                    if (!exited)
                    {
                        try { process.Kill(); } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(train)); }

                        retry = true;

                        try { Task.Delay(new TimeSpan(0, 0, 0, 30 + random.Next(0, 61))).Wait(); } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(train)); }

                        continue;
                    }

                    var stdout_result = process?.StandardOutput?.ReadToEnd() ?? "";
                    var stderr_result = process?.StandardError?.ReadToEnd() ?? "";

                    //var tasks = new List<Task>() { stdout, stderr };
                    //try { Task.WaitAll(tasks.ToArray<Task>()); } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(train)); }

                    if (log) { io_proxy.WriteLine($"Exited process {Path.GetFileName(start.FileName)}: {process.Id}", nameof(libsvm), nameof(train)); }

                    var exit_code = process.ExitCode;

                    //var stdout_result = "";
                    //var stderr_result = "";
                    //try { stdout_result = stdout?.Result; } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(train)); }
                    //try { stderr_result = stderr?.Result; } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(train)); }

                    if (!string.IsNullOrWhiteSpace(stdout_file) && !string.IsNullOrWhiteSpace(stdout_result)) { io_proxy.AppendAllText(stdout_file, stdout_result); }

                    if (!string.IsNullOrWhiteSpace(stderr_file) && !string.IsNullOrWhiteSpace(stderr_result)) { io_proxy.AppendAllText(stderr_file, stderr_result); }

                    if (exit_code == 0) { return (cmd_line, stdout_result, stderr_result); }
                    else
                    {
                        retry = true;
                        io_proxy.WriteLine("libsvm train failed to run");
                        if (!string.IsNullOrWhiteSpace(stdout_result)) io_proxy.WriteLine(stdout_result);
                        if (!string.IsNullOrWhiteSpace(stderr_result)) io_proxy.WriteLine(stderr_result);

                        try { Task.Delay(new TimeSpan(0, 0, 0, 30 + random.Next(0, 61))).Wait(); } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(train)); }

                        continue;
                    }
                }
                catch (Exception e1)
                {
                    retry = true;

                    io_proxy.log_exception(e1, get_params_str(), nameof(libsvm), nameof(train));

                    try { Task.Delay(new TimeSpan(0, 0, 0, 30 + random.Next(0, 61))).Wait(); } catch (Exception e2) { io_proxy.log_exception(e2, get_params_str(), nameof(libsvm), nameof(train)); }
                }
            } while (retry && retry_index < 1_000_000);

            return (cmd_line, null, null);
        }

        internal static (string cmd_line, string stdout, string stderr) predict(string libsvm_predict_exe_file, string test_file, string model_file, string predictions_out_file, bool probability_estimates, string stdout_file = null, string stderr_file = null, bool log = true)
        {
            List<(string key, string value)> get_params()
            {
                try { return new List<(string key, string value)>() { (nameof(libsvm_predict_exe_file), libsvm_predict_exe_file), (nameof(test_file), test_file), (nameof(model_file), model_file), (nameof(predictions_out_file), predictions_out_file), (nameof(stdout_file), stdout_file), (nameof(stderr_file), stderr_file), (nameof(log), log.ToString()) }; } catch (Exception) { }

                return new List<(string key, string value)>();
            }

            string get_params_str()
            {
                try { return string.Join(", ", get_params()); } catch (Exception) { }

                return "";
            }

            var libsvm_params = new List<string>();

            if (probability_estimates) { libsvm_params.Add($@"-b 1"); }

            libsvm_params = libsvm_params.OrderBy(a => a).ToList();

            var test_file_param = test_file;
            var model_file_param = model_file;
            var prediction_file_param = predictions_out_file;

            if (!String.IsNullOrWhiteSpace(test_file)) { libsvm_params.Add($@"{test_file_param}"); }

            if (!String.IsNullOrWhiteSpace(model_file)) { libsvm_params.Add($@"{model_file_param}"); }

            if (!String.IsNullOrWhiteSpace(predictions_out_file)) { libsvm_params.Add($@"{prediction_file_param}"); }

            var args = String.Join(" ", libsvm_params);

            var start = new ProcessStartInfo
            {
                FileName = libsvm_predict_exe_file,
                Arguments = args,
                UseShellExecute = false,
                CreateNoWindow = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true,
                //WorkingDirectory = Path.GetDirectoryName(exe_file) ?? ""
            };

            var cmd_line = string.Join(" ", libsvm_predict_exe_file, args);

            //var priority_boost_enabled = false;
            //var priority_class = ProcessPriorityClass.High;

            var retry_index = -1;
            var retry = false;
            do
            {
                retry_index++;

                try
                {
                    using var process = Process.Start(start);

                    if (process == null)
                    {
                        retry = true;
                        try { Task.Delay(new TimeSpan(0, 0, 0, 30 + random.Next(0, 61))).Wait(); } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(predict)); }

                        continue;
                    }

                    if (log) { io_proxy.WriteLine($"Spawned process {Path.GetFileName(start.FileName)}: {process.Id}", nameof(libsvm), nameof(predict)); }
                    //try { process.PriorityBoostEnabled = priority_boost_enabled; } catch (Exception e) { io_proxy.log_exception(e, "", nameof(svm_ctl), nameof(predict)); }
                    //try { process.PriorityClass = priority_class; } catch (Exception e) { io_proxy.log_exception(e, "", nameof(svm_ctl), nameof(predict)); }

                    //var stdout = process.StandardOutput.ReadToEndAsync();
                    //var stderr = process.StandardError.ReadToEndAsync();

                   
                    var exited = process.WaitForExit((int)Math.Ceiling(new TimeSpan(0, 45, 0).TotalMilliseconds));

                    if (!exited)
                    {
                        try { process.Kill(); } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(predict)); }

                        retry = true;

                        try { Task.Delay(new TimeSpan(0, 0, 0, 30 + random.Next(0, 61))).Wait(); } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(predict)); }

                        continue;
                    }

                    var stdout_result = process?.StandardOutput?.ReadToEnd() ?? "";
                    var stderr_result = process?.StandardError?.ReadToEnd() ?? "";

                    if (log) { io_proxy.WriteLine($"Exited process {Path.GetFileName(start.FileName)}: {process.Id}", nameof(libsvm), nameof(predict)); }

                    var exit_code = process.ExitCode;

                    //var tasks = new List<Task>() { stdout, stderr };
                    //try { Task.WaitAll(tasks.ToArray<Task>()); } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(predict)); }
                    //var stdout_result = "";
                    //var stderr_result = "";
                    //try { stdout_result = stdout?.Result; } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(predict)); }
                    //try { stderr_result = stderr?.Result; } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(predict)); }


                    if (!string.IsNullOrWhiteSpace(stdout_file) && !string.IsNullOrWhiteSpace(stdout_result)) { io_proxy.AppendAllText(stdout_file, stdout_result); }

                    if (!string.IsNullOrWhiteSpace(stderr_file) && !string.IsNullOrWhiteSpace(stderr_result)) { io_proxy.AppendAllText(stderr_file, stderr_result); }

                    if (exit_code == 0) { return (cmd_line, stdout_result, stderr_result); }
                    else
                    {
                        retry = true;
                        io_proxy.WriteLine("libsvm predict failed to run");
                        if (!string.IsNullOrWhiteSpace(stdout_result)) io_proxy.WriteLine(stdout_result);
                        if (!string.IsNullOrWhiteSpace(stderr_result)) io_proxy.WriteLine(stderr_result);

                        try { Task.Delay(new TimeSpan(0, 0, 0, 30 + random.Next(0, 61))).Wait(); } catch (Exception e) { io_proxy.log_exception(e, get_params_str(), nameof(libsvm), nameof(predict)); }

                        continue;
                    }
                }
                catch (Exception e1)
                {
                    retry = true;
                    io_proxy.log_exception(e1, get_params_str(), nameof(libsvm), nameof(predict));
                    try { Task.Delay(new TimeSpan(0, 0, 0, 30 + random.Next(0, 61))).Wait(); } catch (Exception e2) { io_proxy.log_exception(e2, get_params_str(), nameof(libsvm), nameof(predict)); }
                }
            } while (retry && retry_index < 1_000_000);

            return (cmd_line, null, null);
        }
    }
}