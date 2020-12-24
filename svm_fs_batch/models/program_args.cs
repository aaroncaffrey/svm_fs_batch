using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace svm_fs_batch
{
    internal class program_args
    {
        public const string module_name = nameof(program_args);

        // note: use 'folds' to set repetitions, outer_cv_folds and inner_folds to the same value
        internal int? folds = (int?)null;

        internal int repetitions = 1;
        internal int outer_cv_folds = 5;
        internal int outer_cv_folds_to_run = 0;
        internal int inner_folds = 5;
        
        //internal bool run_local = false;
        internal string experiment_name = "";

        internal string job_id = "";
        internal string job_name = "";


        // whole array parameters
        internal int whole_array_index_first = -1; // start index of whole array
        internal int whole_array_index_last = -1; // end index of whole array
        internal int whole_array_step_size = 0; // step size for whole array
        internal int whole_array_length = 0; // number of jobs in array

        // partition array parameters (pbs only provides partition_array_index_first, whole_array_step_size is given by the script, and partition_array_index_last is calculated from it, if not provided)
        internal int partition_array_index_first = -1; // start index for current instance
        internal int partition_array_index_last = -1; // last index for current instance

        // parameters for -setup only to generate pbs script to run array job
        internal bool setup = false;
        internal int setup_total_vcpus = -1;
        internal int setup_instance_vcpus = -1;



        internal (string key, string as_str, int? as_int, double? as_double, bool? as_bool)[] args;

        internal bool args_key_exists(string key)
        {
            if (args == null || args.Length == 0) return false;

            return args.Any(a => string.Equals(a.key, key, StringComparison.OrdinalIgnoreCase));
        }

        internal (string key, string as_str, int? as_int, double? as_double, bool? as_bool) args_value(string key)
        {
            if (args == null || args.Length == 0) return default;

            return args.FirstOrDefault(a => string.Equals(a.key, key, StringComparison.OrdinalIgnoreCase));
        }

        internal static (string key, string as_str, int? as_int, double? as_double, bool? as_bool)[] get_params(string[] args)
        {
            var x = new List<(string key, string value)>();

            if (args == null || args.Length == 0)
            {
                return null;
            }

            args = args.SelectMany(a => a.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries)).Where(a => !string.IsNullOrWhiteSpace(a)).ToArray();

            var name = "";
            var value = "";

            for (var i = 0; i < args.Length; i++)
            {
                var arg = args[i];

                var starts_dash = arg[0] == '-';
                var is_num = double.TryParse(arg, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var val_out);

                var is_name = starts_dash && (!is_num || string.IsNullOrEmpty(name)) && arg.Length > 1;
                var is_value = !is_name;
                var is_final_index = i == args.Length - 1;

                if (is_name)
                {
                    if (!string.IsNullOrWhiteSpace(name) || !string.IsNullOrWhiteSpace(value))
                    {
                        x.Add((name, value));
                    };

                    name = arg[1..];
                    value = "";
                }

                if (is_value) value += (value.Length > 0 ? " " : "") + arg;

                if (is_final_index)
                {
                    x.Add((name, value));
                }
            }

            //var x2 = new List<(string key, string as_str, int? as_int, double? as_double, bool? as_bool)>();

            var x2 = x.Select(a =>
            {
                var as_int = int.TryParse(a.value, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var as_int_out) ? (int?)as_int_out : (int?) null;
                var as_double = double.TryParse(a.value, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var as_double_out) ? (double?)as_double_out : (double?) null;
                var as_bool = bool.TryParse(a.value, out var as_bool_out) ? (bool?)as_bool_out : (bool?) null;

                if (as_bool == null)
                {
                    if (string.IsNullOrWhiteSpace(a.value)) as_bool = true;
                    else if (as_int == 0 && as_double == 0) as_bool = false;
                    else if (as_int == 1 && as_double == 1) as_bool = true;
                }

                return (a.key, a.value, as_int, as_double, as_bool);
            }).ToArray();

            return x2;
        }

        public program_args()
        {

        }

        public program_args(string[] args)
        {
            const string method_name = nameof(program_args);

            var arg_names = new string[]
            {
                    nameof(folds), nameof(repetitions), nameof(outer_cv_folds), nameof(outer_cv_folds_to_run), nameof(inner_folds), nameof(setup), /*nameof(run_local),*/ nameof(experiment_name), nameof(job_id), nameof(job_name),
                    nameof(partition_array_index_first), nameof(partition_array_index_last), nameof(whole_array_length), nameof(whole_array_step_size), nameof(whole_array_index_first), nameof(whole_array_index_last), nameof(setup_total_vcpus), nameof(setup_instance_vcpus),
            };

            this.args = get_params(args);
            var args_given = this.args.Select(a => a.key).ToArray();
            var args_missing = arg_names.Except(args_given, StringComparer.OrdinalIgnoreCase).ToArray();
            var args_known = args_given.Intersect(arg_names, StringComparer.OrdinalIgnoreCase).ToArray();
            var args_unknown = args_given.Except(arg_names, StringComparer.OrdinalIgnoreCase).ToArray();
            var args_count = args_given.Distinct().Select(a => (key: a, count: this.args.Count(b => string.Equals(a, b.key, StringComparison.OrdinalIgnoreCase)))).ToArray();

            io_proxy.WriteLine($@"{nameof(args_given)} = {string.Join(", ", args_given)}", module_name, method_name);
            io_proxy.WriteLine($@"{nameof(args_missing)} = {string.Join(", ", args_missing)}", module_name, method_name);
            io_proxy.WriteLine($@"{nameof(args_known)} = {string.Join(", ", args_known)}", module_name, method_name);
            io_proxy.WriteLine($@"{nameof(args_unknown)} = {string.Join(", ", args_unknown)}", module_name, method_name);

            if (args_unknown.Any()) throw new ArgumentOutOfRangeException(nameof(args), $@"{module_name}.{method_name}: Invalid arguments: {string.Join(", ", args_unknown)}");
            if (args_count.Any(a => a.count > 1)) throw new ArgumentOutOfRangeException(nameof(args), $@"{module_name}.{method_name}: Arguments specified more than once: {string.Join(", ", args_count.Where(a => a.count > 1).ToArray())}");

            if (args_key_exists(nameof(experiment_name))) experiment_name = args_value(nameof(experiment_name)).as_str;
            if (args_key_exists(nameof(job_id))) job_id = args_value(nameof(job_id)).as_str;
            if (args_key_exists(nameof(job_name))) job_name = args_value(nameof(job_name)).as_str;
            
            //if (args_key_exists(nameof(run_local))) run_local = args_value(nameof(run_local)).as_bool ?? default;
            if (args_key_exists(nameof(setup))) setup = args_value(nameof(setup)).as_bool ?? default;

            if (args_key_exists(nameof(repetitions))) repetitions = args_value(nameof(repetitions)).as_int ?? -1;
            if (args_key_exists(nameof(outer_cv_folds))) outer_cv_folds = args_value(nameof(outer_cv_folds)).as_int ?? -1;
            if (args_key_exists(nameof(outer_cv_folds_to_run))) outer_cv_folds_to_run = args_value(nameof(outer_cv_folds_to_run)).as_int ?? -1;
            if (args_key_exists(nameof(inner_folds))) inner_folds = args_value(nameof(inner_folds)).as_int ?? -1;
            
            if (args_key_exists(nameof(partition_array_index_first))) partition_array_index_first = args_value(nameof(partition_array_index_first)).as_int ?? -1;
            if (args_key_exists(nameof(partition_array_index_last))) partition_array_index_last = args_value(nameof(partition_array_index_last)).as_int ?? -1;
            
            if (args_key_exists(nameof(whole_array_length))) whole_array_length = args_value(nameof(whole_array_length)).as_int ?? 0;
            if (args_key_exists(nameof(whole_array_step_size))) whole_array_step_size = args_value(nameof(whole_array_step_size)).as_int ?? 0;
            if (args_key_exists(nameof(whole_array_index_first))) whole_array_index_first = args_value(nameof(whole_array_index_first)).as_int ?? -1;
            if (args_key_exists(nameof(whole_array_index_last))) whole_array_index_last = args_value(nameof(whole_array_index_last)).as_int ?? -1;
            
            if (args_key_exists(nameof(setup_total_vcpus))) setup_total_vcpus = args_value(nameof(setup_total_vcpus)).as_int ?? -1;
            if (args_key_exists(nameof(setup_instance_vcpus))) setup_instance_vcpus = args_value(nameof(setup_instance_vcpus)).as_int ?? -1;


            if (args_key_exists(nameof(folds)))
            {
                folds = args_value(nameof(folds)).as_int ?? -1;

                if (folds != null && folds >= 2)
                {
                    inner_folds = folds.Value;
                    outer_cv_folds = folds.Value;
                    outer_cv_folds_to_run = folds.Value;
                    repetitions = folds.Value;
                }
            }

            if (partition_array_index_last <= -1) { partition_array_index_last = partition_array_index_first + (whole_array_step_size - 1); }
        }
    }
}
