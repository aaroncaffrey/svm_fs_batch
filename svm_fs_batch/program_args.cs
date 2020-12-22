using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace svm_fs_batch
{
    internal class program_args
    {
        public const string module_name = nameof(program_args);

        internal int? folds = (int?)null;
        internal int repetitions = 1;
        internal int outer_cv_folds = 5;
        internal int outer_cv_folds_to_run = 0;
        internal int inner_folds = 5;
        internal bool setup = false;
        internal bool run_local = false;
        internal string experiment_name = "";
        internal string job_id = "";
        internal string job_name = "";

        internal int instance_array_index_start = -1; // start index for current instance
        internal int instance_array_index_end = -1; // end index for current instance 

        internal int array_instances = 0;
        internal int array_step = 0;
        internal int array_start = -1; // start index of whole array
        internal int array_end = -1; // end index of whole array

        internal int setup_total_vcpus = -1;
        internal int setup_instance_vcpus = -1;

        internal List<(string key, string as_str, int? as_int, double? as_double, bool? as_bool)> args = new List<(string key, string as_str, int? as_int, double? as_double, bool? as_bool)>();


        internal static List<(string key, string as_str, int? as_int, double? as_double, bool? as_bool)> get_params(string[] args)
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

            var x2 = new List<(string key, string as_str, int? as_int, double? as_double, bool? as_bool)>();

            x2 = x.Select(a =>
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
            }).ToList();

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
                    nameof(folds), nameof(repetitions), nameof(outer_cv_folds), nameof(outer_cv_folds_to_run), nameof(inner_folds), nameof(setup), nameof(run_local), nameof(experiment_name), nameof(job_id), nameof(job_name),
                    nameof(instance_array_index_start), nameof(instance_array_index_end), nameof(array_instances), nameof(array_step), nameof(array_start), nameof(array_end), nameof(setup_total_vcpus), nameof(setup_instance_vcpus),
            };

            this.args = get_params(args);

            if (this.args.Any(a => !arg_names.Contains(a.key)))
            {
                throw new ArgumentOutOfRangeException(nameof(args), $@"{module_name}.{method_name}: invalid arguments");
            }

            if (this.args.Any(a => this.args.Count(b => string.Equals(a.key, b.key, StringComparison.OrdinalIgnoreCase)) > 1))
            {
                throw new ArgumentOutOfRangeException(nameof(args), $@"{module_name}.{method_name}: arguments specified more than once");
            }


            if (this.args.Any(a => string.Equals(a.key, nameof(experiment_name), StringComparison.OrdinalIgnoreCase)))
            {
                experiment_name = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(experiment_name), StringComparison.OrdinalIgnoreCase)).as_str;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(job_id), StringComparison.OrdinalIgnoreCase)))
            {
                job_id = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(job_id), StringComparison.OrdinalIgnoreCase)).as_str;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(job_name), StringComparison.OrdinalIgnoreCase)))
            {
                job_name = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(job_name), StringComparison.OrdinalIgnoreCase)).as_str;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(run_local), StringComparison.OrdinalIgnoreCase)))
            {
                run_local = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(run_local), StringComparison.OrdinalIgnoreCase)).as_bool.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(setup), StringComparison.OrdinalIgnoreCase)))
            {
                setup = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(setup), StringComparison.OrdinalIgnoreCase)).as_bool.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(folds), StringComparison.OrdinalIgnoreCase)))
            {
                folds = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(folds), StringComparison.OrdinalIgnoreCase)).as_int.Value;
                inner_folds = folds.Value;
                outer_cv_folds = folds.Value;
                outer_cv_folds_to_run = folds.Value;
                repetitions = folds.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(repetitions), StringComparison.OrdinalIgnoreCase)))
            {
                repetitions = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(repetitions), StringComparison.OrdinalIgnoreCase)).as_int.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(outer_cv_folds), StringComparison.OrdinalIgnoreCase)))
            {
                outer_cv_folds = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(outer_cv_folds), StringComparison.OrdinalIgnoreCase)).as_int.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(outer_cv_folds_to_run), StringComparison.OrdinalIgnoreCase)))
            {
                outer_cv_folds_to_run = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(outer_cv_folds_to_run), StringComparison.OrdinalIgnoreCase)).as_int.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(inner_folds), StringComparison.OrdinalIgnoreCase)))
            {
                inner_folds = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(inner_folds), StringComparison.OrdinalIgnoreCase)).as_int.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(instance_array_index_start), StringComparison.OrdinalIgnoreCase)))
            {
                instance_array_index_start = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(instance_array_index_start), StringComparison.OrdinalIgnoreCase)).as_int.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(instance_array_index_end), StringComparison.OrdinalIgnoreCase)))
            {
                instance_array_index_end = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(instance_array_index_end), StringComparison.OrdinalIgnoreCase)).as_int.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(array_instances), StringComparison.OrdinalIgnoreCase)))
            {
                array_instances = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(array_instances), StringComparison.OrdinalIgnoreCase)).as_int.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(array_step), StringComparison.OrdinalIgnoreCase)))
            {
                array_step = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(array_step), StringComparison.OrdinalIgnoreCase)).as_int.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(array_start), StringComparison.OrdinalIgnoreCase)))
            {
                array_start = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(array_start), StringComparison.OrdinalIgnoreCase)).as_int.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(array_end), StringComparison.OrdinalIgnoreCase)))
            {
                array_end = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(array_end), StringComparison.OrdinalIgnoreCase)).as_int.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(setup_total_vcpus), StringComparison.OrdinalIgnoreCase)))
            {
                setup_total_vcpus = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(setup_total_vcpus), StringComparison.OrdinalIgnoreCase)).as_int.Value;
            }

            if (this.args.Any(a => string.Equals(a.key, nameof(setup_instance_vcpus), StringComparison.OrdinalIgnoreCase)))
            {
                setup_instance_vcpus = this.args.FirstOrDefault(a => string.Equals(a.key, nameof(setup_instance_vcpus), StringComparison.OrdinalIgnoreCase)).as_int.Value;
            }

        }
    }
}
