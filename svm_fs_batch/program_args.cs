using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace svm_fs_batch
{
    internal class program_args
    {
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

        internal List<(string key, int int_value, string str_value)> args = new List<(string key, int int_value, string str_value)>();



        public program_args()
        {

        }

        public program_args(string[] args)
        {
            var arg_list = new string[]
            {
                    nameof(folds), nameof(repetitions), nameof(outer_cv_folds), nameof(outer_cv_folds_to_run), nameof(inner_folds), nameof(setup), nameof(run_local), nameof(experiment_name), nameof(job_id), nameof(job_name),
                    nameof(instance_array_index_start), nameof(instance_array_index_end), nameof(array_instances), nameof(array_step), nameof(array_start), nameof(array_end), nameof(setup_total_vcpus), nameof(setup_instance_vcpus),
            };

            var args_given = args.AsParallel().AsOrdered().Where(a => a.StartsWith('-')).Select(a => a[1..]).ToList();

            if (args_given.Any(a => !arg_list.Contains(a))) throw new Exception();

            if (args_given.Any(a => args_given.Count(b => a == b) > 1)) throw new Exception();

            //var arg_list_indexes = arg_list.Select(name => args.ToList().FindIndex(arg => string.Equals(arg, $"-{name}", StringComparison.InvariantCultureIgnoreCase))).ToList();
            var arg_list_indexes = arg_list.AsParallel().AsOrdered().Select(name => 
                (
                    arg_name: name, 
                    arg_index: args.ToList().FindIndex(arg => string.Equals(arg, $"-{name}", StringComparison.InvariantCultureIgnoreCase))))
                .Where(a => a.arg_index != -1).OrderBy(a => a.arg_index).ToList();


            for (var i = 0; i < arg_list_indexes.Count; i++)
            {
                var arg = arg_list_indexes[i];


                var arg_given = arg.arg_index > -1;

                var next_arg_index = arg_list_indexes.FindIndex(a => a.arg_index > arg.arg_index);
                var value_str = arg_given ? string.Join(" ", args.Where((a, k) => k > arg.arg_index && (next_arg_index == -1 || k < arg_list_indexes[next_arg_index].arg_index)).ToList()) : "";
                var value_given = !string.IsNullOrWhiteSpace(value_str);

                if ((arg_given && !value_given) || (!arg_given && value_given)) throw new Exception();

                if (arg_given && value_given)
                {
                    var value_int = int.TryParse(value_str, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_value_int) ? out_value_int : -1;
                    //var value_double = double.TryParse(value_str, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var out_value_double) ? out_value_double : -1;
                    //var value_bool = bool.TryParse(value_str, out var out_value_bool) ? out_value_bool : false;

                    if (this.args.Any(a => a.key == arg.arg_name)) throw new Exception();

                    this.args.Add((arg.arg_name, value_int, value_str));

                    switch (arg.arg_name)
                    {
                        case nameof(experiment_name):
                            experiment_name = value_str;
                            break;
                        case nameof(job_id):
                            job_id = value_str;
                            break;
                        case nameof(job_name):
                            job_name = value_str;
                            break;
                        case nameof(run_local):
                            run_local = value_int == 1;
                            break;
                        case nameof(setup):
                            setup = value_int == 1;
                            break;
                        case nameof(folds):
                            folds = value_int;
                            inner_folds = value_int;
                            outer_cv_folds = value_int;
                            outer_cv_folds_to_run = value_int;
                            repetitions = value_int;
                            break;
                        case nameof(repetitions):
                            repetitions = value_int;
                            break;
                        case nameof(outer_cv_folds):
                            outer_cv_folds = value_int;
                            break;
                        case nameof(outer_cv_folds_to_run):
                            outer_cv_folds_to_run = value_int;
                            break;
                        case nameof(inner_folds):
                            inner_folds = value_int;
                            break;
                        case nameof(instance_array_index_start):
                            instance_array_index_start = value_int;
                            break;
                        case nameof(instance_array_index_end):
                            instance_array_index_end = value_int;
                            break;
                        case nameof(array_instances):
                            array_instances = value_int;
                            break;
                        case nameof(array_step):
                            array_step = value_int;
                            break;
                        case nameof(array_start):
                            array_start = value_int;
                            break;
                        case nameof(array_end):
                            array_end = value_int;
                            break;
                        case nameof(setup_total_vcpus):
                            setup_total_vcpus = value_int;
                            break;
                        case nameof(setup_instance_vcpus):
                            setup_instance_vcpus = value_int;
                            break;
                        default: break;
                    }
                }
            }
        }
    }
}
