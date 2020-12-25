using System;
using System.Globalization;
using System.Linq;

namespace svm_fs_batch
{
    internal class index_data
    {
        public const string module_name = nameof(index_data);
        internal static readonly index_data empty = new index_data();

        internal dataset_group_key group_key;
        internal int group_array_index = -1;
        internal int total_groups = -1;
        internal string group_folder;

        //internal bool is_job_completed;
        internal program.direction selection_direction;
        internal string experiment_name;
        internal int unrolled_whole_index = -1;
        internal int unrolled_partition_index = -1;
        internal int unrolled_instance_id = -1;
        internal int iteration_index = -1;
        
        internal bool calc_11p_thresholds = false;
        internal int repetitions = 5;
        internal int outer_cv_folds = 5;
        internal int outer_cv_folds_to_run = 1;
        internal int inner_cv_folds = 5;

        internal routines.libsvm_svm_type svm_type = routines.libsvm_svm_type.c_svc;
        internal routines.libsvm_kernel_type svm_kernel = routines.libsvm_kernel_type.rbf;
        internal scaling.scale_function scale_function = scaling.scale_function.rescale;

        internal int total_whole_indexes = -1;
        internal int total_partition_indexes = -1;
        internal int total_instances = -1;

        internal int num_groups;
        internal int num_columns;
        internal int[] group_array_indexes;
        internal int[] column_array_indexes;
        internal (int class_id, double class_weight)[] class_weights = null;
        internal (int class_id, int class_size, (int repetitions_index, int outer_cv_index, int[] class_sample_indexes)[] folds)[] class_folds;
        internal (int class_id, int class_size, (int repetitions_index, int outer_cv_index, int[] class_sample_indexes)[] folds)[] down_sampled_training_class_folds;


        public static readonly string[] csv_header_values_array =
            dataset_group_key.csv_header_values_array.Select(a => $@"gk_{a}").ToArray()
                .Concat(
            new string[]
           {
               nameof(group_array_index),
               nameof(total_groups),
               nameof(group_folder),

               //nameof(is_job_completed),
               nameof(selection_direction),
               nameof(experiment_name),
               nameof(unrolled_whole_index),
               nameof(unrolled_partition_index),
               nameof(unrolled_instance_id),
               nameof(iteration_index),
               
               
               nameof(calc_11p_thresholds),
               nameof(repetitions),
               nameof(outer_cv_folds),
               nameof(outer_cv_folds_to_run),
               nameof(inner_cv_folds),
               nameof(svm_type),
               nameof(svm_kernel),
               nameof(scale_function),
               nameof(total_whole_indexes),
               nameof(total_partition_indexes),
               nameof(total_instances),
               nameof(num_groups),
               nameof(num_columns),
               nameof(group_array_indexes),
               nameof(column_array_indexes),
               nameof(class_weights),
               nameof(class_folds),
               nameof(down_sampled_training_class_folds),
           })
                .ToArray();

        public static readonly string csv_header_string = string.Join(",", csv_header_values_array);


        public string[] csv_values_array()
        {
            var x1 = group_key?.csv_values_array() ?? dataset_group_key.empty.csv_values_array();

            var x2 = new string[]
            {
                $@"{group_array_index          }",
                $@"{total_groups               }",
                $@"{group_folder               }",

                //$@"{(is_job_completed?1:0)     }"     ,
                $@"{selection_direction        }",
                $@"{experiment_name      }",
                $@"{unrolled_whole_index       }",
                $@"{unrolled_partition_index   }",
                $@"{unrolled_instance_id       }",
                $@"{iteration_index            }",
                
                
                $@"{(calc_11p_thresholds?1:0)  }",
                $@"{repetitions                }",
                $@"{outer_cv_folds             }",
                $@"{outer_cv_folds_to_run      }",
                $@"{inner_cv_folds             }",
                $@"{svm_type                   }",
                $@"{svm_kernel                 }",
                $@"{scale_function             }",
                $@"{total_whole_indexes        }",
                $@"{total_partition_indexes    }",
                $@"{total_instances            }",
                $@"{num_groups                 }",
                $@"{num_columns                }",
                $@"{string.Join(";", group_array_indexes ?? Array.Empty<int>())}",
                $@"{string.Join(";", column_array_indexes ?? Array.Empty<int>())}",
                $@"{string.Join(";",class_weights?.Select(a=> $"{a.class_id}:{a.class_weight:G17}").ToArray() ?? Array.Empty<string>())}",
                $@"{string.Join(";",class_folds?.Select(a=>string.Join(":", $@"{a.class_id}", $@"{a.class_size}", $@"{string.Join("|",a.folds?.Select(b=> string.Join("~", $@"{b.repetitions_index}", $@"{b.outer_cv_index}", $@"{string.Join("/", b.class_sample_indexes ?? Array.Empty<int>())}")).ToArray() ?? Array.Empty<string>())}")).ToArray()?? Array.Empty<string>())}",
                $@"{string.Join(";",down_sampled_training_class_folds?.Select(a=>string.Join(":", $@"{a.class_id}", $@"{a.class_size}", $@"{string.Join("|",a.folds?.Select(b=> string.Join("~", $@"{b.repetitions_index}", $@"{b.outer_cv_index}", $@"{string.Join("/", b.class_sample_indexes ?? Array.Empty<int>())}")).ToArray() ?? Array.Empty<string>())}")).ToArray() ?? Array.Empty<string>())}",
            };

            var x3 = x1.Concat(x2).Select(a => a == null ? "" : a.Replace(',', ';')).ToArray();

            return x3;
        }

        public index_data(string csv_line, int column_offset = 0) : this(csv_line.Split(','), column_offset)
        {

        }

        public index_data(string[] csv_line, int column_offset = 0)
        {
            set_values(csv_line
                .Skip(column_offset)
                .AsParallel()
                .AsOrdered()
                //.WithCancellation(cts.Token)
                .Select(as_str =>
                {
                    var as_double = double.TryParse(as_str, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var out_double) ? out_double : (double?)null;
                    var as_int = int.TryParse(as_str, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_int) ? out_int : (int?)null;
                    var as_bool = as_int == 1 && as_double == 1 ? (bool?)true : (as_int == 0 && as_double == 0 ? (bool?)false : (bool?)null);
                    if (as_bool == null && bool.TryParse(as_str, out var out_bool)) as_bool = (bool?)out_bool;

                    return (as_str, as_int, as_double, as_bool);
                })
                .ToArray());
        }

        internal index_data((string as_str, int? as_int, double? as_double, bool? as_bool)[] x_type, int column_offset = 0)
        {
            set_values(x_type, column_offset);
        }

        internal void set_values((string as_str, int? as_int, double? as_double, bool? as_bool)[] x_type, int column_offset = 0)
        {
            var k = column_offset;
            
            group_key = new dataset_group_key(x_type[k++].as_str, x_type[k++].as_str, x_type[k++].as_str, x_type[k++].as_str, x_type[k++].as_str, x_type[k++].as_str, x_type[k++].as_str, x_type[k++].as_str, x_type[k++].as_str, int.Parse(x_type[k++].as_str, NumberStyles.Integer, NumberFormatInfo.InvariantInfo));
            group_array_index = x_type[k++].as_int ?? -1;
            total_groups = x_type[k++].as_int ?? -1;
            group_folder = x_type[k++].as_str;

            //is_job_completed = x_type[k++].as_bool ?? default;
            selection_direction = Enum.Parse<program.direction>(x_type[k++].as_str, true);
            experiment_name = x_type[k++].as_str;
            unrolled_whole_index = x_type[k++].as_int ?? -1;
            unrolled_partition_index = x_type[k++].as_int ?? -1;
            unrolled_instance_id = x_type[k++].as_int ?? -1;
            iteration_index = x_type[k++].as_int ?? -1;
            
            calc_11p_thresholds = x_type[k++].as_bool ?? default;
            repetitions = x_type[k++].as_int ?? -1;
            outer_cv_folds = x_type[k++].as_int ?? -1;
            outer_cv_folds_to_run = x_type[k++].as_int ?? -1;
            inner_cv_folds = x_type[k++].as_int ?? -1;

            svm_type = Enum.Parse<routines.libsvm_svm_type>(x_type[k++].as_str, true);
            svm_kernel = Enum.Parse<routines.libsvm_kernel_type>(x_type[k++].as_str, true);
            scale_function = Enum.Parse<scaling.scale_function>(x_type[k++].as_str, true);
            
            total_whole_indexes = x_type[k++].as_int ?? -1;
            total_partition_indexes = x_type[k++].as_int ?? -1;
            total_instances = x_type[k++].as_int ?? -1;

            num_groups = x_type[k++].as_int ?? -1;
            num_columns = x_type[k++].as_int ?? -1;
            group_array_indexes = x_type[k++].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();
            column_array_indexes = x_type[k++].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();

            class_weights = x_type[k++].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => { var b = a.Split(':', StringSplitOptions.RemoveEmptyEntries); return (int.Parse(b[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo), double.Parse(b[1], NumberStyles.Float, NumberFormatInfo.InvariantInfo)); }).ToArray();

            //  ;:|~/
            class_folds = x_type[k++].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a =>
            {
                var b = a.Split(':', StringSplitOptions.RemoveEmptyEntries);
                var class_id = int.Parse(b[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                var class_size = int.Parse(b[1], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                var folds = b[2].Split('|', StringSplitOptions.RemoveEmptyEntries).Select(d =>
                    {
                        var e = d.Split('~', StringSplitOptions.RemoveEmptyEntries);
                        var repetitions_index = int.Parse(e[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                        var outer_cv_index = int.Parse(e[1], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                        var class_sample_indexes = e[2].Split('/', StringSplitOptions.RemoveEmptyEntries).Select(f => int.Parse(f, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();

                        return (repetitions_index, outer_cv_index, class_sample_indexes);
                    })
                    .ToArray();
                return (class_id, class_size, folds);
            }).ToArray();

            //  ;:|~/
            down_sampled_training_class_folds = x_type[k++].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a =>
            {
                var b = a.Split(':', StringSplitOptions.RemoveEmptyEntries);
                var class_id = int.Parse(b[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                var class_size = int.Parse(b[1], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                var folds = b[2].Split('|', StringSplitOptions.RemoveEmptyEntries).Select(d =>
                    {
                        var e = d.Split('~', StringSplitOptions.RemoveEmptyEntries);
                        var repetitions_index = int.Parse(e[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                        var outer_cv_index = int.Parse(e[1], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                        var class_sample_indexes = e[2].Split('/', StringSplitOptions.RemoveEmptyEntries).Select(f => int.Parse(f, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();

                        return (repetitions_index, outer_cv_index, class_sample_indexes);
                    })
                    .ToArray();
                return (class_id, class_size, folds);
            }).ToArray();
        }

        public string csv_values_string()
        {
            return string.Join(",", csv_values_array());
        }

        internal string id_index_str()
        {
            var list = new (string name, string value, string value_max)[]
            {
                (nameof(this.experiment_name), $@"{this.experiment_name}", $@""),
                (nameof(this.iteration_index), $@"{this.iteration_index}", $@""),
                //(nameof(this.iteration_name), $@"{this.iteration_name}", $@""),
                (nameof(this.group_array_index), $@"{this.group_array_index}", this.total_groups > -1 ? $@"{this.total_groups}" : $@""),
                (nameof(this.unrolled_instance_id), $@"{this.unrolled_instance_id}", this.total_instances > -1 ? $@"{this.total_instances}" : $@""),
                (nameof(this.unrolled_whole_index), $@"{this.unrolled_whole_index}", this.total_whole_indexes > -1 ? $@"{this.total_whole_indexes}" : $@""),
                (nameof(this.unrolled_partition_index), $@"{this.unrolled_partition_index}", this.total_partition_indexes > -1 ? $@"{this.total_partition_indexes}" : $@"")
            };

            return $@"[" + string.Join(", ", list.Select(a => $@"{a.name}={a.value}" + (!string.IsNullOrWhiteSpace(a.value_max) ? $@"/{a.value_max}" : $@"")).ToList()) + $@"]";
        }

        internal string id_ml_str()
        {
            var list = new (string name, string value, string value_max)[]
            {
                (nameof(this.svm_type), $@"{this.svm_type}", $@""),
                (nameof(this.svm_kernel), $@"{this.svm_kernel}", $@""),
                (nameof(this.scale_function), $@"{this.scale_function}", $@""),
                (nameof(this.class_weights), $@"{(this.class_weights != null ? string.Join($@"; ", class_weights.Select(a => $@"w{(a.class_id > 0 ? $@"+" : $@"")} {a.class_weight}").ToList()) : "")}", ""),
                (nameof(this.calc_11p_thresholds), $@"{this.calc_11p_thresholds}", "")
            };

            return $@"[" + string.Join(", ", list.Select(a => $@"{a.name}={a.value}" + (!string.IsNullOrWhiteSpace(a.value_max) ? $@"/{a.value_max}" : "")).ToList()) + $@"]";
        }

        internal string id_fold_str()
        {
            var list = new (string name, string value, string value_max)[]
            {
                (nameof(this.repetitions), $@"{this.repetitions}", ""),
                (nameof(this.outer_cv_folds), $@"{this.outer_cv_folds}", ""),
                (nameof(this.outer_cv_folds_to_run), $@"{this.outer_cv_folds_to_run}", ""),
                (nameof(this.inner_cv_folds), $@"{this.inner_cv_folds}", "")
            };

            return $@"[" + string.Join(", ", list.Select(a => $@"{a.name}={a.value}" + (!string.IsNullOrWhiteSpace(a.value_max) ? $@"/{a.value_max}" : "")).ToList()) + $@"]";
        }

        public index_data()
        {

        }



        public index_data(index_data index_data)
        {
            if (index_data == null) return;

            group_key = new dataset_group_key(index_data.group_key);
            group_array_index = index_data.group_array_index;
            total_groups = index_data.total_groups;
            group_folder = index_data.group_folder;

            //is_job_completed = index_data.is_job_completed;
            selection_direction = index_data.selection_direction;
            experiment_name = index_data.experiment_name;
            unrolled_whole_index = index_data.unrolled_whole_index;
            unrolled_partition_index = index_data.unrolled_partition_index;
            unrolled_instance_id = index_data.unrolled_instance_id;
            iteration_index = index_data.iteration_index;

            calc_11p_thresholds = index_data.calc_11p_thresholds;
            repetitions = index_data.repetitions;
            outer_cv_folds = index_data.outer_cv_folds;
            outer_cv_folds_to_run = index_data.outer_cv_folds_to_run;
            inner_cv_folds = index_data.inner_cv_folds;

            svm_type = index_data.svm_type;
            svm_kernel = index_data.svm_kernel;
            scale_function = index_data.scale_function;

            num_groups = index_data.num_groups;
            num_columns = index_data.num_columns;
            group_array_indexes = index_data.group_array_indexes.ToArray();
            column_array_indexes = index_data.column_array_indexes.ToArray();

            class_weights = index_data.class_weights?.ToArray();
            class_folds = index_data.class_folds?.Select(a => (a.class_id, a.class_size, a.folds?.Select(b => (b.repetitions_index, b.outer_cv_index, b.class_sample_indexes?.ToArray())).ToArray())).ToArray();
            down_sampled_training_class_folds = index_data.down_sampled_training_class_folds?.Select(a => (a.class_id, a.class_size, a.folds?.Select(b => (b.repetitions_index, b.outer_cv_index, b.class_sample_indexes?.ToArray())).ToArray())).ToArray();

            total_whole_indexes = index_data.total_whole_indexes;
            total_partition_indexes = index_data.total_partition_indexes;
            total_instances = index_data.total_instances;
        }
    }
}
