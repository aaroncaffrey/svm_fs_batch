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
        internal int iteration_index = -1;
        internal int group_array_index = -1;
        internal int total_groups = -1;
        internal program.direction selection_direction;
        internal string experiment_name;
        internal int repetitions = 5;
        internal int outer_cv_folds = 5;
        internal int outer_cv_folds_to_run = 1;
        internal int inner_cv_folds = 5;
        internal routines.libsvm_svm_type svm_type = routines.libsvm_svm_type.c_svc;
        internal routines.libsvm_kernel_type svm_kernel = routines.libsvm_kernel_type.rbf;
        internal scaling.scale_function scale_function = scaling.scale_function.rescale;
        internal bool calc_11p_thresholds = false;
        internal int num_groups;
        internal int num_columns;
        internal int[] group_array_indexes;
        internal int[] column_array_indexes;
        internal (int class_id, double class_weight)[] class_weights;
        internal (int class_id, int class_size, (int repetitions_index, int outer_cv_index, int[] class_sample_indexes)[] folds)[] class_folds;
        internal (int class_id, int class_size, (int repetitions_index, int outer_cv_index, int[] class_sample_indexes)[] folds)[] down_sampled_train_class_folds;


        internal string group_folder;
        internal int unrolled_whole_index = -1;
        internal int unrolled_partition_index = -1;
        internal int unrolled_instance_id = -1;
        internal int total_whole_indexes = -1;
        internal int total_partition_indexes = -1;
        internal int total_instances = -1;


        public static readonly string[] csv_header_values_array =
            dataset_group_key.csv_header_values_array.Select(a => "id_" + a).ToArray()
                .Concat(
            new string[]
           {
               "id_"+nameof(iteration_index                        ),
               "id_"+nameof(group_array_index                      ),
               "id_"+nameof(total_groups                           ),
               "id_"+nameof(selection_direction                    ),
               "id_"+nameof(experiment_name                        ),
               "id_"+nameof(repetitions                            ),
               "id_"+nameof(outer_cv_folds                         ),
               "id_"+nameof(outer_cv_folds_to_run                  ),
               "id_"+nameof(inner_cv_folds                         ),
               "id_"+nameof(svm_type                               ),
               "id_"+nameof(svm_kernel                             ),
               "id_"+nameof(scale_function                         ),
               "id_"+nameof(calc_11p_thresholds                    ),
               "id_"+nameof(num_groups                             ),
               "id_"+nameof(num_columns                            ),
               "id_"+nameof(group_array_indexes                    ),
               "id_"+nameof(column_array_indexes                   ),
               "id_"+nameof(class_weights                          ),
               "id_"+nameof(class_folds                            ),
               "id_"+nameof(down_sampled_train_class_folds      ),
           })
                .ToArray();

        public static readonly string csv_header_string = string.Join(",", csv_header_values_array);


        public string[] csv_values_array()
        {
            var x1 = (group_key?.csv_values_array() ?? dataset_group_key.empty.csv_values_array());

            var x2 = new string[]
            {
                $@"{iteration_index                  }",
                $@"{group_array_index                }",
                $@"{total_groups                     }",
                $@"{selection_direction              }",
                $@"{experiment_name                  }",
                $@"{repetitions                      }",
                $@"{outer_cv_folds                   }",
                $@"{outer_cv_folds_to_run            }",
                $@"{inner_cv_folds                   }",
                $@"{svm_type                         }",
                $@"{svm_kernel                       }",
                $@"{scale_function                   }",
                $@"{(calc_11p_thresholds?1:0)        }",
                $@"{num_groups                       }",
                $@"{num_columns                      }",
                $@"{string.Join(";", group_array_indexes ?? Array.Empty<int>())}",
                $@"{string.Join(";", column_array_indexes ?? Array.Empty<int>())}",
                $@"{string.Join(";",class_weights?.Select(a=> $"{a.class_id}:{a.class_weight:G17}").ToArray() ?? Array.Empty<string>())}",
                $@"{string.Join(";",class_folds?.Select(a=>string.Join(":", $@"{a.class_id}", $@"{a.class_size}", $@"{string.Join("|",a.folds?.Select(b=> string.Join("~", $@"{b.repetitions_index}", $@"{b.outer_cv_index}", $@"{string.Join("/", b.class_sample_indexes ?? Array.Empty<int>())}")).ToArray() ?? Array.Empty<string>())}")).ToArray()?? Array.Empty<string>())}",
                $@"{string.Join(";",down_sampled_train_class_folds?.Select(a=>string.Join(":", $@"{a.class_id}", $@"{a.class_size}", $@"{string.Join("|",a.folds?.Select(b=> string.Join("~", $@"{b.repetitions_index}", $@"{b.outer_cv_index}", $@"{string.Join("/", b.class_sample_indexes ?? Array.Empty<int>())}")).ToArray() ?? Array.Empty<string>())}")).ToArray() ?? Array.Empty<string>())}",
            };

            var x3 = x1.Concat(x2).Select(a => a == null ? "" : a.Replace(',', ';')).ToArray();

            return x3;
        }

        public index_data(string[] line_header, string csv_line, int column_offset = 0) : this(line_header, csv_line.Split(','), column_offset)
        {

        }

        public index_data(string[] line_header, string[] csv_line, int column_offset = 0)
        {
            //set_values(routines.x_types(null, csv_line.Skip(column_offset).ToArray(), false), 0);
            set_values(line_header, x_types.get_x_types(csv_line.Skip(column_offset).ToArray()), 0);
        }

        internal index_data(string[] line_header, x_types[] x_type, int column_offset = 0)
        {
            set_values(line_header, x_type, column_offset);
        }

        internal void set_values(string[] line_header, x_types[] x_type, int column_offset = 0)
        {
            var k = column_offset;

            var header_indexes = csv_header_values_array.Select((h, i) => (header: h, index: (line_header.Length > 0 ? Array.FindIndex(line_header, a => a.EndsWith(h)) : column_offset + i))).ToArray();

            int hi(string name)
            {
                return header_indexes.First(a => a.header.EndsWith(name, StringComparison.OrdinalIgnoreCase)).index;
            }

            // todo: lookup actual group key instance - not necessary, since the parent index_data will be looked up
            //[hi(nameof(___))].as_str, x_type[hi(nameof(___))].as_str, x_type[hi(nameof(___))].as_str, x_type[hi(nameof(___))].as_str, x_type[hi(nameof(___))].as_str, x_type[hi(nameof(___))].as_str, x_type[hi(nameof(___))].as_str, x_type[hi(nameof(___))].as_str, x_type[hi(nameof(___))].as_str, int.TryParse(x_type[hi(nameof(___))].as_str, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var tp_int1) ? tp_int1 : -1);

            group_key = new dataset_group_key(line_header, x_type, 0);

            if (group_key.value == default) group_key = null;

            iteration_index = x_type[hi(nameof(iteration_index))].as_int ?? -1;
            group_array_index = x_type[hi(nameof(group_array_index))].as_int ?? -1;
            total_groups = x_type[hi(nameof(total_groups))].as_int ?? -1;
            selection_direction = Enum.Parse<program.direction>(x_type[hi(nameof(selection_direction))].as_str, true);
            experiment_name = x_type[hi(nameof(experiment_name))].as_str;
            repetitions = x_type[hi(nameof(repetitions))].as_int ?? -1;
            outer_cv_folds = x_type[hi(nameof(outer_cv_folds))].as_int ?? -1;
            outer_cv_folds_to_run = x_type[hi(nameof(outer_cv_folds_to_run))].as_int ?? -1;
            inner_cv_folds = x_type[hi(nameof(inner_cv_folds))].as_int ?? -1;
            svm_type = Enum.Parse<routines.libsvm_svm_type>(x_type[hi(nameof(svm_type))].as_str, true);
            svm_kernel = Enum.Parse<routines.libsvm_kernel_type>(x_type[hi(nameof(svm_kernel))].as_str, true);
            scale_function = Enum.Parse<scaling.scale_function>(x_type[hi(nameof(scale_function))].as_str, true);
            calc_11p_thresholds = x_type[hi(nameof(calc_11p_thresholds))].as_bool ?? default;
            num_groups = x_type[hi(nameof(num_groups))].as_int ?? -1;
            num_columns = x_type[hi(nameof(num_columns))].as_int ?? -1;
            group_array_indexes = x_type[hi(nameof(group_array_indexes))].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();
            column_array_indexes = x_type[hi(nameof(column_array_indexes))].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();
            class_weights = x_type[hi(nameof(class_weights))].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => { var b = a.Split(':', StringSplitOptions.RemoveEmptyEntries); return (int.Parse(b[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo), double.Parse(b[1], NumberStyles.Float, NumberFormatInfo.InvariantInfo)); }).ToArray();

            //  ;:|~/
            class_folds = x_type[hi(nameof(class_folds))].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a =>
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
            down_sampled_train_class_folds = x_type[hi(nameof(down_sampled_train_class_folds))].as_str.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a =>
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

        internal class index_data_search_options
        {
            internal bool iteration_index = true;
            internal bool group_array_index = true;
            internal bool total_groups = true;
            internal bool selection_direction = true;
            internal bool calc_11p_thresholds = true;
            internal bool svm_type = true;
            internal bool svm_kernel = true;
            internal bool scale_function = true;
            internal bool repetitions = true;
            internal bool outer_cv_folds = true;
            internal bool outer_cv_folds_to_run = true;
            internal bool inner_cv_folds = true;
            internal bool group_key = true;
            internal bool experiment_name = true;
            internal bool num_groups = true;
            internal bool num_columns = true;
            internal bool group_array_indexes = true;
            internal bool column_array_indexes = true;
            internal bool class_weights = true;
        }

        internal static index_data find_first_reference(index_data[] list, index_data data1, index_data_search_options idso = null)
        {
            if (data1 == null) return null;

            // find proper index_data instance for this newly loaded confusion_matrix instance
            var id = list.FirstOrDefault(data2 => compare_reference_data(data1, data2, idso));

            return id;
        }

        internal static index_data find_last_reference(index_data[] list, index_data data1, index_data_search_options idso = null)
        {
            if (data1 == null) return null;

            // find proper index_data instance for this newly loaded confusion_matrix instance
            var id = list.LastOrDefault(data2 => compare_reference_data(data1, data2, idso));

            return id;
        }

        internal static bool compare_reference_data(index_data data1, index_data data2, index_data_search_options idso = null)
        {
            return ((idso != null && !idso.iteration_index) || data2.iteration_index == data1.iteration_index) &&
                   ((idso != null && !idso.group_array_index) || data2.group_array_index == data1.group_array_index) &&
                   ((idso != null && !idso.total_groups) || data2.total_groups == data1.total_groups) &&
                   ((idso != null && !idso.selection_direction) || data2.selection_direction == data1.selection_direction) &&
                   ((idso != null && !idso.calc_11p_thresholds) || data2.calc_11p_thresholds == data1.calc_11p_thresholds) &&
                   ((idso != null && !idso.svm_type) || data2.svm_type == data1.svm_type) &&
                   ((idso != null && !idso.svm_kernel) || data2.svm_kernel == data1.svm_kernel) &&
                   ((idso != null && !idso.scale_function) || data2.scale_function == data1.scale_function) &&
                   ((idso != null && !idso.repetitions) || data2.repetitions == data1.repetitions) &&
                   ((idso != null && !idso.outer_cv_folds) || data2.outer_cv_folds == data1.outer_cv_folds) &&
                   ((idso != null && !idso.outer_cv_folds_to_run) || data2.outer_cv_folds_to_run == data1.outer_cv_folds_to_run) &&
                   ((idso != null && !idso.inner_cv_folds) || data2.inner_cv_folds == data1.inner_cv_folds) &&
                   ((idso != null && !idso.group_key) || data2.group_key == data1.group_key) &&
                   ((idso != null && !idso.experiment_name) || data2.experiment_name == data1.experiment_name) &&
                   ((idso != null && !idso.num_groups) || data2.num_groups == data1.num_groups) &&
                   ((idso != null && !idso.num_columns) || data2.num_columns == data1.num_columns) &&
                   ((idso != null && !idso.group_array_indexes) || (((data2.group_array_indexes == null || data2.group_array_indexes.Length == 0) && (data1.group_array_indexes == null || data1.group_array_indexes.Length == 0)) || (data2.group_array_indexes ?? Array.Empty<int>()).SequenceEqual(data1.group_array_indexes))) &&
                   ((idso != null && !idso.column_array_indexes) || (((data2.column_array_indexes == null || data2.column_array_indexes.Length == 0) && (data1.column_array_indexes == null || data1.column_array_indexes.Length == 0)) || (data2.column_array_indexes ?? Array.Empty<int>()).SequenceEqual(data1.column_array_indexes))) &&
                   ((idso != null && !idso.class_weights) || (((data2.class_weights == null || data2.class_weights.Length == 0) && (data1.class_weights == null || data1.class_weights.Length == 0)) || (data2.class_weights ?? Array.Empty<(int class_id, double class_weight)>()).SequenceEqual(data1.class_weights)));
        }

        //internal static index_data find_best_match_reference(index_data[] list, index_data data)
        //{
        //    if (data == null) return null;
        //
        //    // find proper index_data instance for this newly loaded confusion_matrix instance
        //    var sums = list.Select(id2 =>
        //            (id2.iteration_index == data.iteration_index ? 3 : 0) +
        //            (id2.group_array_index == data.group_array_index ? 3 : 0) +
        //            (id2.total_groups == data.total_groups ? 1 : 0) +
        //            (id2.selection_direction == data.selection_direction ? 1 : 0) +
        //            (id2.calc_11p_thresholds == data.calc_11p_thresholds ? 1 : 0) +
        //            (id2.svm_type == data.svm_type ? 1 : 0) +
        //            (id2.svm_kernel == data.svm_kernel ? 1 : 0) +
        //            (id2.scale_function == data.scale_function ? 1 : 0) +
        //            (id2.repetitions == data.repetitions ? 1 : 0) +
        //            (id2.outer_cv_folds == data.outer_cv_folds ? 1 : 0) +
        //            (id2.outer_cv_folds_to_run == data.outer_cv_folds_to_run ? 1 : 0) +
        //            (id2.inner_cv_folds == data.inner_cv_folds ? 1 : 0) +
        //            (id2.group_key == data.group_key ? 1 : 0) +
        //            (id2.experiment_name == data.experiment_name ? 1 : 0) +
        //            (id2.num_groups == data.num_groups ? 1 : 0) +
        //            (id2.num_columns == data.num_columns ? 1 : 0) +
        //            ((((id2.group_array_indexes == null || id2.group_array_indexes.Length == 0) && (data.group_array_indexes == null || data.group_array_indexes.Length == 0)) || (id2.group_array_indexes ?? Array.Empty<int>()).SequenceEqual(data.group_array_indexes)) ? 1 : 0) +
        //            ((((id2.column_array_indexes == null || id2.column_array_indexes.Length == 0) && (data.column_array_indexes == null || data.column_array_indexes.Length == 0)) || (id2.column_array_indexes ?? Array.Empty<int>()).SequenceEqual(data.column_array_indexes)) ? 1 : 0) +
        //            ((((id2.class_weights == null || id2.class_weights.Length == 0) && (data.class_weights == null || data.class_weights.Length == 0)) || (id2.class_weights ?? Array.Empty<(int class_id, double class_weight)>()).SequenceEqual(data.class_weights)) ? 1 : 0)
        //        ).ToArray();
        //
        //    var max = sums.Max();
        //    var max_index_count = sums.Count(a => a == max);
        //    if (max_index_count > 1) throw new Exception();
        //    var max_index = Array.FindIndex(sums, a => a == max);
        //
        //    var id = list[max_index];
        //    return id;
        //}

        public index_data()
        {

        }



        //public index_data(index_data index_data)
        //{
        //    if (index_data == null) return;
        //
        //    if (index_data.group_key != null) group_key = new dataset_group_key(index_data.group_key);
        //    if (group_key.value == default) group_key = null;
        //
        //    group_array_index = index_data.group_array_index;
        //    total_groups = index_data.total_groups;
        //    group_folder = index_data.group_folder;
        //
        //    //is_job_completed = index_data.is_job_completed;
        //    selection_direction = index_data.selection_direction;
        //    experiment_name = index_data.experiment_name;
        //    unrolled_whole_index = index_data.unrolled_whole_index;
        //    unrolled_partition_index = index_data.unrolled_partition_index;
        //    unrolled_instance_id = index_data.unrolled_instance_id;
        //    iteration_index = index_data.iteration_index;
        //
        //    calc_11p_thresholds = index_data.calc_11p_thresholds;
        //    repetitions = index_data.repetitions;
        //    outer_cv_folds = index_data.outer_cv_folds;
        //    outer_cv_folds_to_run = index_data.outer_cv_folds_to_run;
        //    inner_cv_folds = index_data.inner_cv_folds;
        //
        //    svm_type = index_data.svm_type;
        //    svm_kernel = index_data.svm_kernel;
        //    scale_function = index_data.scale_function;
        //
        //    num_groups = index_data.num_groups;
        //    num_columns = index_data.num_columns;
        //    group_array_indexes = index_data.group_array_indexes.ToArray();
        //    column_array_indexes = index_data.column_array_indexes.ToArray();
        //
        //    class_weights = index_data.class_weights?.ToArray();
        //    class_folds = index_data.class_folds?.Select(a => (a.class_id, a.class_size, a.folds?.Select(b => (b.repetitions_index, b.outer_cv_index, b.class_sample_indexes?.ToArray())).ToArray())).ToArray();
        //    down_sampled_train_class_folds = index_data.down_sampled_train_class_folds?.Select(a => (a.class_id, a.class_size, a.folds?.Select(b => (b.repetitions_index, b.outer_cv_index, b.class_sample_indexes?.ToArray())).ToArray())).ToArray();
        //
        //    total_whole_indexes = index_data.total_whole_indexes;
        //    total_partition_indexes = index_data.total_partition_indexes;
        //    total_instances = index_data.total_instances;
        //}
    }
}
