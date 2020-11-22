using System.Collections.Generic;
using System.Linq;

namespace svm_fs_batch
{
    internal class index_data
    {
        internal int unrolled_whole_index = -1;
        internal int unrolled_partition_index = -1;
        internal int unrolled_instance_index = -1;
        internal int iteration_index = -1;
        internal string iteration_name;
        internal int group_array_index = -1;
        internal int total_groups = -1;
        internal bool calc_11p_thresholds = false;
        internal int repetitions = 5;
        internal int outer_cv_folds = 5;
        internal int outer_cv_folds_to_run = 5;
        internal List<(int class_id, double class_weight)> class_weights = new List<(int class_id, double class_weight)>();
        internal routines.libsvm_svm_type svm_type = routines.libsvm_svm_type.c_svc;
        internal routines.libsvm_kernel_type svm_kernel = routines.libsvm_kernel_type.rbf;
        internal scaling.scale_function scale_function = scaling.scale_function.rescale;
        internal int inner_cv_folds = 5;
        internal List<(int class_id, int class_size, List<(int repetitions_index, int outer_cv_index, List<int> indexes)> folds)> class_folds;
        internal List<(int class_id, int class_size, List<(int repetitions_index, int outer_cv_index, List<int> indexes)> folds)> down_sampled_training_class_folds;

        internal int total_whole_indexes = -1;
        internal int total_partition_indexes = -1;
        internal int total_instances = -1;


        internal string id_index_str()
        {
            var list = new List<(string name, string value, string value_max)>();

            list.Add((nameof(this.iteration_index), $@"{this.iteration_index}", $@""));
            list.Add((nameof(this.iteration_name), $@"{this.iteration_name}", $@""));
            list.Add((nameof(this.group_array_index), $@"{this.group_array_index}", this.total_groups > -1 ? $@"{this.total_groups}" : $@""));

            list.Add((nameof(this.unrolled_instance_index), $@"{this.unrolled_instance_index}", this.total_instances > -1 ? $@"{this.total_instances}" : $@""));

            list.Add((nameof(this.unrolled_whole_index), $@"{this.unrolled_whole_index}", this.total_whole_indexes > -1 ? $@"{this.total_whole_indexes}" : $@""));
            list.Add((nameof(this.unrolled_partition_index), $@"{this.unrolled_partition_index}", this.total_partition_indexes > -1 ? $@"{this.total_partition_indexes}" : $@""));


            return $@"[" + string.Join(", ", list.Select(a => $@"{a.name}={a.value}" + (!string.IsNullOrWhiteSpace(a.value_max) ? $@"/{a.value_max}" : $@"")).ToList()) + $@"]";
        }

        internal string id_ml_str()
        {
            var list = new List<(string name, string value, string value_max)>();

            list.Add((nameof(this.svm_type), $@"{this.svm_type}", $@""));
            list.Add((nameof(this.svm_kernel), $@"{this.svm_kernel}", $@""));
            list.Add((nameof(this.scale_function), $@"{this.scale_function}", $@""));
            list.Add((nameof(this.class_weights), $@"{(this.class_weights != null ? string.Join($@"; ", class_weights.Select(a => $@"w{(a.class_id>0? $@"+": $@"")} {a.class_weight}").ToList()) : "")}", ""));
            list.Add((nameof(this.calc_11p_thresholds), $@"{this.calc_11p_thresholds}", ""));

            return $@"[" + string.Join(", ", list.Select(a => $@"{a.name}={a.value}" + (!string.IsNullOrWhiteSpace(a.value_max) ? $@"/{a.value_max}" : "")).ToList()) + $@"]";
        }

        internal string id_fold_str()
        {
            var list = new List<(string name, string value, string value_max)>();

            list.Add((nameof(this.repetitions), $@"{this.repetitions}", ""));
            list.Add((nameof(this.outer_cv_folds), $@"{this.outer_cv_folds}", ""));
            list.Add((nameof(this.outer_cv_folds_to_run), $@"{this.outer_cv_folds_to_run}", ""));
            list.Add((nameof(this.inner_cv_folds), $@"{this.inner_cv_folds}", ""));

            return $@"[" + string.Join(", ", list.Select(a => $@"{a.name}={a.value}" + (!string.IsNullOrWhiteSpace(a.value_max) ? $@"/{a.value_max}" : "")).ToList()) + $@"]";
        }

        public index_data()
        {

        }

        public index_data(index_data index_data)
        {
            unrolled_whole_index = index_data.unrolled_whole_index;
            unrolled_partition_index = index_data.unrolled_partition_index;
            unrolled_instance_index = index_data.unrolled_instance_index;
            iteration_index = index_data.iteration_index;
            iteration_name = index_data.iteration_name;

            group_array_index = index_data.group_array_index;
            total_groups = index_data.total_groups;
            calc_11p_thresholds = index_data.calc_11p_thresholds;
            repetitions = index_data.repetitions;
            outer_cv_folds = index_data.outer_cv_folds;
            outer_cv_folds_to_run = index_data.outer_cv_folds_to_run;
            svm_type = index_data.svm_type;
            svm_kernel = index_data.svm_kernel;
            scale_function = index_data.scale_function;
            inner_cv_folds = index_data.inner_cv_folds;

            class_weights = index_data.class_weights.ToList();
            class_folds = index_data.class_folds.ToList();
            down_sampled_training_class_folds = index_data.down_sampled_training_class_folds.ToList();

            total_whole_indexes = index_data.total_whole_indexes;
            total_partition_indexes = index_data.total_partition_indexes;
            total_instances = index_data.total_instances;
        }
    }
}
