using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace svm_fs_batch
{
    internal class init_dataset_ret
    {
        internal static readonly string user_home = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\home\k1040015" : @"/home/k1040015";
        internal static readonly string svm_fs_batch_home = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? $@"C:\mmfs1\data\scratch\k1040015\{nameof(svm_fs_batch)}" : $@"/mmfs1/data/scratch/k1040015/{nameof(svm_fs_batch)}";
        internal static readonly string results_root_folder = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? $@"{svm_fs_batch_home}\results\" : $@"{svm_fs_batch_home}/results/";
        internal static readonly string libsvm_predict_runtime = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? $@"C:\libsvm\windows\svm-predict.exe" : $@"{user_home}/libsvm/svm-predict";
        internal static readonly string libsvm_train_runtime = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? $@"C:\libsvm\windows\svm-train.exe" : $@"{user_home}/libsvm/svm-train";

        internal static string dataset_dir = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"E:\caddy\input\" : $@"{user_home}/dataset/";


        internal static int negative_class_id = -1;
        internal static int positive_class_id = +1;
        internal static List<(int class_id, string class_name)> class_names = new List<(int class_id, string class_name)>() { (negative_class_id, "standard_coil"), (positive_class_id, "dimorphic_coil") };

        internal bool group_related_columns = true;


        internal bool required_default;
        internal List<(bool required, string alphabet, string dimension, string category, string source, string @group, string member, string perspective)> required_matches;
        internal dataset dataset;
        internal List<int> class_ids;
        internal List<(int class_id, int class_size)> class_sizes;
        internal List<(int class_id, int size, List<(int repetitions_index, int outer_cv_index, List<int> indexes)> folds)> class_folds;
        internal List<(int class_id, int size, List<(int repetitions_index, int outer_cv_index, List<int> indexes)> folds)> downsampled_training_class_folds;
        internal List<(int class_id, List<(int class_id, int example_id, int class_example_id, List<(string comment_header, string comment_value)> comment_columns, List<(int fid, double fv)> feature_data)> examples)> dataset_instance_list_grouped;
        internal List<(int index, (string alphabet, string dimension, string category, string source, string @group, string member, string perspective) key, List<(int fid, string alphabet, string dimension, string category, string source, string @group, string member, string perspective, int alphabet_id, int dimension_id, int category_id, int source_id, int group_id, int member_id, int perspective_id)> list, List<int> columns)> groups;
        internal List<(string alphabet, string dimension, string category, string source, string @group, string member, string perspective)> group_keys;
        internal List<(string alphabet, string dimension, string category, string source, string @group, string member, string perspective)> group_keys_distinct;

        internal init_dataset_ret()
        {

        }

        internal init_dataset_ret(init_dataset_ret init_dataset_ret)
        {
            this.group_related_columns = init_dataset_ret.group_related_columns;
            this.required_default = init_dataset_ret.required_default;
            this.required_matches = init_dataset_ret.required_matches;
            this.dataset = init_dataset_ret.dataset;
            this.class_ids = init_dataset_ret.class_ids;
            this.class_sizes = init_dataset_ret.class_sizes;
            this.class_folds = init_dataset_ret.class_folds;
            this.downsampled_training_class_folds = init_dataset_ret.downsampled_training_class_folds;
            this.dataset_instance_list_grouped = init_dataset_ret.dataset_instance_list_grouped;
            this.groups = init_dataset_ret.groups;
            this.group_keys = init_dataset_ret.group_keys;
            this.group_keys_distinct = init_dataset_ret.group_keys_distinct;
        }

        internal void set_folds(int repetitions, int outer_cv_folds, int outer_cv_folds_to_run = 0, int fold_size_limit = 0)
        {
            class_folds = class_sizes.Select(a => (class_id: a.class_id, size: a.class_size, folds: routines.folds(a.class_size, repetitions, outer_cv_folds, outer_cv_folds_to_run, fold_size_limit))).ToList();

            downsampled_training_class_folds = class_folds.Select(a => (class_id: a.class_id, size: a.size, folds: a.folds.Select(b =>
            {
                var min_num_items_in_fold =
                    this.class_folds.Min(c => c.folds.Where(e => e.repetitions_index == b.repetitions_index && e.outer_cv_index == b.outer_cv_index)
                        .Min(e => e.indexes.Count));

                return (repetitions_index: b.repetitions_index, outer_cv_index: b.outer_cv_index, indexes: b.indexes.Take(min_num_items_in_fold).ToList());
            })
                    .ToList()))
                .ToList();
        }


        internal static init_dataset_ret init_dataset(bool group_related_columns = true)//init_dataset_ret ida)
        {
            //var module_name = nameof(Program);
            //var method_name = nameof(init_dataset);

            var required_default = false;
            var required_matches = new List<(bool required, string alphabet, string dimension, string category, string source, string group, string member, string perspective)>();
            //required_matches.Add((required: true, alphabet: null, dimension: null, category: null, source: null, group: null, member: null, perspective: null));

            // file tags: 2i, 2n, 2p, 3i, 3n, 3p (2d, 3d, interface, neighbourhood, protein)

            var dataset = dataset_loader.read_binary_dataset(init_dataset_ret.dataset_dir,
                "2i",
                init_dataset_ret.negative_class_id,
                init_dataset_ret.positive_class_id,
                init_dataset_ret.class_names,
                //use_parallel: true,
                perform_integrity_checks: false,
                //fix_double: false,
                required_default,
                required_matches);

            var class_ids = dataset.dataset_instance_list.Select(a => a.class_id).Distinct().ToList();

            var class_sizes = class_ids.Select(a => (class_id: a, class_size: dataset.dataset_instance_list.Count(b => b.class_id == a))).ToList();


            //var class_folds = class_sizes.Select(a => (class_id: a.class_id, size: a.class_size, folds: routines.folds(a.class_size, unrolled_index.outer_cv_folds, unrolled_index.repetitions))).ToList();
            //
            //var downsampled_training_class_folds = class_folds.Select(a => (class_id: a.class_id, size: a.size, folds: a.folds.Select(b =>
            //        {
            //            var min_num_items_in_fold = class_folds.Min(c => c.folds.Where(e => e.repetitions_index == b.repetitions_index && e.outer_cv_index == b.outer_cv_index).Min(e => e.indexes.Count));
            //
            //            return (repetitions_index: b.repetitions_index, outer_cv_index: b.outer_cv_index, indexes: b.indexes.Take(min_num_items_in_fold).ToList());
            //        })
            //        .ToList()))
            //    .ToList();

            var dataset_instance_list_grouped = dataset.dataset_instance_list.GroupBy(a => a.class_id).Select(a => (class_id: a.Key, examples: a.ToList())).ToList();

            var groups = group_related_columns ? dataset.dataset_headers.GroupBy(a => (a.alphabet, a.dimension, a.category, a.source, a.@group, member: "", perspective: "")).Skip(1).Select((a, i) => (index: i, key: a.Key, list: a.ToList(), columns: a.Select(b => b.fid).ToList())).ToList() : dataset.dataset_headers.GroupBy(a => a.fid).Skip(1).Select((a, i) => (index: i, key: (a.First().alphabet, a.First().dimension, a.First().category, a.First().source, a.First().@group, a.First().member, a.First().perspective), list: a.ToList(), columns: a.Select(b => b.fid).ToList())).ToList();

            var group_keys = groups.Select(a => a.key).ToList();
            var group_keys_distinct = group_keys.Distinct().ToList();

            if (group_keys.Count != group_keys_distinct.Count) throw new Exception();

            return new init_dataset_ret()
            {
                group_related_columns = group_related_columns,
                required_default = required_default,
                required_matches = required_matches,
                dataset = dataset,
                class_ids = class_ids,
                class_sizes = class_sizes,
                class_folds = null,
                downsampled_training_class_folds = null,
                dataset_instance_list_grouped = dataset_instance_list_grouped,
                groups = groups,
                group_keys = group_keys,
                group_keys_distinct = group_keys_distinct,
            };
        }
    }
}
