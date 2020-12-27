namespace svm_fs_batch.models
{
    internal class outer_cv_input
    {
        internal int repetitions_index;
        internal int outer_cv_index;
        internal string train_fn;
        internal string grid_fn;
        internal string model_fn;
        internal string test_fn;
        internal string predict_fn;
        internal string cm_fn1;
        internal string cm_fn2;
        internal string[] train_text;
        internal string[] test_text;
        internal (int class_id, int train_size)[] train_sizes;
        internal (int class_id, int test_size)[] test_sizes;
        internal (int class_id, int[] train_indexes)[] train_fold_indexes;
        internal (int class_id, int[] test_indexes)[] test_fold_indexes;
    }
}
