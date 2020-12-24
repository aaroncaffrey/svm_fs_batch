using svm_fs_batch.models;

namespace svm_fs_batch
{
    internal class unrolled_indexes_parameters
    {
        public const string module_name = nameof(unrolled_indexes_parameters);

        internal bool calc_11p_thresholds = false;

        internal routines.libsvm_svm_type[] svm_types = new routines.libsvm_svm_type[] { routines.libsvm_svm_type.c_svc };
        internal scaling.scale_function[] scales = new scaling.scale_function[] { scaling.scale_function.rescale };
        internal routines.libsvm_kernel_type[] kernels = new routines.libsvm_kernel_type[] { routines.libsvm_kernel_type.rbf };

        internal (int class_id, double class_weight)[][] class_weight_sets = null;

        //internal int group_series_start = 0;
        //internal int group_series_end = -1;
        //internal int[] group_series;
        internal group_series_index[] group_series;

        // the following variables can be used for generating different series... to test bias of a particular set of numbers... e.g. do 5/5/5 and 10/10/10 return similar results?

        // number of times to run tests (repeats) (default: 1 for single repetition, or 5 to match default fold number [0 would be none] ... 5 to 5 at step 1)
        internal int r_cv_series_start = 5;
        internal int r_cv_series_end = 5;
        internal int r_cv_series_step = 1;
        internal int[] r_cv_series;

        // number of outer-cross-validations (default: 5 ... 5 to 5 at step 1)
        internal int o_cv_series_start = 5;
        internal int o_cv_series_end = 5;
        internal int o_cv_series_step = 1;
        internal int[] o_cv_series;

        // number of inner-cross-validations (default: 5 ... 5 to 5 at step 1)
        internal int i_cv_series_start = 5;
        internal int i_cv_series_end = 5;
        internal int i_cv_series_step = 1;
        internal int[] i_cv_series;
    }
}
