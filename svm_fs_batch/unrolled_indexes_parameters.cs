using System.Collections.Generic;

namespace svm_fs_batch
{
    internal class unrolled_indexes_parameters
    {
        public const string module_name = nameof(unrolled_indexes_parameters);


        internal bool calc_11p_thresholds = false;

        internal List<routines.libsvm_svm_type> svm_types = new List<routines.libsvm_svm_type>() { routines.libsvm_svm_type.c_svc };
        internal List<scaling.scale_function> scales = new List<scaling.scale_function>() { scaling.scale_function.rescale };
        internal List<routines.libsvm_kernel_type> kernels = new List<routines.libsvm_kernel_type>() { routines.libsvm_kernel_type.rbf };

        internal List<List<(int class_id, double class_weight)>> class_weight_sets = new List<List<(int class_id, double class_weight)>>();

        internal int group_start = 0;
        internal int group_end = -1;


        // the following variables are for generating different series... to test bias of a particular set of numbers... e.g. do 5/5/5 and 10/10/10 return similar results?

        // number of times to run tests (repeats) (default: 1 [0 would be none] ... 1 to 1 at step 1)
        internal int r_cv_start = 1;
        internal int r_cv_end = 1;
        internal int r_cv_step = 1;

        // number of outer-cross-validations (default: 5 ... 5 to 5 at step 1)
        internal int o_cv_start = 5;
        internal int o_cv_end = 5;
        internal int o_cv_step = 1;

        // number of inner-cross-validations (default: 5 ... 5 to 5 at step 1)
        internal int i_cv_start = 5;
        internal int i_cv_end = 5;
        internal int i_cv_step = 1;
    }
}
