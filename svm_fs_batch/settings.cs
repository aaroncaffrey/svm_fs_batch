using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace svm_fs_batch
{
    internal static class settings
    {
        public const string module_name = nameof(settings);

        internal static readonly bool is_win = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

        internal static readonly string user_home =              !is_win ? $@"/home/k1040015"                                      : $@"C:\home\k1040015";
        internal static readonly string svm_fs_batch_home =      !is_win ? $@"/mmfs1/data/scratch/k1040015/{nameof(svm_fs_batch)}" : $@"C:\mmfs1\data\scratch\k1040015\{nameof(svm_fs_batch)}";
        internal static readonly string results_root_folder =    !is_win ? $@"{svm_fs_batch_home}/results/"                        : $@"{svm_fs_batch_home}\results\";
        internal static readonly string libsvm_predict_runtime = !is_win ? $@"{user_home}/libsvm/svm-predict"                      : $@"C:\libsvm\windows\svm-predict.exe";
        internal static readonly string libsvm_train_runtime =   !is_win ? $@"{user_home}/libsvm/svm-train"                        : $@"C:\libsvm\windows\svm-train.exe";
        internal static readonly string dataset_dir =            !is_win ? $@"{user_home}/dataset/"                                : $@"E:\dataset7\merged_files\";
        
        //internal static readonly string dataset_dir =          !is_win ? $@"{user_home}/dataset/"                                : $@"E:\caddy\input\";

        internal static readonly int negative_class_id = -1;
        internal static readonly int positive_class_id = +1;
        internal static readonly string negative_class_name = $@"standard_coil";
        internal static readonly string positive_class_name = $@"dimorphic_coil";

        internal static readonly (int class_id, string class_name)[] class_names = new (int class_id, string class_name)[]
        {
            (negative_class_id, negative_class_name), 
            (positive_class_id, positive_class_name)
        };
    }
}
