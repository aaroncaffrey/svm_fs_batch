using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace svm_fs_batch
{
    internal static class settings
    {
        internal static readonly bool is_win = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

        internal static readonly string user_home = is_win ? @"C:\home\k1040015" : @"/home/k1040015";
        internal static readonly string svm_fs_batch_home = is_win ? $@"C:\mmfs1\data\scratch\k1040015\{nameof(svm_fs_batch)}" : $@"/mmfs1/data/scratch/k1040015/{nameof(svm_fs_batch)}";
        internal static readonly string results_root_folder = is_win ? $@"{svm_fs_batch_home}\results\" : $@"{svm_fs_batch_home}/results/";
        internal static readonly string libsvm_predict_runtime = is_win ? $@"C:\libsvm\windows\svm-predict.exe" : $@"{user_home}/libsvm/svm-predict";
        internal static readonly string libsvm_train_runtime = is_win ? $@"C:\libsvm\windows\svm-train.exe" : $@"{user_home}/libsvm/svm-train";

        internal static string dataset_dir = is_win ? @"E:\caddy\input\" : $@"{user_home}/dataset/";


        internal static int negative_class_id = -1;
        internal static int positive_class_id = +1;
        internal static string negative_class_name = "standard_coil";
        internal static string positive_class_name = "dimorphic_coil";

        internal static List<(int class_id, string class_name)> class_names = new List<(int class_id, string class_name)>()
        {
            (negative_class_id, negative_class_name), 
            (positive_class_id, positive_class_name)
        };
    }
}
