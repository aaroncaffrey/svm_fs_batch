using System.Collections.Generic;

namespace svm_fs_batch
{
    internal class confusion_matrix_data
    {
        internal string line = null;
        internal confusion_matrix cm = null;
        internal List<(string key, string value_str, int? value_int, double? value_double)> key_value_list = new List<(string key, string value_str, int? value_int, double? value_double)>();
        internal List<(string key, string value_str, int? value_int, double? value_double)> unknown_key_value_list = new List<(string key, string value_str, int? value_int, double? value_double)>();
    }
}
