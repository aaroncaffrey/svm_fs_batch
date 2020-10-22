using System;
using System.Collections.Generic;
using System.Text;

namespace svm_fs_batch
{
    internal class scoring_args
    {
        internal static int scoring_class_id = +1;

        internal static string[] scoring_metrics = new string[]
        {
            nameof(perf.confusion_matrix.F1S),
            nameof(perf.confusion_matrix.MCC),
            nameof(perf.confusion_matrix.API_All)
        };

        //internal static string score1_metric = nameof(perf.confusion_matrix.F1S);
        //internal static string score2_metric = nameof(perf.confusion_matrix.MCC);
        //internal static string score3_metric = nameof(perf.confusion_matrix.API_All);

        //internal scoring_args()
        // {
        //
        // }
        //
        // internal scoring_args(scoring_args oca)
        // {
        //     this.scoring_class_id = unrolled_index.scoring_class_id;
        //     this.score1_metric = unrolled_index.score1_metric;
        //     this.score2_metric = unrolled_index.score2_metric;
        //     this.score3_metric = unrolled_index.score3_metric;
        // }
    }
}
