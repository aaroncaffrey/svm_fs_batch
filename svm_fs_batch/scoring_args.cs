namespace svm_fs_batch
{
    internal class scoring_args
    {
        internal static int scoring_class_id = +1;

        internal static string[] scoring_metrics = new string[]
        {
            nameof(metrics_box.F1S),
            nameof(metrics_box.MCC),
            nameof(metrics_box.API_All)
        };
    }
}
