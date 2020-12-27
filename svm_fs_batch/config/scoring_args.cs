namespace svm_fs_batch
{
    internal class scoring_args
    {
        public const string module_name = nameof(scoring_args);

        //internal static int[] scoring_class_ids = new int[] { +1 };
        internal static readonly int scoring_class_id = +1;

        internal static readonly string[] scoring_metrics = new string[]
        {
            nameof(metrics_box.p_F1S),
            //nameof(metrics_box.p_MCC),
            //nameof(metrics_box.p_API_All)
        };
    }
}
