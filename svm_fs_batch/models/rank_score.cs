namespace svm_fs_batch
{
    internal class rank_score
    {
        internal static readonly rank_score empty = new rank_score();

        internal int    iteration_index;
        internal int    group_array_index;

        internal double fs_score;
        internal double fs_score_percentile;
        
        internal int    fs_rank_index;
        internal double fs_rank_index_percentile;


        internal static readonly string[] csv_header_values_array = new string[]
        {
            "rs_"+nameof(iteration_index),
            "rs_"+nameof(group_array_index),
            "rs_"+nameof(fs_score),
            "rs_"+nameof(fs_score_percentile),
            "rs_"+nameof(fs_rank_index),
            "rs_"+nameof(fs_rank_index_percentile),
        };

        internal string[] csv_values_array()
        {
            return new string[]
            {
                $@"{iteration_index}",
                $@"{group_array_index}",
                $@"{fs_score:G17}",
                $@"{fs_score_percentile:G17}",
                $@"{fs_rank_index}",
                $@"{fs_rank_index_percentile:G17}",
            };
        }
    }
}
