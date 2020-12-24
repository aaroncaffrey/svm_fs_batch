namespace svm_fs_batch.models
{
    internal class group_series_index
    {
        internal int group_array_index;
        internal program.direction selection_direction;
        internal int[] group_indexes;
        internal int[] column_indexes;
        internal dataset_group_key group_key;
        internal string group_folder;

        internal bool is_group_index_valid;
        internal bool is_group_selected;
        internal bool is_group_only_selection;
        internal bool is_group_last_winner;
        internal bool is_group_base_group;
        internal bool is_group_blacklisted;
    }
}
