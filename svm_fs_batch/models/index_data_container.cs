namespace svm_fs_batch
{
    internal class index_data_container
    {
        public index_data_container()
        {
            
        }

        internal index_data[] indexes_whole;
        internal index_data[] indexes_partition;
        internal index_data[][] indexes_partitions;

        internal index_data[] indexes_loaded_whole;
        internal index_data[] indexes_loaded_partition;
        internal index_data[][] indexes_loaded_partitions;

        internal index_data[] indexes_missing_whole;
        internal index_data[] indexes_missing_partition;
        internal index_data[][] indexes_missing_partitions;
    }
}
