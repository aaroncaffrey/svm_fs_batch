namespace SvmFsBatch
{
    internal class IndexDataContainer
    {
        internal const string ModuleName = nameof(IndexDataContainer);

        //internal index_data[] indexes_partition;
        //internal index_data[][] indexes_partitions;

        internal IndexData[] IndexesLoadedWhole;
        //internal index_data[] indexes_loaded_partition;
        //internal index_data[][] indexes_loaded_partitions;

        internal IndexData[] IndexesMissingWhole;

        internal IndexData[] IndexesWhole;
        //internal index_data[] indexes_missing_partition;
        //internal index_data[][] indexes_missing_partitions;

        public IndexDataContainer()
        {
            Logging.LogCall(ModuleName);
        }
    }
}