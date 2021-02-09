namespace SvmFsBatch
{
    internal class GroupSeriesIndex
    {
        internal const string ModuleName = nameof(GroupArrayIndex);

        internal int[] ColumnIndexes;
        internal int GroupArrayIndex;
        internal string GroupFolder;
        internal int[] GroupIndexes;
        internal DataSetGroupKey GroupKey;
        internal bool IsGroupBaseGroup;
        internal bool IsGroupBlacklisted;

        internal bool IsGroupIndexValid;
        internal bool IsGroupLastWinner;
        internal bool IsGroupOnlySelection;
        internal bool IsGroupSelected;
        internal Program.Direction SelectionDirection;

        public GroupSeriesIndex()
        {
            Logging.LogCall(ModuleName);
        }
    }
}