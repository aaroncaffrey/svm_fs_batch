namespace SvmFsBatch
{
    internal class GroupSeriesIndex
    {
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
    }
}