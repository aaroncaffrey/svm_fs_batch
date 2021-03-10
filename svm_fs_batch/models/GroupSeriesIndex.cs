namespace SvmFsBatch
{
    public class GroupSeriesIndex
    {
        public const string ModuleName = nameof(GroupArrayIndex);

        public int[] ColumnIndexes;
        public int GroupArrayIndex;
        public string GroupFolder;
        public int[] GroupIndexes;
        public DataSetGroupKey GroupKey;
        public bool IsGroupBaseGroup;
        public bool IsGroupBlacklisted;

        public bool IsGroupIndexValid;
        public bool IsGroupLastWinner;
        public bool IsGroupOnlySelection;
        public bool IsGroupSelected;
        public Program.Direction SelectionDirection;

        public GroupSeriesIndex()
        {
            Logging.LogCall(ModuleName);

            Logging.LogExit(ModuleName);
        }
    }
}