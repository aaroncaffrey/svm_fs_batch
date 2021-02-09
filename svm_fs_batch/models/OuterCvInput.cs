namespace SvmFsBatch
{
    internal class OuterCvInput
    {
        internal const string ModuleName = nameof(OuterCvInput);

        internal string CmFn1;
        internal string CmFn2;
        internal string GridFn;
        internal string ModelFn;
        internal int OuterCvIndex;
        internal string PredictFn;
        internal int RepetitionsIndex;
        internal string TestFn;
        internal (int ClassId, int[] TestIndexes)[] TestFoldIndexes;
        internal (int ClassId, int test_size)[] TestSizes;
        internal string[] TestText;
        internal string TrainFn;
        internal (int ClassId, int[] TrainIndexes)[] TrainFoldIndexes;
        internal (int ClassId, int train_size)[] TrainSizes;
        internal string[] TrainText;

        public OuterCvInput()
        {
            Logging.LogCall(ModuleName);
        }
    }
}