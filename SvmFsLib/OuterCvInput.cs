namespace SvmFsLib
{
    public class OuterCvInput
    {
        public const string ModuleName = nameof(OuterCvInput);

        public string CmFn1;
        public string CmFn2;
        public string GridFn;
        public string ModelFn;
        public int OuterCvIndex;
        public string PredictFn;
        public int RepetitionsIndex;
        public string TestFn;
        public (int ClassId, int[] TestIndexes)[] TestFoldIndexes;
        public (int ClassId, int test_size)[] TestSizes;
        public string[] TestText;
        public string TrainFn;
        public (int ClassId, int[] TrainIndexes)[] TrainFoldIndexes;
        public (int ClassId, int train_size)[] TrainSizes;
        public string[] TrainText;

        public OuterCvInput()
        {
            Logging.LogCall(ModuleName);
        }
    }
}