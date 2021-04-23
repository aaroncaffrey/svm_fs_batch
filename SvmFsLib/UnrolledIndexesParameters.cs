namespace SvmFsLib
{
    public class UnrolledIndexesParameters
    {
        public const string ModuleName = nameof(UnrolledIndexesParameters);

        public bool CalcElevenPointThresholds = false;

        public (int ClassId, double ClassWeight)[][] ClassWeightSets = null;

        public GroupSeriesIndex[] GroupSeries;
        public int[] InnerCvSeries;
        public int InnerCvSeriesEnd = 5;

        // number of inner-cross-validations (default: 5 ... 5 to 5 at step 1)
        public int InnerCvSeriesStart = 5;
        public int InnerCvSeriesStep = 1;

        public Routines.LibsvmKernelType[] LibsvmKernelTypes;

        public int[] OuterCvSeries;
        public int OuterCvSeriesEnd = 5;

        // number of outer-cross-validations (default: 5 ... 5 to 5 at step 1)
        public int OuterCvSeriesStart = 5;
        public int OuterCvSeriesStep = 1;

        // the following variables can be used for generating different series... to test bias of a particular set of numbers... e.g. do 5/5/5 and 10/10/10 Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :similar results?
        // number of times to run tests (repeats) (default: 1 for single repetition, or 5 to match default fold number [0 would be none] ... 5 to 5 at step 1)

        public int[] RepetitionCvSeries;
        public int RepetitionCvSeriesEnd = 5;
        public int RepetitionCvSeriesStart = 5;
        public int RepetitionCvSeriesStep = 1;


        public Scaling.ScaleFunction[] Scales; // = new scaling.scale_function[] { scaling.scale_function.rescale };

        public Routines.LibsvmSvmType[] SvmTypes; // = new routines.libsvm_svm_type[] { routines.libsvm_svm_type.c_svc };

        public UnrolledIndexesParameters()
        {
            Logging.LogCall(ModuleName);
        }
    }
}