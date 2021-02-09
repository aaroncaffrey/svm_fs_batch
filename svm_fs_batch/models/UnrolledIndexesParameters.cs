namespace SvmFsBatch
{
    internal class UnrolledIndexesParameters
    {
        public const string ModuleName = nameof(UnrolledIndexesParameters);

        internal bool CalcElevenPointThresholds = false;

        internal (int ClassId, double ClassWeight)[][] ClassWeightSets = null;

        internal GroupSeriesIndex[] GroupSeries;
        internal int[] InnerCvSeries;
        internal int InnerCvSeriesEnd = 5;

        // number of inner-cross-validations (default: 5 ... 5 to 5 at step 1)
        internal int InnerCvSeriesStart = 5;
        internal int InnerCvSeriesStep = 1;

        internal Routines.LibsvmKernelType[] LibsvmKernelTypes;

        internal int[] OuterCvSeries;
        internal int OuterCvSeriesEnd = 5;

        // number of outer-cross-validations (default: 5 ... 5 to 5 at step 1)
        internal int OuterCvSeriesStart = 5;
        internal int OuterCvSeriesStep = 1;

        // the following variables can be used for generating different series... to test bias of a particular set of numbers... e.g. do 5/5/5 and 10/10/10 Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :similar results?
        // number of times to run tests (repeats) (default: 1 for single repetition, or 5 to match default fold number [0 would be none] ... 5 to 5 at step 1)

        internal int[] RepetitionCvSeries;
        internal int RepetitionCvSeriesEnd = 5;
        internal int RepetitionCvSeriesStart = 5;
        internal int RepetitionCvSeriesStep = 1;


        internal Scaling.ScaleFunction[] Scales; // = new scaling.scale_function[] { scaling.scale_function.rescale };

        internal Routines.LibsvmSvmType[] SvmTypes; // = new routines.libsvm_svm_type[] { routines.libsvm_svm_type.c_svc };

        public UnrolledIndexesParameters()
        {
            Logging.LogCall(ModuleName);
        }
    }
}