using System.Runtime.InteropServices;

namespace SvmFsLib
{
    public class ProgramArgsEnv
    {
        // this class contains the default program settings as they appear in the environment variables or settings before any modifications
        // todo: load these values from a json or env file

        //public const string ModuleName = nameof(ProgramArgsEnv);

#if DEBUG
        public static string PathSvmFsLdr = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\phd\phd_work\SvmFs\SvmFsLdr\bin\x64\Debug\net6.0\SvmFsLdr" : $@"/home/k1040015/SvmFs/SvmFsLdr/bin/x64/Debug/net6.0/SvmFsLdr";
        public static string PathSvmFsCtl = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\phd\phd_work\SvmFs\SvmFsLdr\bin\x64\Debug\net6.0\SvmFsCtl" : $@"/home/k1040015/SvmFs/SvmFsCtl/bin/x64/Debug/net6.0/SvmFsCtl";
        public static string PathSvmFsWkr = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\phd\phd_work\SvmFs\SvmFsLdr\bin\x64\Debug\net6.0\SvmFsWkr" : $@"/home/k1040015/SvmFs/SvmFsWkr/bin/x64/Debug/net6.0/SvmFsWkr";
#else
        public static string PathSvmFsLdr = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\phd\phd_work\SvmFs\SvmFsLdr\bin\x64\Release\net6.0\SvmFsLdr" : $@"/home/k1040015/SvmFs/SvmFsLdr/bin/x64/Release/net6.0/SvmFsLdr";
        public static string PathSvmFsCtl = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\phd\phd_work\SvmFs\SvmFsLdr\bin\x64\Release\net6.0\SvmFsCtl" : $@"/home/k1040015/SvmFs/SvmFsCtl/bin/x64/Release/net6.0/SvmFsCtl";
        public static string PathSvmFsWkr = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\phd\phd_work\SvmFs\SvmFsLdr\bin\x64\Release\net6.0\SvmFsWkr" : $@"/home/k1040015/SvmFs/SvmFsWkr/bin/x64/Release/net6.0/SvmFsWkr";  
#endif
        public static string LaunchMethod = "INVOKE_METHOD"; // "PBS";
        //public static string WorkListInputFile = "";
        //public static string WorkListOutputFile = "";
        public static int TotalNodes = 10;
        public static int ProcessorsPerNode = 64;
        public static int NodeIndex = -1;
        public static int IterationIndex = -1;
        public static int InstanceId = -1;
        public static int Option0 = 0;
        public static int Option1 = 1;
        public static int Option2 = 1;
        public static int Option3 = 1;
        public static int Option4 = 1;
        public static bool CalcElevenPointThresholds = default;
        public static (int ClassId, string ClassName)[] ClassNames = default;
        public static (int ClassId, double ClassWeight)[][] ClassWeights = default;
        //public static string DataSetDir = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"E:\DataSet7\merged_files\" : $@"/home/k1040015/DataSet/";
        //public static string BaseLineDataSetDir = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"E:\DataSet7\merged_files\" : $@"/home/k1040015/DataSet/";

        public static string DataSetDir = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\phd\_march_2022_dataset\aaindex_only\merged_files\" : $@"/home/k1040015/DataSet/";
        public static string BaseLineDataSetDir = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\phd\_march_2022_dataset\aaindex_only\merged_files\" : $@"/home/k1040015/DataSet/";


        public static string[] DataSetNames = { "[1i.aaindex]" };//default;
        public static string[] BaseLineDataSetNames = default;
        public static int[] BaseLineDataSetColumnIndexes = default;
        public static string ExperimentName = "";
        public static int? Folds; // note: use 'folds' to set repetitions, outer_cv_folds and inner_folds to the same value
        public static int InnerFolds = 5;
        public static string JobId = "";
        public static string JobName = "";
        public static Libsvm.LibsvmKernelType[] Kernels = { Libsvm.LibsvmKernelType.Rbf };
        public static string LibsvmPredictRuntime = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\phd\libsvm\windows\svm-predict.exe" : $@"/home/k1040015/libsvm/svm-predict";
        public static string LibsvmTrainRuntime = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? @"C:\phd\libsvm\windows\svm-train.exe" : $@"/home/k1040015/libsvm/svm-train";
        public static int NegativeClassId = -1;
        public static string NegativeClassName = @"standard_coil";
        public static int OuterCvFolds = 5;
        public static int OuterCvFoldsToRun = 5;
        public static int PositiveClassId = +1;
        public static string PositiveClassName = @"dimorphic_coil";
        public static int Repetitions = 1;
        public static string ResultsRootFolder = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? $@"C:\mmfs1\data\scratch\k1040015\SvmFs" : $@"/mmfs1/data/scratch/k1040015/SvmFs"; // root for all experiments - must concat experiment name to it
        public static Scaling.ScaleFunction[] Scales = { Scaling.ScaleFunction.Rescale };
        public static int ScoringClassId = +1;
        public static string[] ScoringMetrics = { nameof(MetricsBox.PF1S) /*nameof(metrics_box.p_MCC),*/ /*nameof(metrics_box.p_API_All)*/};
        public static Libsvm.LibsvmSvmType[] SvmTypes = { Libsvm.LibsvmSvmType.CSvc };
    }
}
