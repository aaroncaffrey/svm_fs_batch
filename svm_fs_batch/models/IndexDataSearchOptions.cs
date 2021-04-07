namespace SvmFsBatch
{
    public class IndexDataSearchOptions
    {
        public const string ModuleName = nameof(IndexDataSearchOptions);

        public bool IdJobUid = true;
        public bool IdBaseLineDatasetFileTags = false;
        public bool IdBaseLineColumnArrayIndexes = false;
        public bool IdDatasetFileTags = false;
        public bool IdCalcElevenPointThresholds = false;
        public bool IdClassWeights = false;
        public bool IdColumnArrayIndexes = false;
        public bool IdExperimentName = false;
        public bool IdGroupArrayIndex = false;
        public bool IdGroupArrayIndexes = false;
        public bool IdGroupFolder = false;
        public bool IdGroupKey = false;
        public bool IdInnerCvFolds = false;
        public bool IdIterationIndex = false;
        public bool IdNumColumns = false;
        public bool IdNumGroups = false;
        public bool IdOuterCvFolds = false;
        public bool IdOuterCvFoldsToRun = false;
        public bool IdRepetitions = false;
        public bool IdScaleFunction = false;
        public bool IdSelectionDirection = false;
        public bool IdSvmKernel = false;
        public bool IdSvmType = false;
        public bool IdTotalGroups = false;
        public bool IdClassFolds = false;
        public bool IdDownSampledTrainClassFolds = false;

        public IndexDataSearchOptions()
        {

        }

        public IndexDataSearchOptions(bool value)
        {
            Set(value);
        }

        public void Set(bool value)
        {
            IdJobUid = value;
            IdBaseLineDatasetFileTags = value;
            IdBaseLineColumnArrayIndexes = value;
            IdDatasetFileTags = value;
            IdCalcElevenPointThresholds = value;
            IdClassWeights = value;
            IdColumnArrayIndexes = value;
            IdExperimentName = value;
            IdGroupArrayIndex = value;
            IdGroupArrayIndexes = value;
            IdGroupFolder = value;
            IdGroupKey = value;
            IdInnerCvFolds = value;
            IdIterationIndex = value;
            IdNumColumns = value;
            IdNumGroups = value;
            IdOuterCvFolds = value;
            IdOuterCvFoldsToRun = value;
            IdRepetitions = value;
            IdScaleFunction = value;
            IdSelectionDirection = value;
            IdSvmKernel = value;
            IdSvmType = value;
            IdTotalGroups = value;
            IdClassFolds = value;
            IdDownSampledTrainClassFolds = value;
        }

        public bool AnyTrue()
        {
            Logging.LogCall(ModuleName);

            var ret =
                IdJobUid ||

                 IdBaseLineDatasetFileTags ||
            IdBaseLineColumnArrayIndexes ||
            IdDatasetFileTags ||

            IdCalcElevenPointThresholds ||
                IdClassWeights ||
                IdColumnArrayIndexes ||
                IdExperimentName ||
                IdGroupArrayIndex ||
                IdGroupArrayIndexes ||
                IdGroupFolder ||
                IdGroupKey ||
                IdInnerCvFolds ||
                IdIterationIndex ||
                IdNumColumns ||
                IdNumGroups ||
                IdOuterCvFolds ||
                IdOuterCvFoldsToRun ||
                IdRepetitions ||
                IdScaleFunction ||
                IdSelectionDirection ||
                IdSvmKernel ||
                IdSvmType ||
                IdTotalGroups ||
                IdClassFolds ||
                IdDownSampledTrainClassFolds
                ;

            Logging.LogExit(ModuleName);
            return ret;
        }

        public bool AllTrue()
        {
            Logging.LogCall(ModuleName);

            var ret =
                IdJobUid &&

                  IdBaseLineDatasetFileTags &&
            IdBaseLineColumnArrayIndexes &&
            IdDatasetFileTags &&


                IdCalcElevenPointThresholds &&
                IdClassWeights &&
                IdColumnArrayIndexes &&
                IdExperimentName &&
                IdGroupArrayIndex &&
                IdGroupArrayIndexes &&
                IdGroupFolder &&
                IdGroupKey &&
                IdInnerCvFolds &&
                IdIterationIndex &&
                IdNumColumns &&
                IdNumGroups &&
                IdOuterCvFolds &&
                IdOuterCvFoldsToRun &&
                IdRepetitions &&
                IdScaleFunction &&
                IdSelectionDirection &&
                IdSvmKernel &&
                IdSvmType &&
                IdTotalGroups &&
                IdClassFolds &&
                IdDownSampledTrainClassFolds
                ;

            Logging.LogExit(ModuleName);
            return ret;
        }

        public bool[] Values()
        {
            Logging.LogCall(ModuleName);

            var ret = new[]
            {
                    IdJobUid,

                      IdBaseLineDatasetFileTags ,
                IdBaseLineColumnArrayIndexes ,
                IdDatasetFileTags ,

                    IdCalcElevenPointThresholds,
                    IdClassWeights,
                    IdColumnArrayIndexes,
                    IdExperimentName,
                    IdGroupArrayIndex,
                    IdGroupArrayIndexes,
                    IdGroupFolder,
                    IdGroupKey,
                    IdInnerCvFolds,
                    IdIterationIndex,
                    IdNumColumns,
                    IdNumGroups,
                    IdOuterCvFolds,
                    IdOuterCvFoldsToRun,
                    IdRepetitions,
                    IdScaleFunction,
                    IdSelectionDirection,
                    IdSvmKernel,
                    IdSvmType,
                    IdTotalGroups,
                    IdClassFolds,
                    IdDownSampledTrainClassFolds
                };

            Logging.LogExit(ModuleName);
            return ret;
        }
    }





}