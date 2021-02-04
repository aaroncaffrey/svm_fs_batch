using System;
using System.Globalization;
using System.Linq;

namespace SvmFsBatch
{
    internal class IndexData
    {
        public const string ModuleName = nameof(IndexData);
        internal static readonly IndexData Empty = new IndexData();

        public static readonly string[] CsvHeaderValuesArray = DataSetGroupKey.CsvHeaderValuesArray.Select(a => "id_" + a).ToArray().Concat(new[]
        {
            nameof(IdIterationIndex),
            nameof(IdGroupArrayIndex),
            nameof(IdTotalGroups),
            nameof(IdSelectionDirection),
            nameof(IdExperimentName),
            nameof(IdRepetitions),
            nameof(IdOuterCvFolds),
            nameof(IdOuterCvFoldsToRun),
            nameof(IdInnerCvFolds),
            nameof(IdSvmType),
            nameof(IdSvmKernel),
            nameof(IdScaleFunction),
            nameof(IdCalcElevenPointThresholds),
            nameof(IdNumGroups),
            nameof(IdNumColumns),
            nameof(IdGroupArrayIndexes),
            nameof(IdColumnArrayIndexes),
            nameof(IdClassWeights),
            nameof(IdClassFolds),
            nameof(IdDownSampledTrainClassFolds),
            nameof(IdGroupFolder)
        }).ToArray();

        public static readonly string CsvHeaderString = string.Join(",", CsvHeaderValuesArray);
        internal bool IdCalcElevenPointThresholds;

        internal (int ClassId, int class_size, (int repetitions_index, int outer_cv_index, int[] class_sample_indexes)[] folds)[] IdClassFolds;

        internal (int ClassId, double ClassWeight)[] IdClassWeights;
        internal int[] IdColumnArrayIndexes;

        internal (int ClassId, int class_size, (int repetitions_index, int outer_cv_index, int[] class_sample_indexes)[] folds)[] IdDownSampledTrainClassFolds;

        internal string IdExperimentName;
        internal int IdGroupArrayIndex = -1;
        internal int[] IdGroupArrayIndexes;
        internal string IdGroupFolder;

        internal DataSetGroupKey IdGroupKey;
        internal int IdInnerCvFolds = 5;
        internal int IdIterationIndex = -1;
        internal int IdNumColumns;
        internal int IdNumGroups;
        internal int IdOuterCvFolds = 5;
        internal int IdOuterCvFoldsToRun = 1;
        internal int IdRepetitions = 5;
        internal Scaling.ScaleFunction IdScaleFunction = Scaling.ScaleFunction.Rescale;
        internal Program.Direction IdSelectionDirection;
        internal Routines.LibsvmKernelType IdSvmKernel = Routines.LibsvmKernelType.Rbf;
        internal Routines.LibsvmSvmType IdSvmType = Routines.LibsvmSvmType.CSvc;
        internal int IdTotalGroups = -1;

        public IndexData()
        {
        }


        public IndexData(string[] lines)
        {
            if (lines == null || lines.Length < 1 || lines.Length > 2) return;

            var lineHeader = lines.Length == 2
                ? lines.First().Split(',')
                : CsvHeaderValuesArray;

            SetValues(lineHeader, XTypes.GetXTypes(lines.Last().Split(',')));
        }

        public IndexData(string[] lineHeader, string csvLine, int columnOffset = 0) : this(lineHeader, csvLine.Split(','), columnOffset)
        {
        }

        public IndexData(string[] lineHeader, string[] csvLine, int columnOffset = 0)
        {
            //set_values(routines.x_types(null, csv_line.Skip(column_offset).ToArray(), false), 0);
            SetValues(lineHeader, XTypes.GetXTypes(csvLine.Skip(columnOffset).ToArray()));
        }

        internal IndexData(string[] lineHeader, XTypes[] xType, int columnOffset = 0)
        {
            SetValues(lineHeader, xType, columnOffset);
        }

        //internal int unrolled_whole_index = -1;
        //internal int unrolled_partition_index = -1;
        //internal int unrolled_instance_id = -1;
        //internal int total_whole_indexes = -1;
        //internal int total_partition_indexes = -1;
        //internal int total_instances = -1;


        internal void ClearSupplemental()
        {
            IdClassFolds = null;
            IdDownSampledTrainClassFolds = null;
        }


        public string[] CsvValuesArray()
        {
            var x1 = IdGroupKey?.CsvValuesArray() ?? DataSetGroupKey.Empty.CsvValuesArray();

            var x2 = new[]
            {
                $@"{IdIterationIndex}",
                $@"{IdGroupArrayIndex}",
                $@"{IdTotalGroups}",
                $@"{IdSelectionDirection}",
                $@"{IdExperimentName}",
                $@"{IdRepetitions}",
                $@"{IdOuterCvFolds}",
                $@"{IdOuterCvFoldsToRun}",
                $@"{IdInnerCvFolds}",
                $@"{IdSvmType}",
                $@"{IdSvmKernel}",
                $@"{IdScaleFunction}",
                $@"{(IdCalcElevenPointThresholds ? 1 : 0)}",
                $@"{IdNumGroups}",
                $@"{IdNumColumns}",
                $@"{string.Join(";", IdGroupArrayIndexes ?? Array.Empty<int>())}",
                $@"{string.Join(";", IdColumnArrayIndexes ?? Array.Empty<int>())}",
                $@"{string.Join(";", IdClassWeights?.Select(a => $"{a.ClassId}:{a.ClassWeight:G17}").ToArray() ?? Array.Empty<string>())}",
                $@"{string.Join(";", IdClassFolds?.Select(a => string.Join(":", $@"{a.ClassId}", $@"{a.class_size}", $@"{string.Join("|", a.folds?.Select(b => string.Join("~", $@"{b.repetitions_index}", $@"{b.outer_cv_index}", $@"{string.Join("/", b.class_sample_indexes ?? Array.Empty<int>())}")).ToArray() ?? Array.Empty<string>())}")).ToArray() ?? Array.Empty<string>())}",
                $@"{string.Join(";", IdDownSampledTrainClassFolds?.Select(a => string.Join(":", $@"{a.ClassId}", $@"{a.class_size}", $@"{string.Join("|", a.folds?.Select(b => string.Join("~", $@"{b.repetitions_index}", $@"{b.outer_cv_index}", $@"{string.Join("/", b.class_sample_indexes ?? Array.Empty<int>())}")).ToArray() ?? Array.Empty<string>())}")).ToArray() ?? Array.Empty<string>())}",
                $@"{IdGroupFolder}"
            };

            var x3 = x1.Concat(x2).Select(a => a == null
                ? ""
                : a.Replace(',', ';')).ToArray();

            return x3;
        }

        internal void SetValues(string[] lineHeader, XTypes[] xType, int columnOffset = 0)
        {
            var k = columnOffset;

            var headerIndexes = CsvHeaderValuesArray.Select((h, i) => (header: h, index: lineHeader.Length > 0
                ? Array.FindIndex(lineHeader, a => a.EndsWith(h))
                : columnOffset + i)).ToArray();

            int Hi(string name)
            {
                return headerIndexes.First(a => a.header.EndsWith(name, StringComparison.OrdinalIgnoreCase)).index;
            }

            // todo: lookup actual group key instance - not necessary, since the parent index_data will be looked up
            //[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, int.TryParse(x_type[hi(nameof(___))].asStr, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var tp_int1) ? tp_int1 : -1);

            IdGroupKey = new DataSetGroupKey(lineHeader, xType);

            if (IdGroupKey.Value == default) IdGroupKey = null;

            var hiIdIterationIndex = Hi(nameof(IdIterationIndex));
            var hiIdGroupArrayIndex = Hi(nameof(IdGroupArrayIndex));
            var hiIdTotalGroups = Hi(nameof(IdTotalGroups));
            var hiIdSelectionDirection = Hi(nameof(IdSelectionDirection));
            var hiIdExperimentName = Hi(nameof(IdExperimentName));
            var hiIdRepetitions = Hi(nameof(IdRepetitions));
            var hiIdOuterCvFolds = Hi(nameof(IdOuterCvFolds));
            var hiIdOuterCvFoldsToRun = Hi(nameof(IdOuterCvFoldsToRun));
            var hiIdInnerCvFolds = Hi(nameof(IdInnerCvFolds));
            var hiIdSvmType = Hi(nameof(IdSvmType));
            var hiIdSvmKernel = Hi(nameof(IdSvmKernel));
            var hiIdScaleFunction = Hi(nameof(IdScaleFunction));
            var hiIdCalcElevenPointThresholds = Hi(nameof(IdCalcElevenPointThresholds));
            var hiIdNumGroups = Hi(nameof(IdNumGroups));
            var hiIdNumColumns = Hi(nameof(IdNumColumns));
            var hiIdGroupArrayIndexes = Hi(nameof(IdGroupArrayIndexes));
            var hiIdColumnArrayIndexes = Hi(nameof(IdColumnArrayIndexes));
            var hiIdClassWeights = Hi(nameof(IdClassWeights));
            var hiIdClassFolds = Hi(nameof(IdClassFolds));
            var hiIdDownSampledTrainClassFolds = Hi(nameof(IdDownSampledTrainClassFolds));
            var hiIdGroupFolder = Hi(nameof(IdGroupFolder));

            if (hiIdIterationIndex > -1) IdIterationIndex = xType[hiIdIterationIndex].AsInt ?? -1;
            if (hiIdGroupArrayIndex > -1) IdGroupArrayIndex = xType[hiIdGroupArrayIndex].AsInt ?? -1;
            if (hiIdTotalGroups > -1) IdTotalGroups = xType[hiIdTotalGroups].AsInt ?? -1;
            if (hiIdSelectionDirection > -1) IdSelectionDirection = Enum.Parse<Program.Direction>(xType[hiIdSelectionDirection].AsStr, true);
            if (hiIdExperimentName > -1) IdExperimentName = xType[hiIdExperimentName].AsStr;
            if (hiIdRepetitions > -1) IdRepetitions = xType[hiIdRepetitions].AsInt ?? -1;
            if (hiIdOuterCvFolds > -1) IdOuterCvFolds = xType[hiIdOuterCvFolds].AsInt ?? -1;
            if (hiIdOuterCvFoldsToRun > -1) IdOuterCvFoldsToRun = xType[hiIdOuterCvFoldsToRun].AsInt ?? -1;
            if (hiIdInnerCvFolds > -1) IdInnerCvFolds = xType[hiIdInnerCvFolds].AsInt ?? -1;
            if (hiIdSvmType > -1) IdSvmType = Enum.Parse<Routines.LibsvmSvmType>(xType[hiIdSvmType].AsStr, true);
            if (hiIdSvmKernel > -1) IdSvmKernel = Enum.Parse<Routines.LibsvmKernelType>(xType[hiIdSvmKernel].AsStr, true);
            if (hiIdScaleFunction > -1) IdScaleFunction = Enum.Parse<Scaling.ScaleFunction>(xType[hiIdScaleFunction].AsStr, true);
            if (hiIdCalcElevenPointThresholds > -1) IdCalcElevenPointThresholds = xType[hiIdCalcElevenPointThresholds].AsBool ?? default;
            if (hiIdNumGroups > -1) IdNumGroups = xType[hiIdNumGroups].AsInt ?? -1;
            if (hiIdNumColumns > -1) IdNumColumns = xType[hiIdNumColumns].AsInt ?? -1;
            if (hiIdGroupArrayIndexes > -1) IdGroupArrayIndexes = xType[hiIdGroupArrayIndexes].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();
            if (hiIdColumnArrayIndexes > -1) IdColumnArrayIndexes = xType[hiIdColumnArrayIndexes].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();
            if (hiIdClassWeights > -1)
                IdClassWeights = xType[hiIdClassWeights].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a =>
                {
                    var b = a.Split(':', StringSplitOptions.RemoveEmptyEntries);
                    return (int.Parse(b[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo), double.Parse(b[1], NumberStyles.Float, NumberFormatInfo.InvariantInfo));
                }).ToArray();

            //  ;:|~/
            if (hiIdClassFolds > -1)
                IdClassFolds = xType[hiIdClassFolds].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a =>
                {
                    var b = a.Split(':', StringSplitOptions.RemoveEmptyEntries);
                    var classId = int.Parse(b[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                    var classSize = int.Parse(b[1], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                    var folds = b[2].Split('|', StringSplitOptions.RemoveEmptyEntries).Select(d =>
                    {
                        var e = d.Split('~', StringSplitOptions.RemoveEmptyEntries);
                        var repetitionsIndex = int.Parse(e[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                        var outerCvIndex = int.Parse(e[1], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                        var classSampleIndexes = e[2].Split('/', StringSplitOptions.RemoveEmptyEntries).Select(f => int.Parse(f, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();

                        return (repetitions_index: repetitionsIndex, outer_cv_index: outerCvIndex, class_sample_indexes: classSampleIndexes);
                    }).ToArray();
                    return (ClassId: classId, class_size: classSize, folds);
                }).ToArray();

            //  ;:|~/
            if (hiIdDownSampledTrainClassFolds > -1)
                IdDownSampledTrainClassFolds = xType[hiIdDownSampledTrainClassFolds].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a =>
                {
                    var b = a.Split(':', StringSplitOptions.RemoveEmptyEntries);
                    var classId = int.Parse(b[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                    var classSize = int.Parse(b[1], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                    var folds = b[2].Split('|', StringSplitOptions.RemoveEmptyEntries).Select(d =>
                    {
                        var e = d.Split('~', StringSplitOptions.RemoveEmptyEntries);
                        var repetitionsIndex = int.Parse(e[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                        var outerCvIndex = int.Parse(e[1], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                        var classSampleIndexes = e[2].Split('/', StringSplitOptions.RemoveEmptyEntries).Select(f => int.Parse(f, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();

                        return (repetitions_index: repetitionsIndex, outer_cv_index: outerCvIndex, class_sample_indexes: classSampleIndexes);
                    }).ToArray();
                    return (ClassId: classId, class_size: classSize, folds);
                }).ToArray();

            if (hiIdGroupFolder > -1) IdGroupFolder = xType[hiIdGroupFolder].AsStr;
        }

        public string CsvValuesString()
        {
            return string.Join(",", CsvValuesArray());
        }

        internal string IdIndexStr()
        {
            var list = new (string name, string value, string value_max)[]
            {
                (nameof(IdExperimentName), $@"{IdExperimentName}", @""),
                (nameof(IdIterationIndex), $@"{IdIterationIndex}", @""),
                //(nameof(this.iteration_name), $@"{this.iteration_name}", $@""),
                (nameof(IdGroupArrayIndex), $@"{IdGroupArrayIndex}", IdTotalGroups > -1
                    ? $@"{IdTotalGroups}"
                    : @"")
                //(nameof(this.unrolled_instance_id), $@"{this.unrolled_instance_id}", this.total_instances > -1 ? $@"{this.total_instances}" : $@""),
                //(nameof(this.unrolled_whole_index), $@"{this.unrolled_whole_index}", this.total_whole_indexes > -1 ? $@"{this.total_whole_indexes}" : $@""),
                //(nameof(this.unrolled_partition_index), $@"{this.unrolled_partition_index}", this.total_partition_indexes > -1 ? $@"{this.total_partition_indexes}" : $@"")
            };

            return @"[" + string.Join(", ",
                list.Select(a => $@"{a.name}={a.value}" + (!string.IsNullOrWhiteSpace(a.value_max)
                    ? $@"/{a.value_max}"
                    : @"")).ToList()) + @"]";
        }

        internal string IdMlStr()
        {
            var list = new (string name, string value, string value_max)[]
            {
                (nameof(IdSvmType), $@"{IdSvmType}", @""),
                (nameof(IdSvmKernel), $@"{IdSvmKernel}", @""),
                (nameof(IdScaleFunction), $@"{IdScaleFunction}", @""),
                (nameof(IdClassWeights), $@"{(IdClassWeights != null ? string.Join(@"; ", IdClassWeights.Select(a => $@"w{(a.ClassId > 0 ? @"+" : @"")} {a.ClassWeight}").ToList()) : "")}", ""),
                (nameof(IdCalcElevenPointThresholds), $@"{IdCalcElevenPointThresholds}", "")
            };

            return @"[" + string.Join(", ",
                list.Select(a => $@"{a.name}={a.value}" + (!string.IsNullOrWhiteSpace(a.value_max)
                    ? $@"/{a.value_max}"
                    : "")).ToList()) + @"]";
        }

        internal string IdFoldStr()
        {
            var list = new (string name, string value, string value_max)[]
            {
                (nameof(IdRepetitions), $@"{IdRepetitions}", ""),
                (nameof(IdOuterCvFolds), $@"{IdOuterCvFolds}", ""),
                (nameof(IdOuterCvFoldsToRun), $@"{IdOuterCvFoldsToRun}", ""),
                (nameof(IdInnerCvFolds), $@"{IdInnerCvFolds}", "")
            };

            return @"[" + string.Join(", ",
                list.Select(a => $@"{a.name}={a.value}" + (!string.IsNullOrWhiteSpace(a.value_max)
                    ? $@"/{a.value_max}"
                    : "")).ToList()) + @"]";
        }

        internal static IndexData FindFirstReference(IndexData[] list, IndexData data1, IndexDataSearchOptions idso = null)
        {
            if (data1 == null) return null;

            // find proper index_data instance for this newly loaded ConfusionMatrix instance
            var id = list.FirstOrDefault(data2 => CompareReferenceData(data1, data2, idso));

            return id;
        }

        internal static IndexData FindLastReference(IndexData[] list, IndexData data1, IndexDataSearchOptions idso = null)
        {
            if (data1 == null) return null;

            // find proper index_data instance for this newly loaded ConfusionMatrix instance
            var id = list.LastOrDefault(data2 => CompareReferenceData(data1, data2, idso));

            return id;
        }

        internal static bool CompareReferenceData(IndexData data1, IndexData data2, IndexDataSearchOptions idso = null)
        {
            return (idso != null && !idso.IterationIndex || data2.IdIterationIndex == data1.IdIterationIndex) && (idso != null && !idso.GroupArrayIndex || data2.IdGroupArrayIndex == data1.IdGroupArrayIndex) && (idso != null && !idso.TotalGroups || data2.IdTotalGroups == data1.IdTotalGroups) && (idso != null && !idso.SelectionDirection || data2.IdSelectionDirection == data1.IdSelectionDirection) && (idso != null && !idso.CalcElevenPointThresholds || data2.IdCalcElevenPointThresholds == data1.IdCalcElevenPointThresholds) && (idso != null && !idso.SvmType || data2.IdSvmType == data1.IdSvmType) && (idso != null && !idso.SvmKernel || data2.IdSvmKernel == data1.IdSvmKernel) && (idso != null && !idso.ScaleFunction || data2.IdScaleFunction == data1.IdScaleFunction) && (idso != null && !idso.Repetitions || data2.IdRepetitions == data1.IdRepetitions) && (idso != null && !idso.OuterCvFolds || data2.IdOuterCvFolds == data1.IdOuterCvFolds) && (idso != null && !idso.OuterCvFoldsToRun || data2.IdOuterCvFoldsToRun == data1.IdOuterCvFoldsToRun) && (idso != null && !idso.InnerCvFolds || data2.IdInnerCvFolds == data1.IdInnerCvFolds) && (idso != null && !idso.GroupKey || data2.IdGroupKey == data1.IdGroupKey) && (idso != null && !idso.ExperimentName || data2.IdExperimentName == data1.IdExperimentName) && (idso != null && !idso.NumGroups || data2.IdNumGroups == data1.IdNumGroups) && (idso != null && !idso.NumColumns || data2.IdNumColumns == data1.IdNumColumns) && (idso != null && !idso.GroupArrayIndexes || (data2.IdGroupArrayIndexes == null || data2.IdGroupArrayIndexes.Length == 0) && (data1.IdGroupArrayIndexes == null || data1.IdGroupArrayIndexes.Length == 0) || (data2.IdGroupArrayIndexes ?? Array.Empty<int>()).SequenceEqual(data1.IdGroupArrayIndexes)) && (idso != null && !idso.ColumnArrayIndexes || (data2.IdColumnArrayIndexes == null || data2.IdColumnArrayIndexes.Length == 0) && (data1.IdColumnArrayIndexes == null || data1.IdColumnArrayIndexes.Length == 0) || (data2.IdColumnArrayIndexes ?? Array.Empty<int>()).SequenceEqual(data1.IdColumnArrayIndexes)) && (idso != null && !idso.ClassWeights || (data2.IdClassWeights == null || data2.IdClassWeights.Length == 0) && (data1.IdClassWeights == null || data1.IdClassWeights.Length == 0) || (data2.IdClassWeights ?? Array.Empty<(int ClassId, double ClassWeight)>()).SequenceEqual(data1.IdClassWeights)) && (idso != null && !idso.GroupFolder || data2.IdGroupFolder == data1.IdGroupFolder);
        }

        internal class IndexDataSearchOptions
        {
            internal bool CalcElevenPointThresholds = true;
            internal bool ClassWeights = true;
            internal bool ColumnArrayIndexes = true;
            internal bool ExperimentName = true;
            internal bool GroupArrayIndex = true;
            internal bool GroupArrayIndexes = true;
            internal bool GroupFolder = true;
            internal bool GroupKey = true;
            internal bool InnerCvFolds = true;
            internal bool IterationIndex = true;
            internal bool NumColumns = true;
            internal bool NumGroups = true;
            internal bool OuterCvFolds = true;
            internal bool OuterCvFoldsToRun = true;
            internal bool Repetitions = true;
            internal bool ScaleFunction = true;
            internal bool SelectionDirection = true;
            internal bool SvmKernel = true;
            internal bool SvmType = true;
            internal bool TotalGroups = true;
        }

        //internal static index_data find_best_match_reference(index_data[] list, index_data data)
        //{
        //    if (data == null) return null;
        //
        //    // find proper index_data instance for this newly loaded ConfusionMatrix instance
        //    var sums = list.Select(id2 =>
        //            (id2.iteration_index == data.iteration_index ? 3 : 0) +
        //            (id2.group_array_index == data.group_array_index ? 3 : 0) +
        //            (id2.total_groups == data.total_groups ? 1 : 0) +
        //            (id2.selection_direction == data.selection_direction ? 1 : 0) +
        //            (id2.calc_ElevenPoint_thresholds == data.calc_ElevenPoint_thresholds ? 1 : 0) +
        //            (id2.svm_type == data.svm_type ? 1 : 0) +
        //            (id2.svm_kernel == data.svm_kernel ? 1 : 0) +
        //            (id2.scale_function == data.scale_function ? 1 : 0) +
        //            (id2.repetitions == data.repetitions ? 1 : 0) +
        //            (id2.outer_cv_folds == data.outer_cv_folds ? 1 : 0) +
        //            (id2.outer_cv_folds_to_run == data.outer_cv_folds_to_run ? 1 : 0) +
        //            (id2.inner_cv_folds == data.inner_cv_folds ? 1 : 0) +
        //            (id2.GroupKey == data.GroupKey ? 1 : 0) +
        //            (id2.experiment_name == data.experiment_name ? 1 : 0) +
        //            (id2.num_groups == data.num_groups ? 1 : 0) +
        //            (id2.num_columns == data.num_columns ? 1 : 0) +
        //            ((((id2.group_array_indexes == null || id2.group_array_indexes.Length == 0) && (data.group_array_indexes == null || data.group_array_indexes.Length == 0)) || (id2.group_array_indexes ?? Array.Empty<int>()).SequenceEqual(data.group_array_indexes)) ? 1 : 0) +
        //            ((((id2.column_array_indexes == null || id2.column_array_indexes.Length == 0) && (data.column_array_indexes == null || data.column_array_indexes.Length == 0)) || (id2.column_array_indexes ?? Array.Empty<int>()).SequenceEqual(data.column_array_indexes)) ? 1 : 0) +
        //            ((((id2.ClassWeights == null || id2.ClassWeights.Length == 0) && (data.ClassWeights == null || data.ClassWeights.Length == 0)) || (id2.ClassWeights ?? Array.Empty<(int ClassId, double ClassWeight)>()).SequenceEqual(data.ClassWeights)) ? 1 : 0)
        //        ).ToArray();
        //
        //    var max = sums.Max();
        //    var max_index_count = sums.Count(a => a == max);
        //    if (max_index_count > 1) throw new Exception();
        //    var max_index = Array.FindIndex(sums, a => a == max);
        //
        //    var id = list[max_index];
        //    return ct.IsCancellationRequested ? default :id;
        //}


        //public index_data(index_data index_data)
        //{
        //    if (index_data == null) return;
        //
        //    if (index_data.GroupKey != null) GroupKey = new DataSetGroupKey(index_data.GroupKey);
        //    if (GroupKey.value == default) GroupKey = null;
        //
        //    group_array_index = index_data.group_array_index;
        //    total_groups = index_data.total_groups;
        //    group_folder = index_data.group_folder;
        //
        //    //is_job_completed = index_data.is_job_completed;
        //    selection_direction = index_data.selection_direction;
        //    experiment_name = index_data.experiment_name;
        //    unrolled_whole_index = index_data.unrolled_whole_index;
        //    unrolled_partition_index = index_data.unrolled_partition_index;
        //    unrolled_instance_id = index_data.unrolled_instance_id;
        //    iteration_index = index_data.iteration_index;
        //
        //    calc_ElevenPoint_thresholds = index_data.calc_ElevenPoint_thresholds;
        //    repetitions = index_data.repetitions;
        //    outer_cv_folds = index_data.outer_cv_folds;
        //    outer_cv_folds_to_run = index_data.outer_cv_folds_to_run;
        //    inner_cv_folds = index_data.inner_cv_folds;
        //
        //    svm_type = index_data.svm_type;
        //    svm_kernel = index_data.svm_kernel;
        //    scale_function = index_data.scale_function;
        //
        //    num_groups = index_data.num_groups;
        //    num_columns = index_data.num_columns;
        //    group_array_indexes = index_data.group_array_indexes.ToArray();
        //    column_array_indexes = index_data.column_array_indexes.ToArray();
        //
        //    ClassWeights = index_data.ClassWeights?.ToArray();
        //    class_folds = index_data.class_folds?.Select(a => (a.ClassId, a.class_size, a.folds?.Select(b => (b.repetitions_index, b.outer_cv_index, b.class_sample_indexes?.ToArray())).ToArray())).ToArray();
        //    down_sampled_train_class_folds = index_data.down_sampled_train_class_folds?.Select(a => (a.ClassId, a.class_size, a.folds?.Select(b => (b.repetitions_index, b.outer_cv_index, b.class_sample_indexes?.ToArray())).ToArray())).ToArray();
        //
        //    total_whole_indexes = index_data.total_whole_indexes;
        //    total_partition_indexes = index_data.total_partition_indexes;
        //    total_instances = index_data.total_instances;
        //}
    }
}