using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;

namespace SvmFsBatch
{
    internal class IndexData
    {
        public const string ModuleName = nameof(IndexData);
        internal static readonly IndexData Empty = new IndexData();

        public static readonly string[] CsvHeaderValuesArray = DataSetGroupKey.CsvHeaderValuesArray.Select(a => /*"id_" +*/ a).ToArray().Concat(new[]
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

        internal (int ClassId, int class_size, (int RepetitionsIndex, int OuterCvIndex, int[] ClassSampleIndexes)[] folds)[] IdClassFolds;

        internal (int ClassId, double ClassWeight)[] IdClassWeights;
        internal int[] IdColumnArrayIndexes;

        internal (int ClassId, int class_size, (int RepetitionsIndex, int OuterCvIndex, int[] ClassSampleIndexes)[] folds)[] IdDownSampledTrainClassFolds;

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


        public IndexData(string line) : this(new[] { line })
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
        //internal int unrolled_InstanceId = -1;
        //internal int total_whole_indexes = -1;
        //internal int total_partition_indexes = -1;
        //internal int TotalInstances = -1;


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
                $@"{string.Join(";", IdClassFolds?.Select(a => string.Join(":", $@"{a.ClassId}", $@"{a.class_size}", $@"{string.Join("|", a.folds?.Select(b => string.Join("~", $@"{b.RepetitionsIndex}", $@"{b.OuterCvIndex}", $@"{string.Join("/", b.ClassSampleIndexes ?? Array.Empty<int>())}")).ToArray() ?? Array.Empty<string>())}")).ToArray() ?? Array.Empty<string>())}",
                $@"{string.Join(";", IdDownSampledTrainClassFolds?.Select(a => string.Join(":", $@"{a.ClassId}", $@"{a.class_size}", $@"{string.Join("|", a.folds?.Select(b => string.Join("~", $@"{b.RepetitionsIndex}", $@"{b.OuterCvIndex}", $@"{string.Join("/", b.ClassSampleIndexes ?? Array.Empty<int>())}")).ToArray() ?? Array.Empty<string>())}")).ToArray() ?? Array.Empty<string>())}",
                $@"{IdGroupFolder}"
            };

            var x3 = x1.Concat(x2).Select(a => a == null
                ? ""
                : a.Replace(',', ';')).ToArray();

#if DEBUG
            var str = string.Join(",", x3);
            var id2 = new  IndexData(str);
            var re = CompareReferenceData2(this, id2);
            if (!re.Values().All(a => a))
            {
                Logging.LogEvent("!!! INDEXDATA NOT EQUAL !!!");
            }
#endif
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

            // todo: lookup actual gkGroup key instance - not necessary, since the parent index_data will be looked up
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

                        return (RepetitionsIndex: repetitionsIndex, OuterCvIndex: outerCvIndex, ClassSampleIndexes: classSampleIndexes);
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

                        return (RepetitionsIndex: repetitionsIndex, OuterCvIndex: outerCvIndex, ClassSampleIndexes: classSampleIndexes);
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
                //(nameof(this.unrolled_InstanceId), $@"{this.unrolled_InstanceId}", this.TotalInstances > -1 ? $@"{this.TotalInstances}" : $@""),
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
            var comp = CompareReferenceData2(data1, data2);
            if (idso == null || idso.Values().All(a => a)) return comp.Values().All(a => a);

            return
                (!idso.IdCalcElevenPointThresholds || comp.IdCalcElevenPointThresholds) &&
                (!idso.IdClassWeights || comp.IdClassWeights) &&
                (!idso.IdColumnArrayIndexes || comp.IdColumnArrayIndexes) &&
                (!idso.IdExperimentName || comp.IdExperimentName) &&
                (!idso.IdGroupArrayIndex || comp.IdGroupArrayIndex) &&
                (!idso.IdGroupArrayIndexes || comp.IdGroupArrayIndexes) &&
                (!idso.IdGroupFolder || comp.IdGroupFolder) &&
                (!idso.IdGroupKey || comp.IdGroupKey) &&
                (!idso.IdInnerCvFolds || comp.IdInnerCvFolds) &&
                (!idso.IdIterationIndex || comp.IdIterationIndex) &&
                (!idso.IdNumColumns || comp.IdNumColumns) &&
                (!idso.IdNumGroups || comp.IdNumGroups) &&
                (!idso.IdOuterCvFolds || comp.IdOuterCvFolds) &&
                (!idso.IdOuterCvFoldsToRun || comp.IdOuterCvFoldsToRun) &&
                (!idso.IdRepetitions || comp.IdRepetitions) &&
                (!idso.IdScaleFunction || comp.IdScaleFunction) &&
                (!idso.IdSelectionDirection || comp.IdSelectionDirection) &&
                (!idso.IdSvmKernel || comp.IdSvmKernel) &&
                (!idso.IdSvmType || comp.IdSvmType) &&
                (!idso.IdTotalGroups || comp.IdTotalGroups) &&
                (!idso.IdClassFolds || comp.IdClassFolds) &&
                (!idso.IdDownSampledTrainClassFolds || comp.IdDownSampledTrainClassFolds);

        }

        internal static IndexDataSearchOptions CompareReferenceData2(IndexData x1a, IndexData x2a)
        {
            var comp = new IndexDataSearchOptions();

            // primitives
            comp.IdCalcElevenPointThresholds = x1a.IdCalcElevenPointThresholds == x2a.IdCalcElevenPointThresholds;
            comp.IdExperimentName = x1a.IdExperimentName == x2a.IdExperimentName || (string.IsNullOrEmpty(x1a.IdExperimentName) && string.IsNullOrEmpty(x2a.IdExperimentName));
            comp.IdGroupArrayIndex = x1a.IdGroupArrayIndex == x2a.IdGroupArrayIndex;
            comp.IdGroupFolder = x1a.IdGroupFolder == x2a.IdGroupFolder || (string.IsNullOrEmpty(x1a.IdGroupFolder) && string.IsNullOrEmpty(x2a.IdGroupFolder));

            comp.IdInnerCvFolds = x1a.IdInnerCvFolds == x2a.IdInnerCvFolds;
            comp.IdIterationIndex = x1a.IdIterationIndex == x2a.IdIterationIndex;
            comp.IdNumColumns = x1a.IdNumColumns == x2a.IdNumColumns;
            comp.IdNumGroups = x1a.IdNumGroups == x2a.IdNumGroups;
            comp.IdOuterCvFolds = x1a.IdOuterCvFolds == x2a.IdOuterCvFolds;
            comp.IdOuterCvFoldsToRun = x1a.IdOuterCvFoldsToRun == x2a.IdOuterCvFoldsToRun;
            comp.IdRepetitions = x1a.IdRepetitions == x2a.IdRepetitions;
            comp.IdScaleFunction = x1a.IdScaleFunction == x2a.IdScaleFunction;
            comp.IdSelectionDirection = x1a.IdSelectionDirection == x2a.IdSelectionDirection;
            comp.IdSvmKernel = x1a.IdSvmKernel == x2a.IdSvmKernel;
            comp.IdSvmType = x1a.IdSvmType == x2a.IdSvmType;
            comp.IdTotalGroups = x1a.IdTotalGroups == x2a.IdTotalGroups;

            // special cases
            comp.IdGroupKey = x1a.IdGroupKey == x2a.IdGroupKey;

            comp.IdClassWeights = (x1a.IdClassWeights == null || x1a.IdClassWeights.Length == 0) ^ (x2a.IdClassWeights == null || x2a.IdClassWeights.Length == 0) ? false : x1a.IdClassWeights == x2a.IdClassWeights  || ((x1a.IdClassWeights == null || x1a.IdClassWeights.Length == 0) && (x2a.IdClassWeights == null || x2a.IdClassWeights.Length == 0)) || x1a.IdClassWeights.SequenceEqual(x2a.IdClassWeights);
            comp.IdGroupArrayIndexes = (x1a.IdGroupArrayIndexes == null || x1a.IdGroupArrayIndexes.Length == 0) ^ (x2a.IdGroupArrayIndexes == null || x2a.IdGroupArrayIndexes.Length == 0) ? false : x1a.IdGroupArrayIndexes == x2a.IdGroupArrayIndexes || ((x1a.IdGroupArrayIndexes == null || x1a.IdGroupArrayIndexes.Length == 0) && (x2a.IdGroupArrayIndexes == null || x2a.IdGroupArrayIndexes.Length == 0)) || x1a.IdGroupArrayIndexes.SequenceEqual(x2a.IdGroupArrayIndexes);
            comp.IdColumnArrayIndexes = (x1a.IdColumnArrayIndexes == null || x1a.IdColumnArrayIndexes.Length == 0) ^ (x2a.IdColumnArrayIndexes == null || x2a.IdColumnArrayIndexes.Length == 0) ? false : x1a.IdColumnArrayIndexes == x2a.IdColumnArrayIndexes || ((x1a.IdColumnArrayIndexes == null || x1a.IdColumnArrayIndexes.Length == 0) && (x2a.IdColumnArrayIndexes == null || x2a.IdColumnArrayIndexes.Length == 0)) || x1a.IdColumnArrayIndexes.SequenceEqual(x2a.IdColumnArrayIndexes);

            
            if ((x1a.IdClassFolds == null || x1a.IdClassFolds.Length == 0) ^ (x2a.IdClassFolds == null || x2a.IdClassFolds.Length == 0))
            {
                // one array null/empty, other array not null/empty
                comp.IdClassFolds = false;
            }
            else if (x1a.IdClassFolds == x2a.IdClassFolds || ((x1a.IdClassFolds == null || x1a.IdClassFolds.Length == 0) && (x2a.IdClassFolds == null || x2a.IdClassFolds.Length == 0)))
            {
                // same reference (both null or both same instance), or different reference and both arrays empty
                comp.IdClassFolds = true;
            }
            else
            {
                var x1aIdClassFoldsFlat =
                    x1a.IdClassFolds?
                        .Select(a => (a.ClassId, a.class_size, folds: a.folds?
                            .SelectMany(b => b.ClassSampleIndexes?.Select(c => (b.RepetitionsIndex, b.OuterCvIndex, ClassSampleIndexes: c)).ToArray() ?? Array.Empty<(int, int, int)>()).ToArray() ?? Array.Empty<(int, int, int)>()))
                        .SelectMany(a => a.folds?.Select(b => (a.ClassId, a.class_size, b.RepetitionsIndex, b.OuterCvIndex, b.ClassSampleIndexes)).ToArray() ?? Array.Empty<(int, int, int, int, int)>())
                        .SelectMany(a => new[] { a.ClassId, a.class_size, a.RepetitionsIndex, a.OuterCvIndex, a.ClassSampleIndexes })
                        .ToArray() ?? Array.Empty<int>();

                var x2aIdClassFoldsFlat =
                    x2a.IdClassFolds?
                        .Select(a => (a.ClassId, a.class_size, folds: a.folds?
                            .SelectMany(b => b.ClassSampleIndexes?.Select(c => (b.RepetitionsIndex, b.OuterCvIndex, ClassSampleIndexes: c)).ToArray() ?? Array.Empty<(int, int, int)>()).ToArray() ?? Array.Empty<(int, int, int)>()))
                        .SelectMany(a => a.folds?.Select(b => (a.ClassId, a.class_size, b.RepetitionsIndex, b.OuterCvIndex, b.ClassSampleIndexes)).ToArray() ?? Array.Empty<(int, int, int, int, int)>())
                        .SelectMany(a => new[] { a.ClassId, a.class_size, a.RepetitionsIndex, a.OuterCvIndex, a.ClassSampleIndexes })
                        .ToArray() ?? Array.Empty<int>();


                comp.IdClassFolds = (x1aIdClassFoldsFlat.SequenceEqual(x2aIdClassFoldsFlat));
            }


            if ((x1a.IdDownSampledTrainClassFolds == null || x1a.IdDownSampledTrainClassFolds.Length == 0) ^ (x2a.IdDownSampledTrainClassFolds == null || x2a.IdDownSampledTrainClassFolds.Length == 0))
            {
                // one array null/empty, other array not null/empty
                comp.IdDownSampledTrainClassFolds = false;
            }
            else if (x1a.IdDownSampledTrainClassFolds == x2a.IdDownSampledTrainClassFolds || ((x1a.IdDownSampledTrainClassFolds == null || x1a.IdDownSampledTrainClassFolds.Length == 0) && (x2a.IdDownSampledTrainClassFolds == null || x2a.IdDownSampledTrainClassFolds.Length == 0)))
            {
                // same reference, or different reference and both arrays empty
                comp.IdDownSampledTrainClassFolds = true;
            }
            else
            {
                var x1aIdDownSampledTrainClassFoldsFlat =
                    x1a.IdDownSampledTrainClassFolds?
                        .Select(a => (a.ClassId, a.class_size, folds: a.folds?
                            .SelectMany(b => b.ClassSampleIndexes?.Select(c => (b.RepetitionsIndex, b.OuterCvIndex, ClassSampleIndexes: c)).ToArray() ?? Array.Empty<(int, int, int)>()).ToArray() ?? Array.Empty<(int, int, int)>()))
                        .SelectMany(a => a.folds?.Select(b => (a.ClassId, a.class_size, b.RepetitionsIndex, b.OuterCvIndex, b.ClassSampleIndexes)).ToArray() ?? Array.Empty<(int, int, int, int, int)>())
                        .SelectMany(a => new[] { a.ClassId, a.class_size, a.RepetitionsIndex, a.OuterCvIndex, a.ClassSampleIndexes })
                        .ToArray() ?? Array.Empty<int>();

                var x2aIdDownSampledTrainClassFoldsFlat =
                    x2a.IdDownSampledTrainClassFolds?
                        .Select(a => (a.ClassId, a.class_size, folds: a.folds?
                            .SelectMany(b => b.ClassSampleIndexes?.Select(c => (b.RepetitionsIndex, b.OuterCvIndex, ClassSampleIndexes: c)).ToArray() ?? Array.Empty<(int, int, int)>()).ToArray() ?? Array.Empty<(int, int, int)>()))
                        .SelectMany(a => a.folds?.Select(b => (a.ClassId, a.class_size, b.RepetitionsIndex, b.OuterCvIndex, b.ClassSampleIndexes)).ToArray() ?? Array.Empty<(int, int, int, int, int)>())
                        .SelectMany(a => new[] { a.ClassId, a.class_size, a.RepetitionsIndex, a.OuterCvIndex, a.ClassSampleIndexes })
                        .ToArray() ?? Array.Empty<int>();


                comp.IdDownSampledTrainClassFolds = (x1aIdDownSampledTrainClassFoldsFlat.SequenceEqual(x2aIdDownSampledTrainClassFoldsFlat));
            }


            return comp;
        }

        internal class IndexDataSearchOptions
        {
            internal bool IdCalcElevenPointThresholds = true;
            internal bool IdClassWeights = true;
            internal bool IdColumnArrayIndexes = true;
            internal bool IdExperimentName = true;
            internal bool IdGroupArrayIndex = true;
            internal bool IdGroupArrayIndexes = true;
            internal bool IdGroupFolder = true;
            internal bool IdGroupKey = true;
            internal bool IdInnerCvFolds = true;
            internal bool IdIterationIndex = true;
            internal bool IdNumColumns = true;
            internal bool IdNumGroups = true;
            internal bool IdOuterCvFolds = true;
            internal bool IdOuterCvFoldsToRun = true;
            internal bool IdRepetitions = true;
            internal bool IdScaleFunction = true;
            internal bool IdSelectionDirection = true;
            internal bool IdSvmKernel = true;
            internal bool IdSvmType = true;
            internal bool IdTotalGroups = true;
            internal bool IdClassFolds = true;
            internal bool IdDownSampledTrainClassFolds = true;

            internal bool[] Values()
            {
                return new[]
                {
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
            }
        }


        //internal static index_data find_best_match_reference(index_data[] list, index_data data)
        //{
        //    if (data == null) return null;
        //
        //    // find proper index_data instance for this newly loaded ConfusionMatrix instance
        //    var sums = list.Select(id2 =>
        //            (id2.IterationIndex == data.IterationIndex ? 3 : 0) +
        //            (id2.group_array_index == data.group_array_index ? 3 : 0) +
        //            (id2.TotalGroups == data.TotalGroups ? 1 : 0) +
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
        //            (id2.ExperimentName == data.ExperimentName ? 1 : 0) +
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
        //    TotalGroups = index_data.TotalGroups;
        //    group_folder = index_data.group_folder;
        //
        //    //is_job_completed = index_data.is_job_completed;
        //    selection_direction = index_data.selection_direction;
        //    ExperimentName = index_data.ExperimentName;
        //    unrolled_whole_index = index_data.unrolled_whole_index;
        //    unrolled_partition_index = index_data.unrolled_partition_index;
        //    unrolled_InstanceId = index_data.unrolled_InstanceId;
        //    IterationIndex = index_data.IterationIndex;
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
        //    class_folds = index_data.class_folds?.Select(a => (a.ClassId, a.class_size, a.folds?.Select(b => (b.RepetitionsIndex, b.OuterCvIndex, b.ClassSampleIndexes?.ToArray())).ToArray())).ToArray();
        //    down_sampled_train_class_folds = index_data.down_sampled_train_class_folds?.Select(a => (a.ClassId, a.class_size, a.folds?.Select(b => (b.RepetitionsIndex, b.OuterCvIndex, b.ClassSampleIndexes?.ToArray())).ToArray())).ToArray();
        //
        //    total_whole_indexes = index_data.total_whole_indexes;
        //    total_partition_indexes = index_data.total_partition_indexes;
        //    TotalInstances = index_data.TotalInstances;
        //}
    }
}