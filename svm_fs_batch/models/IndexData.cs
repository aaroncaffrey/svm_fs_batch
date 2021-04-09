using System;
using System.Globalization;
using System.Linq;

namespace SvmFsBatch
{
    public class IndexData
    {
        public const string ModuleName = nameof(IndexData);
        public static readonly IndexData Empty = new IndexData();

        public static readonly string[] CsvHeaderValuesArray = DataSetGroupKey.CsvHeaderValuesArray.Select(a => /*"id_" +*/ a).ToArray().Concat(new[]
        {
            nameof(IdJobUid),
            nameof(IdIterationIndex),
            nameof(IdGroupArrayIndex),
            nameof(IdBaseLineDatasetFileTags),
            nameof(IdBaseLineColumnArrayIndexes),
            nameof(IdDatasetFileTags),
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

        public bool IdCalcElevenPointThresholds;

        public (int ClassId, string ClassName, int ClassSize, int DownSampledClassSize, int ClassFeatures, (int RepetitionsIndex, int OuterCvIndex, int[] ClassSampleIndexes)[] folds)[] IdClassFolds;

        public (int ClassId, double ClassWeight)[] IdClassWeights;
        public int[] IdColumnArrayIndexes;

        public (int ClassId, string ClassName, int ClassSize, int DownSampledClassSize, int ClassFeatures, (int RepetitionsIndex, int OuterCvIndex, int[] ClassSampleIndexes)[] folds)[] IdDownSampledTrainClassFolds;

        public string[] IdDatasetFileTags;
        public string[] IdBaseLineDatasetFileTags;
        public int[] IdBaseLineColumnArrayIndexes;

        public int IdJobUid;

        public string IdExperimentName;
        public int IdGroupArrayIndex = -1;
        public int[] IdGroupArrayIndexes;
        public string IdGroupFolder;

        public DataSetGroupKey IdGroupKey;
        public int IdInnerCvFolds = 5;
        public int IdIterationIndex = -1;
        public int IdNumColumns;
        public int IdNumGroups;
        public int IdOuterCvFolds = 5;
        public int IdOuterCvFoldsToRun = 1;
        public int IdRepetitions = 5;
        public Scaling.ScaleFunction IdScaleFunction = Scaling.ScaleFunction.Rescale;
        public Program.Direction IdSelectionDirection;
        public Routines.LibsvmKernelType IdSvmKernel = Routines.LibsvmKernelType.Rbf;
        public Routines.LibsvmSvmType IdSvmType = Routines.LibsvmSvmType.CSvc;
        public int IdTotalGroups = -1;

        public IndexData()
        {
            Logging.LogCall(ModuleName);

            Logging.LogExit(ModuleName);
        }


        public IndexData(string line) : this(new[] { line })
        {
            Logging.LogCall(ModuleName);

            Logging.LogExit(ModuleName);
        }

        public IndexData(string[] lines)
        {
            Logging.LogCall(ModuleName);

            if (lines == null || lines.Length < 1 || lines.Length > 2) { Logging.LogExit(ModuleName); return; }

            var lineHeader = lines.Length == 2
                ? lines.First().Split(',')
                : CsvHeaderValuesArray;

            SetValues(lineHeader, XTypes.GetXTypes(lines.Last().Split(',')));

            Logging.LogExit(ModuleName);
        }

        public IndexData(string[] lineHeader, string csvLine, int columnOffset = 0) : this(lineHeader, csvLine.Split(','), columnOffset)
        {
            Logging.LogCall(ModuleName);

            Logging.LogExit(ModuleName);
        }

        public IndexData(string[] lineHeader, string[] csvLine, int columnOffset = 0)
        {
            Logging.LogCall(ModuleName);

            //set_values(routines.x_types(null, csv_line.Skip(column_offset).ToArray(), false), 0);
            SetValues(lineHeader, XTypes.GetXTypes(csvLine.Skip(columnOffset).ToArray()));

            Logging.LogExit(ModuleName);
        }

        public IndexData(string[] lineHeader, XTypes[] xType, int columnOffset = 0)
        {
            Logging.LogCall(ModuleName);

            SetValues(lineHeader, xType, columnOffset);

            Logging.LogExit(ModuleName);
        }

        //public int unrolled_whole_index = -1;
        //public int unrolled_partition_index = -1;
        //public int unrolled_InstanceId = -1;
        //public int total_whole_indexes = -1;
        //public int total_partition_indexes = -1;
        //public int TotalInstances = -1;


        public void ClearSupplemental()
        {
            Logging.LogCall(ModuleName);

            IdClassFolds = null;
            IdDownSampledTrainClassFolds = null;

            Logging.LogExit(ModuleName);
        }


        public string[] CsvValuesArray()
        {
            Logging.LogCall(ModuleName);

            var x1 = IdGroupKey?.CsvValuesArray() ?? DataSetGroupKey.Empty.CsvValuesArray();

            var x2 = new[]
            {
                $@"{IdJobUid}",
                $@"{IdIterationIndex}",
                $@"{IdGroupArrayIndex}",

                $@"{string.Join(";", IdBaseLineDatasetFileTags ?? Array.Empty<string>())}",
                $@"{string.Join(";", IdBaseLineColumnArrayIndexes ?? Array.Empty<int>())}",
                $@"{string.Join(";", IdDatasetFileTags ?? Array.Empty<string>())}",


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
                $@"{string.Join(";", IdClassFolds?.Select(a => string.Join(":", $@"{a.ClassId}", $@"{a.ClassName}", $@"{a.ClassSize}", $@"{a.DownSampledClassSize}", $@"{a.ClassFeatures}", $@"{string.Join("|", a.folds?.Select(b => string.Join("~", $@"{b.RepetitionsIndex}", $@"{b.OuterCvIndex}", $@"{string.Join("/", b.ClassSampleIndexes ?? Array.Empty<int>())}")).ToArray() ?? Array.Empty<string>())}")).ToArray() ?? Array.Empty<string>())}",
                $@"{string.Join(";", IdDownSampledTrainClassFolds?.Select(a => string.Join(":", $@"{a.ClassId}", $@"{a.ClassName}", $@"{a.ClassSize}", $@"{a.DownSampledClassSize}", $@"{a.ClassFeatures}", $@"{string.Join("|", a.folds?.Select(b => string.Join("~", $@"{b.RepetitionsIndex}", $@"{b.OuterCvIndex}", $@"{string.Join("/", b.ClassSampleIndexes ?? Array.Empty<int>())}")).ToArray() ?? Array.Empty<string>())}")).ToArray() ?? Array.Empty<string>())}",
                $@"{IdGroupFolder}"
            };

            var ret = x1.Concat(x2).Select(a => a == null
                ? ""
                : a.Replace(',', ';')).ToArray();

            //#if DEBUG
            //            var str = string.Join(",", x3);
            //            var id2 = new  IndexData(str);
            //            var re = CompareReferenceData2(this, id2);
            //            if (!re.AllTrue())
            //            {
            //                Logging.LogEvent("!!! INDEXDATA NOT EQUAL !!!");
            //            }
            //#endif
            Logging.LogExit(ModuleName);
            return ret;
        }

        public void SetValues(string[] lineHeader, XTypes[] xType, int columnOffset = 0)
        {
            Logging.LogCall(ModuleName);

            var k = columnOffset;

            //var headerIndexes = CsvHeaderValuesArray.Select((h, i) => (header: h, index: lineHeader.Length > 0
            //    ? Array.FindIndex(lineHeader, a => a.EndsWith(h))
            //    : columnOffset + i)).ToArray();

            var hasHeader = lineHeader != null && lineHeader.Length > 0;

            var headerIndexes = CsvHeaderValuesArray.Select((h, i) =>
            {
                var index = columnOffset + i;
                if (hasHeader)
                {
                    var indexFirst = Array.FindIndex(lineHeader, columnOffset, a => a.EndsWith(h));
                    var indexLast = Array.FindLastIndex(lineHeader, a => a.EndsWith(h));

                    if (indexFirst != indexLast) throw new Exception();

                    index = indexFirst;
                }
                return (header: h, index: index);
            }).ToArray();

            int Hi(string name)
            {
                var matches = headerIndexes.Where(a => a.header.EndsWith(name, StringComparison.OrdinalIgnoreCase)).ToArray();
                if (matches.Length == 1) return matches[0].index;
                throw new Exception();
            }

            // todo: lookup actual gkGroup key instance - not necessary, since the parent index_data will be looked up
            //[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, x_type[hi(nameof(___))].asStr, int.TryParse(x_type[hi(nameof(___))].asStr, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var tp_int1) ? tp_int1 : -1);

            IdGroupKey = new DataSetGroupKey(lineHeader, xType);

            if (IdGroupKey.Value == default) IdGroupKey = null;

            var hiIdJobUid = Hi(nameof(IdJobUid));
            var hiIdIterationIndex = Hi(nameof(IdIterationIndex));
            var hiIdGroupArrayIndex = Hi(nameof(IdGroupArrayIndex));

            var hiIdBaseLineDatasetFileTags = Hi(nameof(IdBaseLineDatasetFileTags));
            var hiIdBaseLineColumnArrayIndexes = Hi(nameof(IdBaseLineColumnArrayIndexes));
            var hiIdDatasetFileTags = Hi(nameof(IdDatasetFileTags));

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

            if (hiIdJobUid > -1) IdJobUid = xType[hiIdJobUid].AsInt ?? -1;
            if (hiIdIterationIndex > -1) IdIterationIndex = xType[hiIdIterationIndex].AsInt ?? -1;
            if (hiIdGroupArrayIndex > -1) IdGroupArrayIndex = xType[hiIdGroupArrayIndex].AsInt ?? -1;

            if (hiIdBaseLineDatasetFileTags > -1) IdBaseLineDatasetFileTags = xType[hiIdBaseLineDatasetFileTags].AsStr?.Split(new char[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries) ?? Array.Empty<string>();
            if (hiIdBaseLineColumnArrayIndexes > -1) IdBaseLineColumnArrayIndexes = xType[hiIdBaseLineColumnArrayIndexes].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => int.Parse(a, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();
            if (hiIdDatasetFileTags > -1) IdDatasetFileTags = xType[hiIdDatasetFileTags].AsStr?.Split(new char[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries) ?? Array.Empty<string>();

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
                    Logging.LogExit(ModuleName); return (int.Parse(b[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo), double.Parse(b[1], NumberStyles.Float, NumberFormatInfo.InvariantInfo));
                }).ToArray();

            //  ;:|~/
            if (hiIdClassFolds > -1)
                IdClassFolds = xType[hiIdClassFolds].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a =>
                {
                    var b = a.Split(':', StringSplitOptions.RemoveEmptyEntries);
                    var classId = int.Parse(b[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                    var className = b[1];
                    var classSize = int.Parse(b[2], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                    var downSampledClassSize = int.Parse(b[3], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                    var classFeatures = int.Parse(b[4], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                    var folds = b[5].Split('|', StringSplitOptions.RemoveEmptyEntries).Select(d =>
                    {
                        var e = d.Split('~', StringSplitOptions.RemoveEmptyEntries);
                        var repetitionsIndex = int.Parse(e[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                        var outerCvIndex = int.Parse(e[1], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                        var classSampleIndexes = e[2].Split('/', StringSplitOptions.RemoveEmptyEntries).Select(f => int.Parse(f, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();

                        Logging.LogExit(ModuleName); return (RepetitionsIndex: repetitionsIndex, OuterCvIndex: outerCvIndex, ClassSampleIndexes: classSampleIndexes);
                    }).ToArray();
                    Logging.LogExit(ModuleName); return (ClassId: classId, ClassName: className, ClassSize: classSize, DownSampledClassSize: downSampledClassSize, ClassFeatures: classFeatures, folds);
                }).ToArray();

            //  ;:|~/
            if (hiIdDownSampledTrainClassFolds > -1)
                IdDownSampledTrainClassFolds = xType[hiIdDownSampledTrainClassFolds].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a =>
                {
                    var b = a.Split(':', StringSplitOptions.RemoveEmptyEntries);
                    var classId = int.Parse(b[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                    var className = b[1];
                    var classSize = int.Parse(b[2], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                    var downSampledClassSize = int.Parse(b[3], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                    var classFeatures = int.Parse(b[4], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                    var folds = b[5].Split('|', StringSplitOptions.RemoveEmptyEntries).Select(d =>
                    {
                        var e = d.Split('~', StringSplitOptions.RemoveEmptyEntries);
                        var repetitionsIndex = int.Parse(e[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                        var outerCvIndex = int.Parse(e[1], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
                        var classSampleIndexes = e[2].Split('/', StringSplitOptions.RemoveEmptyEntries).Select(f => int.Parse(f, NumberStyles.Integer, NumberFormatInfo.InvariantInfo)).ToArray();

                        Logging.LogExit(ModuleName); return (RepetitionsIndex: repetitionsIndex, OuterCvIndex: outerCvIndex, ClassSampleIndexes: classSampleIndexes);
                    }).ToArray();
                    Logging.LogExit(ModuleName); return (ClassId: classId, ClassName: className, ClassSize: classSize, DownSampledClassSize: downSampledClassSize, ClassFeatures: classFeatures, folds);
                }).ToArray();

            if (hiIdGroupFolder > -1) IdGroupFolder = xType[hiIdGroupFolder].AsStr;

            Logging.LogExit(ModuleName);
        }

        public string CsvValuesString()
        {
            Logging.LogCall(ModuleName);

            var ret = string.Join(",", CsvValuesArray());

            Logging.LogExit(ModuleName);
            return ret;
        }

        public string IdIndexStr()
        {
            Logging.LogCall(ModuleName);

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

            var ret = @"[" + string.Join(", ",
                list.Select(a => $@"{a.name}={a.value}" + (!string.IsNullOrWhiteSpace(a.value_max)
                    ? $@"/{a.value_max}"
                    : @"")).ToList()) + @"]";

            Logging.LogExit(ModuleName);
            return ret;
        }

        public string IdMlStr()
        {
            Logging.LogCall(ModuleName);

            var list = new (string name, string value, string value_max)[]
            {
                (nameof(IdSvmType), $@"{IdSvmType}", @""),
                (nameof(IdSvmKernel), $@"{IdSvmKernel}", @""),
                (nameof(IdScaleFunction), $@"{IdScaleFunction}", @""),
                (nameof(IdClassWeights), $@"{(IdClassWeights != null ? string.Join(@"; ", IdClassWeights.Select(a => $@"w{(a.ClassId > 0 ? @"+" : @"")} {a.ClassWeight}").ToList()) : "")}", ""),
                (nameof(IdCalcElevenPointThresholds), $@"{IdCalcElevenPointThresholds}", "")
            };

            var ret = @"[" + string.Join(", ",
                list.Select(a => $@"{a.name}={a.value}" + (!string.IsNullOrWhiteSpace(a.value_max)
                    ? $@"/{a.value_max}"
                    : "")).ToList()) + @"]";

            Logging.LogExit(ModuleName);
            return ret;
        }

        public string IdFoldStr()
        {
            Logging.LogCall(ModuleName);

            var list = new (string name, string value, string value_max)[]
            {
                (nameof(IdRepetitions), $@"{IdRepetitions}", ""),
                (nameof(IdOuterCvFolds), $@"{IdOuterCvFolds}", ""),
                (nameof(IdOuterCvFoldsToRun), $@"{IdOuterCvFoldsToRun}", ""),
                (nameof(IdInnerCvFolds), $@"{IdInnerCvFolds}", "")
            };

            var ret = @"[" + string.Join(", ",
                list.Select(a => $@"{a.name}={a.value}" + (!string.IsNullOrWhiteSpace(a.value_max)
                    ? $@"/{a.value_max}"
                    : "")).ToList()) + @"]";

            Logging.LogExit(ModuleName);
            return ret;
        }


        public static (int index, IndexData id) FindFirstReference((int index, IndexData id)[] list, IndexData data1, IndexDataSearchOptions idso = null)
        {
            Logging.LogCall(ModuleName);

            if (data1 == null) { Logging.LogExit(ModuleName); return default; }

            if (list.Length > data1.IdJobUid && (list[data1.IdJobUid].id == data1 || CompareReferenceData(data1, list[data1.IdJobUid].id, idso))) return list[data1.IdJobUid];

            // find proper index_data instance for this newly loaded ConfusionMatrix instance
            var ret = list.AsParallel().AsOrdered().FirstOrDefault(data2 => CompareReferenceData(data1, data2.id, idso));

            Logging.LogExit(ModuleName);
            return ret;
        }


        public static (int index, IndexData id) FindLastReference((int index, IndexData id)[] list, IndexData data1, IndexDataSearchOptions idso = null)
        {
            Logging.LogCall(ModuleName);

            if (data1 == null) { Logging.LogExit(ModuleName); return default; }

            if (list.Length > data1.IdJobUid && (list[data1.IdJobUid].id == data1 || CompareReferenceData(data1, list[data1.IdJobUid].id, idso))) return list[data1.IdJobUid];

            // find proper index_data instance for this newly loaded ConfusionMatrix instance
            var ret = list.LastOrDefault(data2 => CompareReferenceData(data1, data2.id, idso));

            Logging.LogExit(ModuleName);
            return ret;
        }

        public static IndexData FindFirstReference(IndexData[] list, IndexData data1, IndexDataSearchOptions idso = null)
        {
            Logging.LogCall(ModuleName);

            if (data1 == default)
            {
                Logging.LogEvent("data1 was default");
                Logging.LogExit(ModuleName);
                return null;
            }

            if (list.Length > data1.IdJobUid && (list[data1.IdJobUid] == data1 || CompareReferenceData(data1, list[data1.IdJobUid], idso))) return list[data1.IdJobUid];

            // find proper index_data instance for this newly loaded ConfusionMatrix instance
            var ret = list.AsParallel().AsOrdered().FirstOrDefault(data2 => CompareReferenceData(data1, data2, idso));

            Logging.LogExit(ModuleName);
            return ret;
        }

        public static IndexData FindLastReference(IndexData[] list, IndexData data1, IndexDataSearchOptions idso = null)
        {
            Logging.LogCall(ModuleName);

            if (data1 == null) { Logging.LogExit(ModuleName); return null; }

            if (list.Length > data1.IdJobUid && (list[data1.IdJobUid] == data1 || CompareReferenceData(data1, list[data1.IdJobUid], idso))) return list[data1.IdJobUid];

            // find proper index_data instance for this newly loaded ConfusionMatrix instance
            var ret = list.LastOrDefault(data2 => CompareReferenceData(data1, data2, idso));

            Logging.LogExit(ModuleName);
            return ret;
        }

        private static IndexDataSearchOptions idso = new IndexDataSearchOptions();

        public static bool CompareReferenceData(IndexData data1, IndexData data2, IndexDataSearchOptions idso = null)
        {
            //Logging.LogCall(ModuleName);

            if (ReferenceEquals(data1, data2)) return true;

            var comp = CompareReferenceData2(data1, data2, idso);


            if (idso == null)
            {
                idso = IndexData.idso;
            }

            if (idso == null || idso.AllTrue())
            {
                //Logging.LogExit(ModuleName);
                return comp.AllTrue();
            }

            var ret =
                (!idso.IdCalcElevenPointThresholds || comp.IdCalcElevenPointThresholds) &&
                (!idso.IdClassWeights || comp.IdClassWeights) &&
                (!idso.IdColumnArrayIndexes || comp.IdColumnArrayIndexes) &&

                (!idso.IdBaseLineDatasetFileTags || comp.IdBaseLineDatasetFileTags) &&
                (!idso.IdBaseLineColumnArrayIndexes || comp.IdBaseLineColumnArrayIndexes) &&
                (!idso.IdDatasetFileTags || comp.IdDatasetFileTags) &&


                (!idso.IdExperimentName || comp.IdExperimentName) &&
                (!idso.IdGroupArrayIndex || comp.IdGroupArrayIndex) &&
                (!idso.IdGroupArrayIndexes || comp.IdGroupArrayIndexes) &&
                (!idso.IdGroupFolder || comp.IdGroupFolder) &&
                (!idso.IdGroupKey || comp.IdGroupKey) &&
                (!idso.IdInnerCvFolds || comp.IdInnerCvFolds) &&
                (!idso.IdJobUid || comp.IdJobUid) &&
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

            //Logging.LogExit(ModuleName);
            return ret;
        }

        public static IndexDataSearchOptions CompareReferenceData2(IndexData x1a, IndexData x2a, IndexDataSearchOptions idso = null)
        {
            Logging.LogCall(ModuleName);

            var ret = new IndexDataSearchOptions(false);


            // primitives
            ret.IdJobUid = x1a.IdJobUid == x2a.IdJobUid;



            ret.IdCalcElevenPointThresholds = x1a.IdCalcElevenPointThresholds == x2a.IdCalcElevenPointThresholds;
            ret.IdGroupArrayIndex = x1a.IdGroupArrayIndex == x2a.IdGroupArrayIndex;
            ret.IdInnerCvFolds = x1a.IdInnerCvFolds == x2a.IdInnerCvFolds;
            ret.IdIterationIndex = x1a.IdIterationIndex == x2a.IdIterationIndex;
            ret.IdNumColumns = x1a.IdNumColumns == x2a.IdNumColumns;
            ret.IdNumGroups = x1a.IdNumGroups == x2a.IdNumGroups;
            ret.IdOuterCvFolds = x1a.IdOuterCvFolds == x2a.IdOuterCvFolds;
            ret.IdOuterCvFoldsToRun = x1a.IdOuterCvFoldsToRun == x2a.IdOuterCvFoldsToRun;
            ret.IdRepetitions = x1a.IdRepetitions == x2a.IdRepetitions;
            ret.IdScaleFunction = x1a.IdScaleFunction == x2a.IdScaleFunction;
            ret.IdSelectionDirection = x1a.IdSelectionDirection == x2a.IdSelectionDirection;
            ret.IdSvmKernel = x1a.IdSvmKernel == x2a.IdSvmKernel;
            ret.IdSvmType = x1a.IdSvmType == x2a.IdSvmType;
            ret.IdTotalGroups = x1a.IdTotalGroups == x2a.IdTotalGroups;

            // strings
            if ((idso == null || idso.IdExperimentName)) ret.IdExperimentName = (string.IsNullOrEmpty(x1a.IdExperimentName) && string.IsNullOrEmpty(x2a.IdExperimentName)) || string.Equals(x1a.IdExperimentName, x2a.IdExperimentName, StringComparison.OrdinalIgnoreCase);
            if ((idso == null || idso.IdGroupFolder)) ret.IdGroupFolder = (string.IsNullOrEmpty(x1a.IdGroupFolder) && string.IsNullOrEmpty(x2a.IdGroupFolder)) || string.Equals(x1a.IdGroupFolder, x2a.IdGroupFolder, StringComparison.OrdinalIgnoreCase);

            if (idso == null || idso.IdBaseLineDatasetFileTags) ret.IdBaseLineDatasetFileTags = /*(x1a.IdBaseLineDatasetFileTags?.Length??0) == (x2a.IdBaseLineDatasetFileTags?.Length??0) &&*/ (x1a.IdBaseLineDatasetFileTags == x2a.IdBaseLineDatasetFileTags) || (x1a.IdBaseLineDatasetFileTags ?? Array.Empty<string>()).SequenceEqual(x2a.IdBaseLineDatasetFileTags ?? Array.Empty<string>());
            if (idso == null || idso.IdBaseLineColumnArrayIndexes) ret.IdBaseLineColumnArrayIndexes = (x1a.IdBaseLineColumnArrayIndexes == x2a.IdBaseLineColumnArrayIndexes) || ((x1a.IdBaseLineColumnArrayIndexes ?? Array.Empty<int>()).SequenceEqual(x2a.IdBaseLineColumnArrayIndexes ?? Array.Empty<int>()));
            if (idso == null || idso.IdDatasetFileTags) ret.IdDatasetFileTags = /*(x1a.IdDatasetFileTags?.Length??0) == (x2a.IdDatasetFileTags?.Length??0) &&*/ (x1a.IdDatasetFileTags == x2a.IdDatasetFileTags) || (x1a.IdDatasetFileTags ?? Array.Empty<string>()).SequenceEqual(x2a.IdDatasetFileTags ?? Array.Empty<string>());

            // special cases
            if (idso == null || idso.IdGroupKey) ret.IdGroupKey = x1a.IdGroupKey == x2a.IdGroupKey;
            //if (idso == null || idso.IdClassWeights) ret.IdClassWeights = (x1a.IdClassWeights == null || x1a.IdClassWeights.Length == 0) ^ (x2a.IdClassWeights == null || x2a.IdClassWeights.Length == 0) ? false : x1a.IdClassWeights == x2a.IdClassWeights || ((x1a.IdClassWeights == null || x1a.IdClassWeights.Length == 0) && (x2a.IdClassWeights == null || x2a.IdClassWeights.Length == 0)) || x1a.IdClassWeights.SequenceEqual(x2a.IdClassWeights);
            if (idso == null || idso.IdClassWeights) ret.IdClassWeights = (x1a.IdClassWeights == x2a.IdClassWeights) || (x1a.IdClassWeights ?? Array.Empty<(int ClassId, double ClassWeight)>()).SequenceEqual(x2a.IdClassWeights ?? Array.Empty<(int ClassId, double ClassWeight)>());
            //if (idso == null || idso.IdGroupArrayIndexes) ret.IdGroupArrayIndexes = (x1a.IdGroupArrayIndexes == null || x1a.IdGroupArrayIndexes.Length == 0) ^ (x2a.IdGroupArrayIndexes == null || x2a.IdGroupArrayIndexes.Length == 0) ? false : x1a.IdGroupArrayIndexes == x2a.IdGroupArrayIndexes || ((x1a.IdGroupArrayIndexes == null || x1a.IdGroupArrayIndexes.Length == 0) && (x2a.IdGroupArrayIndexes == null || x2a.IdGroupArrayIndexes.Length == 0)) || x1a.IdGroupArrayIndexes.SequenceEqual(x2a.IdGroupArrayIndexes);
            //if (idso == null || idso.IdColumnArrayIndexes) ret.IdColumnArrayIndexes = (x1a.IdColumnArrayIndexes == null || x1a.IdColumnArrayIndexes.Length == 0) ^ (x2a.IdColumnArrayIndexes == null || x2a.IdColumnArrayIndexes.Length == 0) ? false : x1a.IdColumnArrayIndexes == x2a.IdColumnArrayIndexes || ((x1a.IdColumnArrayIndexes == null || x1a.IdColumnArrayIndexes.Length == 0) && (x2a.IdColumnArrayIndexes == null || x2a.IdColumnArrayIndexes.Length == 0)) || x1a.IdColumnArrayIndexes.SequenceEqual(x2a.IdColumnArrayIndexes);

            //if (idso == null || idso.IdGroupArrayIndexes) ret.IdGroupArrayIndexes = !((x1a.IdGroupArrayIndexes == null || x1a.IdGroupArrayIndexes.Length == 0) ^ (x2a.IdGroupArrayIndexes == null || x2a.IdGroupArrayIndexes.Length == 0)) && (x1a.IdGroupArrayIndexes == x2a.IdGroupArrayIndexes || ((x1a.IdGroupArrayIndexes == null || x1a.IdGroupArrayIndexes.Length == 0) && (x2a.IdGroupArrayIndexes == null || x2a.IdGroupArrayIndexes.Length == 0)) || x1a.IdGroupArrayIndexes.SequenceEqual(x2a.IdGroupArrayIndexes));
            //if (idso == null || idso.IdColumnArrayIndexes) ret.IdColumnArrayIndexes = !((x1a.IdColumnArrayIndexes == null || x1a.IdColumnArrayIndexes.Length == 0) ^ (x2a.IdColumnArrayIndexes == null || x2a.IdColumnArrayIndexes.Length == 0)) && (x1a.IdColumnArrayIndexes == x2a.IdColumnArrayIndexes || ((x1a.IdColumnArrayIndexes == null || x1a.IdColumnArrayIndexes.Length == 0) && (x2a.IdColumnArrayIndexes == null || x2a.IdColumnArrayIndexes.Length == 0)) || x1a.IdColumnArrayIndexes.SequenceEqual(x2a.IdColumnArrayIndexes));

            if (idso == null || idso.IdGroupArrayIndexes) ret.IdGroupArrayIndexes = (x1a.IdGroupArrayIndexes == x2a.IdGroupArrayIndexes) || (x1a.IdGroupArrayIndexes ?? Array.Empty<int>()).SequenceEqual(x2a.IdGroupArrayIndexes ?? Array.Empty<int>());
            if (idso == null || idso.IdColumnArrayIndexes) ret.IdColumnArrayIndexes = (x1a.IdColumnArrayIndexes == x2a.IdColumnArrayIndexes) || (x1a.IdColumnArrayIndexes ?? Array.Empty<int>()).SequenceEqual(x2a.IdColumnArrayIndexes ?? Array.Empty<int>());


            if (idso == null || idso.IdClassFolds)
            {
                if ((x1a.IdClassFolds?.Length ?? 0) != (x2a.IdClassFolds?.Length ?? 0))
                {
                    // different length
                    ret.IdClassFolds = false;
                }
                else
                {
                    ret.IdClassFolds = true;

                    // check not same reference, check length > 0 (already know same length from previous if)
                    if (x1a.IdClassFolds != x2a.IdClassFolds && ((x1a.IdClassFolds?.Length ?? 0) > 0) && ((x2a.IdClassFolds?.Length ?? 0) > 0))
                    {

                        for (var i = 0; i < x1a.IdClassFolds.Length; i++)
                        {
                            if (x1a.IdClassFolds[i].ClassId != x2a.IdClassFolds[i].ClassId || x1a.IdClassFolds[i].ClassSize != x2a.IdClassFolds[i].ClassSize || (x1a.IdClassFolds[i].folds?.Length ?? 0) != (x2a.IdClassFolds[i].folds?.Length ?? 0) || !string.Equals(x1a.IdClassFolds[i].ClassName, x2a.IdClassFolds[i].ClassName, StringComparison.OrdinalIgnoreCase))
                            {
                                ret.IdClassFolds = false;
                                break;
                            }

                            for (var k = 0; k < (x1a.IdClassFolds[i].folds?.Length ?? 0); k++)
                            {
                                if (x1a.IdClassFolds[i].folds[k].OuterCvIndex != x2a.IdClassFolds[i].folds[k].OuterCvIndex || x1a.IdClassFolds[i].folds[k].RepetitionsIndex != x2a.IdClassFolds[i].folds[k].RepetitionsIndex || (x1a.IdClassFolds[i].folds[k].ClassSampleIndexes?.Length ?? 0) != (x2a.IdClassFolds[i].folds[k].ClassSampleIndexes?.Length ?? 0) || !x1a.IdClassFolds[i].folds[k].ClassSampleIndexes.SequenceEqual(x2a.IdClassFolds[i].folds[k].ClassSampleIndexes))
                                {
                                    ret.IdClassFolds = false;
                                    break;
                                }
                            }
                        }
                    }

                }
            }

            if (idso == null || idso.IdDownSampledTrainClassFolds)
            {
                if ((x1a.IdDownSampledTrainClassFolds?.Length ?? 0) != (x2a.IdDownSampledTrainClassFolds?.Length ?? 0))
                {
                    // different length
                    ret.IdDownSampledTrainClassFolds = false;
                }
                else
                {
                    ret.IdDownSampledTrainClassFolds = true;

                    // check not same reference, check length > 0 (already know same length from previous if)
                    if (x1a.IdDownSampledTrainClassFolds != x2a.IdDownSampledTrainClassFolds && ((x1a.IdDownSampledTrainClassFolds?.Length ?? 0) > 0) && ((x2a.IdDownSampledTrainClassFolds?.Length ?? 0) > 0))
                    {

                        for (var i = 0; i < x1a.IdDownSampledTrainClassFolds.Length; i++)
                        {
                            if (x1a.IdDownSampledTrainClassFolds[i].ClassId != x2a.IdDownSampledTrainClassFolds[i].ClassId || x1a.IdDownSampledTrainClassFolds[i].ClassSize != x2a.IdDownSampledTrainClassFolds[i].ClassSize || (x1a.IdDownSampledTrainClassFolds[i].folds?.Length ?? 0) != (x2a.IdDownSampledTrainClassFolds[i].folds?.Length ?? 0) || !string.Equals(x1a.IdDownSampledTrainClassFolds[i].ClassName, x2a.IdDownSampledTrainClassFolds[i].ClassName, StringComparison.OrdinalIgnoreCase))
                            {
                                ret.IdDownSampledTrainClassFolds = false;
                                break;
                            }

                            for (var k = 0; k < (x1a.IdDownSampledTrainClassFolds[i].folds?.Length ?? 0); k++)
                            {
                                if (x1a.IdDownSampledTrainClassFolds[i].folds[k].OuterCvIndex != x2a.IdDownSampledTrainClassFolds[i].folds[k].OuterCvIndex || x1a.IdDownSampledTrainClassFolds[i].folds[k].RepetitionsIndex != x2a.IdDownSampledTrainClassFolds[i].folds[k].RepetitionsIndex || (x1a.IdDownSampledTrainClassFolds[i].folds[k].ClassSampleIndexes?.Length ?? 0) != (x2a.IdDownSampledTrainClassFolds[i].folds[k].ClassSampleIndexes?.Length ?? 0) || !x1a.IdDownSampledTrainClassFolds[i].folds[k].ClassSampleIndexes.SequenceEqual(x2a.IdDownSampledTrainClassFolds[i].folds[k].ClassSampleIndexes))
                                {
                                    ret.IdDownSampledTrainClassFolds = false;
                                    break;
                                }
                            }
                        }
                    }

                }
            }

            Logging.LogExit(ModuleName);
            return ret;
        }


        //public static index_data find_best_match_reference(index_data[] list, index_data data)
        //{
        //    if (data == null) { Logging.LogExit(ModuleName);  return null; }
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
        //    Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :id;
        //}


        //public index_data(index_data index_data)
        //{
        //    if (index_data == null) { Logging.LogExit(ModuleName); return; }
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
        //    class_folds = index_data.class_folds?.Select(a => (a.ClassId, a.ClassSize, a.folds?.Select(b => (b.RepetitionsIndex, b.OuterCvIndex, b.ClassSampleIndexes?.ToArray())).ToArray())).ToArray();
        //    down_sampled_train_class_folds = index_data.down_sampled_train_class_folds?.Select(a => (a.ClassId, a.ClassSize, a.folds?.Select(b => (b.RepetitionsIndex, b.OuterCvIndex, b.ClassSampleIndexes?.ToArray())).ToArray())).ToArray();
        //
        //    total_whole_indexes = index_data.total_whole_indexes;
        //    total_partition_indexes = index_data.total_partition_indexes;
        //    TotalInstances = index_data.TotalInstances;
        //}
    }
}