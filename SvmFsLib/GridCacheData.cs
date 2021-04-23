using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsLib
{
    public class GridCacheData
    {
        public const string ModuleName = nameof(GridCacheData);
        //public double rate;


        public static readonly string[] CsvHeaderValuesArray =
        {
            nameof(SvmType),
            nameof(SvmKernel),
            nameof(Repetitions),
            nameof(RepetitionsIndex),
            nameof(OuterCvFolds),
            //nameof(outer_cv_folds_to_run),
            nameof(OuterCvIndex),
            nameof(InnerCvFolds),
            nameof(ProbabilityEstimates),
            nameof(ShrinkingHeuristics),
            nameof(global::SvmFsLib.GridPoint.GpCost),
            nameof(global::SvmFsLib.GridPoint.GpGamma),
            nameof(global::SvmFsLib.GridPoint.GpEpsilon),
            nameof(global::SvmFsLib.GridPoint.GpCoef0),
            nameof(global::SvmFsLib.GridPoint.GpDegree),
            nameof(global::SvmFsLib.GridPoint.GpCvRate)
        };

        public static readonly string CsvHeaderString = string.Join(",", CsvHeaderValuesArray);
        public GridPoint GridPoint;
        public int InnerCvFolds;

        public int OuterCvFolds;

        //public int outer_cv_folds_to_run;
        public int OuterCvIndex;
        public bool ProbabilityEstimates;
        public int Repetitions;
        public int RepetitionsIndex;
        public bool ShrinkingHeuristics;
        public Routines.LibsvmKernelType SvmKernel;

        public Routines.LibsvmSvmType SvmType;

        public GridCacheData()
        {
            Logging.LogCall(ModuleName);
        }

        public GridCacheData(string[] line)
        {
            Logging.LogCall(ModuleName);

            var k = -1;

            SvmType = (Routines.LibsvmSvmType) Enum.Parse(typeof(Routines.LibsvmSvmType), line[++k]);
            SvmKernel = (Routines.LibsvmKernelType) Enum.Parse(typeof(Routines.LibsvmKernelType), line[++k]);
            Repetitions = int.Parse(line[++k], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            RepetitionsIndex = int.Parse(line[++k], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            OuterCvFolds = int.Parse(line[++k], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            //outer_cv_folds_to_run = inr.Parse(line[++k], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            OuterCvIndex = int.Parse(line[++k], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            InnerCvFolds = int.Parse(line[++k], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            ProbabilityEstimates = bool.Parse(line[++k]);
            ShrinkingHeuristics = bool.Parse(line[++k]);
            GridPoint = new GridPoint
            {
                GpCost = double.TryParse(line[++k], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var pCost)
                    ? pCost
                    : (double?) null,
                GpGamma = double.TryParse(line[++k], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var pGamma)
                    ? pGamma
                    : (double?) null,
                GpEpsilon = double.TryParse(line[++k], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var pEpsilon)
                    ? pEpsilon
                    : (double?) null,
                GpCoef0 = double.TryParse(line[++k], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var pCoef0)
                    ? pCoef0
                    : (double?) null,
                GpDegree = double.TryParse(line[++k], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var pDegree)
                    ? pDegree
                    : (double?) null,
                GpCvRate = double.TryParse(line[++k], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var pRate)
                    ? pRate
                    : 0d
            };
        }

        public string CsvValuesString()
        {
            Logging.LogCall(ModuleName);

            Logging.LogExit(ModuleName); return string.Join(",", CsvValuesArray());
        }

        public string[] CsvValuesArray()
        {
            Logging.LogCall(ModuleName);

            Logging.LogExit(ModuleName); return new[]
            {
                $@"{SvmType}",
                $@"{SvmKernel}",
                $@"{Repetitions}",
                $@"{RepetitionsIndex}",
                $@"{OuterCvFolds}",
                //$@"{outer_cv_folds_to_run}",
                $@"{OuterCvIndex}",
                $@"{InnerCvFolds}",
                $@"{ProbabilityEstimates}",
                $@"{ShrinkingHeuristics}",
                $@"{GridPoint.GpCost:G17}",
                $@"{GridPoint.GpGamma:G17}",
                $@"{GridPoint.GpEpsilon:G17}",
                $@"{GridPoint.GpCoef0:G17}",
                $@"{GridPoint.GpDegree:G17}",
                $@"{GridPoint.GpCvRate:G17}"
            }.Select(a => a.Replace(",", ";", StringComparison.OrdinalIgnoreCase)).ToArray();
        }

        public static async Task<GridCacheData[]> ReadCacheFileAsync(string cacheTrainGridCsv, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            const string MethodName = nameof(ReadCacheFileAsync);

            

            var cache = Array.Empty<GridCacheData>();

            if (await IoProxy.IsFileAvailableAsync(true, ct, cacheTrainGridCsv, false, callerModuleName: ModuleName, callerMethodName: MethodName).ConfigureAwait(false))
                cache = (await IoProxy.ReadAllLinesAsync(true, ct, cacheTrainGridCsv, callerModuleName: ModuleName, callerMethodName: MethodName).ConfigureAwait(false) ?? Array.Empty<string>()).Skip(1 /* skip header line */).Select(a =>
                {
                    try
                    {
                        var line = a.Split(',');

                        Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :new GridCacheData(line);
                    }
                    catch (Exception e)
                    {
                        Logging.LogException(e, "", ModuleName, MethodName);
                        Logging.LogExit(ModuleName); return default;
                    }
                }).ToArray();

            cache = cache.Where(a => a.GridPoint.GpCvRate > 0).ToArray();

            Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :cache;
        }

        public static async Task WriteCacheFileAsync(string cacheTrainGridCsv, GridCacheData[] gridCacheDataList, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            const string MethodName = nameof(WriteCacheFileAsync);

            

            var lines = new string[gridCacheDataList.Length + 1];
            lines[0] = CsvHeaderString;

            for (var i = 0; i < gridCacheDataList.Length; i++) lines[i + 1] = gridCacheDataList[i].CsvValuesString();

            await IoProxy.WriteAllLinesAsync(true, ct, cacheTrainGridCsv, lines, callerModuleName: ModuleName, callerMethodName: MethodName).ConfigureAwait(false);

            Logging.LogExit(ModuleName);
        }
    }
}