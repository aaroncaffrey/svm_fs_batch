﻿using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal class GridCacheData
    {
        public const string ModuleName = nameof(GridCacheData);
        //internal double rate;


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
            nameof(SvmFsBatch.GridPoint.GpCost),
            nameof(SvmFsBatch.GridPoint.GpGamma),
            nameof(SvmFsBatch.GridPoint.GpEpsilon),
            nameof(SvmFsBatch.GridPoint.GpCoef0),
            nameof(SvmFsBatch.GridPoint.GpDegree),
            nameof(SvmFsBatch.GridPoint.GpCvRate)
        };

        public static readonly string CsvHeaderString = string.Join(",", CsvHeaderValuesArray);
        internal GridPoint GridPoint;
        internal int InnerCvFolds;

        internal int OuterCvFolds;

        //internal int outer_cv_folds_to_run;
        internal int OuterCvIndex;
        internal bool ProbabilityEstimates;
        internal int Repetitions;
        internal int RepetitionsIndex;
        internal bool ShrinkingHeuristics;
        internal Routines.LibsvmKernelType SvmKernel;

        internal Routines.LibsvmSvmType SvmType;

        public GridCacheData()
        {
        }

        public GridCacheData(string[] line)
        {
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
            return string.Join(",", CsvValuesArray());
        }

        public string[] CsvValuesArray()
        {
            return new[]
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

        internal static async Task<GridCacheData[]> ReadCacheFileAsync(string cacheTrainGridCsv, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(ReadCacheFileAsync);

            

            var cache = Array.Empty<GridCacheData>();

            if (await IoProxy.IsFileAvailableAsync(true, ct, cacheTrainGridCsv, false, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false))
                cache = (await IoProxy.ReadAllLinesAsync(true, ct, cacheTrainGridCsv, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false) ?? Array.Empty<string>()).Skip(1 /* skip header line */).Select(a =>
                {
                    try
                    {
                        var line = a.Split(',');

                        return ct.IsCancellationRequested ? default :new GridCacheData(line);
                    }
                    catch (Exception e)
                    {
                        Logging.LogException(e, "", ModuleName, methodName);
                        return default;
                    }
                }).ToArray();

            cache = cache.Where(a => a.GridPoint.GpCvRate > 0).ToArray();

            return ct.IsCancellationRequested ? default :cache;
        }

        internal static async Task WriteCacheFileAsync(string cacheTrainGridCsv, GridCacheData[] gridCacheDataList, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return;

            const string methodName = nameof(WriteCacheFileAsync);

            

            var lines = new string[gridCacheDataList.Length + 1];
            lines[0] = CsvHeaderString;

            for (var i = 0; i < gridCacheDataList.Length; i++) lines[i + 1] = gridCacheDataList[i].CsvValuesString();

            await IoProxy.WriteAllLinesAsync(true, ct, cacheTrainGridCsv, lines, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false);
        }
    }
}