using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal class ConfusionMatrix
    {
        public const string ModuleName = nameof(ConfusionMatrix);

        internal static readonly ConfusionMatrix Empty = new ConfusionMatrix {UnrolledIndexData = IndexData.Empty, GridPoint = GridPoint.Empty, Metrics = MetricsBox.Empty};


        internal static readonly string[] CsvHeaderValuesArray =
            //index_data.CsvHeaderValuesArray.Select(a => $"id_{a}").ToArray()
            GridPoint.CsvHeaderValuesArray.Concat(new[]
            {
                nameof(XTimeGrid),
                nameof(XTimeTrain),
                nameof(XTimeTest),
                nameof(XPredictionThreshold),
                nameof(XPredictionThresholdClass),
                nameof(XRepetitionsIndex),
                nameof(XOuterCvIndex),

                nameof(XClassId),
                nameof(XClassWeight),
                nameof(XClassName),
                nameof(XClassSize),
                nameof(XClassTrainSize),
                nameof(XClassTestSize)
            }).Concat(MetricsBox.CsvHeaderValuesArray).Concat(new[]
            {
                nameof(RocXyStrAll),
                nameof(RocXyStrElevenPoint),
                nameof(PrXyStrAll),
                nameof(PrXyStrElevenPoint),
                nameof(PriXyStrAll),
                nameof(PriXyStrElevenPoint),

                nameof(Thresholds),
                nameof(Predictions)
            }).ToArray();

        internal static readonly string CsvHeaderString = string.Join(",", CsvHeaderValuesArray);
        internal GridPoint GridPoint;
        internal MetricsBox Metrics;
        internal Prediction[] Predictions;
        internal (double x, double y)[] PriXyStrAll;
        internal (double x, double y)[] PriXyStrElevenPoint;
        internal (double x, double y)[] PrXyStrAll;
        internal (double x, double y)[] PrXyStrElevenPoint;
        internal (double x, double y)[] RocXyStrAll;
        internal (double x, double y)[] RocXyStrElevenPoint;
        internal double[] Thresholds;

        internal IndexData UnrolledIndexData;
        internal int? XClassId;
        internal string XClassName;
        internal double XClassSize;
        internal double XClassTestSize;
        internal double XClassTrainSize;
        internal double? XClassWeight;
        internal int XOuterCvIndex = -1;
        internal double? XPredictionThreshold = -1;
        internal double? XPredictionThresholdClass;
        internal int XRepetitionsIndex = -1;
        internal TimeSpan? XTimeGrid;
        internal TimeSpan? XTimeTest;
        internal TimeSpan? XTimeTrain;

        // note: load does not load the rd/sd part of the cm file.  this has to be recalculated after loading the cm.

        internal void ClearSupplemental()
        {
            UnrolledIndexData = null;
            GridPoint = null;
            XTimeGrid = null;
            XTimeTrain = null;
            XTimeTest = null;
            RocXyStrAll = null;
            RocXyStrElevenPoint = null;
            PrXyStrAll = null;
            PrXyStrElevenPoint = null;
            PriXyStrAll = null;
            PriXyStrElevenPoint = null;
            Thresholds = null;
            Predictions = null;
        }

        internal static async Task SaveAsync(string cmFullFilename, /*string cm_summary_filename,*/
            bool overwrite, ConfusionMatrix[] xList, bool asParallel = true, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return;

            await SaveAsync(cmFullFilename, /*cm_summary_filename,*/ overwrite, null, xList, null, asParallel, ct).ConfigureAwait(false);
        }

        internal static async Task SaveAsync(string cmFullFilename, /*string cm_summary_filename,*/
            bool overwrite, (IndexData id, ConfusionMatrix cm, RankScore rs)[] xList, bool asParallel = true, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return;

            await SaveAsync(cmFullFilename,
                //cm_summary_filename,
                overwrite,
                xList.Select(a => a.id).ToArray(),
                xList.Select(a => a.cm).ToArray(),
                xList.Select(a => a.rs).ToArray(),
                asParallel,
                ct).ConfigureAwait(false);
        }

        internal static async Task SaveAsync(string cmFullFilename, /*string cm_summary_filename,*/
            bool overwrite, (IndexData id, ConfusionMatrix cm)[] xList, bool asParallel = true, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return;

            await SaveAsync(cmFullFilename,
                //cm_summary_filename,
                overwrite,
                xList.Select(a => a.id).ToArray(),
                xList.Select(a => a.cm).ToArray(),
                null,
                asParallel,
                ct).ConfigureAwait(false);
        }


        internal static async Task SaveAsync(string cmFullFilename, /*string cm_summary_filename,*/ bool overwrite, IndexData[] idList, ConfusionMatrix[] cmList, RankScore[] rsList, bool asParallel = true, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return;

            const string methodName = nameof(SaveAsync);

            

            //if (cm_full_filename == cm_summary_filename) throw new Exception(@"Filenames are the same.");

            var saveFullReq = !string.IsNullOrWhiteSpace(cmFullFilename);
            //var save_summary_req = !string.IsNullOrWhiteSpace(cm_summary_filename);
            if (!saveFullReq /* && !save_summary_req*/) throw new Exception(@"No filenames provided to save data to.");

            var lens = new[] {idList?.Length ?? 0, cmList?.Length ?? 0, rsList?.Length ?? 0}.Where(a => a > 0).ToArray();
            var lensDistinctCount = lens.Distinct().Count();
            if (lens.Length == 0 || lensDistinctCount > 1) throw new Exception($@"Array length of {nameof(idList)}, {nameof(cmList)}, and {nameof(rsList)} do not match.");
            var lensMax = lens.Max();

            var saveFull = saveFullReq && (overwrite || !await IoProxy.IsFileAvailableAsync(true, ct, cmFullFilename, false, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false));
            //var save_summary = save_summary_req && (overwrite || !await io_proxy.IsFileAvailable(true, ct, cm_summary_filename, false, _CallerModuleName: ModuleName, _CallerMethodName: MethodName));

            if (saveFullReq && !saveFull) Logging.WriteLine($@"Not overwriting file: {cmFullFilename}", ModuleName, methodName);

            //if (save_summary_req && !save_summary) Logging.WriteLine($@"Not overwriting file: {cm_summary_filename}", ModuleName, MethodName);

            if (!saveFull /* && !save_summary*/) return;


            var linesFull = saveFull
                ? new string[lens.Max() + 1]
                : null;
            //var lines_summary = save_summary ? new string[lens.Max() + 1] : null;

            var csvHeaderValuesArray = new List<string>();
            //if (rs_list != null && rs_list.Length > 0) { CsvHeaderValuesArray.AddRange(rank_score.CsvHeaderValuesArray); }
            //if (id_list != null && id_list.Length > 0) { CsvHeaderValuesArray.AddRange(index_data.CsvHeaderValuesArray); }
            //if (CmList != null && CmList.Length > 0) { CsvHeaderValuesArray.AddRange(ConfusionMatrix.CsvHeaderValuesArray); }

            csvHeaderValuesArray.AddRange(RankScore.CsvHeaderValuesArray);
            csvHeaderValuesArray.AddRange(IndexData.CsvHeaderValuesArray);
            csvHeaderValuesArray.AddRange(CsvHeaderValuesArray);

            var csvHeaderValuesString = string.Join(",", csvHeaderValuesArray);
            if (linesFull != null) linesFull[0] = csvHeaderValuesString;
            //if (lines_summary != null) lines_summary[0] = csv_header_values_string;


            if (asParallel)
                Parallel.For(0,
                    lensMax,
                    i =>
                    {
                        var values1 = new List<string>();

                        values1?.AddRange(rsList != null && rsList.Length > i
                            ? rsList[i].CsvValuesArray()
                            : RankScore.Empty.CsvValuesArray());
                        values1?.AddRange(idList != null && idList.Length > i
                            ? idList[i].CsvValuesArray()
                            : IndexData.Empty.CsvValuesArray());
                        values1?.AddRange(cmList != null && cmList.Length > i
                            ? cmList[i].CsvValuesArray()
                            : Empty.CsvValuesArray());

                        if (linesFull != null) linesFull[i + 1] = string.Join(",", values1);
                        //if (lines_summary != null) lines_summary[i + 1] = string.Join(",", values1.Select(a => a.Length <= 255 ? a : "").ToArray());
                    });
            else
                for (var i = 0; i < lensMax; i++)
                {
                    var values1 = new List<string>();

                    values1?.AddRange(rsList != null && rsList.Length > i
                        ? rsList[i].CsvValuesArray()
                        : RankScore.Empty.CsvValuesArray());
                    values1?.AddRange(idList != null && idList.Length > i
                        ? idList[i].CsvValuesArray()
                        : IndexData.Empty.CsvValuesArray());
                    values1?.AddRange(cmList != null && cmList.Length > i
                        ? cmList[i].CsvValuesArray()
                        : Empty.CsvValuesArray());

                    if (linesFull != null) linesFull[i + 1] = string.Join(",", values1);
                    //if (lines_summary != null) lines_summary[i + 1] = string.Join(",", values1.Select(a => a.Length <= 255 ? a : "").ToArray());
                }

            if (linesFull != null && linesFull.Length > 0)
            {
                await IoProxy.WriteAllLinesAsync(true, ct, cmFullFilename, linesFull, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false);
                Logging.WriteLine($@"Saved: {cmFullFilename} ({linesFull.Length} lines)", ModuleName, methodName);
            }

            //if (lines_summary != null && lines_summary.Length > 0)
            //{
            //    await io_proxy.WriteAllLines(true, ct, cm_summary_filename, lines_summary,
            //        _CallerModuleName: ModuleName, _CallerMethodName: MethodName).ConfigureAwait(false);
            //    Logging.WriteLine($@"Saved: {cm_summary_filename} ({lines_summary.Length} lines)", ModuleName,
            //        MethodName);
            //}
        }

        internal static async Task<ConfusionMatrix[]> LoadAsync(string filename, int columnOffset = -1, bool asParallel = true, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(LoadAsync);
            
            var lines = await IoProxy.ReadAllLinesAsync(true, ct, filename, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false);
            var ret = Load(lines, columnOffset, asParallel, ct);
            return ct.IsCancellationRequested ? default :ret;
        }

        internal static ConfusionMatrix[] Load(string[] lines, int columnOffset = -1, bool asParallel = true, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            var lineHeader = lines[0].Split(',');
            var hasHeaderLine = false;
            if (columnOffset == -1)
            {
                // find column position in csv of the header, and set column_offset accordingly
                for (var i = 0; i <= lineHeader.Length - CsvHeaderValuesArray.Length; i++)
                    if (lineHeader.Skip(i).Take(CsvHeaderValuesArray.Length).SequenceEqual(CsvHeaderValuesArray, StringComparer.OrdinalIgnoreCase))
                    {
                        hasHeaderLine = true;
                        columnOffset = i;
                        break;
                    }

                if (columnOffset == -1) columnOffset = 0;
            }

            if (!hasHeaderLine) lineHeader = null;

            var cmList = asParallel
                ? lines.Skip(hasHeaderLine
                    ? 1
                    : 0).Where(a => !string.IsNullOrWhiteSpace(a)).AsParallel().AsOrdered().WithCancellation(ct).Select(line => LoadLine(columnOffset, lineHeader, line, asParallel, ct)).Where(a => a != null).ToArray()
                : lines.Skip(hasHeaderLine
                    ? 1
                    : 0).Where(a => !string.IsNullOrWhiteSpace(a)).Select(line => LoadLine(columnOffset, lineHeader, line, asParallel, ct)).Where(a => a != null).ToArray();


            return ct.IsCancellationRequested ? default :cmList;
        }

        private static ConfusionMatrix LoadLine(int columnOffset, string[] lineHeader, string line, bool asParallel = true, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            var sAll = line.Split(',');
            var columnCount = sAll.Length - columnOffset;
            if (columnCount < CsvHeaderValuesArray.Length) return null;
            var xType = XTypes.GetXTypes(sAll, asParallel, ct);
            var headerIndexes = CsvHeaderValuesArray.Select((h, i) => (header: h, index: lineHeader.Length > 0
                ? Array.FindIndex(lineHeader, a => a.EndsWith(h))
                : columnOffset + i)).ToArray();

            int Hi(string name)
            {
                return ct.IsCancellationRequested ? default :headerIndexes.First(a => a.header.EndsWith(name, StringComparison.OrdinalIgnoreCase)).index;
            }

            var cm = new ConfusionMatrix();

            cm.UnrolledIndexData = new IndexData(lineHeader, xType);

            var hiGpCost = Hi(nameof(SvmFsBatch.GridPoint.GpCost));
            var hiGpGamma = Hi(nameof(SvmFsBatch.GridPoint.GpGamma));
            var hiGpEpsilon = Hi(nameof(SvmFsBatch.GridPoint.GpEpsilon));
            var hiGpCoef0 = Hi(nameof(SvmFsBatch.GridPoint.GpCoef0));
            var hiGpDegree = Hi(nameof(SvmFsBatch.GridPoint.GpDegree));
            var hiGpCvRate = Hi(nameof(SvmFsBatch.GridPoint.GpCvRate));
            var hiXTimeGrid = Hi(nameof(XTimeGrid));
            var hiXTimeTrain = Hi(nameof(XTimeTrain));
            var hiXTimeTest = Hi(nameof(XTimeTest));
            var hiXPredictionThreshold = Hi(nameof(XPredictionThreshold));
            var hiXPredictionThresholdClass = Hi(nameof(XPredictionThresholdClass));
            var hiXRepetitionsIndex = Hi(nameof(XRepetitionsIndex));
            var hiXOuterCvIndex = Hi(nameof(XOuterCvIndex));
            var hiXClassId = Hi(nameof(XClassId));
            var hiXClassWeight = Hi(nameof(XClassWeight));
            var hiXClassName = Hi(nameof(XClassName));
            var hiXClassSize = Hi(nameof(XClassSize));
            var hiXClassTrainSize = Hi(nameof(XClassTrainSize));
            var hiXClassTestSize = Hi(nameof(XClassTestSize));
            var hiCmP = Hi(nameof(MetricsBox.CmP));
            var hiCmN = Hi(nameof(MetricsBox.CmN));
            var hiCmPTp = Hi(nameof(MetricsBox.CmPTp));
            var hiCmPFn = Hi(nameof(MetricsBox.CmPFn));
            var hiCmNTn = Hi(nameof(MetricsBox.CmNTn));
            var hiCmNFp = Hi(nameof(MetricsBox.CmNFp));
            var hiPTpr = Hi(nameof(MetricsBox.PTpr));
            var hiPTnr = Hi(nameof(MetricsBox.PTnr));
            var hiPPpv = Hi(nameof(MetricsBox.PPpv));
            var hiPPrecision = Hi(nameof(MetricsBox.PPrecision));
            var hiPPrevalence = Hi(nameof(MetricsBox.PPrevalence));
            var hiPMcr = Hi(nameof(MetricsBox.PMcr));
            var hiPEr = Hi(nameof(MetricsBox.PEr));
            var hiPNer = Hi(nameof(MetricsBox.PNer));
            var hiPCner = Hi(nameof(MetricsBox.PCner));
            var hiPKappa = Hi(nameof(MetricsBox.PKappa));
            var hiPOverlap = Hi(nameof(MetricsBox.POverlap));
            var hiPRndAcc = Hi(nameof(MetricsBox.PRndAcc));
            var hiPSupport = Hi(nameof(MetricsBox.PSupport));
            var hiPBaseRate = Hi(nameof(MetricsBox.PBaseRate));
            var hiPYoudenIndex = Hi(nameof(MetricsBox.PYoudenIndex));
            var hiPNpv = Hi(nameof(MetricsBox.PNpv));
            var hiPFnr = Hi(nameof(MetricsBox.PFnr));
            var hiPFpr = Hi(nameof(MetricsBox.PFpr));
            var hiPFdr = Hi(nameof(MetricsBox.PFdr));
            var hiPFor = Hi(nameof(MetricsBox.PFor));
            var hiPAcc = Hi(nameof(MetricsBox.PAcc));
            var hiPGMean = Hi(nameof(MetricsBox.PGMean));
            var hiPF1S = Hi(nameof(MetricsBox.PF1S));
            var hiPG1S = Hi(nameof(MetricsBox.PG1S));
            var hiPMcc = Hi(nameof(MetricsBox.PMcc));
            var hiPInformedness = Hi(nameof(MetricsBox.PInformedness));
            var hiPMarkedness = Hi(nameof(MetricsBox.PMarkedness));
            var hiPBalancedAccuracy = Hi(nameof(MetricsBox.PBalancedAccuracy));
            var hiPRocAucApproxAll = Hi(nameof(MetricsBox.PRocAucApproxAll));
            var hiPRocAucApproxElevenPoint = Hi(nameof(MetricsBox.PRocAucApproxElevenPoint));
            var hiPRocAucAll = Hi(nameof(MetricsBox.PRocAucAll));
            var hiPPrAucApproxAll = Hi(nameof(MetricsBox.PPrAucApproxAll));
            var hiPPrAucApproxElevenPoint = Hi(nameof(MetricsBox.PPrAucApproxElevenPoint));
            var hiPPriAucApproxAll = Hi(nameof(MetricsBox.PPriAucApproxAll));
            var hiPPriAucApproxElevenPoint = Hi(nameof(MetricsBox.PPriAucApproxElevenPoint));
            var hiPApAll = Hi(nameof(MetricsBox.PApAll));
            var hiPApElevenPoint = Hi(nameof(MetricsBox.PApElevenPoint));
            var hiPApiAll = Hi(nameof(MetricsBox.PApiAll));
            var hiPApiElevenPoint = Hi(nameof(MetricsBox.PApiElevenPoint));
            var hiPBrierInverseAll = Hi(nameof(MetricsBox.PBrierInverseAll));
            var hiPLrp = Hi(nameof(MetricsBox.PLrp));
            var hiPLrn = Hi(nameof(MetricsBox.PLrn));
            var hiPDor = Hi(nameof(MetricsBox.PDor));
            var hiPPrevalenceThreshold = Hi(nameof(MetricsBox.PPrevalenceThreshold));
            var hiPCriticalSuccessIndex = Hi(nameof(MetricsBox.PCriticalSuccessIndex));
            var hiPF1B00 = Hi(nameof(MetricsBox.PF1B00));
            var hiPF1B01 = Hi(nameof(MetricsBox.PF1B01));
            var hiPF1B02 = Hi(nameof(MetricsBox.PF1B02));
            var hiPF1B03 = Hi(nameof(MetricsBox.PF1B03));
            var hiPF1B04 = Hi(nameof(MetricsBox.PF1B04));
            var hiPF1B05 = Hi(nameof(MetricsBox.PF1B05));
            var hiPF1B06 = Hi(nameof(MetricsBox.PF1B06));
            var hiPF1B07 = Hi(nameof(MetricsBox.PF1B07));
            var hiPF1B08 = Hi(nameof(MetricsBox.PF1B08));
            var hiPF1B09 = Hi(nameof(MetricsBox.PF1B09));
            var hiPF1B10 = Hi(nameof(MetricsBox.PF1B10));
            var hiRocXyStrAll = Hi(nameof(RocXyStrAll));
            var hiRocXyStrElevenPoint = Hi(nameof(RocXyStrElevenPoint));
            var hiPrXyStrAll = Hi(nameof(PrXyStrAll));
            var hiPrXyStrElevenPoint = Hi(nameof(PrXyStrElevenPoint));
            var hiPriXyStrAll = Hi(nameof(PriXyStrAll));
            var hiPriXyStrElevenPoint = Hi(nameof(PriXyStrElevenPoint));
            var hiThresholds = Hi(nameof(Thresholds));
            var hiPredictions = Hi(nameof(Predictions));

            cm.GridPoint = new GridPoint
            {
                GpCost = hiGpCost > -1
                    ? xType[hiGpCost].AsDouble
                    : default,
                GpGamma = hiGpGamma > -1
                    ? xType[hiGpGamma].AsDouble
                    : default,
                GpEpsilon = hiGpEpsilon > -1
                    ? xType[hiGpEpsilon].AsDouble
                    : default,
                GpCoef0 = hiGpCoef0 > -1
                    ? xType[hiGpCoef0].AsDouble
                    : default,
                GpDegree = hiGpDegree > -1
                    ? xType[hiGpDegree].AsDouble
                    : default,
                GpCvRate = hiGpCvRate > -1
                    ? xType[hiGpCvRate].AsDouble
                    : default
            };

            if (hiXTimeGrid > -1)
                cm.XTimeGrid = !string.IsNullOrWhiteSpace(xType[hiXTimeGrid].AsStr)
                    ? TimeSpan.TryParseExact(xType[hiXTimeGrid].AsStr, "G", DateTimeFormatInfo.InvariantInfo, out var tsResult1) ? tsResult1 : (TimeSpan?) null
                    : null;
            if (hiXTimeTrain > -1)
                cm.XTimeTrain = !string.IsNullOrWhiteSpace(xType[hiXTimeTrain].AsStr)
                    ? TimeSpan.TryParseExact(xType[hiXTimeTrain].AsStr, "G", DateTimeFormatInfo.InvariantInfo, out var tsResult2) ? tsResult2 : (TimeSpan?) null
                    : null;
            if (hiXTimeTest > -1)
                cm.XTimeTest = !string.IsNullOrWhiteSpace(xType[hiXTimeTest].AsStr)
                    ? TimeSpan.TryParseExact(xType[hiXTimeTest].AsStr, "G", DateTimeFormatInfo.InvariantInfo, out var tsResult3) ? tsResult3 : (TimeSpan?) null
                    : null;
            if (hiXPredictionThreshold > -1) cm.XPredictionThreshold = xType[hiXPredictionThreshold].AsDouble;
            if (hiXPredictionThresholdClass > -1) cm.XPredictionThresholdClass = xType[hiXPredictionThresholdClass].AsDouble;
            if (hiXRepetitionsIndex > -1) cm.XRepetitionsIndex = xType[hiXRepetitionsIndex].AsInt ?? 0;
            if (hiXOuterCvIndex > -1) cm.XOuterCvIndex = xType[hiXOuterCvIndex].AsInt ?? 0;
            if (hiXClassId > -1) cm.XClassId = xType[hiXClassId].AsInt;
            if (hiXClassWeight > -1) cm.XClassWeight = xType[hiXClassWeight].AsDouble;
            if (hiXClassName > -1) cm.XClassName = xType[hiXClassName].AsStr;
            if (hiXClassSize > -1) cm.XClassSize = xType[hiXClassSize].AsDouble ?? 0;
            if (hiXClassTrainSize > -1) cm.XClassTrainSize = xType[hiXClassTrainSize].AsDouble ?? 0;
            if (hiXClassTestSize > -1) cm.XClassTestSize = xType[hiXClassTestSize].AsDouble ?? 0;

            cm.Metrics = new MetricsBox();
            if (hiCmP > -1) cm.Metrics.CmP = xType[hiCmP].AsDouble ?? 0;
            if (hiCmN > -1) cm.Metrics.CmN = xType[hiCmN].AsDouble ?? 0;
            if (hiCmPTp > -1) cm.Metrics.CmPTp = xType[hiCmPTp].AsDouble ?? 0;
            if (hiCmPFn > -1) cm.Metrics.CmPFn = xType[hiCmPFn].AsDouble ?? 0;
            if (hiCmNTn > -1) cm.Metrics.CmNTn = xType[hiCmNTn].AsDouble ?? 0;
            if (hiCmNFp > -1) cm.Metrics.CmNFp = xType[hiCmNFp].AsDouble ?? 0;
            if (hiPTpr > -1) cm.Metrics.PTpr = xType[hiPTpr].AsDouble ?? 0;
            if (hiPTnr > -1) cm.Metrics.PTnr = xType[hiPTnr].AsDouble ?? 0;
            if (hiPPpv > -1) cm.Metrics.PPpv = xType[hiPPpv].AsDouble ?? 0;
            if (hiPPrecision > -1) cm.Metrics.PPrecision = xType[hiPPrecision].AsDouble ?? 0;
            if (hiPPrevalence > -1) cm.Metrics.PPrevalence = xType[hiPPrevalence].AsDouble ?? 0;
            if (hiPMcr > -1) cm.Metrics.PMcr = xType[hiPMcr].AsDouble ?? 0;
            if (hiPEr > -1) cm.Metrics.PEr = xType[hiPEr].AsDouble ?? 0;
            if (hiPNer > -1) cm.Metrics.PNer = xType[hiPNer].AsDouble ?? 0;
            if (hiPCner > -1) cm.Metrics.PCner = xType[hiPCner].AsDouble ?? 0;
            if (hiPKappa > -1) cm.Metrics.PKappa = xType[hiPKappa].AsDouble ?? 0;
            if (hiPOverlap > -1) cm.Metrics.POverlap = xType[hiPOverlap].AsDouble ?? 0;
            if (hiPRndAcc > -1) cm.Metrics.PRndAcc = xType[hiPRndAcc].AsDouble ?? 0;
            if (hiPSupport > -1) cm.Metrics.PSupport = xType[hiPSupport].AsDouble ?? 0;
            if (hiPBaseRate > -1) cm.Metrics.PBaseRate = xType[hiPBaseRate].AsDouble ?? 0;
            if (hiPYoudenIndex > -1) cm.Metrics.PYoudenIndex = xType[hiPYoudenIndex].AsDouble ?? 0;
            if (hiPNpv > -1) cm.Metrics.PNpv = xType[hiPNpv].AsDouble ?? 0;
            if (hiPFnr > -1) cm.Metrics.PFnr = xType[hiPFnr].AsDouble ?? 0;
            if (hiPFpr > -1) cm.Metrics.PFpr = xType[hiPFpr].AsDouble ?? 0;
            if (hiPFdr > -1) cm.Metrics.PFdr = xType[hiPFdr].AsDouble ?? 0;
            if (hiPFor > -1) cm.Metrics.PFor = xType[hiPFor].AsDouble ?? 0;
            if (hiPAcc > -1) cm.Metrics.PAcc = xType[hiPAcc].AsDouble ?? 0;
            if (hiPGMean > -1) cm.Metrics.PGMean = xType[hiPGMean].AsDouble ?? 0;
            if (hiPF1S > -1) cm.Metrics.PF1S = xType[hiPF1S].AsDouble ?? 0;
            if (hiPG1S > -1) cm.Metrics.PG1S = xType[hiPG1S].AsDouble ?? 0;
            if (hiPMcc > -1) cm.Metrics.PMcc = xType[hiPMcc].AsDouble ?? 0;
            if (hiPInformedness > -1) cm.Metrics.PInformedness = xType[hiPInformedness].AsDouble ?? 0;
            if (hiPMarkedness > -1) cm.Metrics.PMarkedness = xType[hiPMarkedness].AsDouble ?? 0;
            if (hiPBalancedAccuracy > -1) cm.Metrics.PBalancedAccuracy = xType[hiPBalancedAccuracy].AsDouble ?? 0;
            if (hiPRocAucApproxAll > -1) cm.Metrics.PRocAucApproxAll = xType[hiPRocAucApproxAll].AsDouble ?? 0;
            if (hiPRocAucApproxElevenPoint > -1) cm.Metrics.PRocAucApproxElevenPoint = xType[hiPRocAucApproxElevenPoint].AsDouble ?? 0;
            if (hiPRocAucAll > -1) cm.Metrics.PRocAucAll = xType[hiPRocAucAll].AsDouble ?? 0;
            if (hiPPrAucApproxAll > -1) cm.Metrics.PPrAucApproxAll = xType[hiPPrAucApproxAll].AsDouble ?? 0;
            if (hiPPrAucApproxElevenPoint > -1) cm.Metrics.PPrAucApproxElevenPoint = xType[hiPPrAucApproxElevenPoint].AsDouble ?? 0;
            if (hiPPriAucApproxAll > -1) cm.Metrics.PPriAucApproxAll = xType[hiPPriAucApproxAll].AsDouble ?? 0;
            if (hiPPriAucApproxElevenPoint > -1) cm.Metrics.PPriAucApproxElevenPoint = xType[hiPPriAucApproxElevenPoint].AsDouble ?? 0;
            if (hiPApAll > -1) cm.Metrics.PApAll = xType[hiPApAll].AsDouble ?? 0;
            if (hiPApElevenPoint > -1) cm.Metrics.PApElevenPoint = xType[hiPApElevenPoint].AsDouble ?? 0;
            if (hiPApiAll > -1) cm.Metrics.PApiAll = xType[hiPApiAll].AsDouble ?? 0;
            if (hiPApiElevenPoint > -1) cm.Metrics.PApiElevenPoint = xType[hiPApiElevenPoint].AsDouble ?? 0;
            if (hiPBrierInverseAll > -1) cm.Metrics.PBrierInverseAll = xType[hiPBrierInverseAll].AsDouble ?? 0;
            if (hiPLrp > -1) cm.Metrics.PLrp = xType[hiPLrp].AsDouble ?? 0;
            if (hiPLrn > -1) cm.Metrics.PLrn = xType[hiPLrn].AsDouble ?? 0;
            if (hiPDor > -1) cm.Metrics.PDor = xType[hiPDor].AsDouble ?? 0;
            if (hiPPrevalenceThreshold > -1) cm.Metrics.PPrevalenceThreshold = xType[hiPPrevalenceThreshold].AsDouble ?? 0;
            if (hiPCriticalSuccessIndex > -1) cm.Metrics.PCriticalSuccessIndex = xType[hiPCriticalSuccessIndex].AsDouble ?? 0;
            if (hiPF1B00 > -1) cm.Metrics.PF1B00 = xType[hiPF1B00].AsDouble ?? 0;
            if (hiPF1B01 > -1) cm.Metrics.PF1B01 = xType[hiPF1B01].AsDouble ?? 0;
            if (hiPF1B02 > -1) cm.Metrics.PF1B02 = xType[hiPF1B02].AsDouble ?? 0;
            if (hiPF1B03 > -1) cm.Metrics.PF1B03 = xType[hiPF1B03].AsDouble ?? 0;
            if (hiPF1B04 > -1) cm.Metrics.PF1B04 = xType[hiPF1B04].AsDouble ?? 0;
            if (hiPF1B05 > -1) cm.Metrics.PF1B05 = xType[hiPF1B05].AsDouble ?? 0;
            if (hiPF1B06 > -1) cm.Metrics.PF1B06 = xType[hiPF1B06].AsDouble ?? 0;
            if (hiPF1B07 > -1) cm.Metrics.PF1B07 = xType[hiPF1B07].AsDouble ?? 0;
            if (hiPF1B08 > -1) cm.Metrics.PF1B08 = xType[hiPF1B08].AsDouble ?? 0;
            if (hiPF1B09 > -1) cm.Metrics.PF1B09 = xType[hiPF1B09].AsDouble ?? 0;
            if (hiPF1B10 > -1) cm.Metrics.PF1B10 = xType[hiPF1B10].AsDouble ?? 0;
            if (hiRocXyStrAll > -1)
                cm.RocXyStrAll = !string.IsNullOrWhiteSpace(xType[hiRocXyStrAll].AsStr)
                    ? xType[hiRocXyStrAll].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Skip(1 /* skip axis names*/).Select(a =>
                    {
                        var xy = a.Split(':', StringSplitOptions.RemoveEmptyEntries).Select(b => double.Parse(b, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray();
                        return ct.IsCancellationRequested ? default :(x: xy[0], y: xy[1]);
                    }).ToArray()
                    : null;
            if (hiRocXyStrElevenPoint > -1)
                cm.RocXyStrElevenPoint = !string.IsNullOrWhiteSpace(xType[hiRocXyStrElevenPoint].AsStr)
                    ? xType[hiRocXyStrElevenPoint].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Skip(1 /* skip axis names*/).Select(a =>
                    {
                        var xy = a.Split(':', StringSplitOptions.RemoveEmptyEntries).Select(b => double.Parse(b, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray();
                        return ct.IsCancellationRequested ? default :(x: xy[0], y: xy[1]);
                    }).ToArray()
                    : null;
            if (hiPrXyStrAll > -1)
                cm.PrXyStrAll = !string.IsNullOrWhiteSpace(xType[hiPrXyStrAll].AsStr)
                    ? xType[hiPrXyStrAll].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Skip(1 /* skip axis names*/).Select(a =>
                    {
                        var xy = a.Split(':', StringSplitOptions.RemoveEmptyEntries).Select(b => double.Parse(b, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray();
                        return ct.IsCancellationRequested ? default :(x: xy[0], y: xy[1]);
                    }).ToArray()
                    : null;
            if (hiPrXyStrElevenPoint > -1)
                cm.PrXyStrElevenPoint = !string.IsNullOrWhiteSpace(xType[hiPrXyStrElevenPoint].AsStr)
                    ? xType[hiPrXyStrElevenPoint].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Skip(1 /* skip axis names*/).Select(a =>
                    {
                        var xy = a.Split(':', StringSplitOptions.RemoveEmptyEntries).Select(b => double.Parse(b, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray();
                        return ct.IsCancellationRequested ? default :(x: xy[0], y: xy[1]);
                    }).ToArray()
                    : null;
            if (hiPriXyStrAll > -1)
                cm.PriXyStrAll = !string.IsNullOrWhiteSpace(xType[hiPriXyStrAll].AsStr)
                    ? xType[hiPriXyStrAll].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Skip(1 /* skip axis names*/).Select(a =>
                    {
                        var xy = a.Split(':', StringSplitOptions.RemoveEmptyEntries).Select(b => double.Parse(b, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray();
                        return ct.IsCancellationRequested ? default :(x: xy[0], y: xy[1]);
                    }).ToArray()
                    : null;
            if (hiPriXyStrElevenPoint > -1)
                cm.PriXyStrElevenPoint = !string.IsNullOrWhiteSpace(xType[hiPriXyStrElevenPoint].AsStr)
                    ? xType[hiPriXyStrElevenPoint].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Skip(1 /* skip axis names*/).Select(a =>
                    {
                        var xy = a.Split(':', StringSplitOptions.RemoveEmptyEntries).Select(b => double.Parse(b, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray();
                        return ct.IsCancellationRequested ? default :(x: xy[0], y: xy[1]);
                    }).ToArray()
                    : null;
            if (hiThresholds > -1)
                cm.Thresholds = !string.IsNullOrWhiteSpace(xType[hiThresholds].AsStr)
                    ? xType[hiThresholds].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => double.TryParse(a, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var thOut)
                        ? thOut
                        : -1).ToArray()
                    : null;
            if (hiPredictions > -1)
                cm.Predictions = !string.IsNullOrWhiteSpace(xType[hiPredictions].AsStr)
                    ? xType[hiPredictions].AsStr.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(a => new Prediction(a.Split('|'))).ToArray()
                    : null;

            return ct.IsCancellationRequested ? default :cm;
        }


        internal void CalculateThesholdMetrics(MetricsBox metrics, bool calculateAuc, Prediction[] predictionList, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return;

            if (XClassId != null && predictionList != null && predictionList.Length > 0 && predictionList.Any(a => a.ProbabilityEstimates != null && a.ProbabilityEstimates.Length > 0))
            {
                var pBrierScoreAll = PerformanceMeasure.Brier(predictionList, XClassId.Value, ct);
                metrics.PBrierInverseAll = 1 - pBrierScoreAll;

                if (calculateAuc)
                {
                    //var (p_brier_score_all, p_roc_auc_all, p_roc_auc2_all, p_pr_auc_all, p_pri_auc_all, p_ap_all, p_api_all, p_roc_xy_all, p_pr_xy_all, p_pri_xy_all) = performance_measure.Calculate_ROC_PR_AUC(prediction_list, ClassId.Value, false);
                    //var (p_brier_score_ElevenPoint, p_roc_auc_ElevenPoint, p_roc_auc2_ElevenPoint, p_pr_auc_ElevenPoint, p_pri_auc_ElevenPoint, p_ap_ElevenPoint, p_api_ElevenPoint, p_roc_xy_ElevenPoint, p_pr_xy_ElevenPoint, p_pri_xy_ElevenPoint) = performance_measure.Calculate_ROC_PR_AUC(prediction_list, ClassId.Value, true);

                    var (pRocAucApproxAll, pRocAucActualAll, pPrAucApproxAll, pPriAucApproxAll, pApAll, pApiAll, pRocXyAll, pPrXyAll, pPriXyAll) = PerformanceMeasure.Calculate_ROC_PR_AUC(predictionList, XClassId.Value, ct: ct);
                    var (pRocAucApproxElevenPoint, pRocAucActualElevenPoint, pPrAucApproxElevenPoint, pPriAucApproxElevenPoint, pApElevenPoint, pApiElevenPoint, pRocXyElevenPoint, pPrXyElevenPoint, pPriXyElevenPoint) = PerformanceMeasure.Calculate_ROC_PR_AUC(predictionList, XClassId.Value, PerformanceMeasure.ThresholdType.ElevenPoints, ct);


                    metrics.PRocAucApproxAll = pRocAucApproxAll;
                    metrics.PRocAucApproxElevenPoint = pRocAucApproxElevenPoint;

                    metrics.PRocAucAll = pRocAucActualAll;
                    //ROC_AUC_ElevenPoint = p_roc_auc_actual_ElevenPoint;

                    metrics.PPrAucApproxAll = pPrAucApproxAll;
                    metrics.PPrAucApproxElevenPoint = pPrAucApproxElevenPoint;

                    metrics.PPriAucApproxAll = pPriAucApproxAll;
                    metrics.PPriAucApproxElevenPoint = pPriAucApproxElevenPoint;

                    metrics.PApAll = pApAll;
                    metrics.PApElevenPoint = pApElevenPoint;
                    metrics.PApiAll = pApiAll;
                    metrics.PApiElevenPoint = pApiElevenPoint;

                    // PR (x: a.TPR, y: a.PPV)
                    // ROC (x: a.FPR, y: a.TPR)

                    // roc x,y points (11 point and all thresholds)
                    //roc_xy_str_all = p_roc_xy_all != null && p_roc_xy_all.Count > 0 ? $"FPR;TPR/{string.Join("/", p_roc_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToArray())}" : "";
                    //roc_xy_str_ElevenPoint = p_roc_xy_ElevenPoint != null && p_roc_xy_ElevenPoint.Count > 0 ? $"FPR;TPR/{string.Join("/", p_roc_xy_ElevenPoint.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToArray())}" : "";
                    //// precision-recall chart x,y points (11 point and all thresholds)
                    //pr_xy_str_all = p_pr_xy_all != null && p_pr_xy_all.Count > 0 ? $"TPR;PPV/{string.Join("/", p_pr_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToArray())}" : "";
                    //pr_xy_str_ElevenPoint = p_pr_xy_ElevenPoint != null && p_pr_xy_ElevenPoint.Count > 0 ? $"TPR;PPV/{string.Join("/", p_pr_xy_ElevenPoint.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToArray())}" : "";
                    //// precision-recall interpolated x,y points (11 point and all thresholds)
                    //pri_xy_str_all = p_pri_xy_all != null && p_pri_xy_all.Count > 0 ? $"TPR;PPV/{string.Join("/", p_pri_xy_all.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToArray())}" : "";
                    //pri_xy_str_ElevenPoint = p_pri_xy_ElevenPoint != null && p_pri_xy_ElevenPoint.Count > 0 ? $"TPR;PPV/{string.Join("/", p_pri_xy_ElevenPoint.Select(a => $"{Math.Round(a.x, 6)};{Math.Round(a.y, 6)}").ToArray())}" : "";

                    //roc x, y points(11 point and all thresholds)
                    RocXyStrAll = pRocXyAll;
                    RocXyStrElevenPoint = pRocXyElevenPoint;
                    // precision-recall chart x,y points (11 point and all thresholds)
                    PrXyStrAll = pPrXyAll;
                    PrXyStrElevenPoint = pPrXyElevenPoint;
                    // precision-recall interpolated x,y points (11 point and all thresholds)
                    PriXyStrAll = pPriXyAll;
                    PriXyStrElevenPoint = pPriXyElevenPoint;
                }
            }

            metrics.CalculateMetrics();
        }


        internal string[] CsvValuesArray()
        {
            return
                //(unrolled_index_data?.CsvValuesArray() ?? index_data.empty.CsvValuesArray())
                //.Concat(GridPoint?.CsvValuesArray() ?? GridPoint.empty.CsvValuesArray())
                (GridPoint?.CsvValuesArray() ?? GridPoint.Empty.CsvValuesArray()).Concat(new[]
                {
                    $@"{(XTimeGrid != null ? $@"{XTimeGrid.Value:G}" : "")}",
                    $@"{(XTimeTrain != null ? $@"{XTimeTrain.Value:G}" : "")}",
                    $@"{(XTimeTest != null ? $@"{XTimeTest.Value:G}" : "")}",

                    XPredictionThreshold?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    XPredictionThresholdClass?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    XRepetitionsIndex.ToString(NumberFormatInfo.InvariantInfo),
                    XOuterCvIndex.ToString(NumberFormatInfo.InvariantInfo),

                    XClassId?.ToString(NumberFormatInfo.InvariantInfo) ?? "",
                    XClassWeight?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                    XClassName ?? "",
                    XClassSize.ToString("G17", NumberFormatInfo.InvariantInfo),
                    XClassTrainSize.ToString("G17", NumberFormatInfo.InvariantInfo),
                    XClassTestSize.ToString("G17", NumberFormatInfo.InvariantInfo)
                }).Concat(Metrics?.CsvValuesArray() ?? MetricsBox.Empty.CsvValuesArray()).Concat(new[]
                {
                    //// roc x,y points (11 point and all thresholds) // $"{Math.Round(a.x, 6)}:{Math.Round(a.y, 6)}"?
                    RocXyStrAll != null && RocXyStrAll.Length > 0
                        ? $"FPR:TPR;{string.Join(";", RocXyStrAll?.Select(a => $"{a.x:G17}:{a.y:G17}").ToArray() ?? Array.Empty<string>())}"
                        : "",
                    RocXyStrElevenPoint != null && RocXyStrElevenPoint.Length > 0
                        ? $"FPR:TPR;{string.Join(";", RocXyStrElevenPoint?.Select(a => $"{a.x:G17}:{a.y:G17}").ToArray() ?? Array.Empty<string>())}"
                        : "",

                    //// precision-recall chart x,y points (11 point and all thresholds)
                    PrXyStrAll != null && PrXyStrAll.Length > 0
                        ? $"TPR:PPV;{string.Join(";", PrXyStrAll?.Select(a => $"{a.x:G17}:{a.y:G17}").ToArray() ?? Array.Empty<string>())}"
                        : "",
                    PrXyStrElevenPoint != null && PrXyStrElevenPoint.Length > 0
                        ? $"TPR:PPV;{string.Join(";", PrXyStrElevenPoint?.Select(a => $"{a.x:G17}:{a.y:G17}").ToArray() ?? Array.Empty<string>())}"
                        : "",

                    //// precision-recall interpolated x,y points (11 point and all thresholds)
                    PriXyStrAll != null && PriXyStrAll.Length > 0
                        ? $"TPR:PPV;{string.Join(";", PriXyStrAll?.Select(a => $"{a.x:G17}:{a.y:G17}").ToArray() ?? Array.Empty<string>())}"
                        : "",
                    PriXyStrElevenPoint != null && PriXyStrElevenPoint.Length > 0
                        ? $"TPR:PPV;{string.Join(";", PriXyStrElevenPoint?.Select(a => $"{a.x:G17}:{a.y:G17}").ToArray() ?? Array.Empty<string>())}"
                        : "",

                    Thresholds != null && Thresholds.Length > 0
                        ? string.Join(";", Thresholds?.Select(a => $"{a:G17}").ToArray() ?? Array.Empty<string>())
                        : "",
                    Predictions != null && Predictions.Length > 0
                        ? string.Join(";", Predictions?.Select(a => string.Join("|", a?.CsvValuesArray() ?? Prediction.Empty.CsvValuesArray())).ToArray() ?? Array.Empty<string>())
                        : ""
                }).Select(a => a?.Replace(",", ";", StringComparison.OrdinalIgnoreCase) ?? "").ToArray();
        }

        public string CsvValuesString()
        {
            return string.Join(",", CsvValuesArray());
        }
    }
}