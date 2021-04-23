using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SvmFsLib;

namespace SvmFsWkr
{
    public class SvmFsWkr
    {
        public static async Task Main(string[] args)
        {
            // parameters: -ExperimentName [] -IterationIndex [] -InstanceId []

            // get [experiment name, iteraton index, instance id] from command line

            // load work file for this instance id

            // load instance cache, if exists

            // load dataset

            // run outer/inner cross-validation and performance estimation

            // save results
        }

        public static int workShareInstanceNumStarted = 0;
        public static object workShareInstanceNumStartedLock = new object();
        public static int workShareInstanceNumComplete = 0;
        public static object workShareInstanceNumCompleteLock = new object();

        public async Task<(SvmFsCtl.IndexData id, ConfusionMatrix cm)[]> ProcessJob(IndexData indexData, int workShareInstanceIndex, int workShareInstanceSize, CancellationToken mainCt, CancellationTokenSource loopCts, CancellationToken loopCt)
        {
            try
            {
                lock (workShareInstanceNumStartedLock) workShareInstanceNumStarted++;

                if (indexData == default)
                {
                    Logging.LogEvent($@"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] Job: Exiting: indexData was default...");

                    return default;
                }

                if (callerCt.IsCancellationRequested || mainCt.IsCancellationRequested || loopCt.IsCancellationRequested)
                {
                    if (workShareInstanceSize - workShareInstanceNumComplete > 1)
                    {
                        Logging.LogEvent($@"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] Job: Exiting: Cancellation requested...");
                        return default;
                    }
                }

                // ensure WriteInstance has been called...
                try { await WriteInstance(instanceGuid, experimentName, iterationIndex, force: false, ct: loopCt).ConfigureAwait(false); }
                catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }

                if (syncRequest == default)
                {
                    syncRequest = await GetSyncRequest(true, instanceGuid, experimentName, iterationIndex, requestSync: false, syncGuids, loopCt).ConfigureAwait(false);
                }
                if (syncRequest != default)
                {
                    try { if (loopCts != null && !loopCts.IsCancellationRequested) loopCts?.Cancel(); } catch (Exception) { }
                    return default;
                }


                var mocvi = CrossValidate.MakeOuterCvInputs(baseLineDataSet, baseLineColumnIndexes, dataSet, indexData, ct: loopCt);
                if (mocvi == default || mocvi.outerCvInputs.Length == 0)
                {
                    Logging.LogEvent($@"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] Job: Exiting: MakeOuterCvInputs returned default...");
                    return default;
                }

                // ensure WriteInstance has been called...
                try { await WriteInstance(instanceGuid, experimentName, iterationIndex, force: false, ct: loopCt).ConfigureAwait(false); }
                catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }

                if (syncRequest == default)
                {
                    syncRequest = await GetSyncRequest(true, instanceGuid, experimentName, iterationIndex, requestSync: false, syncGuids, loopCt).ConfigureAwait(false);
                }
                if (syncRequest != default)
                {
                    try { if (loopCts != null && !loopCts.IsCancellationRequested) loopCts?.Cancel(); } catch (Exception) { }
                    return default;
                }


                var ret = await CrossValidate.CrossValidatePerformanceAsync( libsvmTrainRuntime,  libsvmPredictRuntime, mocvi.outerCvInputs, mocvi.mergedCvInput, indexData, ct: loopCt).ConfigureAwait(false);
                if (ret == default || ret.Length == 0 || ret.Any(a => a.id == default || a.cm == default))
                {
                    Logging.LogEvent($@"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] Job: Exiting: CrossValidatePerformanceAsync returned default...");

                    return default;
                }

                // ensure WriteInstance has been called...
                try { await WriteInstance(instanceGuid, experimentName, iterationIndex, force: false, ct: loopCt).ConfigureAwait(false); }
                catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }

                if (syncRequest == default)
                {
                    syncRequest = await GetSyncRequest(true, instanceGuid, experimentName, iterationIndex, requestSync: false, syncGuids, loopCt).ConfigureAwait(false);
                }
                if (syncRequest != default)
                {
                    try { if (loopCts != null && !loopCts.IsCancellationRequested) loopCts?.Cancel(); } catch (Exception) { }
                    return default;
                }


                Logging.LogEvent($@"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] Job: Completed job {workShareInstanceIndex} of {workShareInstanceSize} (IdJobUid=[{indexData.IdJobUid}]; IdGroupArrayIndex=[{indexData.IdGroupArrayIndex}])");

                lock (workShareInstanceNumCompleteLock) workShareInstanceNumComplete++;

                return ret;
            }
            catch (Exception e)
            {
                Logging.LogException(e, $@"[{instanceGuid:N}]");
                return default;
            }
        }
    }
}
