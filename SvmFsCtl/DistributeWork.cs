using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SvmFsLib;

namespace SvmFsCtl
{

    public class DistributeWork
    {
        public const string ModuleName = nameof(DistributeWork);


       


        





        public async Task<List<(IndexData id, ConfusionMatrix cm)>> ServeIpcJobsAsync(Guid instanceGuid, string experimentName, int iterationIndex, DataSet baseLineDataSet, int[] baseLineColumnIndexes, DataSet dataSet, IndexData[] indexesWhole, ulong lvl = 0, bool asParallel = true, CancellationToken callerCt = default)
        {
            if (callerCt.IsCancellationRequested)
            {
                Logging.LogEvent($"[{instanceGuid:N}] Cancellation requested.");
                Logging.LogExit();
                return default;
            }

            //var iterFn = Program.GetIterationFilename(indexesWhole, callerCt);
            //folder = Path.Combine(Program.ProgramArgs.ResultsRootFolder, $@"_ipc_{Program.ProgramArgs.ServerGuid:N}", $"_{iterFn}");//$@"_{iterationIndex}_{experimentName}");

            var folder = Path.Combine(CacheLoad.GetIterationFolder(SvmFsCtl.ProgramArgs.ResultsRootFolder, experimentName, iterationIndex, ct: callerCt), $@"_ipc_{Sha1(SvmFsCtl.GetIterationFilename(indexesWhole, callerCt))}");

            //using var methodCts = new CancellationTokenSource();
            //var methodCt = methodCts.Token;
            //using var methodLinkedCts = CancellationTokenSource.CreateLinkedTokenSource(callerCt, methodCt);
            //var methodLinkedCt = methodLinkedCts.Token;


            Logging.LogGap(2);
            Logging.LogEvent($@"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] Start of ServeIpcJobsAsync for iteration [{iterationIndex}] for experiment [{experimentName}].");
            Logging.LogGap(2);


            // syncResults is the list of job item indexes completed within the cluster
            var syncResultIds = Array.Empty<int>();
            var allInnerResultIds = Array.Empty<int>();

            // load cache
            //var cacheFiles = await IoProxy.GetFilesAsync(true, ct, cacheFolder, "_cache_*.csv", SearchOption.TopDirectoryOnly).ConfigureAwait(false);
            //var outerResultIds = Array.Empty<int>();
            //var outerResults = Array.Empty<(IndexData id, ConfusionMatrix cm)>();

            var instanceIterationCmLoaded = new List<(IndexData id, ConfusionMatrix cm)>();
            var masterIterationCmLoaded = new List<(IndexData id, ConfusionMatrix cm)>();
            var (indexesLoaded, indexesNotLoaded) = CacheLoad.UpdateMissing(masterIterationCmLoaded, indexesWhole, true, callerCt);
            var cacheFilesLoaded = new List<string>();
            var syncGuids = new[] { instanceGuid };

            


            

            //todo: why is iteration 0 being repeated?
            var instanceCacheFileIndex = 0;

            async Task SaveInstanceCache()
            {
                if ((instanceIterationCmLoaded?.Count ?? 0) == 0) return;

                var cacheFolder = folder/*GetIpcCommsFolder(experimentName, iterationIndex)*/;
                var cacheSaveFn = "";

                try
                {
                    do
                    {
                        var x = Array.IndexOf(syncGuids ?? Array.Empty<Guid>(), instanceGuid);
                        var xl = (syncGuids?.Length ?? 0);

                        cacheSaveFn = Path.Combine(cacheFolder, $"_cache_{iterationIndex}_{instanceGuid:N}_{instanceCacheFileIndex++}_{x}_{xl}.csv");
                    } while (File.Exists(cacheSaveFn));
                }
                catch (Exception e)
                {
                    Logging.LogException(e);
                    throw;
                }


                var cacheSaveLines = instanceIterationCmLoaded.AsParallel().AsOrdered().Select(a => $@"{a.id?.CsvValuesString() ?? IndexData.Empty.CsvValuesString()},{a.cm.CsvValuesString() ?? ConfusionMatrix.Empty.CsvValuesString()}").ToList();
                cacheSaveLines.Insert(0, $@"{IndexData.CsvHeaderString},{ConfusionMatrix.CsvHeaderString}");
                await IoProxy.WriteAllLinesAsync(true, callerCt, cacheSaveFn, cacheSaveLines).ConfigureAwait(false);

                instanceIterationCmLoaded = new List<(IndexData id, ConfusionMatrix cm)>();
            }

            

            (string file, string[] data, string syn, Guid sourceGuid, string requestCode, Guid responseGuid, DateTime requestTime, Guid[] syncActiveInstances) syncRequest = default;

            Task KeepSynchronizedTask(Guid[] syncGuids, CancellationToken mainCt, CancellationTokenSource loopCts)
            {
                var loopCt = loopCts.Token;

                var syncTask = Task.Run(async () =>
                    {
                        while (!callerCt.IsCancellationRequested && !mainCt.IsCancellationRequested && !loopCt.IsCancellationRequested)
                        {
                            try
                            {
                                try { await Task.Delay(TaskGetSyncRequestLoopDelay, loopCt).ConfigureAwait(false); }
                                catch (OperationCanceledException e) { continue; }
                                catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); continue; }

                                syncRequest = await GetSyncRequest(true, instanceGuid, experimentName, iterationIndex, requestSync: false, syncGuids, loopCt).ConfigureAwait(false);

                                var cancel = false;

#if DEBUG
                                while (Console.KeyAvailable)
                                {
                                    Console.ReadKey(true);
                                    cancel = true;
                                }
#endif
                                if (syncRequest != default || cancel)
                                {

                                    //try
                                    //{
                                    //    Logging.LogEvent($"[{instanceGuid:N}] Cancelling...");
                                    //    loopCts?.Cancel();
                                    //}
                                    //catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }

                                    break;
                                }
                            }
                            catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
                        }

                        try
                        {
                            Logging.LogEvent($"[{instanceGuid:N}] [{nameof(KeepSynchronizedTask)}] Cancellation requested from: {(callerCt.IsCancellationRequested ? $" {nameof(callerCt)}" : "")}{(mainCt.IsCancellationRequested ? $" {nameof(mainCt)}" : "")}{(loopCt.IsCancellationRequested ? $" {nameof(loopCt)}" : "")}");
                            loopCts?.Cancel();
                        }
                        catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }

                    },
                    loopCt);

                return syncTask;
            }


            var workShareInstanceNumStarted = 0;
            var workShareInstanceNumStartedLock = new object();
            var workShareInstanceNumComplete = 0;
            var workShareInstanceNumCompleteLock = new object();
            Task EtaTask(int workShareInstanceSize, CancellationToken mainCt, CancellationToken loopCt)
            {
                lock (workShareInstanceNumStartedLock) workShareInstanceNumStarted = 0;
                lock (workShareInstanceNumCompleteLock) workShareInstanceNumComplete = 0;

                var etaTask = Task.Run(async () =>
                {
                    var startTime = DateTime.UtcNow;
                    var total = workShareInstanceSize;
                    var started = 0;
                    var completed = 0;

                    while (!callerCt.IsCancellationRequested && !mainCt.IsCancellationRequested && !loopCt.IsCancellationRequested)
                    {
                        try
                        {
                            lock (workShareInstanceNumStartedLock) started = workShareInstanceNumStarted;
                            lock (workShareInstanceNumCompleteLock) completed = workShareInstanceNumComplete;
                            var itemsRemaining = total - completed;
                            var timeNow = DateTime.UtcNow;
                            var timeElapsed = timeNow - startTime;
                            var timeEach = completed > 0
                                ? timeElapsed / completed
                                : (started > 0
                                    ? timeElapsed / started
                                    : timeElapsed);
                            var timeRemaining = timeEach * itemsRemaining;

                            Logging.LogEvent($"[{instanceGuid:N}] ETA. Jobs: (Total: [{total}], Started: [{started}], Complete: [{completed}], Remaining: [{itemsRemaining}]) Time Elapsed: [{timeElapsed:dd\\:hh\\:mm\\:ss}]. Average Time Per Job: [{timeEach:dd\\:hh\\:mm\\:ss}]. Estimated Time Remaining: [{timeRemaining:dd\\:hh\\:mm\\:ss}].");

                            try { await Task.Delay(TaskEtaLoopDelay, loopCt).ConfigureAwait(false); }
                            catch (OperationCanceledException) { }
                            catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
                        }
                        catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
                    }

                },
                    loopCt);

                return etaTask;
            }

            async Task<(IndexData id, ConfusionMatrix cm)[]> ProcessJob(string libsvmTrainRuntime, string libsvmPredictRuntime, IndexData indexData, int workShareInstanceIndex, int workShareInstanceSize, CancellationToken mainCt, CancellationTokenSource loopCts, CancellationToken loopCt)
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


                    var mocvi = CrossValidate.MakeOuterCvInputs(baseLineDataSet, baseLineColumnIndexes, dataSet, indexData, ct: loopCt);
                    if (mocvi == default || mocvi.outerCvInputs.Length == 0)
                    {
                        Logging.LogEvent($@"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] Job: Exiting: MakeOuterCvInputs returned default...");
                        return default;
                    }

                    
                    var ret = await CrossValidate.CrossValidatePerformanceAsync( libsvmTrainRuntime,  libsvmPredictRuntime, mocvi.outerCvInputs, mocvi.mergedCvInput, indexData, ct: loopCt).ConfigureAwait(false);
                    if (ret == default || ret.Length == 0 || ret.Any(a => a.id == default || a.cm == default))
                    {
                        Logging.LogEvent($@"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] Job: Exiting: CrossValidatePerformanceAsync returned default...");

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



            await RefreshCache().ConfigureAwait(false);

            //var gsr1 = GetSyncRequest(instanceGuid, true, syncGuids);
            //var gsr2 = GetSyncResponse(instanceGuid, gsr1, Array.Empty<int>());


            if (!indexesNotLoaded.Any())
            {
                Logging.LogGap(2);
                Logging.LogEvent($@"All jobs already cached of ServeIpcJobsAsync for iteration [{iterationIndex}] for experiment [{experimentName}].");
                Logging.LogGap(2);
            }


            var countOuter = 0;
            while (indexesNotLoaded.Any())
            {

                Logging.LogGap(2);
                Logging.LogEvent($@"Start work sharing in ServeIpcJobsAsync (outer = [{countOuter}]) for iteration [{iterationIndex}] for experiment [{experimentName}].");
                Logging.LogGap(2);

                countOuter++;

                if (callerCt.IsCancellationRequested) return default;

                var workIds = indexesWhole.Select(a => a.IdJobUid).ToArray();
                //var workCompleteIds = indexesLoaded.Select(a => a.IdJobUid).ToArray();
                //var workIncompleteIds = indexesNotLoaded.Select(a => a.IdJobUid).ToArray();

                //////////////
                Logging.LogEvent($"{DateTime.UtcNow} Guid: {instanceGuid:N}");
                //WriteInstance(instanceGuid, experimentName, iterationIndex, true);



                var mainCts = new CancellationTokenSource();
                var mainCt = mainCts.Token;

                var instanceGuidWriterTask = await InstanceGuidWriterTask(mainCt).ConfigureAwait(false);





                // todo: add another variable to store actual results and save them.


                var isWorkOutOfSync = false;
                var isSyncOk = false;
                var w = -1;
                (Guid instanceGuid, int[] instanceWork)[] workShareList = null;
                int[] workShareInstance = null;
                var loopDidRun = false;
                var loopDidWork = false;
                var isWorkShareSetAndEmpty = false;
                var finalSync = false;
                var countInner = 0;
                while (!callerCt.IsCancellationRequested && !mainCt.IsCancellationRequested)
                {
                    Console.WriteLine();
                    Console.WriteLine();
                    Logging.LogEvent($@"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Start work sharing in ServeIpcJobsAsync (experiment = [{experimentName}], iteration = [{iterationIndex}], outer = [{countOuter}], inner = [{countInner}]).");
                    Console.WriteLine();
                    Console.WriteLine();
                    countInner++;

                    await SaveInstanceCache().ConfigureAwait(false);

                    w++;
                    var isFirstIteration = w == 0;

                    if (isFirstIteration && MainLoopInitDelay != TimeSpan.Zero)
                    {
                        try { await Logging.WaitAsync(MainLoopInitDelay, $"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Main loop init delay", ct: mainCt).ConfigureAwait(false); }
                        catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}]"); }
                    }

                    if (syncRequest != default)
                    {
                        loopDidRun = false;
                        loopDidWork = false;

                        var syncResponse = await GetSyncResponse(instanceGuid, experimentName, iterationIndex, syncRequest, syncResultIds, mainCt).ConfigureAwait(false);
                        isSyncOk = syncResponse.didSync;

                        if (isSyncOk)
                        {
                            syncGuids = syncRequest.syncActiveInstances;

                            if (syncResponse.syncData != null)
                            {
                                var syncWorkCompleteIds = syncResponse.syncData;

                                Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Sync returned Ids: {string.Join(", ", syncWorkCompleteIds.Select(a => $"{a}").ToArray())}");

                                var syncResultIdsSame = (syncWorkCompleteIds ?? Array.Empty<int>()).OrderBy(a => a).Distinct().SequenceEqual((syncResultIds ?? Array.Empty<int>()).OrderBy(a => a).Distinct());

                                if (!syncResultIdsSame)
                                {
                                    var newIndexes = (syncWorkCompleteIds ?? Array.Empty<int>()).Except((syncResultIds ?? Array.Empty<int>())).ToArray();
                                    var removedIndexes = (syncResultIds ?? Array.Empty<int>()).Except((syncWorkCompleteIds ?? Array.Empty<int>())).ToArray();

                                    Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Synchronized indexes have changed. New indexes [{newIndexes.Length}]: {string.Join(", ", newIndexes)}. Removed indexes [{removedIndexes.Length}]: {string.Join(", ", removedIndexes)}.");

                                    syncResultIds = syncResultIds.Union(syncWorkCompleteIds).ToArray();
                                }
                                else
                                {
                                    Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Synchronized indexes haven't changed from previous synchronization.");
                                }
                            }

                            Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Sync merged Ids: {string.Join(", ", syncResultIds.Select(a => $"{a}").ToArray())}");

                            isWorkOutOfSync = false;
                            isFirstIteration = false;
                            //isSyncOk = true;
                            loopDidRun = false;
                            loopDidWork = false;
                            //isAllWorkDone = false;
                            //finalSync = false;

                            syncRequest = default;
                            await RefreshCache().ConfigureAwait(false);
                            Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Sync request completed... continue...");
                        }
                        else
                        {
                            Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Sync failed. Continue.");
                            syncRequest = default;

                            continue;
                        }



                        continue;
                    }

                    var isAllWorkDone = !workIds.Except(syncResultIds).Any();

                    // always request sync, since, it's either A) first time, B) instances changed, C) last sync failed, D) no work left, need to steal some (unless there was never any work to start with)
                    // except: if all work is already done?...  which would cause a sync-call loop

                    // finalSync makes sure a sync is done after all work is complete, this ensures instance cache is saved before continuing... as that is done before sync code.
                    isWorkShareSetAndEmpty = workShareInstance != null && workShareInstance.Length == 0; // if null, not set yet (null doesn't mean empty)
                    var requestSync = isFirstIteration || isWorkOutOfSync || !isSyncOk || (loopDidRun && loopDidWork) || (isAllWorkDone && (!finalSync || !isSyncOk));

                    if (isWorkOutOfSync) Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Synchronization required: Work out of sync.");
                    if (isFirstIteration) Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Synchronization required: First iteration.");
                    if (!isSyncOk) Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Synchronization required: Last synchronization failed.");
                    if (loopDidRun && loopDidWork) Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Synchronization required: New work has been done.");
                    if (isAllWorkDone && (!finalSync || !isSyncOk))
                    {
                        finalSync = true;
                        Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Synchronization required: All work is done, final synchronization.");
                    }
                    // problem: if no work is allocated...?
                    // problem: if all allocated work is complete, so need to work steal?


                    syncRequest = await GetSyncRequest(false, instanceGuid, experimentName, iterationIndex, requestSync, syncGuids, mainCt).ConfigureAwait(false);

                    if (syncRequest != default)
                    {
                        Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Sync request found. Continue to run synchronization.");
                        continue;
                    }

                    isSyncOk = true;
                    isWorkOutOfSync = false;
                    isFirstIteration = false;
                    loopDidRun = false;
                    loopDidWork = false;

                    if (isWorkShareSetAndEmpty && !isAllWorkDone)
                    {
                        await RefreshCache().ConfigureAwait(false);

                        try { await Logging.WaitAsync(MainLoopNoWorkDelay, $"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] There is no work for this instance to do, waiting for retry.", ct: mainCt).ConfigureAwait(false); }
                        catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}]"); }

                        continue;
                    }

                    if (isAllWorkDone)
                    {
                        // todo: finished all work, so sync to share/save it with other instances?

                        // todo: or just save to an overall merged cache file?

                        Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] All jobs complete.");
                        break;
                    }

                    Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Main loop continuing...");

                    var workCompleteIds = indexesLoaded.Select(a => a.IdJobUid).ToArray();
                    var workIncompleteIds = indexesNotLoaded.Select(a => a.IdJobUid).ToArray();

                    if (workIncompleteIds.Length == 0)
                    {
                        // ???
                    }
                    // check if workIncompleteIds same sequence as ...

                    workShareList = RedistributeWork(workIds, workCompleteIds, workIncompleteIds, syncGuids);
                    workShareInstance = workShareList.FirstOrDefault(a => a.instanceGuid == instanceGuid).instanceWork;
                    if (workShareInstance.Length == 0)
                    {
                        Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Work share is empty... continue...");
                        continue;
                    }

                    var workShareInstanceItems = indexesWhole.AsParallel().AsOrdered().Where(a => workShareInstance.AsParallel().AsOrdered().Any(b => a.IdJobUid == b)).ToArray();

                    // if there are items in the todo list which are already done, need re-sync?
                    // are all of the sync ids actually loaded?  if not, an instance must be out of sync due to e.g. latency

                    var checkWorkSynced = (workCompleteIds?.OrderBy(a => a).Distinct().ToArray() ?? Array.Empty<int>()).SequenceEqual(syncResultIds?.OrderBy(a => a).Distinct().ToArray() ?? Array.Empty<int>());
                    if (!checkWorkSynced)
                    {
                        isWorkOutOfSync = true;
                        Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Work is out of sync... continue to re-sync...");
                        continue;
                    }

                    var loopCts = new CancellationTokenSource();
                    var loopCt = loopCts.Token;
                    var syncTask = KeepSynchronizedTask(syncGuids, mainCt, loopCts);
                    var etaTask = EtaTask(workShareInstance.Length, mainCt, loopCt);

                    Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Tasks starting...");
                    Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Work share Ids ({workShareInstance.Length}): {string.Join(", ", workShareInstance.Select(a => $"{a}").ToArray())}");




                    var innerResultsTasks = asParallel
                        ? workShareInstanceItems.AsParallel().AsOrdered().Select(async (indexData, workShareInstanceIndex) => await ProcessJob(indexData, workShareInstanceIndex, workShareInstance?.Length ?? 0, mainCt, loopCts, loopCt).ConfigureAwait(false)).ToArray()
                        : workShareInstanceItems.Select(async (indexData, workShareInstanceIndex) => await ProcessJob(indexData, workShareInstanceIndex, workShareInstance?.Length ?? 0, mainCt, loopCts, loopCt).ConfigureAwait(false)).ToArray();

                    //var innerResultsTasksIncomplete = innerResultsTasks.ToArray();
                    //
                    //while (innerResultsTasksIncomplete.Any(a=>!a.IsCompleted))
                    //{
                    //    try
                    //    {
                    //        var completedTask = await Task.WhenAny(innerResultsTasksIncomplete).ConfigureAwait(false);
                    //        innerResultsTasksIncomplete = innerResultsTasksIncomplete.Except(new[] { completedTask }).ToArray();
                    //    }
                    //    catch (Exception e)
                    //    {
                    //        Logging.LogException(e);
                    //    }
                    //}

                    try { await Task.WhenAll(innerResultsTasks).ConfigureAwait(false); }
                    catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }

                    var innerResults = innerResultsTasks.Where(a => a.IsCompletedSuccessfully && a.Result != default && a.Result.Length > 0).SelectMany(a => a.Result).ToArray();
                    innerResults = innerResults.Where(a => a != default && a.cm != default && a.id != default).ToArray();

                    instanceIterationCmLoaded.AddRange(innerResults);

                    var innerResultIds = innerResults.Select(a => a.id.IdJobUid).Distinct().ToArray();
                    allInnerResultIds = allInnerResultIds.Union(innerResultIds).Distinct().ToArray();

                    syncResultIds = syncResultIds.Union(innerResultIds).ToArray();

                    Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Tasks complete...");
                    Logging.LogEvent($"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] [{countInner}] Inner results: {innerResultIds.Length} items. Ids: {string.Join(", ", innerResultIds.Select(a => $"{a}").ToArray())}");



                    loopCts.Cancel();
                    loopDidRun = workShareInstance.Length > 0;
                    loopDidWork = innerResults.Length > 0;
                    try { await Task.WhenAll(etaTask, syncTask).ConfigureAwait(false); }
                    catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
                    loopCts.Dispose();
                }//while (!ct.IsCancellationRequested && !mainCt.IsCancellationRequested)

                // save instance results - already saved in loop above
                //await SaveInstanceCache().ConfigureAwait(false);

                // load results from other instances ... these will be already written as a final sync is done before they are saved.
                await RefreshCache().ConfigureAwait(false);
                //syncResultIds = Array.Empty<int>();


                mainCts.Cancel();
                try { await Task.WhenAll(instanceGuidWriterTask).ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
                mainCts.Dispose();



                // delete instance id file

            }//while (indexesNotLoaded.Any())

            // no need to save master cache, individual is fine and will save time
            //await SaveMasterCache().ConfigureAwait(false);

            try { await WriteInstance(instanceGuid, experimentName, iterationIndex, true, true, callerCt).ConfigureAwait(false); }
            catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }

            //var instanceJobIdsCompleted = instanceIterationCmLoaded.Select(a => a.id.IdJobUid).OrderBy(a => a).Distinct().ToArray();

            Logging.LogGap(2);
            Logging.LogEvent($@"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] End of ServeIpcJobsAsync for experiment [{experimentName}] iteration [{iterationIndex}].");
            Logging.LogEvent($@"[{instanceGuid:N}] [{experimentName}] [{iterationIndex}] [{countOuter}] Jobs completed by this instance [{allInnerResultIds.Length}]: {string.Join(", ", allInnerResultIds)}.");
            Logging.LogGap(2);


            return masterIterationCmLoaded;
        }
    }
}
