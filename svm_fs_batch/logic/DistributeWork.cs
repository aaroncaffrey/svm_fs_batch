using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{

    public class DistributeWork
    {
        public const string ModuleName = nameof(DistributeWork);

        //// how long to write the instance guid
        //public static TimeSpan InstanceNotifyRate = TimeSpan.FromSeconds(2);

        //// how often to update instance list
        //public static TimeSpan InstanceUpdateRate = TimeSpan.FromSeconds(2);

        //// how long before an instance times out without an update
        //public static TimeSpan InstanceTimeoutRate = TimeSpan.FromSeconds(10);






        //public static async Task ReloadCache(IndexDataContainer indexDataContainer, CancellationToken ct)
        //{
        /*
        
        var serverCmFile = Path.Combine(ServerFolder, $@"_server_cache_{Program.GetIterationFilename(indexDataContainer.IndexesWhole.Select(a => a.id).ToArray(), ct)}.csv");
        var iterationWholeResults = new List<(IndexData id, ConfusionMatrix cm)>();
        var iterationWholeResultsLines = new List<string>();
        iterationWholeResultsLines.Add($"{IndexData.CsvHeaderString},{ConfusionMatrix.CsvHeaderString}");

        if (IoProxy.ExistsFile(true, serverCmFile, ModuleName, ct: ct) && IoProxy.FileLength(serverCmFile, ct) > 0)
        {
            // todo: var cache = await CacheLoad.LoadCacheFileAsync(serverCmFile, indexDataContainer, ct).ConfigureAwait(false);
            // todo: if (cache != null && cache.Length > 0)
            // todo: {
            // todo: iterationWholeResults.AddRange(cache);
            // todo: iterationWholeResultsLines.AddRange(cache.Select(a => $"{a.id?.CsvValuesString() ?? IndexData.Empty.CsvValuesString()},{a.cm?.CsvValuesString() ?? ConfusionMatrix.Empty.CsvValuesString()}").ToArray());
            // todo: }
        }
        else
        {
            // create cache file, if doesn't exist
            var header = $"{IndexData.CsvHeaderString},{ConfusionMatrix.CsvHeaderString}";
            await IoProxy.WriteAllLinesAsync(true, ct, serverCmFile, new[] { header }).ConfigureAwait(false);
        }

        // todo: CacheLoad.UpdateMissing(iterationWholeResults, indexDataContainer, ct: ct);
        var ids = indexDataContainer.IndexesWhole.Select(a => a.index).ToArray();
        var completeIds = indexDataContainer.IndexesLoadedWhole.Select(a => a.index).ToArray();
        var incompleteIds = indexDataContainer.IndexesMissingWhole.Select(a => a.index).ToArray();
        */
        //}

















        // how often to write instance id to file
        public TimeSpan WriteInstanceCacheTimeout = TimeSpan.FromSeconds(10);

        // how long to wait before re-checking when instances have changed
        public TimeSpan ReadInstancesChangedRetry = TimeSpan.FromSeconds(15);

        // how long do instances survive from their time stamp
        public TimeSpan ReadInstancesInstanceTimeout = TimeSpan.FromSeconds(60);

        // how long to cache instance guids for before re-reading them
        public TimeSpan ReadInstancesCacheTimeout = TimeSpan.FromSeconds(15);

        // how long to wait before timing out a sync request
        public TimeSpan GetSyncRequestTimeout = TimeSpan.FromSeconds(60);

        // how long to wait for remote instance sync request file i/o
        public TimeSpan GetSyncRequestIoDelay = TimeSpan.FromSeconds(10);

        // how long to wait for sync response before timing out
        public TimeSpan GetSyncResponseTimeout = TimeSpan.FromSeconds(60);

        // how long to wait for remote instance sync response file i/o
        public TimeSpan GetSyncResponseIoDelay = TimeSpan.FromSeconds(10);


        public TimeSpan GetSyncResponseSyncTimeout1 = TimeSpan.FromSeconds(60);
        public TimeSpan GetSyncResponseSyncTimeout2 = TimeSpan.FromSeconds(60);

        // loop delays
        public TimeSpan TaskWriteInstanceLoopDelay = TimeSpan.FromSeconds(15);
        public TimeSpan MainLoopInitDelay = TimeSpan.FromSeconds(10);
        public TimeSpan MainLoopNoWorkDelay = TimeSpan.FromSeconds(10);
        public TimeSpan TaskGetSyncRequestLoopDelay = TimeSpan.FromSeconds(5);
        public TimeSpan TaskEtaLoopDelay = TimeSpan.FromSeconds(15);

        public string LastWriteInstanceFile;
        public DateTime LastWriteInstanceTime = default;
        public DateTime ReadInstancesTime = default;
        public Guid[] ReadInstancesGuids = Array.Empty<Guid>();
        //public object SyncingLock = new object();



        public async Task WriteInstance(Guid instanceGuid, string experimentName, int iterationIndex, bool force = false, bool cleanOnly = false, CancellationToken ct = default)
        {
            var now = DateTime.UtcNow;
            var elapsed = now - LastWriteInstanceTime;


            if (force || elapsed >= WriteInstanceCacheTimeout)
            {
                // note: write first, then delete old file, otherwise instance would temporarily be missing...
                try
                {

                    var fn = Path.Combine(folder/*GetIpcCommsFolder(experimentName, iterationIndex)*/, $@"instance_{instanceGuid:N}_{now:yyyyMMddHHmmssfffffff}.txt");

                    try
                    {
                        //if (!File.Exists(fn) || new FileInfo(fn).Length == 0)
                        if (!cleanOnly)
                        {
                            await File.WriteAllBytesAsync(fn, new byte[] { 0 }, ct).ConfigureAwait(false);
                            LastWriteInstanceTime = now;

                        }
                    }
                    catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }

                    try
                    {
                        if (!string.IsNullOrEmpty(LastWriteInstanceFile))
                            try { File.Delete(LastWriteInstanceFile); }
                            catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }

                        LastWriteInstanceFile = fn;
                    }
                    catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }

                }
                catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
            }
        }


        public Guid[] ReadInstances(Guid instanceGuid, string experimentName, int iterationIndex, Guid[] knownInstances, bool force = false)
        {
            // filename format: [0] field name, [1] source instance guid, [-1] datetime


            var now = DateTime.UtcNow;
            var elapsed = now - ReadInstancesTime;

            if (force || elapsed >= ReadInstancesCacheTimeout)
            {
                while (true)
                {
                    ReadInstancesTime = now;

                    var readInstancesEnter = ReadInstancesGuids?.ToArray();

                    var instanceMarkerFiles = Directory.GetFiles(folder/*GetIpcCommsFolder(experimentName, iterationIndex)*/, "instance*_?*.txt");


                    var instanceMarkers = instanceMarkerFiles.Select(a =>
                    {
                        var b = Path.GetFileNameWithoutExtension(a).Split('_');
                        if (b.Length != 3) return default;
                        if (b[1].Length != 32) return default;
                        if (b[^1].Length != "yyyyMMddHHmmssfffffff".Length) return default;

                        //var inst = b[0];
                        var sourceGuid = new Guid(b[1]);
                        var requestTime = DateTime.ParseExact(b[^1], "yyyyMMddHHmmssfffffff", DateTimeFormatInfo.InvariantInfo);

                        return (sourceGuid, requestTime);
                    }).Where(a => a != default).ToArray();


                    var instanceMarkersTimedOut = instanceMarkers.Where(a => (now - a.requestTime) >= ReadInstancesInstanceTimeout).ToArray();

                    if (instanceMarkersTimedOut.Length > 0)
                    {
                        Logging.LogEvent($"Instances timed out: {string.Join(", ", instanceMarkersTimedOut.Select(a => $"{a.sourceGuid:N} (timed out: {((now - a.requestTime) - ReadInstancesInstanceTimeout):dd\\:hh\\:mm\\:ss})").ToArray())}");
                    }

                    //var missingInstances = instanceMarkers.Select(a => a.sourceGuid).Except(knownInstances ?? Array.Empty<Guid>()).ToArray();
                    var missingInstances = (knownInstances ?? Array.Empty<Guid>()).Except(instanceMarkers?.Select(a => a.sourceGuid).ToArray() ?? Array.Empty<Guid>()).ToArray();

                    if (missingInstances.Length > 0)
                    {
                        Logging.LogEvent($"Instances missing: {string.Join(", ", missingInstances.Select(a => $"{a}").ToArray())}");
                    }

                    instanceMarkers = instanceMarkers.Except(instanceMarkersTimedOut).ToArray();
                    var instanceMarkerGuids = instanceMarkers.Select(a => a.sourceGuid).ToArray();
                    if (!instanceMarkerGuids.Contains(instanceGuid)) instanceMarkerGuids = instanceMarkerGuids.Concat(new[] { instanceGuid }).ToArray();

                    var instanceGuids = instanceMarkerGuids.Distinct().OrderBy(a => a).ToArray();

                    

                    var instancesChanged = false;
                    //if (!instancesChanged && knownInstances != null && knownInstances.Length > 0) instancesChanged = !(knownInstances ?? Array.Empty<Guid>()).SequenceEqual(instanceGuids ?? Array.Empty<Guid>());
                    if (!instancesChanged && ReadInstancesGuids != null && ReadInstancesGuids.Length > 0) instancesChanged = !(ReadInstancesGuids ?? Array.Empty<Guid>()).SequenceEqual(instanceGuids ?? Array.Empty<Guid>());

                    ReadInstancesGuids = instanceGuids;

                    if (instancesChanged && (readInstancesEnter?? Array.Empty<Guid>()).SequenceEqual(ReadInstancesGuids?? Array.Empty<Guid>()))
                    {
                        instancesChanged = false;
                    }

                    if (instancesChanged)
                    {
                        Logging.LogEvent($"[{instanceGuid:N}] Instances changed - checking again after {ReadInstancesChangedRetry:dd\\:hh\\:mm\\:ss}");

                        Logging.Wait(ReadInstancesChangedRetry, "Read instances - instances changed - retry delay", ModuleName);
                        continue;
                    }

                    return instanceGuids;
                }
            }
            else { return ReadInstancesGuids; }
        }

        public static string Sha1(string query)
        {
            if (string.IsNullOrEmpty(query)) return default;

            var crypt = new System.Security.Cryptography.SHA1Managed();//.SHA256Managed();
            var hash = crypt.ComputeHash(Encoding.UTF8.GetBytes(query));

            var sb = new StringBuilder();
            foreach (var b in hash)
            {
                sb.Append($"{b:x2}");
            }
            return sb.ToString();
        }


        public static int[] GetInstanceWorkSizes(int numInstances, int numWork)
        {
            if (numInstances == 0) return Array.Empty<int>();

            var y = numWork / numInstances;
            var r = numWork % numInstances;
            var list = Enumerable.Range(0, numInstances).Select((b, i) => y + (i < r ? 1 : 0)).ToArray();

            return list;
        }



        public static (Guid instanceGuid, int[] instanceWork)[] RedistributeWork(int[] work, int[] workComplete, int[] workIncomplete, Guid[] instances)
        {
            if (instances == null || instances.Length == 0) return default;

            if (workComplete == null && work != null && workIncomplete != null) workComplete = work.Except(workIncomplete).ToArray();
            if (workIncomplete == null && work != null && workComplete != null) workIncomplete = work.Except(workComplete).ToArray();

            var instanceWorkSizes = GetInstanceWorkSizes(instances.Length, workIncomplete.Length);
            var list = new (Guid instanceGuid, int[] instanceWork)[instances.Length];
            for (var i = 0; i < list.Length; i++)
            {
                list[i].instanceGuid = instances[i];
                list[i].instanceWork = new int[instanceWorkSizes[i]];
            }

            var instanceWorkIndexes = new int[instances.Length];

            var instanceIndex = -1;
            for (var workItemIndex = 0; workItemIndex < workIncomplete.Length; workItemIndex++)
            {
                do
                {
                    instanceIndex++;
                    if (instanceIndex >= instances.Length) instanceIndex = 0;
                } while (instanceWorkIndexes[instanceIndex] >= instanceWorkSizes[instanceIndex]);


                list[instanceIndex].instanceWork[instanceWorkIndexes[instanceIndex]++] = workIncomplete[workItemIndex];
            }

            return list;
        }




        //private static DateTime RequestSyncTime = DateTime.MinValue;
        //private static DateTime ResponseSyncTime = DateTime.MinValue;

        public async Task<(string file, string[] data, string syn, Guid sourceGuid, string requestCode, Guid responseGuid, DateTime requestTime, Guid[] syncActiveInstances)> GetSyncRequest(Guid instanceGuid, string experimentName, int iterationIndex, bool requestSync, Guid[] knownInstances, CancellationToken ct)
        {
            Logging.LogCall();

            if (ct.IsCancellationRequested)
            {
                Logging.LogExit();
                return default;
            }

            //lock (SyncingLock)
            {

                //var now1 = DateTime.UtcNow;
                //var elapsed1 = now1 - RequestSyncTime;
                //var elapsed2 = now1 - ResponseSyncTime;
                //if (!requestSync && (elapsed1 < TimeSpan.FromSeconds(5)))// || elapsed2 < TimeSpan.FromSeconds(5)))
                //{
                //Logging.LogEvent(DateTime.UtcNow + " " + "Exit sync1() - frequency too high");

                //return default;
                //}

                //RequestSyncTime = now1;



                if (knownInstances == null) { knownInstances = Array.Empty<Guid>(); }


                (string file, string[] data, string syn, Guid sourceGuid, string requestCode, Guid responseGuid, DateTime requestTime) syncRequest = default;

                //var syncRequestFound = false;

                // sync if, requestSync is true, another instance requested sync, or known guids have changed
                while (true)
                {
                    await WriteInstance(instanceGuid, experimentName, iterationIndex, ct: ct).ConfigureAwait(false);

                    if (syncRequest != default) { await Logging.WaitAsync(GetSyncRequestIoDelay, $"[{instanceGuid:N}] Sync request IO delay", ModuleName, ct: ct).ConfigureAwait(false); }

                    // check for sync requests from any instance
                    var activeInstances = GetSyncRequestFiles(instanceGuid, experimentName, iterationIndex, knownInstances, GetSyncRequestTimeout, out var syncRequests, out var now, out syncRequest);

                    if (activeInstances == default)
                    {
                        continue;
                    }

                    //var syncRequestInstances = syncRequests.SelectMany(a => new[] { a.sourceGuid, a.responseGuid }).Distinct().OrderBy(a => a).ToArray();
                    //activeInstances = activeInstances.Union(syncRequestInstances).OrderBy(a => a).ToArray();

                    if (syncRequest == default)
                    {
                        // check for unknown instance GUIDs
                        if (activeInstances.Except(knownInstances).Any() || knownInstances.Except(activeInstances).Any())
                        {
                            requestSync = true;
                            Logging.LogEvent($"[{instanceGuid:N}] Known instances have changed, synchronization required...");
                        }
                        var syncRequestCode = SyncRequestCode(activeInstances);

                        if (requestSync)
                        {


                            var syncRequestFile = Path.Combine(folder/*GetIpcCommsFolder(experimentName, iterationIndex)*/, $@"syn_{instanceGuid:N}_{syncRequestCode}_{now:yyyyMMddHHmmssfffffff}.txt");

                            try
                            {
                                if (!File.Exists(syncRequestFile) || new FileInfo(syncRequestFile).Length == 0)
                                {
                                    Logging.LogEvent($"[{instanceGuid:N}] Sending synchronization request...");
                                    try { await File.WriteAllBytesAsync(syncRequestFile, new byte[] { 0 }, ct).ConfigureAwait(false); }
                                    catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
                                }

                                requestSync = false;
                            }
                            catch (Exception e)
                            {
                                Logging.LogException(e, $"[{instanceGuid:N}]");
                            }


                            continue;
                        }

                        break;
                    }


                    // delete outdated sync requests
                    for (var i = 1; i < syncRequests.Length; i++)
                    {
                        if (syncRequests[i].sourceGuid != syncRequest.sourceGuid || syncRequests[i].requestCode != syncRequest.requestCode || syncRequests[i].requestTime != syncRequest.requestTime)
                            // delete all sync requests older than the latest known.
                            try { File.Delete(syncRequests[i].file); }
                            catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
                    }

                    //Logging.LogEvent($"[{instanceGuid:N}] Exit sync1() {(syncRequest != default ? syncRequest.ToString() : "")}");

                    Logging.LogExit();
                    return (syncRequest.file, syncRequest.data, syncRequest.syn, syncRequest.sourceGuid, syncRequest.requestCode, syncRequest.responseGuid, syncRequest.requestTime, activeInstances);
                }

                //Logging.LogEvent($"[{instanceGuid:N}] Exit sync1() {(syncRequest != default ? syncRequest.ToString() : "")}");
                Logging.LogExit();
                return default;
            }
        }

        public static string SyncRequestCode(Guid[] activeInstances)
        {
            var syncRequestCode = Sha1(string.Join("", activeInstances.OrderBy(a => a).Distinct().Select(a => $"{a:N}").ToArray()));
            return syncRequestCode;
        }

        public Guid[] GetSyncRequestFiles(Guid instanceGuid, string experimentName, int iterationIndex, Guid[] knownInstances, TimeSpan syncRequestTimeout, out (string file, string[] data, string syn, Guid sourceGuid, string requestCode, Guid responseGuid, DateTime requestTime)[] syncRequests, out DateTime now, out (string file, string[] data, string syn, Guid sourceGuid, string requestCode, Guid responseGuid, DateTime requestTime) syncRequest)
        {
            try
            {
                var syncFolder = folder/*GetIpcCommsFolder(experimentName, iterationIndex)*/;
                var syncRequestFiles = Directory.GetFiles(syncFolder, "syn_*.txt");

                var activeInstances = ReadInstances(instanceGuid, experimentName, iterationIndex, knownInstances);
                var syncRequestCode = SyncRequestCode(activeInstances);

                syncRequests = syncRequestFiles.Select(a =>
                {
                    var b = Path.GetFileNameWithoutExtension(a).Split('_');
                    if (b.Length != 4 && b.Length != 5) return default;
                    if (b[1].Length != 32) return default;
                    //if (b[2].Length != 32) return default;
                    if (b.Length == 5 && b[3].Length != 32) return default;
                    if (b[^1].Length != "yyyyMMddHHmmssfffffff".Length) return default;

                    var syn = b[0];
                    var sourceGuid = new Guid(b[1]);
                    var requestCode = b[2]; //new Guid(b[2]);
                    var responseGuid = b.Length == 5
                        ? new Guid(b[3])
                        : default;
                    var requestTime = DateTime.ParseExact(b[^1], $@"yyyyMMddHHmmssfffffff", DateTimeFormatInfo.InvariantInfo);
                    //Logging.LogEvent(DateTime.UtcNow + " " + requestTime + "!!!!");
                    return (file: a, data: b, syn, sourceGuid, requestCode, responseGuid, requestTime);
                }).Where(a => a != default).ToArray();

                var now1 = DateTime.UtcNow;
                now = now1;
                syncRequests = syncRequests.Where(a => syncRequestCode == a.requestCode).ToArray();
                syncRequests = syncRequests.Where(a => now1 - a.requestTime <= syncRequestTimeout).ToArray();
                syncRequests = syncRequests.OrderByDescending(a => a.requestTime).ThenBy(a => a.requestCode).ThenBy(a => a.sourceGuid).ToArray();
                syncRequest = syncRequests.FirstOrDefault(a => a.data.Length == 4);


                return activeInstances;
            }
            catch (Exception e)
            {
                Logging.LogException(e, $"[{instanceGuid:N}]");
                now = DateTime.UtcNow;
                syncRequests = default;
                syncRequest = default;
                return default;
            }
        }

        //(out Guid[] syncGuids, out string[] allData, string[] expectedSyncFiles, string ackResponseFile, (string file, string[] data, string syn, Guid sourceGuid, string requestCode, Guid responseGuid, DateTime requestTime) syncRequest, Guid[] activeInstances, bool didSync, string[] expectedAckFiles, ref bool syncRequestFound)

        public static byte[] IntsToByteBlock(int[] ints)
        {
            var bytes = new byte[ints.Length * sizeof(int)];
            Buffer.BlockCopy(ints, 0, bytes, 0, bytes.Length);
            return bytes;
        }

        public static int[] ByteBlockToInts(byte[] bytes)
        {
            var ints = new int[bytes.Length / sizeof(int)];
            Buffer.BlockCopy(bytes, 0, ints, 0, bytes.Length);
            return ints;
        }

        public async Task<(bool didSync, int[] syncData)> GetSyncResponse(
            Guid instanceGuid, string experimentName, int iterationIndex,
            (string file, string[] data, string syn, Guid sourceGuid, string requestCode, Guid responseGuid, DateTime requestTime, Guid[] syncActiveInstances) syncRequest
        , int[] responseSyncData, CancellationToken ct)
        {
            Logging.LogCall();

            var syncStart = DateTime.UtcNow;

            //Logging.LogEvent($"[{instanceGuid:N}] Enter sync2()");
            //lock (SyncingLock)
            {
                var syncFolder = folder/*GetIpcCommsFolder(experimentName, iterationIndex)*/;
                var expectedSyncFiles = syncRequest.syncActiveInstances.Select(syncGuid => (syncGuid, fn: Path.Combine(syncFolder, $@"syn_{syncRequest.sourceGuid:N}_{syncRequest.requestCode}_{syncGuid:N}_{syncRequest.requestTime:yyyyMMddHHmmssfffffff}.txt"))).ToArray();
                var expectedAckFiles = syncRequest.syncActiveInstances.Select(syncGuid => (syncGuid, fn: Path.Combine(syncFolder, $@"ack_{syncRequest.sourceGuid:N}_{syncRequest.requestCode}_{syncGuid:N}_{syncRequest.requestTime:yyyyMMddHHmmssfffffff}.txt"))).ToArray();

                void del()//string[] expectedSyncFiles, string[] expectedAckFiles)
                {
                    try { File.Delete(syncRequest.file); }
                    catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }

                    for (var i = 0; i < expectedSyncFiles.Length; i++)
                    {
                        try { File.Delete(expectedSyncFiles[i].fn); }
                        catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
                    }

                    for (var i = 0; i < expectedAckFiles.Length; i++)
                    {
                        try { File.Delete(expectedAckFiles[i].fn); }
                        catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
                    }
                }

                var syncResponseFile = Path.Combine(syncFolder, $@"syn_{syncRequest.sourceGuid:N}_{syncRequest.requestCode}_{instanceGuid:N}_{syncRequest.requestTime:yyyyMMddHHmmssfffffff}.txt");
                var ackResponseFile = Path.Combine(syncFolder, $@"ack_{syncRequest.sourceGuid:N}_{syncRequest.requestCode}_{instanceGuid:N}_{syncRequest.requestTime:yyyyMMddHHmmssfffffff}.txt");

                try
                {
                    //if (!File.Exists(syncResponseFile) /*|| new FileInfo(syncResponseFile).Length == 0*/) 
                    //File.WriteAllLines(syncResponseFile, responseSyncData);

                    var responseSyncDataBytes = IntsToByteBlock(responseSyncData);
                    await File.WriteAllBytesAsync(syncResponseFile, responseSyncDataBytes, ct).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Logging.LogException(e, $"[{instanceGuid:N}]");
                    Logging.LogEvent($"[{instanceGuid:N}] Exit {nameof(GetSyncResponse)}() - couldn't write sync response file.");
                    Logging.LogExit();

                    return default;
                }

                var synLoaded = false;
                var ackLoaded = syncRequest.sourceGuid != instanceGuid;
                var syncFileExists = true;

                int[] syncData = null;

                var w = -1;
                while (true)
                {
                    var now = DateTime.UtcNow;
                    var elapsed1 = now - syncRequest.requestTime;
                    var elapsed2 = now - syncStart;
                    if (elapsed1 >= GetSyncResponseSyncTimeout1 || elapsed2 >= GetSyncResponseSyncTimeout2)
                    {
                        del();
                        Logging.LogEvent($"[{instanceGuid:N}] Exit {nameof(GetSyncResponse)}() - sync timeout");
                        Logging.LogExit();

                        return default;
                    }

                    w++;
                    var activeInstances = GetSyncRequestFiles(instanceGuid, experimentName, iterationIndex, syncRequest.syncActiveInstances, GetSyncResponseTimeout, out var syncRequests, out var now1, out var syncRequest1);

                    if (activeInstances == default)
                    {
                        Logging.LogEvent($"[{instanceGuid:N}] Exit {nameof(GetSyncResponse)}() - active instances couldn't be read");
                        Logging.LogExit();

                        return default;
                    }

                    if (syncRequest1 != default)
                    {
                        var syncTimeNewer = syncRequest1.requestTime > syncRequest.requestTime;
                        var syncCodeMismatch = syncRequest1.requestCode != syncRequest.requestCode;

                        if (syncTimeNewer || syncCodeMismatch)
                        {
                            del();
                            Logging.LogEvent($"[{instanceGuid:N}] Exit {nameof(GetSyncResponse)}(){(syncTimeNewer ? " - newer sync request found" : "")}{(syncCodeMismatch ? " - sync code mismatch" : "")}");
                            Logging.LogExit();

                            return default;
                        }
                    }

                    if (w > 0) { try { await Logging.WaitAsync(GetSyncResponseIoDelay, $"[{instanceGuid:N}] Sync response IO delay", ModuleName, ct: ct).ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); } }

                    if (activeInstances.Except(syncRequest.syncActiveInstances).Any() || syncRequest.syncActiveInstances.Except(activeInstances).Any())
                    {
                        del();
                        Logging.LogEvent($"[{instanceGuid:N}] Exit {nameof(GetSyncResponse)}() - instances changed");
                        Logging.LogExit();

                        return default;
                    }

                    if (!synLoaded)
                    {
                        try
                        {
                            if (!File.Exists(syncRequest.file))
                            {
                                // sync request was deleted... probably because a newer request exists?
                                Logging.LogEvent($"[{instanceGuid:N}] Exit {nameof(GetSyncResponse)}() - sync file missing");
                                Logging.LogExit();

                                return default;
                            }
                        }
                        catch (Exception e)
                        {
                            Logging.LogException(e, $"[{instanceGuid:N}]");
                            Logging.LogEvent($"[{instanceGuid:N}] Exit {nameof(GetSyncResponse)}() - sync file couldn't be accessed");
                            Logging.LogExit();

                            return default;
                        }
                    }


                    if (!synLoaded)
                    {
                        if (expectedSyncFiles.All(a =>
                        {
                            try
                            {
                                var exists = File.Exists(a.fn);
                                Logging.LogEvent($"[{instanceGuid:N}] {a.fn} exists: {exists}");
                                return exists;
                            }
                            catch (Exception e)
                            {
                                Logging.LogException(e, $"[{instanceGuid:N}]");
                                return false;
                            }
                        }))
                        {

                            var data = expectedSyncFiles.AsParallel().AsOrdered().Select(a =>
                            {
                                try
                                {
                                    //return File.ReadAllLines(a);
                                    var bytes = File.ReadAllBytes(a.fn);
                                    var responseSyncDataInts = ByteBlockToInts(bytes);
                                    return (a.syncGuid, responseSyncDataInts);
                                }
                                catch (Exception e)
                                {
                                    Logging.LogException(e, $"[{instanceGuid:N}]");
                                    return (a.syncGuid, null);
                                }
                            }).ToArray();

                            if (data.Any(a => a.responseSyncDataInts == null))
                            {
                                continue;
                            }

                            syncData = data.SelectMany(a => a.responseSyncDataInts).ToArray();
                            synLoaded = true;

                            //try
                            //{
                            //if (File.Exists(syncResponseFile) && new FileInfo(syncResponseFile).Length > 0)
                            try { await File.WriteAllBytesAsync(ackResponseFile, new byte[] { 0 }, ct).ConfigureAwait(false); }
                            catch (Exception e)
                            {
                                Logging.LogException(e, $"[{instanceGuid:N}]");
                                continue;
                            }
                            //}
                            //catch (Exception e) { Logging.LogEvent(DateTime.UtcNow + " " + "Exception: " + e.Message); }

                            //syncRequest.syncActiveInstances

                            var isSourceSelf = instanceGuid == syncRequest.sourceGuid;

                            Logging.LogEvent($"[{instanceGuid:N}] Sync successful with instance [{syncRequest.sourceGuid:N}] ({(isSourceSelf ? "self" : "remote")}). Synchronized instances [{syncRequest.syncActiveInstances.Length}]: " +
                                             string.Join(", ", syncRequest.syncActiveInstances.Select(a=>$@"[{a}]").ToArray()));

                            // todo: find out why execution is stuck here. .. e.g.Sync successful with instance 9efd2b26-1c6d-4898-848a-f8db69173e8f .... then nothing else

                            //ResponseSyncTime = DateTime.UtcNow;
                        }
                    }

                    if (!synLoaded)
                    {
                        continue;
                    }

                    // wait for ACK files on the sync source instance
                    if (!ackLoaded)
                    {
                        if (expectedAckFiles.All(a =>
                        {
                            try { return File.Exists(a.fn) && new FileInfo(a.fn).Length > 0; }
                            catch (Exception e)
                            {
                                Logging.LogException(e, $"[{instanceGuid:N}]");
                                return false;
                            }
                        }))
                        {
                            ackLoaded = true;

                            del();

                        }

                    }

                    if (!ackLoaded) continue;

                    try { syncFileExists = File.Exists(syncRequest.file) && new FileInfo(syncRequest.file).Length > 0; }
                    catch (Exception e)
                    {
                        Logging.LogException(e, $"[{instanceGuid:N}]");
                        continue;
                    }

                    if (syncFileExists)
                    {
                        continue;
                    }

                    Logging.LogEvent($"[{instanceGuid:N}] Exit {nameof(GetSyncResponse)}() - sync ok");
                    Logging.LogExit();
                    return (true, syncData);
                }
            }
        }

        //public static string GetIpcCommsFolder(string experimentName, int iterationIndex)
        //{
        //    var folder = Path.Combine(Program.ProgramArgs.ResultsRootFolder, $@"_ipc_{Program.ProgramArgs.ServerGuid:N}", $@"_{iterationIndex}_{experimentName}");
        //    return folder;
        //}

        public string folder;

        public async Task<List<(IndexData id, ConfusionMatrix cm)>> ServeIpcJobsAsync(Guid instanceGuid, string experimentName, int iterationIndex, DataSet baseLineDataSet, int[] baseLineColumnIndexes, DataSet dataSet, IndexData[] indexesWhole, ulong lvl = 0, bool asParallel = true, CancellationToken callerCt = default)
        {
            if (callerCt.IsCancellationRequested)
            {
                Logging.LogEvent($"[{instanceGuid:N}] Cancellation requested");
                Logging.LogExit();
                return default;
            }
            
            //var iterFn = Program.GetIterationFilename(indexesWhole, callerCt);
            //folder = Path.Combine(Program.ProgramArgs.ResultsRootFolder, $@"_ipc_{Program.ProgramArgs.ServerGuid:N}", $"_{iterFn}");//$@"_{iterationIndex}_{experimentName}");

            folder = Path.Combine(Program.GetIterationFolder(Program.ProgramArgs.ResultsRootFolder, experimentName, iterationIndex,ct:callerCt), "_ipc");
            //using var methodCts = new CancellationTokenSource();
            //var methodCt = methodCts.Token;
            //using var methodLinkedCts = CancellationTokenSource.CreateLinkedTokenSource(callerCt, methodCt);
            //var methodLinkedCt = methodLinkedCts.Token;


            Console.WriteLine();
            Console.WriteLine();
            Logging.LogEvent($@"[{instanceGuid:N}] Start of ServeIpcJobsAsync for iteration [{iterationIndex}] for experiment [{experimentName}].");
            Console.WriteLine();
            Console.WriteLine();


            // load cache
            //var cacheFiles = await IoProxy.GetFilesAsync(true, ct, cacheFolder, "_cache_*.csv", SearchOption.TopDirectoryOnly).ConfigureAwait(false);
            //var outerResultIds = Array.Empty<int>();
            //var outerResults = Array.Empty<(IndexData id, ConfusionMatrix cm)>();

            var instanceIterationCmLoaded = new List<(IndexData id, ConfusionMatrix cm)>();
            var masterIterationCmLoaded = new List<(IndexData id, ConfusionMatrix cm)>();
            var (indexesLoaded, indexesNotLoaded) = CacheLoad.UpdateMissing(masterIterationCmLoaded, indexesWhole, true, callerCt);

            async Task RefreshCache()
            {
                var cacheFolder = folder/*GetIpcCommsFolder(experimentName, iterationIndex)*/;
                var cacheFiles = await IoProxy.GetFilesAsync(true, callerCt, cacheFolder, "_cache_*.csv", SearchOption.TopDirectoryOnly).ConfigureAwait(false);

                if ((cacheFiles?.Length??0) > 0)
                {
                    var cache = await CacheLoad.LoadCacheFileListAsync(indexesWhole, cacheFiles, true, callerCt).ConfigureAwait(false);
                    if (cache != default && cache.IdCmSd != default && cache.IdCmSd.Length > 0)
                    {
                        masterIterationCmLoaded = cache.IdCmSd.Where(a => a.cm != default && a.id != default).ToList();
                        //masterIterationCmLoaded.AddRange(cache.IdCmSd);
                    }
                }

                (indexesLoaded, indexesNotLoaded) = CacheLoad.UpdateMissing(masterIterationCmLoaded, indexesWhole, true, callerCt);
            }

            /*
            async Task SaveMasterCache()
            {
                var cacheFolder = GetIpcCommsFolder(iterationIndex);
                var cacheSaveFn = Path.Combine(cacheFolder, $"_cache_{iterationIndex}_{instanceGuid:N}_master.csv");
                var cacheSaveFn2 = Path.Combine(cacheFolder, $"_cache_{iterationIndex}_master.csv");
                var cacheSaveLines = masterIterationCmLoaded.AsParallel().AsOrdered().Select(a => $@"{a.id?.CsvValuesString() ?? IndexData.Empty.CsvValuesString()},{a.cm.CsvValuesString() ?? ConfusionMatrix.Empty.CsvValuesString()}").ToList();
                cacheSaveLines.Insert(0, $@"{IndexData.CsvHeaderString},{ConfusionMatrix.CsvHeaderString}");
                await IoProxy.WriteAllLinesAsync(true, ct, cacheSaveFn, cacheSaveLines).ConfigureAwait(false);

                try
                {
                    while (!File.Exists(cacheSaveFn2))
                    {
                        try { File.Move(cacheSaveFn, cacheSaveFn2); }
                        catch (Exception e) { Logging.LogException(e); }
                    }
                }
                catch (Exception e)
                {
                    Logging.LogException(e);
                }
            }
            */

            //todo: why is iteration 0 being repeated?

            async Task SaveInstanceCache()
            {
                var cacheFolder = folder/*GetIpcCommsFolder(experimentName, iterationIndex)*/;
                var cacheSaveFn = Path.Combine(cacheFolder, $"_cache_{iterationIndex}_{instanceGuid:N}.csv");
                var cacheSaveLines = instanceIterationCmLoaded.AsParallel().AsOrdered().Select(a => $@"{a.id?.CsvValuesString() ?? IndexData.Empty.CsvValuesString()},{a.cm.CsvValuesString() ?? ConfusionMatrix.Empty.CsvValuesString()}").ToList();
                cacheSaveLines.Insert(0, $@"{IndexData.CsvHeaderString},{ConfusionMatrix.CsvHeaderString}");
                await IoProxy.WriteAllLinesAsync(true, callerCt, cacheSaveFn, cacheSaveLines).ConfigureAwait(false);
            }

            async Task<Task> InstanceGuidWriterTask(CancellationToken mainCt)
            {
                try { await WriteInstance(instanceGuid, experimentName, iterationIndex, true, ct: mainCt).ConfigureAwait(false); }
                catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }

                var instanceGuidWriterTask = Task.Run(async () =>
                    {
                        while (!callerCt.IsCancellationRequested && !mainCt.IsCancellationRequested)
                        {
                            try
                            {
                                await WriteInstance(instanceGuid, experimentName, iterationIndex, ct: mainCt).ConfigureAwait(false);
                                try { await Task.Delay(TaskWriteInstanceLoopDelay, mainCt).ConfigureAwait(false); }
                                catch (OperationCanceledException) { }
                                catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
                            }
                            catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
                        }
                    },
                    mainCt);

                return instanceGuidWriterTask;
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

                                syncRequest = await GetSyncRequest(instanceGuid, experimentName, iterationIndex, false, syncGuids, loopCt).ConfigureAwait(false);

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

            async Task<(IndexData id, ConfusionMatrix cm)[]> ProcessJob(IndexData indexData, int workShareInstanceIndex, int workShareInstanceSize, CancellationToken mainCt, CancellationToken loopCt)
            {
                try
                {
                    if (indexData == default)
                    {
                        Logging.LogEvent($@"[{instanceGuid:N}] Job: Exiting: indexData was default...");

                        return default;
                    }

                    lock (workShareInstanceNumStartedLock) workShareInstanceNumStarted++;

                    if (callerCt.IsCancellationRequested || mainCt.IsCancellationRequested || loopCt.IsCancellationRequested)
                    {
                        if (workShareInstanceSize - workShareInstanceNumComplete > 1)
                        {
                            Logging.LogEvent($@"[{instanceGuid:N}] Job: Exiting: Cancellation requested...");
                            return default;
                        }
                    }

                    var mocvi = CrossValidate.MakeOuterCvInputs(baseLineDataSet, baseLineColumnIndexes, dataSet, indexData, ct: loopCt);
                    if (mocvi == default || mocvi.outerCvInputs.Length == 0)
                    {
                        Logging.LogEvent($@"[{instanceGuid:N}] Job: Exiting: MakeOuterCvInputs returned default...");
                        return default;
                    }

                    var ret = await CrossValidate.CrossValidatePerformanceAsync(null, CrossValidate.RpcPoint.None, mocvi.outerCvInputs, mocvi.mergedCvInput, indexData, ct: loopCt).ConfigureAwait(false);
                    if (ret == default || ret.Length == 0 || ret.Any(a => a.id == default || a.cm == default))
                    {
                        Logging.LogEvent($@"[{instanceGuid:N}] Job: Exiting: CrossValidatePerformanceAsync returned default...");

                        return default;
                    }

                    Logging.LogEvent($@"[{instanceGuid:N}] Job: Completed job {workShareInstanceIndex} of {workShareInstanceSize} (IdJobUid=[{indexData.IdJobUid}]; IdGroupArrayIndex=[{indexData.IdGroupArrayIndex}])");

                    lock (workShareInstanceNumCompleteLock) workShareInstanceNumComplete++;

                    return ret;
                }
                catch (Exception e)
                {
                    Logging.LogException(e, $@"[{instanceGuid:N}]");
                    return default;
                }
            }

            var syncGuids = new[] { instanceGuid };


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



                // syncResults is the list of job item indexes completed within the cluster
                var syncResultIds = Array.Empty<int>();

                // todo: add another variable to store actual results and save them.



                var isWorkOutOfSync = false;
                var isSyncOk = false;
                var w = -1;
                (Guid instanceGuid, int[] instanceWork)[] workShareList = null;
                int[] workShareInstance = null;
                var loopDidRun = false;
                var finalSync = false;
                var countInner = 0;
                while (!callerCt.IsCancellationRequested && !mainCt.IsCancellationRequested)
                {
                    Console.WriteLine();
                    Console.WriteLine();
                    Logging.LogEvent($@"Start work sharing in ServeIpcJobsAsync (outer = [{countOuter}]; inner = [{countInner}]) for iteration [{iterationIndex}] for experiment [{experimentName}].");
                    Console.WriteLine();
                    Console.WriteLine();
                    countInner++;

                    await SaveInstanceCache().ConfigureAwait(false);

                    w++;
                    var isFirstIteration = w == 0;

                    if (isFirstIteration)
                    {
                        try { await Logging.WaitAsync(MainLoopInitDelay, $"[{instanceGuid:N}] Main loop init delay", ct: mainCt).ConfigureAwait(false); }
                        catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
                    }

                    if (syncRequest != default)
                    {
                        var syncResponse = await GetSyncResponse(instanceGuid, experimentName, iterationIndex, syncRequest, syncResultIds, mainCt).ConfigureAwait(false);
                        isSyncOk = syncResponse.didSync;

                        if (syncResponse.didSync)
                        {
                            syncGuids = syncRequest.syncActiveInstances;

                            if (syncResponse.syncData != null)
                            {
                                var syncWorkCompleteIds = syncResponse.syncData;

                                Logging.LogEvent($"[{instanceGuid:N}] Sync returned Ids: {string.Join(", ", syncResultIds.Select(a => $"{a}").ToArray())}");

                                syncResultIds = syncResultIds.Union(syncWorkCompleteIds).ToArray();
                            }

                            Logging.LogEvent($"[{instanceGuid:N}] Sync merged Ids: {string.Join(", ", syncResultIds.Select(a => $"{a}").ToArray())}");

                        }

                        syncRequest = default;
                        loopDidRun = false;
                        continue;
                    }

                    var isAllWorkDone = !workIds.Except(syncResultIds).Any();

                    // always request sync, since, it's either A) first time, B) instances changed, C) last sync failed, D) no work left, need to steal some (unless there was never any work to start with)
                    // except: if all work is already done?...  which would cause a sync-call loop

                    // finalSync makes sure a sync is done after all work is complete, this ensures instance cache is saved before continuing... as that is done before sync code.
                    var isWorkShareSetAndEmpty = workShareInstance != null && workShareInstance.Length == 0; // if null, not set yet (null doesn't mean empty)
                    var requestSync = isFirstIteration || isWorkOutOfSync || !isSyncOk || loopDidRun || (isAllWorkDone && !finalSync);

                    if (isWorkOutOfSync) Logging.LogEvent("Work out of sync, synchronization required...");
                    if (isFirstIteration) Logging.LogEvent("First iteration, synchronization required...");
                    if (!isSyncOk) Logging.LogEvent("Last synchronization failed, synchronization required...");
                    if (loopDidRun) Logging.LogEvent("Work has been done, synchronization required...");
                    if (isAllWorkDone && (!finalSync || !isSyncOk)) Logging.LogEvent("All work is done, final synchronization required...");
                    // problem: if no work is allocated...?
                    // problem: if all allocated work is complete, so need to work steal?

                    if (isAllWorkDone) finalSync = true;
                    syncRequest = await GetSyncRequest(instanceGuid, experimentName, iterationIndex, requestSync, syncGuids, mainCt).ConfigureAwait(false);

                    if (syncRequest != default) continue;
                    isSyncOk = true;
                    loopDidRun = false;
                    isWorkOutOfSync = false;

                    if (isWorkShareSetAndEmpty && !isAllWorkDone)
                    {
                        try { await Logging.WaitAsync(MainLoopNoWorkDelay, $"{instanceGuid:N} No work to do for this instance... waiting for retry.", ct: mainCt).ConfigureAwait(false); }
                        catch (Exception e) { Logging.LogException(e, $"[{instanceGuid:N}]"); }
                    }

                    if (isAllWorkDone)
                    {
                        // todo: finished all work, so sync to share/save it with other instances?

                        // todo: or just save to an overall merged cache file?

                        Logging.LogEvent($"[{instanceGuid:N}] Jobs complete...");
                        break;
                    }

                    Logging.LogEvent($"[{instanceGuid:N}] Main loop continuing...");

                    var workCompleteIds = indexesLoaded.Select(a => a.IdJobUid).ToArray();
                    var workIncompleteIds = indexesNotLoaded.Select(a => a.IdJobUid).ToArray();
                    workShareList = RedistributeWork(workIds, workCompleteIds, workIncompleteIds, syncGuids);
                    workShareInstance = workShareList.FirstOrDefault(a => a.instanceGuid == instanceGuid).instanceWork;
                    if (workShareInstance.Length == 0) { continue; }
                    var workShareInstanceItems = indexesWhole.AsParallel().AsOrdered().Where(a => workShareInstance.AsParallel().AsOrdered().Any(b => a.IdJobUid == b)).ToArray();

                    // if there are items in the todo list which are already done, need re-sync?
                    var workNotSynced = instanceIterationCmLoaded.Select(a => a.id).Intersect(workShareInstanceItems).ToArray();
                    if (workNotSynced.Length > 0)
                    {
                        isWorkOutOfSync = true;
                        Logging.LogEvent("Work is out of sync");
                        continue;
                    }

                    var loopCts = new CancellationTokenSource();
                    var loopCt = loopCts.Token;
                    var syncTask = KeepSynchronizedTask(syncGuids, mainCt, loopCts);
                    var etaTask = EtaTask(workShareInstance.Length, mainCt, loopCt);

                    Logging.LogEvent($"[{instanceGuid:N}] Tasks starting...");
                    Logging.LogEvent($"[{instanceGuid:N}] Work share Ids ({workShareInstance.Length}): {string.Join(", ", workShareInstance.Select(a => $"{a}").ToArray())}");




                    var innerResultsTasks = asParallel
                        ? workShareInstanceItems.AsParallel().AsOrdered().Select(async (indexData, workShareInstanceIndex) => await ProcessJob(indexData, workShareInstanceIndex, workShareInstance?.Length ?? 0, mainCt, loopCt).ConfigureAwait(false)).ToArray()
                        : workShareInstanceItems.Select(async (indexData, workShareInstanceIndex) => await ProcessJob(indexData, workShareInstanceIndex, workShareInstance?.Length ?? 0, mainCt, loopCt).ConfigureAwait(false)).ToArray();

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
                    innerResults = innerResults.Where(a => a.cm != default && a.id != default).ToArray();

                    instanceIterationCmLoaded.AddRange(innerResults);

                    var innerResultIds = innerResults.Select(a => a.id.IdJobUid).Distinct().ToArray();

                    syncResultIds = syncResultIds.Union(innerResultIds).ToArray();

                    Logging.LogEvent($"[{instanceGuid:N}] Tasks complete...");
                    Logging.LogEvent($"[{instanceGuid:N}] Inner results: {innerResultIds.Length} items. Ids: {string.Join(", ", innerResultIds.Select(a => $"{a}").ToArray())}");



                    loopCts.Cancel();
                    loopDidRun = workShareInstance.Length > 0;
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

            var instanceJobIdsCompleted = instanceIterationCmLoaded.Select(a => a.id.IdJobUid).OrderBy(a => a).Distinct().ToArray();

            Logging.LogGap(2);
            Logging.LogEvent($@"End of ServeIpcJobsAsync for iteration [{iterationIndex}] for experiment [{experimentName}].  Jobs completed by this instance [{instanceJobIdsCompleted.Length}]: {string.Join(", ", instanceJobIdsCompleted)}.");
            Logging.LogGap(2);


            return masterIterationCmLoaded;
        }
    }
}
