using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SvmFsBatch.logic;

namespace SvmFsBatch
{
    public static class FsClient
    {
        public const string ModuleName = nameof(FsClient);


        //public static async Task<string> CrossValidatePerformanceRequestAsync(DataSet DataSet, bool asParallel, IndexData id, ulong lvl = 0, CancellationToken ct = default)
        //{
           // Logging.LogCall(ModuleName,lvl:lvl+1);
            //if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName,lvl:lvl+1);  return default; }

            //try
            //{
            //    var unrolledIndexList = new[] {id};

            //    var resultsTasks = asParallel
            //        ? unrolledIndexList.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select(async unrolledIndexData => await CrossValidate.CrossValidatePerformanceAsync(null, CrossValidate.RpcPoint.None, DataSet, unrolledIndexData, ct: ct).ConfigureAwait(false)).Where(a => a != default)
            //            //.SelectMany(a => a)
            //            .ToArray()
            //        : unrolledIndexList.Select(async unrolledIndexData => await CrossValidate.CrossValidatePerformanceAsync(null, CrossValidate.RpcPoint.None, DataSet, unrolledIndexData, ct: ct).ConfigureAwait(false)).Where(a => a != default)
            //            //.SelectMany(a => a)
            //            .ToArray();

            //    var results = (await Task.WhenAll(resultsTasks).ConfigureAwait(false)).Where(a => a != default).SelectMany(a => a).Where(a => a != default).ToArray();

            //    var lines = results.Select(a => $"{a.id.CsvValuesString()},{a.cm.CsvValuesString()}").ToList();

            //    lines.Insert(0, $"{IndexData.CsvHeaderString},{ConfusionMatrix.CsvHeaderString}");

            //    var text = string.Join(Environment.NewLine, lines) + Environment.NewLine;

            //    Logging.LogExit(ModuleName, lvl: lvl + 1);
            //    return ct.IsCancellationRequested ? default : text;
            //}
            //catch (Exception e)
            //{
            //    Logging.LogException(e, "", ModuleName);
            //}
            
            //Logging.LogExit(ModuleName, lvl: lvl + 1);
          //  return default;
        //}


        public static async Task FeatureSelectionClientInitializationAsync(DataSet DataSet, string ExperimentName, int instanceId, int totalInstances, ulong lvl = 0, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName, lvl:lvl+1);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName,lvl:lvl+1); return; }

            //var serverGuidBytes = Program.ProgramArgs.ServerGuid.ToByteArray();

            var clientGuid = Program.ProgramArgs.ClientGuid;
            //var clientGuidBytes = clientGuid.ToByteArray();


            // no need to gkGroup DataSet on client - server provides column indexes within whole DataSet

            const string MethodName = nameof(FeatureSelectionClientInitializationAsync);
            Logging.WriteLine($"{MethodName}()", ModuleName, MethodName);


            var tasks = new List<Task>();


            Mpstat mpstat = null;

            if (Program.ProgramArgs.IsUnix)
            {
                mpstat = new Mpstat();
                await mpstat.Start(ct);
            }

            //var r = new Random();


            var remoteServerPoolGuid = Program.ProgramArgs.ServerGuid;
            var cp = new ConnectionPool(callChain: null, lvl: lvl + 1);
            var poolName = "Client";
            cp.Start(false, poolName, clientGuid, remoteServerPoolGuid, callChain: null, lvl: lvl + 1, ct: ct);

            var lastCountActive = (0, 0, 0);
            while (true)
            {
                try
                {
                    if (ct.IsCancellationRequested)
                    {
                        Logging.LogEvent($"{poolName}: Main Loop: Cancellation requested on {nameof(ct)}...", ModuleName);
                        break;
                    }

                    var cpm = cp.GetNextClient(callChain: null, lvl: lvl + 1);

                    if (cpm == default)
                    {
                        var countActive = cp.CountActive();

                        if (countActive != lastCountActive)
                        {
                            lastCountActive = countActive;
                            Logging.LogEvent($"{poolName}: Main Loop: Connection pool was empty. Pool reserved: {countActive.reserved}. Pool unreserved: {countActive.unreserved}. Pool total: {countActive.total}.", ModuleName);
                        }

                        try { await Task.Delay(TimeSpan.FromSeconds(1), ct).ConfigureAwait(false); }
                        catch (Exception e) { Logging.LogException(e, "", ModuleName); }

                        continue;
                    }

                    var task = Task.Run(async () =>
                        {
                            Logging.LogEvent($"{poolName}: Main Loop: Starting task {Task.CurrentId}.", ModuleName);

                            try
                            {
                                var ret = await IpcMessaging.IpcAsync($"client_{cpm.LocalHost}:{cpm.LocalPort}", $"server_{cpm.RemoteHost}:{cpm.RemotePort}", cpm, DataSet, true, null, lvl: lvl + 1, ct:ct).ConfigureAwait(false);
                            }
                            catch (Exception e) { Logging.LogException(e, "", ModuleName); }
                            finally { cpm?.Unreserve(callChain: null, lvl: lvl + 1); }

                            Logging.LogEvent($"{poolName}: Main Loop: Exiting task {Task.CurrentId}.", ModuleName);
                        },
                        ct);

                    tasks.Add(task);


                    // wait while free cpu less than 10%
                    if (Program.ProgramArgs.IsUnix && mpstat != null && mpstat.IsResponsive())
                    {
                        var averageIdle = 00.00;
                        var currentIdle = 00.00;

                        do
                        {
                            averageIdle = mpstat.GetAverageIdle();
                            currentIdle = mpstat.GetIdle();

                            Logging.LogEvent($"{poolName}: Main Loop: CPU Idle in past {mpstat.Interval} seconds: {currentIdle:00.00}%. Average for past {mpstat.DataDoubleHistory.Count * mpstat.Interval} seconds: {averageIdle:00.00}%.", ModuleName);

                            if (averageIdle <= 85.00 || currentIdle <= 85.00)
                                try
                                {
                                    Logging.LogEvent($"{poolName}: Main Loop: CPU not idle... waiting...", ModuleName);
                                    await Task.Delay(TimeSpan.FromSeconds(1), ct).ConfigureAwait(false);
                                }
                                catch (Exception e)
                                {
                                    Logging.LogException(e, "", ModuleName);
                                }
                        } while (!ct.IsCancellationRequested && mpstat.IsResponsive() && (averageIdle <= 85.00 || currentIdle <= 85.00));
                    }

                    //await Task.Delay(TimeSpan.FromMilliseconds(1), ct:ct).ConfigureAwait(false);
                }
                catch (Exception e) { Logging.LogException(e, "", ModuleName); }

                tasks.RemoveAll(a => a.IsCompleted);
                Logging.LogEvent($"{poolName}: Main Loop: Active clients: tasks: {tasks.Count(a => !a.IsCompleted)}", ModuleName);

                while (!ct.IsCancellationRequested && tasks.Count(a => !a.IsCompleted) >= Program.ProgramArgs.ClientConnectionPoolSize)
                {
                    await Task.WhenAny(tasks.ToArray()).ConfigureAwait(false);
                    tasks.RemoveAll(a => a.IsCompleted);
                }

                try { await Task.Delay(TimeSpan.FromMilliseconds(1), ct).ConfigureAwait(false); }
                catch (Exception e) { Logging.LogException(e, "", ModuleName); }
            }

            if (tasks.Count > 0)
                try { await Task.WhenAll(tasks).ConfigureAwait(false); }
                catch (Exception e) { Logging.LogException(e, "", ModuleName); }

            await cp.StopAsync(callChain: null, lvl: lvl + 1).ConfigureAwait(false);

            try { if (mpstat!=null)await mpstat.StopAsync(); }
            catch (Exception e) { Logging.LogException(e, "", ModuleName); }

            Logging.LogEvent($"{poolName}: Reached end of {nameof(FsClient)}.{nameof(FeatureSelectionClientInitializationAsync)}...", ModuleName);

            Logging.LogExit(ModuleName, lvl:lvl+1);
        }
    }
}