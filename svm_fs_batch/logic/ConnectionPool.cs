using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SvmFsBatch
{
    public class ConnectionPool
    {
        public const string ModuleName = nameof(ConnectionPool);

        public bool IsDisposed;
        public List<ConnectionPoolMember> Pool = new List<ConnectionPoolMember>();
        public CancellationToken PoolCt;
        public CancellationTokenSource PoolCts = new CancellationTokenSource();
        public readonly object PoolLock = new object();
        public Queue<byte[]> PoolRemoteGuidQueue = new Queue<byte[]>();
        public Task Task;
        public bool IsServer;
        public Guid LocalPoolGuid;
        public byte[] LocalPoolGuidBytes;
        public CancellationTokenSource LinkedCts;
        public CancellationToken LinkedCt;
        public string PoolName = "Pool";

        
        public Guid RemoteServerPoolGuid = default;
        public byte[] RemoteServerPoolGuidBytes = default;

      

        public void RandomLocalGuid()
        {
            LocalPoolGuid = Guid.NewGuid();
            LocalPoolGuidBytes = LocalPoolGuid.ToByteArray();
        }

        public ConnectionPool()
        {
            RandomLocalGuid();
            StartStatsTask();
        }

        public Task StartStatsTask()
        {
            // write number of clients etc.
            var task = Task.Run(async ()=> {
                while (!IsDisposed && !this.LinkedCt.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(10));

                    lock (PoolLock)
                    {
                        if (IsDisposed) return;

                        var active = CountActive();
                        
                        Console.WriteLine($@"Connection pool status: LocalPoolGuid = ""{LocalPoolGuid:N}"", RemoteServerPoolGuid = ""{RemoteServerPoolGuid:N}"", PoolRemoteGuidQueue = ""{(PoolRemoteGuidQueue?.Count??0)} items"", Pool = ""Reserved = {active.reserved}, Unreserved = {active.unreserved}, Total = {active.total}"".");
                    }
                }

            });

            return task;
            // timeout clients which didn't read/write for timeout duration
        }

        public ConnectionPool(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0) : this()
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            PoolCt = PoolCts.Token;
            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }



        public int Count(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {

            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

            lock (PoolLock)
            {
                if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

                var ret = PoolCt.IsCancellationRequested
                    ? default
                    : Pool.Count;

                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return ret;
            }
        }

        public (int reserved, int unreserved, int total) CountActive(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {

            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

            lock (PoolLock)
            {
                if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
                if (PoolCt.IsCancellationRequested) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

                var numActiveReserved = 0;
                var numActiveUnreserved = 0;
                for (var index = Pool.Count - 1; index >= 0; index--)
                {
                    if (Pool[index].IsActive(callChain: callChain, lvl: lvl + 1))
                    {
                        if (Pool[index].IsReserved) numActiveReserved++;
                        else numActiveUnreserved++;
                    }
                }

                var ret = PoolCt.IsCancellationRequested
                  ? default
                  : (numActiveReserved, numActiveUnreserved, numActiveReserved + numActiveUnreserved);

                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return ret;
            }
        }

        public int CountRemoteGuids(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {

            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

            lock (PoolLock)
            {
                var ret = PoolCt.IsCancellationRequested
                    ? default
                    : PoolRemoteGuidQueue.Count;
                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return ret;
            }
        }

        public void PollPool(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {

            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            lock (PoolLock) { Pool = Pool.AsParallel().AsOrdered().Where(a => a.IsActive(callChain: callChain, lvl: lvl + 1)).ToList(); }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        public ConnectionPoolMember GetNextClient(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

            lock (PoolLock)
            {
                if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
                if (Pool == null || Pool.Count == 0) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
                if (PoolRemoteGuidQueue == null || PoolRemoteGuidQueue.Count == 0) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

                for (var remoteGuidIndex = PoolRemoteGuidQueue.Count - 1; remoteGuidIndex >= 0; remoteGuidIndex--)
                {
                    var remoteGuidBytes = PoolRemoteGuidQueue.Dequeue();
                    PoolRemoteGuidQueue.Enqueue(remoteGuidBytes);

                    for (var index = Pool.Count - 1; index >= 0; index--)
                    {
                        var cpm = Pool[index];

                        // IsActive checks if tcp connected and not cancelled
                        if (!cpm.IsReserved && cpm.IsActive(callChain: callChain, lvl: lvl + 1) && cpm.HasRemoteGuid(remoteGuidBytes, callChain: callChain, lvl: lvl + 1))
                        {
                            cpm.Reserve(callChain: callChain, lvl: lvl + 1);

                            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                            return cpm;
                        }
                    }
                }
            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
            return default;
        }

        //public bool Poll(ConnectionPoolMember cpm)
        //{
        //    lock (_poolLock)
        //    {
        //        var pollOk = TcpClientExtra.PollTcpClientConnection(cpm.Client) || cpm.Ct.IsCancellationRequested || PoolCt.IsCancellationRequested;
        //        if (!pollOk)
        //        {
        //            cpm.Close();
        //        }
        //        Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl+1); return pollOk;
        //    }
        //}

        public void Clean(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            lock (PoolLock)
            {
                if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }


                for (var i = Pool.Count - 1; i >= 0; i--)
                {
                    var cpm = Pool[i];
                    //if (!cpm.Client.Connected)
                    if (!cpm.IsActive(callChain: callChain, lvl: lvl + 1))
                    {
                        Pool.RemoveAt(i);
                        //cpm.Close(callChain: callChain, lvl: lvl+1);
                    }
                }
            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        public void CloseAll(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }


            lock (PoolLock)
            {
                if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }
                


                for (var index = Pool.Count - 1; index >= 0; index--)
                {
                    var c = Pool[index];
                    try { c?.Close(callChain: callChain, lvl: lvl + 1); }
                    catch (Exception) { }
                }

                Pool.Clear();
            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        public void QueueGuid(byte[] RemoteGuidBytes, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (this.IsDisposed || this.Pool == null) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            lock (PoolLock)
            {
                if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

                if (!PoolRemoteGuidQueue.Any(a => a.SequenceEqual(RemoteGuidBytes))) { PoolRemoteGuidQueue.Enqueue(RemoteGuidBytes); }
            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        public void AddCPM(ConnectionPoolMember cpm, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (cpm == null || !cpm.IsActive(callChain: callChain, lvl: lvl + 1)) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }
            if (this.IsDisposed || this.Pool == null) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            //if (cpm.Ct.IsCancellationRequested || cpm.Client == null || !cpm.Client.Connected || _pool == null || _isDisposed)
            //{
            //    cpm.Close();
            //    Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl+1); return;
            //}

            lock (PoolLock)
            {
                if (this.IsDisposed || this.Pool == null) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

                if (Pool.Contains(cpm))
                {
                    Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                    return;
                }

                var activeConnections = CountActive(callChain: callChain, lvl: lvl + 1);

                //if (IsServer || (!IsServer && activeConnections.total < Program.ProgramArgs.ClientConnectionPoolSize))
                //{
                    Pool.Add(cpm);
                    cpm.Cp = this;
                    QueueGuid(cpm.RemoteGuidBytes, callChain: callChain, lvl: lvl + 1);
                //}
                //else
                //{
                    //cpm.Close(callChain: callChain, lvl: lvl + 1);
                //}
            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        public void Remove(ConnectionPoolMember cpm, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            lock (PoolLock)
            {
                if (this.IsDisposed || this.Pool == null) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

                try
                {
                    Pool.Remove(cpm);
                }
                catch (Exception) { }
            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        public async Task StopAsync(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);


            if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            lock (PoolLock)
            {
                PoolCts.Cancel();
                PoolCts.Dispose();
                CloseAll(callChain: callChain, lvl: lvl + 1);
            }

            if (Task != null && !Task.IsCompleted) await Task.ConfigureAwait(false);

            lock (PoolLock)
            {
                PoolRemoteGuidQueue = null;
                Pool = null;
                PoolCts = null;
                PoolCt = default;
                Task = null;
                PoolName = null;

                IsDisposed = true;
            }
            //_poolLock = null;

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        public async Task<ConnectionPoolMember> ConnectAcceptAsync(TcpListener listener, int timeoutSeconds = 10)
        {
            Logging.LogCall(ModuleName);

            var incomingClientConnect = Task.Run(async () =>
            {
                if (!LinkedCt.IsCancellationRequested) { try { return await listener.AcceptTcpClientAsync().ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e, "", ModuleName); } }
                return default;
            }, LinkedCt);

            do
            {
                // await up to 10 seconds
                await Task.WhenAny(incomingClientConnect, Task.Delay(TimeSpan.FromSeconds(timeoutSeconds), LinkedCt)).ConfigureAwait(false);

                // check if client_connect task didn't finish
                if (!incomingClientConnect.IsCompleted)
                {
                    // if didn't finish, check if cancellation requested
                    if (LinkedCt.IsCancellationRequested)
                    {
                        Logging.LogEvent($@"Pool ""{PoolName}"".  Cancellation requested on {nameof(LinkedCt)}.", ModuleName);
                        this.CloseAll();
                        break;
                    }
                }
                //else { break; }
            } while (!incomingClientConnect.IsCompleted);

            if (incomingClientConnect.IsCompletedSuccessfully && incomingClientConnect.Result != default)
            {
                var client = incomingClientConnect.Result;

                if (client.Connected)
                {
                    var cpm = await AddClient(client, default/*, callChain: callChain, lvl: lvl + 1*/).ConfigureAwait(false);

                    if (cpm != default)
                    {
                        Logging.LogExit(ModuleName);
                        return cpm;
                    }
                }

                try { client?.Close(); } catch (Exception) { }
                Logging.LogExit(ModuleName);
                return default;
            }

            Logging.LogExit(ModuleName);
            return default;
        }

        public async Task<ConnectionPoolMember> ConnectAsync(string host, int port, int timeoutSeconds = 10, int reconnectDelaySeconds = 1, int reconnectRetries = 0)
        {
            Logging.LogCall(ModuleName);
            var tries = 0;
            while (!LinkedCt.IsCancellationRequested && tries <= reconnectRetries)
            {
                tries++;

                try
                {
                    using var connectCts = new CancellationTokenSource();
                    var connectCt = connectCts.Token;
                    using var connectLinkedCts = CancellationTokenSource.CreateLinkedTokenSource(LinkedCt, connectCt);
                    var connectLinkedCt = connectLinkedCts.Token;

                    var client = new TcpClient();
                    var outgoingClientConnect = Task.Run(async () =>
                    {
                        try
                        {
                            Logging.LogEvent($"Connection pool: Establishing outgoing connecting to {host}:{port}.", ModuleName);
                            await client.ConnectAsync(host, port, connectLinkedCt).ConfigureAwait(false);
                        }
                        catch (Exception e) { Logging.LogException(e, "", ModuleName); }
                    }, connectLinkedCt);

                    try { await Task.WhenAny(outgoingClientConnect, Task.Delay(TimeSpan.FromSeconds(timeoutSeconds), connectLinkedCt)).ConfigureAwait(false); } catch (Exception) { }
                    try { connectCts.Cancel(); } catch (Exception) { }

                    if (!outgoingClientConnect.IsCompletedSuccessfully || client == null || client.Client == null || !client.Connected)
                    {
                        Logging.LogEvent("Connection pool: Outgoing connection timed out.", ModuleName);

                        try { client.Close(); } catch (Exception) { }
                        try { await Task.Delay(TimeSpan.FromSeconds(reconnectDelaySeconds), LinkedCt).ConfigureAwait(false); } catch (Exception) { }
                        continue;
                    }

                    if (client.Connected)
                    {
                        var cpm = await AddClient(client, default/*, callChain: callChain, lvl: lvl + 1*/).ConfigureAwait(false);

                        if (cpm != default)
                        {
                            cpm.ConnectHost = host;
                            cpm.ConnectPort = port;

                            Logging.LogExit(ModuleName);
                            return cpm;
                        }
                    }

                    try { client?.Close(); } catch (Exception) { }
                    continue;
                }
                catch (Exception e)
                {
                    Logging.LogEvent("Connection pool: Outgoing connection timed out.", ModuleName);
                    Logging.LogException(e, "", ModuleName);

                    try { await Task.Delay(TimeSpan.FromSeconds(1), LinkedCt).ConfigureAwait(false); } catch (Exception) { }
                }
            }

            Logging.LogExit(ModuleName);
            return default;
        }

        public void Start(bool isServer, string localPoolName, Guid localPoolGuid, Guid remoteServerPoolGuid, CancellationToken ct, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (IsDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            if (string.IsNullOrWhiteSpace(localPoolName)) throw new ArgumentOutOfRangeException(nameof(localPoolName));
            if (localPoolGuid == default) throw new ArgumentOutOfRangeException(nameof(localPoolGuid));


            this.PoolName = localPoolName;
            this.IsServer = isServer;
            this.LocalPoolGuid = localPoolGuid;
            this.LocalPoolGuidBytes = localPoolGuid.ToByteArray();
            this.RemoteServerPoolGuid = remoteServerPoolGuid;
            this.RemoteServerPoolGuidBytes = remoteServerPoolGuid != default ? remoteServerPoolGuid.ToByteArray() : null;
            this.LinkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, PoolCt);
            this.LinkedCt = LinkedCts.Token;


            Task = Task.Run(async () =>
                {


                    Logging.LogEvent($@"Pool ""{localPoolName}"".  Starting.", ModuleName);

                    TcpListener listener = null;


                    if (isServer)
                    {
                        listener = new TcpListener(IPAddress.Any, Program.ProgramArgs.ServerPort);
                        while (!listener.Server.IsBound)
                        {
                            if (LinkedCt.IsCancellationRequested)
                            {
                                Logging.LogEvent($@"Pool ""{localPoolName}"".  Cancellation requested on {nameof(LinkedCt)}.", ModuleName);
                                break;
                            }

                            try { listener.Start(Program.ProgramArgs.ServerBacklog); }
                            catch (Exception e)
                            {
                                Logging.LogException(e, $@"{localPoolName}: Connection pool");
                                try { await Task.Delay(TimeSpan.FromMilliseconds(1), LinkedCt).ConfigureAwait(false); }
                                catch (Exception) { }
                            }
                        }

                        if (!listener.Server.IsBound) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

                        var lid = TcpClientExtra.ReadTcpClientRemoteAddress(listener);
                        Logging.LogEvent($@"Pool ""{localPoolName}"".  Listening for incoming connections on {lid.localAddress}:{lid.localPort} {lid.remoteAddress}:{lid.remotePort}", ModuleName);
                    }


                    var lastActiveConnections = (0, 0, 0);
                    while (true)
                    {
                        try
                        {

                            if (LinkedCt.IsCancellationRequested)
                            {
                                Logging.LogEvent($@"Pool ""{localPoolName}"".  Cancellation requested on {nameof(LinkedCt)}.", ModuleName);
                                break;
                            }

                            var activeConnections = CountActive(callChain: callChain, lvl: lvl + 1);

                            if (activeConnections != lastActiveConnections)
                            {
                                lastActiveConnections = activeConnections;
                                var totalHosts = CountRemoteGuids(ModuleName, callChain: callChain, lvl: lvl + 1);
                                Logging.LogEvent($@"Pool ""{PoolName}"". Pool reserved: {activeConnections.reserved}. Pool unreserved: {activeConnections.unreserved}. Pool total: {activeConnections.total}. Total hosts: {totalHosts}.", ModuleName);
                            }

                            try { await Task.Delay(TimeSpan.FromMilliseconds(1), LinkedCt).ConfigureAwait(false); } catch (Exception) { }
                            

                            ConnectionPoolMember cpm = null;

                            if (!IsServer)
                            {
                                if (activeConnections.total < Program.ProgramArgs.ClientConnectionPoolSize)
                                {
                                    await ConnectAsync(host: Program.ProgramArgs.ServerIp, port: Program.ProgramArgs.ServerPort, timeoutSeconds: 10, reconnectDelaySeconds: 1, reconnectRetries: 0).ConfigureAwait(false);
                                }
                            }
                            else if (IsServer)
                            {
                                await ConnectAcceptAsync(listener: listener, timeoutSeconds: 10).ConfigureAwait(false);
                            }

                            if (cpm == null)
                            {
                                await Task.Delay(TimeSpan.FromSeconds(1), LinkedCt).ConfigureAwait(false);
                                continue;
                            }
                        }
                        catch (Exception)
                        {
                            try { await Task.Delay(TimeSpan.FromMilliseconds(1), LinkedCt).ConfigureAwait(false); }
                            catch (Exception) { }
                        }

                        Clean(callChain: callChain, lvl: lvl + 1);
                        try { await Task.Delay(TimeSpan.FromMilliseconds(1), LinkedCt).ConfigureAwait(false); }
                        catch (Exception) { }
                    }

                    if (isServer)
                        try { listener.Stop(); }
                        catch (Exception e) { Logging.LogException(e, $@"{localPoolName}: Connection pool", ModuleName); }


                    CloseAll(callChain: callChain, lvl: lvl + 1);

                    LinkedCts.Dispose();

                    Logging.LogEvent($@"Pool ""{localPoolName}"".  Exiting.", ModuleName);
                },
                LinkedCt);

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        public async Task<ConnectionPoolMember> AddClient(TcpClient client, CancellationToken ct, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);


            if (IsDisposed || ct.IsCancellationRequested) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

            var rid = TcpClientExtra.ReadTcpClientRemoteAddress(client);
            Logging.LogEvent($@"Pool ""{PoolName}"".  Connection established. Local address: {rid.localAddress}:{rid.localPort}. Remote address: {rid.remoteAddress}:{rid.remotePort}.", ModuleName);

            try
            {
                if (client != null && client.Connected)
                {
                    var stream = client.GetStream();

                    // check if server guid matches... if this is an outgoing connection to the server

                    var handshake = await TcpClientExtra.ChallengeRequestAsync(IsServer, client, stream, LocalPoolGuidBytes, RemoteServerPoolGuidBytes, ct: LinkedCt).ConfigureAwait(false);
                    var remoteGuid = handshake.remote_guid;
                    var remoteGuidBytes = remoteGuid.ToByteArray();

                    Logging.LogEvent($@"Pool ""{PoolName}"".  Challenge {(handshake.challenge_correct ? "successful" : "failed")}. Local address: {rid.localAddress}:{rid.localPort} ({LocalPoolGuid:N}). Remote address: {rid.remoteAddress}:{rid.remotePort} ({handshake.remote_guid:N}).");

                    if (handshake.challenge_correct)
                    {
                        var cpm = new ConnectionPoolMember(callChain: callChain, lvl: lvl + 1)
                        {
                            Client = client,
                            Stream = stream,
                            LocalGuid = LocalPoolGuid,
                            LocalGuidBytes = LocalPoolGuidBytes,
                            RemoteGuid = remoteGuid,
                            RemoteGuidBytes = remoteGuidBytes,
                            LocalHost = rid.localAddress.ToString(),
                            LocalPort = rid.localPort,
                            RemoteHost = rid.remoteAddress.ToString(),
                            RemotePort = rid.remotePort,
                        };

                        AddCPM(cpm,
                            callChain: callChain,
                            lvl: lvl + 1);

                        return cpm;
                    }
                }
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
            }

            Logging.LogExit(ModuleName);
            return default;
        }
    }
}