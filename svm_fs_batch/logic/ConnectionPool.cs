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
    internal class ConnectionPool
    {
        internal const string ModuleName = nameof(ConnectionPool);

        private bool _isDisposed;

        
        private List<ConnectionPoolMember> _pool = new List<ConnectionPoolMember>();
        internal CancellationToken PoolCt { get; private set; }
        private CancellationTokenSource _poolCts = new CancellationTokenSource();
        private readonly object _poolLock = new object();
        private Queue<byte[]> _poolRemoteGuidQueue = new Queue<byte[]>();
        private Task _task;
        private bool _isIncoming;
        private bool _isOutgoing;

        public ConnectionPool(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            PoolCt = _poolCts.Token;
            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        internal string PoolName { get; private set; }

        internal int Count(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {

            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

            lock (_poolLock)
            {
                if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

                var ret = PoolCt.IsCancellationRequested
                    ? default
                    : _pool.Count;

                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return ret;
            }
        }

        internal (int reserved, int unreserved, int total) CountActive(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {

            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

            lock (_poolLock)
            {
                if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
                if (PoolCt.IsCancellationRequested) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

                var numActiveReserved = 0;
                var numActiveUnreserved = 0;
                for (var index = _pool.Count - 1; index >= 0; index--)
                {
                    if (_pool[index].IsActive(callChain: callChain, lvl: lvl + 1))
                    {
                        if (_pool[index].IsReserved) numActiveReserved++;
                        else numActiveUnreserved++;
                    }
                }

                var ret = PoolCt.IsCancellationRequested
                  ? default
                  : (numActiveReserved, numActiveUnreserved, numActiveReserved+numActiveUnreserved);

                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return ret;
            }
        }

        internal int CountRemoteGuids(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {

            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

            lock (_poolLock)
            {
                var ret = PoolCt.IsCancellationRequested
                    ? default
                    : _poolRemoteGuidQueue.Count;
                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return ret;
            }
        }

        internal void PollPool(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {

            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            lock (_poolLock) { _pool = _pool.AsParallel().AsOrdered().Where(a => a.IsActive(callChain: callChain, lvl: lvl + 1)).ToList(); }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        internal ConnectionPoolMember GetNextClient(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

            lock (_poolLock)
            {
                if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
                if (_pool == null || _pool.Count == 0) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
                if (_poolRemoteGuidQueue == null || _poolRemoteGuidQueue.Count == 0) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

                for (var remoteGuidIndex = _poolRemoteGuidQueue.Count - 1; remoteGuidIndex >= 0; remoteGuidIndex--)
                {
                    var remoteGuidBytes = _poolRemoteGuidQueue.Dequeue();
                    _poolRemoteGuidQueue.Enqueue(remoteGuidBytes);

                    for (var index = _pool.Count - 1; index >= 0; index--)
                    {
                        var cpm = _pool[index];

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

        //internal bool Poll(ConnectionPoolMember cpm)
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

        internal void Clean(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            lock (_poolLock)
            {
                if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }


                for (var i = _pool.Count - 1; i >= 0; i--)
                {
                    var cpm = _pool[i];
                    //if (!cpm.Client.Connected)
                    if (!cpm.IsActive(callChain: callChain, lvl: lvl + 1))
                    {
                        _pool.RemoveAt(i);
                        //cpm.Close(callChain: callChain, lvl: lvl+1);
                    }
                }
            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        internal void CloseAll(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            lock (_poolLock)
            {
                if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }


                for (var index = _pool.Count - 1; index >= 0; index--)
                {
                    var c = _pool[index];
                    try { c?.Close(callChain: callChain, lvl: lvl + 1); }
                    catch (Exception) { }
                }

                _pool.Clear();
            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        internal void QueueGuid(byte[] RemoteGuidBytes, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (this._isDisposed || this._pool == null) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            lock (_poolLock)
            {
                if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

                if (!_poolRemoteGuidQueue.Any(a => a.SequenceEqual(RemoteGuidBytes))) { _poolRemoteGuidQueue.Enqueue(RemoteGuidBytes); }
            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        internal void Add(ConnectionPoolMember cpm, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (cpm == null || !cpm.IsActive(callChain: callChain, lvl: lvl + 1)) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }
            if (this._isDisposed || this._pool == null) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            //if (cpm.Ct.IsCancellationRequested || cpm.Client == null || !cpm.Client.Connected || _pool == null || _isDisposed)
            //{
            //    cpm.Close();
            //    Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl+1); return;
            //}

            lock (_poolLock)
            {
                if (this._isDisposed || this._pool == null) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

                if (_pool.Contains(cpm))
                {
                    Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                    return;
                }

                var activeConnections = CountActive(callChain: callChain, lvl: lvl + 1);

                if (_isIncoming || (_isOutgoing && activeConnections.total < Program.ProgramArgs.ClientConnectionPoolSize))
                {
                    _pool.Add(cpm);
                    cpm.Cp = this;
                    QueueGuid(cpm.RemoteGuidBytes, callChain: callChain, lvl: lvl + 1);
                }
                else
                {
                    cpm.Close(callChain: callChain, lvl: lvl + 1);
                }
            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        internal void Remove(ConnectionPoolMember cpm, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            lock (_poolLock)
            {
                if (this._isDisposed || this._pool == null) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

                _pool.Remove(cpm);

            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        internal async Task StopAsync(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);


            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            lock (_poolLock)
            {
                _poolCts.Cancel();
                _poolCts.Dispose();
                CloseAll(callChain: callChain, lvl: lvl + 1);
            }

            if (_task != null && !_task.IsCompleted) await _task.ConfigureAwait(false);

            lock (_poolLock)
            {
                _poolRemoteGuidQueue = null;
                _pool = null;
                _poolCts = null;
                PoolCt = default;
                _task = null;
                PoolName = null;
            }

            _isDisposed = true;
            //_poolLock = null;

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        internal void Start(bool isServer, string localPoolName, Guid localPoolGuid, Guid remoteServerPoolGuid,  CancellationToken ct, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            if (string.IsNullOrWhiteSpace(localPoolName)) throw new ArgumentOutOfRangeException(nameof(localPoolName));
            if (localPoolGuid == default) throw new ArgumentOutOfRangeException(nameof(localPoolGuid));
            
            PoolName = localPoolName;

            _isIncoming = isServer;
            _isOutgoing = !isServer;

            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, PoolCt);
            var linkedCt = linkedCts.Token;


            _task = Task.Run(async () =>
                {
                    var poolGuidBytes = localPoolGuid.ToByteArray();
                    var remoteServerPoolGuidBytes = remoteServerPoolGuid != default
                        ? remoteServerPoolGuid.ToByteArray()
                        : null;

                    Logging.LogEvent($@"Pool ""{localPoolName}"".  Starting.", ModuleName);

                    TcpListener listener = null;


                    if (isServer)
                    {
                        listener = new TcpListener(IPAddress.Any, Program.ProgramArgs.ServerPort);
                        while (!listener.Server.IsBound)
                        {
                            if (linkedCt.IsCancellationRequested)
                            {
                                Logging.LogEvent($@"Pool ""{localPoolName}"".  Cancellation requested on {nameof(linkedCt)}.", ModuleName);
                                break;
                            }

                            try { listener.Start(Program.ProgramArgs.ServerBacklog); }
                            catch (Exception e)
                            {
                                Logging.LogException(e, $@"{localPoolName}: Connection pool");
                                try { await Task.Delay(TimeSpan.FromMilliseconds(1), linkedCt).ConfigureAwait(false); }
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

                            if (linkedCt.IsCancellationRequested)
                            {
                                Logging.LogEvent($@"Pool ""{localPoolName}"".  Cancellation requested on {nameof(linkedCt)}.", ModuleName);
                                break;
                            }
                            
                            var activeConnections = CountActive(callChain: callChain, lvl: lvl + 1);

                            if (activeConnections != lastActiveConnections)
                            {
                                lastActiveConnections = activeConnections;
                                var totalHosts = CountRemoteGuids(ModuleName, callChain: callChain, lvl: lvl + 1);
                                Logging.LogEvent($@"Pool ""{PoolName}"". Pool reserved: {activeConnections.reserved}. Pool unreserved: {activeConnections.unreserved}. Pool total: {activeConnections.total}. Total hosts: {totalHosts}.", ModuleName);
                            }
                            
                            TcpClient client = null;
                            var isClientIncoming = false;

                            if (!isServer)
                            {
                                try { await Task.Delay(TimeSpan.FromMilliseconds(1), linkedCt).ConfigureAwait(false); } catch (Exception) { }

                                

                                if (activeConnections.total < Program.ProgramArgs.ClientConnectionPoolSize)

                                    try
                                    {
                                        var outgoingClient = new TcpClient();
                                        var outgoingClientConnect = Task.Run(async () =>
                                            {
                                                try
                                                {
                                                    Logging.LogEvent($"Connection pool: Establishing outgoing connecting to {Program.ProgramArgs.ServerIp}:{Program.ProgramArgs.ServerPort}.", ModuleName);
                                                    await outgoingClient.ConnectAsync(Program.ProgramArgs.ServerIp, Program.ProgramArgs.ServerPort, linkedCt).ConfigureAwait(false);
                                                }
                                                catch (Exception e) { Logging.LogException(e, "", ModuleName); }
                                            },
                                            linkedCt);

                                        try { await Task.WhenAny(outgoingClientConnect, Task.Delay(TimeSpan.FromSeconds(10), linkedCt)).ConfigureAwait(false); } catch (Exception) { }

                                        if (!outgoingClientConnect.IsCompletedSuccessfully || outgoingClient == null || outgoingClient.Client == null || !outgoingClient.Connected)
                                        {
                                            Logging.LogEvent("Connection pool: Outgoing connection timed out.", ModuleName);
                                            try { outgoingClient.Close(); } catch (Exception) { }
                                            try { await Task.Delay(TimeSpan.FromSeconds(1), linkedCt).ConfigureAwait(false); } catch (Exception) { }
                                            continue;
                                        }

                                        client = outgoingClient;
                                        isClientIncoming = false;
                                    }
                                    catch (Exception e)
                                    {
                                        Logging.LogEvent("Connection pool: Outgoing connection timed out.", ModuleName);
                                        Logging.LogException(e, "", ModuleName);

                                        try { await Task.Delay(TimeSpan.FromSeconds(1), linkedCt).ConfigureAwait(false); } catch (Exception) { }
                                    }
                            }

                            else if (isServer)
                            {
                                try { await Task.Delay(TimeSpan.FromMilliseconds(1), linkedCt).ConfigureAwait(false); } catch (Exception) { }

                                var incomingClientConnect = Task.Run(async () =>
                                    {
                                        if (!linkedCt.IsCancellationRequested) { return ct.IsCancellationRequested ? default : await listener.AcceptTcpClientAsync().ConfigureAwait(false); }
                                        return default;
                                    },
                                    linkedCt);

                                do
                                {
                                    // await up to 10 seconds
                                    var first = await Task.WhenAny(incomingClientConnect, Task.Delay(TimeSpan.FromSeconds(10), linkedCt)).ConfigureAwait(false);

                                    // check if client_connect task didn't finish
                                    if (!incomingClientConnect.IsCompleted) //Successfully)
                                    {
                                        // if didn't finish, check if cancellation requested
                                        if (linkedCt.IsCancellationRequested)
                                        {
                                            Logging.LogEvent($@"Pool ""{localPoolName}"".  Cancellation requested on {nameof(linkedCt)}.", ModuleName);
                                            break;
                                        }
                                    }
                                    else { break; }
                                } while (!incomingClientConnect.IsCompleted);

                                if (incomingClientConnect.IsCompletedSuccessfully)
                                {
                                    client = incomingClientConnect.Result;
                                    isClientIncoming = true;
                                }
                            }

                            if (linkedCt.IsCancellationRequested)
                            {
                                if (client != null)
                                    try { client.Close(); }
                                    catch (Exception) { }

                                break;
                            }

                            if (client == null)
                            {
                                await Task.Delay(TimeSpan.FromSeconds(1), linkedCt).ConfigureAwait(false);
                                continue;
                            }


                            var rid = TcpClientExtra.ReadTcpClientRemoteAddress(client);
                            Logging.LogEvent($@"Pool ""{localPoolName}"".  Connection established. Local address: {rid.localAddress}:{rid.localPort}. Remote address: {rid.remoteAddress}:{rid.remotePort}.", ModuleName);

                            if (client != null && client.Connected)
                            {
                                var stream = client.GetStream();

                                // check if server guid matches... if this is an outgoing connection to the server
                                
                                var handshake = await TcpClientExtra.ChallengeRequestAsync(isServer, client, stream, poolGuidBytes, remoteServerPoolGuidBytes, ct: linkedCt).ConfigureAwait(false);
                                var remoteGuid = handshake.remote_guid;
                                var remoteGuidBytes = remoteGuid.ToByteArray();

                                Logging.LogEvent($@"Pool ""{localPoolName}"".  Challenge {(handshake.challenge_correct ? "successful" : "failed")}. Local address: {rid.localAddress}:{rid.localPort} ({localPoolGuid}). Remote address: {rid.remoteAddress}:{rid.remotePort} ({handshake.remote_guid}).");
                                
                                if (!handshake.challenge_correct) continue;
                                


                                Add(new ConnectionPoolMember(callChain: callChain, lvl: lvl + 1)
                                {
                                    Client = client,
                                    Stream = stream,
                                    LocalGuid = localPoolGuid,
                                    LocalGuidBytes = poolGuidBytes,
                                    RemoteGuid = remoteGuid,
                                    RemoteGuidBytes = remoteGuidBytes,
                                    LocalHost = rid.localAddress.ToString(),
                                    LocalPort = rid.localPort,
                                    RemoteHost = rid.remoteAddress.ToString(),
                                    RemotePort = rid.remotePort,
                                }, callChain: callChain, lvl: lvl + 1);
                            }
                        }
                        catch (Exception)
                        {
                            try { await Task.Delay(TimeSpan.FromMilliseconds(1), linkedCt).ConfigureAwait(false); }
                            catch (Exception) { }
                        }

                        Clean(callChain: callChain, lvl: lvl + 1);
                        try { await Task.Delay(TimeSpan.FromMilliseconds(1), linkedCt).ConfigureAwait(false); }
                        catch (Exception) { }
                    }

                    if (isServer)
                        try { listener.Stop(); }
                        catch (Exception e) { Logging.LogException(e, $@"{localPoolName}: Connection pool", ModuleName); }


                    CloseAll(callChain: callChain, lvl: lvl + 1);

                    linkedCts.Dispose();

                    Logging.LogEvent($@"Pool ""{localPoolName}"".  Exiting.", ModuleName);
                },
                linkedCt);

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }


    }
}