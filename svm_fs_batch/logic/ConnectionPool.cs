using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal class ConnectionPool
    {
        internal const string ModuleName = nameof(ConnectionPool);

        private bool _isDisposed;
        private List<ConnectionPoolMember> _pool = new List<ConnectionPoolMember>();
        internal CancellationToken PoolCt { get; private set; }
        private CancellationTokenSource _poolCts = new CancellationTokenSource();
        private object _poolLock = new object();
        private Queue<byte[]> _poolRemoteGuidQueue = new Queue<byte[]>();
        private Task _task;

        public ConnectionPool()
        {
            PoolCt = _poolCts.Token;
        }

        internal string PoolName { get; private set; }

        internal int Count()
        {
            if (_isDisposed) return default;

            lock (_poolLock)
            {
            if (_isDisposed) return default;

            return PoolCt.IsCancellationRequested ? default : _pool.Count;
            }
        }

        internal int CountActive()
        {
            if (_isDisposed) return default;

            lock (_poolLock)
            {
                if (_isDisposed) return default;

                var numActive = 0;
                for (var index = _pool.Count - 1; index >= 0; index--)
                {
                    if (_pool[index].IsActive()) numActive++;
                }

                return PoolCt.IsCancellationRequested
                    ? default
                    : numActive;
                
                // _pool.Count(a => a.IsActive());
            }
        }

        internal int CountRemoteGuids()
        {
            if (_isDisposed) return default;

            lock (_poolLock) { return PoolCt.IsCancellationRequested ? default : _poolRemoteGuidQueue.Count; }
        }

        internal void PollPool()
        {
            if (_isDisposed) return;

            lock (_poolLock) { _pool = _pool.AsParallel().AsOrdered().Where(a => a.IsConnected()).ToList(); }
        }

        internal ConnectionPoolMember GetNextClient()
        {
            if (_isDisposed) return default;

            lock (_poolLock)
            {
                if (_isDisposed) return default;

                if (_pool.Count == 0) return default;

                if (_poolRemoteGuidQueue.Count == 0) return default;

                for (var remoteGuidIndex = _poolRemoteGuidQueue.Count - 1; remoteGuidIndex >= 0; remoteGuidIndex--)
                {
                    var remoteGuidBytes = _poolRemoteGuidQueue.Dequeue();
                    _poolRemoteGuidQueue.Enqueue(remoteGuidBytes);

                    for (var index = _pool.Count - 1; index >= 0; index--)
                    {
                        var cpm = _pool[index];

                        // IsActive checks if tcp connected and not cancelled
                        if (cpm.IsActive() && cpm.HasRemoteGuid(remoteGuidBytes))
                        {
                            cpm.LeavePool();

                            return cpm;
                        }
                    }

                    //var cpm = _pool.FirstOrDefault(a => a.IsActive() && a.HasRemoteGuid(remoteGuidBytes));

                    // could poll client here to check connected...

                    //if (cpm != default)
                    //{
                    //    cpm.LeavePool();

                    //    if (cpm.IsConnected())
                    //    {
                    //        if (!PoolCt.IsCancellationRequested && !cpm.Ct.IsCancellationRequested)
                    //        {
                    //            return cpm;
                    //        }
                    //    }
                    //}
                }

                return default;
            }
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
        //        return pollOk;
        //    }
        //}

        internal void Clean()
        {
            if (_isDisposed) return;

            lock (_poolLock)
            {
                if (_isDisposed) return;


                for (var i = _pool.Count - 1; i >= 0; i--)
                {
                    var cpm = _pool[i];
                    if (!cpm.Client.Connected)
                    {
                        _pool.RemoveAt(i);
                        cpm.Close();
                    }
                }
            }
        }

        internal void CloseAll()
        {
            if (_isDisposed) return;

            lock (_poolLock)
            {
                if (_isDisposed) return;


                for (var index = _pool.Count - 1; index >= 0; index--)
                {
                    var c = _pool[index];
                    try { c?.Close(); }
                    catch (Exception) { }
                }

                _pool.Clear();
            }
        }

        internal void QueueGuid(byte[] RemoteGuidBytes)
        {
            if (this._isDisposed || this._pool == null) return;

            lock (_poolLock)
            {
                if (_isDisposed) return;

                if (!_poolRemoteGuidQueue.Any(a => a.SequenceEqual(RemoteGuidBytes))) _poolRemoteGuidQueue.Enqueue(RemoteGuidBytes);
            }
        }

        internal void Add(ConnectionPoolMember cpm)
        {
            if (cpm == null || !cpm.IsConnected()) return;
            if (this._isDisposed || this._pool == null) return;

            //if (cpm.Ct.IsCancellationRequested || cpm.Client == null || !cpm.Client.Connected || _pool == null || _isDisposed)
            //{
            //    cpm.Close();
            //    return;
            //}

            lock (_poolLock)
            {
                if (this._isDisposed || this._pool == null) return;


                if (!_pool.Contains(cpm)) { _pool.Add(cpm); }

                cpm.Cp = this;

                QueueGuid(cpm.RemoteGuidBytes);
            }
        }

        internal void Remove(ConnectionPoolMember cpm)
        {
            lock (_poolLock)
            {
                if (this._isDisposed || this._pool == null) return;

                _pool.Remove(cpm);

            }
        }

        internal async Task StopAsync()
        {
            if (_isDisposed) return;

            lock (_poolLock)
            {
                _poolCts.Cancel();
                _poolCts.Dispose();
                CloseAll();
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
        }

        internal void Start(string poolName, Guid poolGuid, bool incoming, bool outgoing, CancellationToken ct)
        {
            if (_isDisposed) return;

            PoolName = poolName;

            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, PoolCt);
            var linkedCt = linkedCts.Token;


            _task = Task.Run(async () =>
                {
                    var poolGuidBytes = poolGuid.ToByteArray();

                    Logging.LogEvent($"{poolName}: Connection pool: Starting.", ModuleName);


                    var listener = incoming
                        ? new TcpListener(IPAddress.Any, Program.ProgramArgs.ServerPort)
                        : null;

                    if (incoming)
                    {
                        while (!listener.Server.IsBound)
                        {
                            if (linkedCt.IsCancellationRequested)
                            {
                                Logging.LogEvent($"{poolName}: Connection pool: Cancellation requested on {nameof(linkedCt)}.", ModuleName);
                                break;
                            }

                            try { listener.Start(Program.ProgramArgs.ServerBacklog); }
                            catch (Exception e)
                            {
                                Logging.LogException(e, $@"{poolName}: Connection pool");
                                try { await Task.Delay(TimeSpan.FromMilliseconds(1), linkedCt).ConfigureAwait(false); }
                                catch (Exception) { }
                            }
                        }

                        if (!listener.Server.IsBound) return;

                        var lid = TcpClientExtra.ReadTcpClientRemoteAddress(listener);
                        Logging.LogEvent($"{poolName}: Connection pool: Listening for incoming connections on {lid.localAddress}:{lid.localPort} {lid.remoteAddress}:{lid.remotePort}", ModuleName);
                    }


                    while (true)
                    {
                        try
                        {
                            try { await Task.Delay(TimeSpan.FromMilliseconds(1), linkedCt).ConfigureAwait(false); }
                            catch (Exception) { }

                            if (linkedCt.IsCancellationRequested)
                            {
                                Logging.LogEvent($"{poolName}: Connection pool: Cancellation requested on {nameof(linkedCt)}.", ModuleName);
                                break;
                            }


                            TcpClient client = null;

                            if (outgoing)
                            {
                                var activeConnections = CountActive();
                                if (activeConnections < Program.ProgramArgs.ClientConnectionPoolSize)
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
                                                catch (Exception) { }
                                            },
                                            linkedCt);

                                        try { await Task.WhenAny(outgoingClientConnect, Task.Delay(TimeSpan.FromSeconds(10), linkedCt)).ConfigureAwait(false); }
                                        catch (Exception) { }

                                        if (!outgoingClientConnect.IsCompletedSuccessfully || outgoingClient == null || outgoingClient.Client == null || !outgoingClient.Connected)
                                        {
                                            Logging.LogEvent("Connection pool: Outgoing connection timed out.", ModuleName);
                                            try { outgoingClient.Close(); }
                                            catch (Exception) { }

                                            continue;
                                        }

                                        client = outgoingClient;
                                    }
                                    catch (Exception)
                                    {
                                        Logging.LogEvent("Connection pool: Outgoing connection timed out.", ModuleName);

                                        try { await Task.Delay(TimeSpan.FromMilliseconds(1), linkedCt).ConfigureAwait(false); }
                                        catch (Exception) { }
                                    }
                            }

                            if (incoming)
                            {
                                var incomingClientConnect = Task.Run(async () =>
                                    {
                                        if (!linkedCt.IsCancellationRequested) return ct.IsCancellationRequested ? default : await listener.AcceptTcpClientAsync().ConfigureAwait(false);
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
                                            Logging.LogEvent($"{poolName}: Connection pool: Cancellation requested on {nameof(linkedCt)}.", ModuleName);
                                            break;
                                        }
                                    }
                                    else { break; }
                                } while (!incomingClientConnect.IsCompleted);

                                if (incomingClientConnect.IsCompletedSuccessfully) client = incomingClientConnect.Result;
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
                                await Task.Delay(TimeSpan.FromMilliseconds(1), linkedCt).ConfigureAwait(false);
                                continue;
                            }


                            var rid = TcpClientExtra.ReadTcpClientRemoteAddress(client);
                            Logging.LogEvent($"{poolName}: Connection pool: Connection established. Local address: {rid.localAddress}:{rid.localPort}. Remote address: {rid.remoteAddress}:{rid.remotePort}.", ModuleName);

                            if (client != null && client.Connected)
                            {
                                var stream = client.GetStream();
                                TcpClientExtra.KeepAlive(client.Client);
                                TcpClientExtra.SetTimeouts(client, stream);
                                await stream.WriteAsync(new byte[1], 0, 0, linkedCt).ConfigureAwait(false);

                                var pollOk = TcpClientExtra.PollTcpClientConnection(client);

                                if (!pollOk)
                                {
                                    try { stream?.Close(); }
                                    catch (Exception)
                                    {
                                        /*Logging.LogException( e, "", ModuleName); */
                                    }

                                    try { client?.Close(); }
                                    catch (Exception)
                                    {
                                        /*Logging.LogException( e, "", ModuleName);*/
                                    }

                                    continue;
                                }

                                var handshake = await TcpClientExtra.ChallengeRequestAsync(client, stream, poolGuidBytes, ct: linkedCt).ConfigureAwait(false);

                                var remoteGuid = handshake.remote_guid;
                                var remoteGuidBytes = remoteGuid.ToByteArray();
                                var totalConnections = CountActive();
                                var totalHosts = CountRemoteGuids();

                                Logging.LogEvent($"{poolName}: Connection pool: Challenge {(handshake.challenge_correct ? "successful" : "failed")}. Local address: {rid.localAddress}:{rid.localPort} ({poolGuid}). Remote address: {rid.remoteAddress}:{rid.remotePort} ({handshake.remote_guid}). Total connections: {totalConnections}. Total hosts: {totalHosts}.", ModuleName);

                                if (!handshake.challenge_correct)
                                {
                                    try { stream?.Close(); }
                                    catch (Exception)
                                    {
                                        /*Logging.LogException( e, "", ModuleName); */
                                    }

                                    try { client?.Close(); }
                                    catch (Exception)
                                    {
                                        /*Logging.LogException( e, "", ModuleName);*/
                                    }

                                    continue;
                                }


                                Add(new ConnectionPoolMember
                                {
                                    Client = client,
                                    Stream = stream,
                                    LocalGuidBytes = poolGuidBytes,
                                    RemoteGuidBytes = remoteGuidBytes,
                                    LocalHost = rid.localAddress.ToString(),
                                    LocalPort = rid.localPort,
                                    RemoteHost = rid.remoteAddress.ToString(),
                                    RemotePort = rid.remotePort,
                                });
                            }
                        }
                        catch (Exception)
                        {
                            try { await Task.Delay(TimeSpan.FromMilliseconds(1), linkedCt).ConfigureAwait(false); }
                            catch (Exception) { }
                        }

                        Clean();
                        try { await Task.Delay(TimeSpan.FromMilliseconds(1), linkedCt).ConfigureAwait(false); }
                        catch (Exception) { }
                    }

                    if (incoming)
                        try { listener.Stop(); }
                        catch (Exception e) { Logging.LogException(e, $@"{poolName}: Connection pool", ModuleName); }


                    CloseAll();

                    linkedCts.Dispose();

                    Logging.LogEvent($@"{poolName}: Connection pool: Exiting.", ModuleName);
                },
                linkedCt);
        }


    }
}