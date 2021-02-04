using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch.logic
{
    internal class ConnectionPool
    {
        internal const string ModuleName = nameof(ConnectionPool);

        private bool _isDisposed;
        private List<ConnectionPoolMember> Pool = new List<ConnectionPoolMember>();
        private CancellationToken PoolCt;
        private CancellationTokenSource PoolCts = new CancellationTokenSource();
        private object PoolLock = new object();
        private Queue<byte[]> PoolRemoteGuidQueue = new Queue<byte[]>();
        private Task Task;

        public ConnectionPool()
        {
            PoolCt = PoolCts.Token;
        }

        internal string PoolName { get; private set; }

        internal int Count()
        {
            if (_isDisposed) return default;

            lock (PoolLock) { return PoolCt.IsCancellationRequested ? default :Pool.Count; }
        }

        internal int CountActive()
        {
            if (_isDisposed) return default;

            lock (PoolLock) { return PoolCt.IsCancellationRequested ? default :Pool.Count(a => a.client != null && a.client.Connected); }
        }

        internal int CountRemoteGuids()
        {
            if (_isDisposed) return default;

            lock (PoolLock) { return PoolCt.IsCancellationRequested ? default :PoolRemoteGuidQueue.Count; }
        }

        internal void Poll()
        {
            if (_isDisposed) return;

            lock (PoolLock) { Pool = Pool.AsParallel().AsOrdered().Where(a => TcpClientExtra.PollTcpClientConnection(a.client)).ToList(); }
        }

        internal ConnectionPoolMember GetNextClient()
        {
            if (_isDisposed) return default;

            lock (PoolLock)
            {
                if (Pool.Count == 0) return default;

                if (PoolRemoteGuidQueue.Count == 0) return default;

                for (var i = PoolRemoteGuidQueue.Count - 1; i >= 0; i--)
                {
                    var remoteGuidBytes = PoolRemoteGuidQueue.Dequeue();
                    PoolRemoteGuidQueue.Enqueue(remoteGuidBytes);

                    var client = Pool.FirstOrDefault(a => a.client != null && a.client.Connected && a.remoteGuidBytes.SequenceEqual(remoteGuidBytes));

                    // could poll client here to check connected...

                    if (client != default)
                    {
                        Pool.Remove(client);

                        var pollOk = TcpClientExtra.PollTcpClientConnection(client.client);

                        if (pollOk) return PoolCt.IsCancellationRequested ? default :client;

                        client.Close();
                        
                        Logging.LogEvent($@"{PoolName}: Connection pool: Connection closed.", ModuleName);
                    }
                }

                return default;
            }
        }

        internal void Clean()
        {
            if (_isDisposed) return;

            lock (PoolLock)
            {
                for (var i = Pool.Count - 1; i >= 0; i--)
                    if (!Pool[i].client.Connected)
                    {
                        Pool.RemoveAt(i);
                        Logging.LogEvent($@"{PoolName}: Connection pool: Connection closed.", ModuleName);
                    }
            }
        }

        internal void CloseAll()
        {
            if (_isDisposed) return;

            lock (PoolLock)
            {
                foreach (var c in Pool)
                {
                    try { c?.Close(); }
                    catch (Exception) { }

                    Logging.LogEvent($@"{PoolName}: Connection pool: Connection closed.", ModuleName);
                }

                Pool.Clear();
            }
        }

        internal void Add(ConnectionPoolMember client)
        {
            //if (_isDisposed) return;

            if (client == null) return;

            if (client.ct.IsCancellationRequested || client.client == null || !client.client.Connected || Pool == null || _isDisposed)
            {
                client.Close();

                Logging.LogEvent($"{PoolName}: Connection pool: Connection closed.", ModuleName);
                return;
            }

            lock (PoolLock)
            {
                Pool.Add(client);

                if (!PoolRemoteGuidQueue.Any(a => a.SequenceEqual(client.remoteGuidBytes))) PoolRemoteGuidQueue.Enqueue(client.remoteGuidBytes);
            }
        }

        internal async Task StopAsync()
        {
            if (_isDisposed) return;

            _isDisposed = true;

            lock (PoolLock)
            {
                PoolCts.Cancel();
                PoolCts.Dispose();
                CloseAll();
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
            }

            PoolLock = null;
        }

        internal void Start(string poolName, Guid poolGuid, bool incoming, bool outgoing, CancellationToken ct)
        {
            if (_isDisposed) return;

            PoolName = poolName;

            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, PoolCt);
            var linkedCt = linkedCts.Token;


            Task = Task.Run(async () =>
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
                                        if (!linkedCt.IsCancellationRequested) return ct.IsCancellationRequested ? default :await listener.AcceptTcpClientAsync().ConfigureAwait(false);
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
                                        /*Logging.LogException( e, "", _ModuleName); */
                                    }

                                    try { client?.Close(); }
                                    catch (Exception)
                                    {
                                        /*Logging.LogException( e, "", _ModuleName);*/
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
                                        /*Logging.LogException( e, "", _ModuleName); */
                                    }

                                    try { client?.Close(); }
                                    catch (Exception)
                                    {
                                        /*Logging.LogException( e, "", _ModuleName);*/
                                    }

                                    continue;
                                }


                                Add(new ConnectionPoolMember
                                {
                                    client = client,
                                    stream = stream,
                                    localGuidBytes = poolGuidBytes,
                                    remoteGuidBytes = remoteGuidBytes,
                                    localHost = rid.localAddress.ToString(),
                                    localPort = rid.localPort,
                                    remoteHost = rid.remoteAddress.ToString(),
                                    remotePort = rid.remotePort
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

        internal class ConnectionPoolMember
        {
            private bool _IsDisposed;
            internal CancellationTokenSource cts = new CancellationTokenSource();
            internal CancellationToken ct = default;

            internal TcpClient client;

            internal byte[] localGuidBytes;
            internal string localHost;
            internal int localPort;

            internal byte[] remoteGuidBytes;
            internal string remoteHost;
            internal int remotePort;
            internal NetworkStream stream;

            internal ConnectionPoolMember()
            {
                ct = cts.Token;
            }

            internal void Close()
            {
                if (_IsDisposed) return;

                _IsDisposed = true;

                try {stream?.Close();} catch (Exception) { }
                try {client?.Close(); } catch (Exception) { }
                try {cts?.Cancel();} catch (Exception) { }
                try {cts?.Dispose();} catch (Exception) { }

                cts = default;
                client = default;
                stream = default;
                localGuidBytes = default;
                remoteGuidBytes = default;
                localHost = default;
                localPort = default;
                remoteHost = default;
                remotePort = default;
            }
        }
    }
}