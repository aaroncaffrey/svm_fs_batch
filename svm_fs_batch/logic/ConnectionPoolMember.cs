using System;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using SvmFsBatch.logic;

namespace SvmFsBatch
{
    internal class ConnectionPoolMember
    {
        internal async Task<(bool readOk, ulong frameId, ulong frameType, int frameLength, byte[] bytesTextIn, string textIn, string[] textInLines)> ReadFrameAsync()//TcpClient client, NetworkStream stream, CancellationToken ct)
        {
            if (_isDisposed) return default;

            var frame = await TcpClientExtra.ReadFrameAsync(Client, Stream, Ct);

            if (!frame.readOk) IsActive();

            return frame;
        }

        internal void JoinPool(ConnectionPool cp)
        {
            if (_isDisposed || cp == null) return;

            lock (ObjectLock)
            {
                if (_isDisposed || cp == null) return;

                try
                {
                    this.Cp = cp;
                    this.Cp?.Add(this);
                }
                catch (Exception e)
                {
                    Logging.LogException(e, "", ModuleName);
                }
            }
        }

        internal void LeavePool()
        {
            if (_isDisposed) return;

            lock (ObjectLock) {
                if (_isDisposed) return;

                try
                {
                    Cp?.Remove(this);
                    Cp = null;
                }
                catch (Exception e)
                {
                    Logging.LogException(e, "", ModuleName);
                }
            }
        }

        private const string ModuleName = nameof(ConnectionPoolMember);
        private bool _isDisposed;
        internal CancellationTokenSource Cts = new CancellationTokenSource();
        internal CancellationToken Ct = default;

        internal ConnectionPool Cp;

        internal TcpClient Client;

        internal byte[] LocalGuidBytes;
        internal string LocalHost;
        internal int LocalPort;

        internal byte[] RemoteGuidBytes;
        internal string RemoteHost;
        internal int RemotePort;
        internal NetworkStream Stream;
        internal object ObjectLock = new object();

        internal bool IsCancelled()
        {
            if (_isDisposed) return true;

            lock (ObjectLock)
            {
                if (_isDisposed) return true;
                if (Ct.IsCancellationRequested)
                {
                    Close();
                    return true;
                }


                try
                {
                    if (Cp != null && Cp.PoolCt.IsCancellationRequested)
                    {
                        Close();
                        return true;
                    }
                }
                catch (Exception e)
                {
                    Logging.LogException(e, "", ModuleName);
                }

                return false;
            }
        }

        internal bool HasRemoteGuid(byte[] queryRemoteGuidBytes)
        {
            if (_isDisposed) return false;

            lock (ObjectLock)
            {
                if (_isDisposed) return false;

                try
                {
                    if (RemoteGuidBytes == null && queryRemoteGuidBytes == null) return true;
                    if (RemoteGuidBytes == null || queryRemoteGuidBytes == null) return false;

                    return RemoteGuidBytes.SequenceEqual(queryRemoteGuidBytes);
                }
                catch (Exception e)
                {
                    Logging.LogException(e,"", ModuleName);
                }

                return false;
            }
        }

        internal bool IsActive()
        {
            // does not actually check if connected, use Poll() for that.

            if (_isDisposed) return false;

            lock (ObjectLock)
            {
                if (_isDisposed) return false;
                if (IsCancelled()) return false;
                if (IsConnected()) return true;
                //try
                //{
                //    if (Client != null && Client.Connected) return true;
                //}
                //catch (Exception e)
                //{
                //    Logging.LogException(e, "", ModuleName);
                //}

                Close();
                return false;
            }
        }

        internal ConnectionPoolMember()
        {
            Ct = Cts.Token;
        }

        internal bool IsConnected()
        {
            if (_isDisposed) return false;

            try
            {
                lock (ObjectLock)
                {
                    if (_isDisposed) return false;

                    if (IsCancelled()) return false;

                    var pollOk = TcpClientExtra.PollTcpClientConnection(Client);
                    if (!pollOk) { Close(); }

                    return pollOk;
                }
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);

                Close();

                return false;
            }
        }

        internal void Close()
        {
            if (_isDisposed) return;

            try
            {
                lock (ObjectLock)
                {
                    if (_isDisposed) return;

                    Logging.LogEvent($@"{Cp?.PoolName ?? "Not pooled"}: Connection pool: Connection closed.", ModuleName);

                    LeavePool();

                    _isDisposed = true;

                    try { Stream?.Close(); }
                    catch (Exception) { }

                    try { Client?.Close(); }
                    catch (Exception) { }

                    try { Cts?.Cancel(); }
                    catch (Exception) { }

                    try { Cts?.Dispose(); }
                    catch (Exception) { }

                    Cts = default;
                    Ct = default;
                    Client = default;
                    Stream = default;
                    LocalGuidBytes = default;
                    RemoteGuidBytes = default;
                    LocalHost = default;
                    LocalPort = default;
                    RemoteHost = default;
                    RemotePort = default;
                    Cp = default;
                }

                //ObjectLock = default;
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
            }
        }
    }
}
