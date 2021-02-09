using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SvmFsBatch.logic;

namespace SvmFsBatch
{
    internal class ConnectionPoolMember
    {
        private ulong _FrameId;
        //private ulong _WriteFrameId;
        //private Queue<(ulong frameId, byte[])> _WriteQueue = new Queue<(ulong frameId, byte[])>();


        internal async Task<bool> WriteFrameAsync(ulong frameType, string text, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {

            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            byte[] _bytesFrameCode = new byte[] { 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5 };

            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
            if (Ct.IsCancellationRequested) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }


            try
            {
                if (!_isDisposed && Client != null && Stream != null && Client.Connected && Stream.CanWrite)
                {
                    ulong frameId = 0;

                    lock (ObjectLock) { frameId = ++_FrameId; }

                    var bytesText = !string.IsNullOrEmpty(text) ? Encoding.UTF8.GetBytes(text)
                        : Array.Empty<byte>();
                    var bytesFrameId = BitConverter.GetBytes(frameId);
                    var bytesFrameType = BitConverter.GetBytes(frameType);
                    var bytesFrameLength = BitConverter.GetBytes((ulong)bytesText.Length);
                    var frame = new[] { _bytesFrameCode, bytesFrameId, bytesFrameType, bytesFrameLength, bytesText/*, _bytesFrameCode*/ }.SelectMany(a => a).ToArray();

                    //lock (ObjectLock)
                    //{
                    //    _WriteQueue.Enqueue(frame);
                    //    var writeFrameId = _WriteFrameId++;
                    //}

                    await Stream.WriteAsync(frame, Ct).ConfigureAwait(false);
                    await Stream.FlushAsync(Ct).ConfigureAwait(false);
                    timeWrite = DateTime.Now;

                    Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return true;
                }
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
            }

            Close(callChain: callChain, lvl: lvl + 1);
            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return false;
        }

        internal async Task<byte[]> ReadRawFixedLengthAsync(int frameLength, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {

            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
            if (Ct.IsCancellationRequested) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
            if (frameLength <= 0) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }


            var bytes = new byte[frameLength];
            var readLength = 0;

            ulong whileCnt1 = 0;

            try
            {
                while (!_isDisposed && !Ct.IsCancellationRequested && readLength < frameLength && Client != null && Client.Connected && Stream != null && Stream.CanRead)
                {
                    whileCnt1++;

                    Logging.LogEvent($"!!!! {nameof(ReadRawFixedLengthAsync)} WHILE 1 !!!! {whileCnt1}");

                    var bytesRead = await Stream.ReadAsync(bytes, readLength, bytes.Length - readLength, Ct).ConfigureAwait(false);
                    readLength += bytesRead;

                    if (bytesRead > 0)
                    {
                        timeReceive = DateTime.UtcNow;
                    }
                    /*
                    else if (bytesRead == 0)
                    {
                        var now = DateTime.UtcNow;
                        var receiveTimeout = TimeSpan.FromMinutes(1);
                        var writeTimeout = TimeSpan.FromMinutes(1);
                        
                        if (now - timeReceive > receiveTimeout && now - timeWrite > writeTimeout)
                        {
                            Logging.LogEvent("Read timeout.");
                            Close(ModuleName);
                            return default;
                        }
                    }
                    */
                    if (readLength != frameLength)//(bytesRead == 0 && readLength < frameLength && !Ct.IsCancellationRequested)
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(1), Ct).ConfigureAwait(false);

                        if (!IsActive(callChain: callChain, lvl: lvl + 1)) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
                    } else break;
                }
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
                Close(callChain: callChain, lvl: lvl + 1);
                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return default;
            }

            if (readLength == 0) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

            if (readLength != frameLength)
            {
                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return Ct.IsCancellationRequested
                    ? default
                    : bytes[..readLength];
            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return Ct.IsCancellationRequested ? default : bytes;
        }

        internal async Task<(bool readOk, ulong frameId, ulong frameType, int frameLength, byte[] bytesTextIn, string textIn, string[] textInLines)> ReadFrameAsync(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);


            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
            if (Ct.IsCancellationRequested) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
            if (Client == null || Stream == null || !Client.Connected || !Stream.CanRead) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

            byte[] _bytesFrameCode = new byte[] { 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5 };

            var headerLen = _bytesFrameCode.Length + (sizeof(ulong) * 3);

            byte[] header = Array.Empty<byte>();

            ulong whileCnt1 = 0;

            while (!_isDisposed && !Ct.IsCancellationRequested && header.Length != headerLen)
            {
                whileCnt1++;

                Logging.LogEvent($"!!!! {nameof(ReadFrameAsync)} WHILE 1 !!!! {whileCnt1}");

                var headerExt = await ReadRawFixedLengthAsync(headerLen - header.Length, callChain: callChain, lvl: lvl + 1).ConfigureAwait(false);
                if (headerExt != null && headerExt.Length > 0) header = header.Concat(headerExt).ToArray();

                if (Ct.IsCancellationRequested)
                { 
                    Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                    return default;
                }

                if (header.Length != headerLen)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(1), Ct).ConfigureAwait(false);

                    if (!IsActive(callChain: callChain, lvl: lvl + 1))
                    {
                        Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); 
                        return default;
                    }
                }
                else break;

            }

            if (header == null || header.Length != headerLen)
            {
                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); 
                return default;
            }



            var frameFrameCode = header.Take(_bytesFrameCode.Length).ToArray();
            if (!_bytesFrameCode.SequenceEqual(frameFrameCode))
            {
                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default;
            }
            var offset = _bytesFrameCode.Length;

            var frameId = BitConverter.ToUInt64(header, offset);
            offset += sizeof(ulong);

            var frameType = BitConverter.ToUInt64(header, offset);
            offset += sizeof(ulong);

            var frameLength = (int)BitConverter.ToUInt64(header, offset);
            offset += sizeof(ulong);

            var bytesTextIn = Array.Empty<byte>();

            ulong whileCnt2 = 0;

            while (frameLength > 0 && !_isDisposed && !Ct.IsCancellationRequested && bytesTextIn.Length != frameLength)
            {
                whileCnt2++;

                Logging.LogEvent($"!!!! {nameof(ReadFrameAsync)} WHILE 2 !!!! {whileCnt2}");

                var bytesTextInExt = frameLength > 0 ? await ReadRawFixedLengthAsync(frameLength - bytesTextIn.Length, callChain: callChain, lvl: lvl + 1).ConfigureAwait(false) : default;

                if (bytesTextInExt != null && bytesTextInExt.Length > 0) bytesTextIn = bytesTextIn.Concat(bytesTextInExt).ToArray();

                if (Ct.IsCancellationRequested) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

                if (bytesTextIn.Length != frameLength)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(1), Ct).ConfigureAwait(false);

                    if (!IsActive(callChain: callChain, lvl: lvl + 1)) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
                }
                else break;
            }

            if (bytesTextIn == null || bytesTextIn.Length != frameLength) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
            var textIn = bytesTextIn != null && bytesTextIn.Length > 0 ? Encoding.UTF8.GetString(bytesTextIn) : null;
            var textInLines = textIn?.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);


            var ret = Ct.IsCancellationRequested
                ? default
                : (true, frameId, frameType, frameLength, bytesTextIn, textIn, textInLines);

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
            return ret;
        }

        //internal void JoinPool(ConnectionPool cp, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        //{
        //    callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
        //    Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);
        //    if (_isDisposed || cp == null) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }
        //    lock (ObjectLock)
        //    {
        //        if (_isDisposed || cp == null) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }
        //        try
        //        {
        //            this.Cp = cp;
        //            this.Cp?.Add(this, callChain: callChain, lvl:lvl+1);
        //        }
        //        catch (Exception e)
        //        {
        //            Logging.LogException(e, "", ModuleName);
        //        }
        //    }
        //}

        internal void Unreserve(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);


            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            lock (ObjectLock)
            {
                if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

                IsReserved = false;
            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        internal bool Reserve(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return false; }

            lock (ObjectLock)
            {
                if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return false; }
                if (IsReserved) return false;

                IsReserved = true;

                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return true;
            }
        }

        private const string ModuleName = nameof(ConnectionPoolMember);
        private bool _isDisposed;
        internal CancellationTokenSource Cts = new CancellationTokenSource();
        internal CancellationToken Ct = default;

        internal bool IsReserved { get; private set; }

        internal ConnectionPool Cp;

        internal TcpClient Client;

        internal DateTime timeWrite;
        internal DateTime timeReceive;
        internal DateTime timePoll;

        internal Guid LocalGuid;
        internal byte[] LocalGuidBytes;

        internal string LocalHost;
        internal int LocalPort;

        internal Guid RemoteGuid;
        internal byte[] RemoteGuidBytes;
        internal string RemoteHost;
        internal int RemotePort;
        internal NetworkStream Stream;
        internal object ObjectLock = new object();


        internal bool HasRemoteGuid(byte[] queryRemoteGuidBytes, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);


            if (_isDisposed)
            {
                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return false;
            }

            lock (ObjectLock)
            {
                if (_isDisposed)
                {
                    Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                    return false;
                }

                var ret = false;

                try
                {
                    var isEmptyRemoteGuidBytes = (RemoteGuidBytes?.Length ?? 0) == 0;
                    var isEmptyQueryRemoteGuidBytes = (queryRemoteGuidBytes?.Length ?? 0) == 0;

                    if (isEmptyRemoteGuidBytes && isEmptyQueryRemoteGuidBytes)
                    {
                        ret = true;
                    } else if (isEmptyRemoteGuidBytes || isEmptyQueryRemoteGuidBytes)
                    {
                        ret = false;
                    }
                    else
                    {
                        ret = RemoteGuidBytes.SequenceEqual(queryRemoteGuidBytes);
                    }
                }
                catch (Exception e)
                {
                    Logging.LogException(e, "", ModuleName);
                }

                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return ret;
            }
        }

        internal bool IsActive(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (_isDisposed)
            {
                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return false;
            }

            lock (ObjectLock)
            {
                // not disposed
                if (_isDisposed)
                {
                    Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                    return false;
                }

                // not cancelled
                if (Ct.IsCancellationRequested)
                {
                    Close(callChain: callChain, lvl: lvl + 1);
                    Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                    return false;
                }

                try
                {
                    if (Cp != null && Cp.PoolCt.IsCancellationRequested)
                    {
                        Close(callChain: callChain, lvl: lvl + 1);
                        Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                        return false;
                    }
                }
                catch (Exception e)
                {
                    Close(callChain: callChain, lvl: lvl + 1);
                    Logging.LogException(e, "", ModuleName);
                    return false;
                }

                // tcp connected
                var pollOk = TcpClientExtra.PollTcpClientConnection(Client);
                timePoll = DateTime.UtcNow;
                
                if (!pollOk)
                {
                    Close(callChain: callChain, lvl: lvl + 1);
                    Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                    return false;
                }

                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return true;
            }
        }

        internal ConnectionPoolMember(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            Ct = Cts.Token;

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        //internal bool IsConnected(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        //{
        //    callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
        //    Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);
        //    if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return false; }
        //    try
        //    {
        //        lock (ObjectLock)
        //        {
        //            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return false; }
        //            if (IsCancelled(callChain: callChain, lvl: lvl + 1)) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return false; }
        //            var pollOk = TcpClientExtra.PollTcpClientConnection(Client);
        //            if (!pollOk) { Close(callChain: callChain, lvl: lvl + 1); }
        //            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        //            return pollOk;
        //        }
        //    }
        //    catch (Exception e)
        //    {
        //        Logging.LogException(e, "", ModuleName);
        //        Close(callChain: callChain, lvl: lvl + 1);
        //        Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return false;
        //    }
        //}

        internal void Close(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);


            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            try
            {
                lock (ObjectLock)
                {
                    if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

                    Logging.LogEvent($@"{Cp?.PoolName ?? "Not pooled"}: Connection pool: Connection closed.", ModuleName);

                    Reserve(callChain: callChain, lvl: lvl + 1);

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
                    LocalGuid = default;
                    LocalGuidBytes = default;
                    RemoteGuid = default;
                    RemoteGuidBytes = default;
                    LocalHost = default;
                    LocalPort = default;
                    RemoteHost = default;
                    RemotePort = default;
                    Cp = default;
                    timeWrite = default;
                    timeReceive = default;
                    timePoll = default;
                    IsReserved = default;
                    _FrameId = default;
                }

                //ObjectLock = default;
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
            }

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }
    }
}
