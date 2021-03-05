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
    public class ConnectionPoolMember
    {
        public ulong _FrameId;
        //public ulong _WriteFrameId;
        //public Queue<(ulong frameId, byte[])> _WriteQueue = new Queue<(ulong frameId, byte[])>();

        public ConnectionPoolMember()
        {

        }

        public async Task<bool> WriteFrameAsync((ulong frameType, string text)[] dataArray, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
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
                    var frameData = new List<byte[]>();

                    foreach (var data in dataArray)
                    {
                        ulong frameId = 0;

                        lock (ObjectLock) { frameId = ++_FrameId; }

                        var bytesText = !string.IsNullOrEmpty(data.text) ? Encoding.UTF8.GetBytes(data.text)
                            : Array.Empty<byte>();
                        var bytesFrameId = BitConverter.GetBytes(frameId);
                        var bytesFrameType = BitConverter.GetBytes(data.frameType);
                        var bytesFrameLength = BitConverter.GetBytes((ulong)bytesText.Length);

                        frameData.Add(_bytesFrameCode);
                        frameData.Add(bytesFrameId);
                        frameData.Add(bytesFrameType);
                        frameData.Add(bytesFrameLength);
                        frameData.Add(bytesText);

                        //var frame = new[] { _bytesFrameCode, bytesFrameId, bytesFrameType, bytesFrameLength, bytesText }.SelectMany(a => a).ToArray();
                        //frameList.Add(frame);
                    }

                    var frameDataBytes = frameData.SelectMany(a => a).ToArray();

                    await Stream.WriteAsync(frameDataBytes, Ct).ConfigureAwait(false);
                    await Stream.FlushAsync(Ct).ConfigureAwait(false);

                    if (frameDataBytes.Length > 0)
                    {
                        timeWrite = DateTime.Now;
                    }

                    Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                    return true;
                }
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
            }


            Close(callChain: callChain, lvl: lvl + 1);
            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
            return default;
        }

        public async Task<byte[]> ReadRawFixedLengthTimeoutAsync(int timeoutSeconds, int frameLength, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            var readTask = Task.Run(async () => await ReadRawFixedLengthAsync(frameLength, callerModuleName, callerMethodName, callerLineNumber, callChain, lvl).ConfigureAwait(false));
            var timeoutTask = Task.Run(async () => await Task.Delay(TimeSpan.FromSeconds(timeoutSeconds)).ConfigureAwait(false));
            try { await Task.WhenAny(readTask, timeoutTask).ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e, "", ModuleName); }

            if (!readTask.IsCompletedSuccessfully)
            {
                Logging.LogEvent("ReadRawFixedLengthTimeoutAsync: Timeout.");
                Close();
                return default;
            }

            return readTask.Result;
        }

        public async Task<byte[]> ReadRawFixedLengthAsync(int frameLength, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
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
                    }
                    else break;
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


        public async Task<(bool readOk, ulong frameId, ulong frameType, int frameLength, byte[] bytesTextIn, string textIn, string[] textInLines)> ReadFrameTimeoutAsync(int timeoutSeconds = 60, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            var readTask = Task.Run(async () => await ReadFrameAsync().ConfigureAwait(false));
            var timeoutTask = Task.Run(async () => await Task.Delay(TimeSpan.FromSeconds(timeoutSeconds)).ConfigureAwait(false));

            try { await Task.WhenAny(readTask, timeoutTask).ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e, "", ModuleName); }

            if (!readTask.IsCompletedSuccessfully)
            {
                Logging.LogEvent("ReadFrameTimeoutAsync: Timeout.");
                Close();
                return default;
            }

            return readTask.Result;
        }

        public async Task<(bool readOk, ulong frameId, ulong frameType, int frameLength, byte[] bytesTextIn, string textIn, string[] textInLines)> ReadFrameAsync(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);


            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
            if (Ct.IsCancellationRequested) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }
            if (Client == null || Stream == null || !Client.Connected || !Stream.CanRead) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default; }

            byte[] _bytesFrameBeginCode = new byte[] { 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5 };

            var headerLen = _bytesFrameBeginCode.Length + (sizeof(ulong) * 3);

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
                else if (header.Length == headerLen)
                {
                    // skip any keep alive data by finding frame begin code sequence
                    for (var i = 0; i <= header.Length - _bytesFrameBeginCode.Length; i++)
                    {
                        if (header.Skip(i).Take(_bytesFrameBeginCode.Length).SequenceEqual(_bytesFrameBeginCode))
                        {
                            if (i > 0)
                            {
                                header = header.Skip(i).ToArray();
                            }

                            break;
                        }
                    }

                    if (header.Length == headerLen) { break; }
                }
            }

            if ((header?.Length ?? 0) != headerLen)
            {
                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return default;
            }



            var frameFrameCode = header.Take(_bytesFrameBeginCode.Length).ToArray();
            if (!_bytesFrameBeginCode.SequenceEqual(frameFrameCode))
            {
                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return default;
            }
            var offset = _bytesFrameBeginCode.Length;

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

                var bytesTextInExt = await ReadRawFixedLengthAsync(frameLength - bytesTextIn.Length, callChain: callChain, lvl: lvl + 1).ConfigureAwait(false);

                if ((bytesTextInExt?.Length ?? 0) > 0)
                {
                    bytesTextIn = bytesTextIn.Concat(bytesTextInExt).ToArray();
                }

                if (Ct.IsCancellationRequested)
                {
                    Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                    return default;
                }

                if ((bytesTextIn?.Length ?? 0) != frameLength)
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

            if ((bytesTextIn?.Length ?? 0) != frameLength)
            {
                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return default;
            }

            var textIn = (bytesTextIn?.Length ?? 0) > 0 ? Encoding.UTF8.GetString(bytesTextIn) : null;
            var textInLines = textIn?.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);


            var ret = Ct.IsCancellationRequested
                ? default
                : (true, frameId, frameType, frameLength, bytesTextIn, textIn, textInLines);

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
            return ret;
        }

        //public void JoinPool(ConnectionPool cp, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
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

        public void Unreserve(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
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

        public bool Reserve(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
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

        public const string ModuleName = nameof(ConnectionPoolMember);
        public bool _isDisposed;
        public CancellationTokenSource Cts = new CancellationTokenSource();
        public CancellationToken Ct = default;

        public bool IsReserved;

        public ConnectionPool Cp;

        public TcpClient Client;
        public string ConnectHost;
        public int ConnectPort;

        public DateTime timeWrite = DateTime.UtcNow;
        public DateTime timeReceive = DateTime.UtcNow;
        public DateTime timePoll = DateTime.UtcNow;

        public Guid LocalGuid;
        public byte[] LocalGuidBytes;

        public string LocalHost;
        public int LocalPort;

        public Guid RemoteGuid;
        public byte[] RemoteGuidBytes;
        public string RemoteHost;
        public int RemotePort;
        public NetworkStream Stream;
        public object ObjectLock = new object();


        public bool HasRemoteGuid(byte[] queryRemoteGuidBytes, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
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
                    }
                    else if (isEmptyRemoteGuidBytes || isEmptyQueryRemoteGuidBytes)
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

        public bool IsActive(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
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

                var now = DateTime.UtcNow;
                var elapsedRead = now - timeReceive;
                var elapsedWrite = now - timeWrite;
                var elapsedPoll = now - timePoll;
                var timeout = TimeSpan.FromSeconds(10);

                if (elapsedPoll >= timeout && elapsedRead >= timeout && elapsedWrite >= timeout)
                {
                    // tcp connected
                    timePoll = now;
                    var pollOk = TcpClientExtra.PollTcpClientConnection(Client);

                    if (!pollOk)
                    {
                        Close(callChain: callChain, lvl: lvl + 1);
                        Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                        return false;
                    }
                }

                Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
                return true;
            }
        }

        public ConnectionPoolMember(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            Ct = Cts.Token;

            Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1);
        }

        //public bool IsConnected(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
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

        public void Close(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);


            if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }

            try
            {
                lock (ObjectLock)
                {
                    if (_isDisposed) { Logging.LogExit(ModuleName, callChain: callChain, lvl: lvl + 1); return; }
                    _isDisposed = true;
                }

                Logging.LogEvent($@"{Cp?.PoolName ?? "Not pooled"}: Connection pool: Connection closed.", ModuleName);

                Cp?.Remove(this, ModuleName, callChain: callChain, lvl: lvl + 1);

                IsReserved = true;
                //Reserve(callChain: callChain, lvl: lvl + 1);

                

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
                ConnectHost = default;
                ConnectPort = default;




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
