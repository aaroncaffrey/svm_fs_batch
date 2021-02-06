using System;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal static class TcpClientExtra
    {
        internal const string ModuleName = nameof(TcpClientExtra);

        //internal static TimeSpan TimeoutConnect = TimeSpan.FromMilliseconds(10);
        //internal static TimeSpan TimeoutReconnect = TimeSpan.FromDays(10);

        ////internal static TimeSpan timeout_check_tcp_state = TimeSpan.FromMilliseconds(1);
        //internal static TimeSpan TimeoutTcpConnectionNoReceiveSinceConnect = TimeSpan.FromMilliseconds(20);
        //internal static TimeSpan TimeoutTcpConnectionNoReceive = TimeSpan.FromMilliseconds(60);
        //internal static TimeSpan TimeoutTcpNeedPing = TimeSpan.FromMilliseconds(20);

        //internal static TimeSpan DelayConnectLoop = TimeSpan.FromMilliseconds(1);
        //internal static TimeSpan DelayReadLoop = TimeSpan.FromMilliseconds(1);

        //internal static TimeSpan DelayWriteLoop = TimeSpan.FromMilliseconds(1);

        ////internal static TimeSpan delay_check_tcp_state = TimeSpan.FromMilliseconds(10);
        //internal static TimeSpan DelayTcpKeepAlive = TimeSpan.FromMilliseconds(10);
        //internal static TimeSpan DelayListenLoop = TimeSpan.FromMilliseconds(1);


        internal static void SetTimeouts(TcpClient client, NetworkStream stream)
        {
            var timeout = 20_000;

            if (client != null)
            {
                client.NoDelay = true;
                client.ReceiveTimeout = timeout;
                client.SendTimeout = timeout;
            }

            if (stream != null)
            {
                stream.ReadTimeout = timeout;
                stream.WriteTimeout = timeout;
            }
        }

        internal static async Task WriteRawFixedLengthAsync(TcpClient client, NetworkStream stream, byte[] data, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return;

            try
            {
                if (client != null && stream != null && client.Connected && stream.CanWrite)
                {
                    await stream.WriteAsync(data, ct).ConfigureAwait(false);
                    await stream.FlushAsync(ct).ConfigureAwait(false);
                }
            }
            catch (Exception e) { Logging.LogException(e, "", ModuleName); }
        }


        internal static void WriteRawFixedLength(TcpClient client, NetworkStream stream, byte[] data)
        {
            try
            {
                if (client != null && stream != null && client.Connected && stream.CanWrite)
                {
                    stream.Write(data);
                    stream.Flush();
                }
            }
            catch (Exception e) { Logging.LogException(e, "", ModuleName); }
        }

        internal static async Task<byte[]> ReadRawFixedLengthAsync(TcpClient client, NetworkStream stream, int length, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            if (length <= 0) return null;

            var bytes = new byte[length];
            var pos = 0;
            //lock (stream)
            //{
            while (!ct.IsCancellationRequested && pos < length && client.Connected && stream.CanRead)
                try
                {
                    var bytesRead = await stream.ReadAsync(bytes, pos, bytes.Length - pos, ct).ConfigureAwait(false);
                    pos += bytesRead;

                    if (bytesRead == 0 && pos < length && !ct.IsCancellationRequested)
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(1), ct).ConfigureAwait(false);

                        var pollOk = TcpClientExtra.PollTcpClientConnection(client);

                        if (!pollOk) break;
                    }
                }
                //catch (OperationCanceledException)
                //{
                //    return null;
                //}
                catch (Exception e)
                {
                    Logging.LogException(e, "", ModuleName);
                    return null;
                }
            //}

            if (pos == 0) return null;
            if (pos != length) return ct.IsCancellationRequested ? default : bytes[..pos];

            return ct.IsCancellationRequested ? default : bytes;
        }

        internal static byte[] ReadRawFixedLength(TcpClient client, NetworkStream stream, int length, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            if (length <= 0) return null;

            var bytes = new byte[length];
            var pos = 0;

            //lock (stream)
            {
                while (!ct.IsCancellationRequested && pos < length && client.Connected && stream.CanRead)
                    try
                    {
                        var bytesRead = stream.Read(bytes, pos, bytes.Length - pos);
                        pos += bytesRead;

                        if (bytesRead == 0 && pos < length) Task.Delay(TimeSpan.FromMilliseconds(1), ct).Wait(ct);
                    }
                    //catch (OperationCanceledException)
                    //{
                    //    return null;
                    //}
                    catch (Exception e)
                    {
                        Logging.LogException(e, "", ModuleName);
                        return null;
                    }
            }

            if (pos == 0) return null;
            if (pos != length) return ct.IsCancellationRequested ? default : bytes[..pos];

            return ct.IsCancellationRequested ? default : bytes;
        }

        internal static async Task<(bool challenge_correct, Guid remote_guid)> ChallengeRequestAsync(TcpClient client, NetworkStream stream, byte[] localClientGuid = null, byte[] remoteClientGuidExpected = null, CancellationToken ct = default)
        {
            //#if DEBUG
            //            Logging.LogCall( ModuleName);
            //#endif

            //const bool logOkEvent = false;
            //const bool logErrEvent = false;

            // send challenge request
            if (ct.IsCancellationRequested) return default;

            using var localCts = new CancellationTokenSource();
            var localCt = localCts.Token;

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, localCt);//, externalCancellationToken);
            var linkedCt = linkedCts.Token;

            var secretKey = nameof(TcpClientExtra) + Program.ProgramArgs.ServerGuid;
            var secret = Encoding.UTF8.GetBytes(Convert.ToBase64String(Encoding.UTF8.GetBytes(secretKey)));
            const int guidBytesLen = 128 / 8; //Guid.Empty.ToByteArray().Length;
            const int shaBytesLen = 256 / 8; //Sha.ComputeHash(new byte[1]).Length;

            var task = Task.Run(async () =>
                {
                    if (linkedCt.IsCancellationRequested)
                        //if (logErrEvent) Logging.LogEvent(, "challenge cancelled before it ran");

                        return default;

                    var challengeCorrect = false;

                    var remoteClientGuidReceived = Guid.Empty;
                    byte[] remoteClientGuidReceivedBytes = null;

                    // 1. send local challenge code
                    // 2. read remote challenge code
                    // 3. both client and server have a challenge code to solve
                    // 4. reply to remote challenge
                    // 5. read challenge response


                    try
                    {
                        // 1. send local challenge code
                        var challengeSent = Guid.NewGuid().ToByteArray();

                        await WriteRawFixedLengthAsync(client, stream, challengeSent, linkedCt).ConfigureAwait(false);

                        //if (logOkEvent) Logging.LogEvent(, $"local challenge has been sent {string.Join("-", challengeSent.Select(a => $"{a:000}"))}");


                        // 2. read remote challenge code
                        var challengeReceived = await ReadRawFixedLengthAsync(client, stream, guidBytesLen, linkedCt).ConfigureAwait(false);
                        if (challengeReceived == null || challengeReceived.Length != guidBytesLen)
                            //if (logErrEvent) Logging.LogEvent(, $"local and remote challenge lengths are different (local: {(challengeSent?.Length ?? 0)}; remote: {(challengeReceived?.Length ?? 0)}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");

                            return default;

                        //if (logOkEvent) Logging.LogEvent(, $"remote challenge has been read {string.Join("-", challengeReceived.Select(a => $"{a:000}").ToArray())}");

                        // 3. solve challenges

                        // local_hash|remote_hash|secret
                        using var sha = SHA256.Create();
                        var challengeAnswerSent = sha.ComputeHash(new[] { challengeSent, challengeReceived, secret }.SelectMany(a => a).ToArray());
                        var challengeAnswerExpected = sha.ComputeHash(new[] { challengeReceived, challengeSent, secret }.SelectMany(a => a).ToArray());
                        if (challengeAnswerExpected.Length != shaBytesLen || challengeAnswerSent.Length != shaBytesLen)
                            //if (logErrEvent) Logging.LogEvent(, "challenge problem");

                            return default;

                        //if (logOkEvent) Logging.LogEvent(, "solved challenges");

                        // 4. reply to remote challenge
                        await WriteRawFixedLengthAsync(client, stream, challengeAnswerSent, linkedCt).ConfigureAwait(false);
                        //if (logOkEvent) Logging.LogEvent(, $"replied solution to challenge {string.Join("-", challengeAnswerSent.Select(a => $"{a:000}").ToArray())}");

                        // 5. read challenge response
                        var challengeAnswerReceived = await ReadRawFixedLengthAsync(client, stream, shaBytesLen, linkedCt).ConfigureAwait(false);
                        if (challengeAnswerReceived == null || challengeAnswerReceived.Length != challengeAnswerExpected.Length)
                            //if (logErrEvent) Logging.LogEvent(, $"local challenge response was wrong length (expected: {(challengeAnswerExpected?.Length ?? 0)}; received: {(challengeAnswerReceived?.Length ?? 0)}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");
                            return default;

                        //if (logOkEvent) Logging.LogEvent(, $"local challenge response received {string.Join("-", challengeAnswerReceived.Select(a => $"{a:000}").ToArray())} - {string.Join("-", challengeAnswerExpected.Select(a => $"{a:000}").ToArray())}");
                        var challengeAnswerCorrect = challengeAnswerReceived.SequenceEqual(challengeAnswerExpected);
                        // shaRemoteChallengeAnswer?

                        if (!challengeAnswerCorrect)
                            //if (logErrEvent) Logging.LogEvent(, $"local challenge response was wrong; (ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");
                            return default;
                        //if (logOkEvent) Logging.LogEvent(, "local challenge response was correct");

                        // 6. send local guid to identify this connection
                        if (localClientGuid != null) await WriteRawFixedLengthAsync(client, stream, localClientGuid, linkedCt).ConfigureAwait(false);

                        // 7. read remote connection guid
                        remoteClientGuidReceivedBytes = await ReadRawFixedLengthAsync(client, stream, guidBytesLen, linkedCt).ConfigureAwait(false);
                        if (remoteClientGuidReceivedBytes == null || remoteClientGuidReceivedBytes.Length != guidBytesLen)
                            //if (logErrEvent) Logging.LogEvent(, $"remote guid length is incorrect ({(remoteClientGuidReceivedBytes?.Length ?? 0)}); ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");

                            return default;


                        var remoteGuidCorrect = remoteClientGuidExpected == null || remoteClientGuidExpected.Length == 0 || remoteClientGuidExpected.SequenceEqual(remoteClientGuidReceivedBytes);
                        if (!remoteGuidCorrect)
                            //if (logErrEvent) Logging.LogEvent(, $"remote connection guid received - {(remoteGuidCorrect ? "correct" : "wrong")}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested}");
                            return default;
                        //if (logOkEvent) Logging.LogEvent(, $"remote connection guid received - {(remoteGuidCorrect ? "correct" : "wrong")}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested}");

                        remoteClientGuidReceived = new Guid(remoteClientGuidReceivedBytes);
                        challengeCorrect = true;

                        //if (logOkEvent) Logging.LogEvent(, $"challenge successful");
                    }
                    catch (Exception e) { Logging.LogException(e); }

                    return linkedCt.IsCancellationRequested ? default : (challengeCorrect, remoteClientGuidReceived);
                },
                linkedCt);


            //var taskDelay = Task.Run(async () => { if (!linkedCt.IsCancellationRequested) try { await Task.Delay(TimeSpan.FromSeconds(10), linkedCt).ConfigureAwait(false); } catch (Exception) { } }, linkedCt);

            await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(10), localCt)).ConfigureAwait(false);

            //linkedCts.Dispose();
            localCts.Cancel();
            //cts.Dispose();

            if (task.IsCompletedSuccessfully)
            {
                if (task.Result != default) return ct.IsCancellationRequested ? default : task.Result;
                Logging.LogEvent("connection challenge wrong", ModuleName);
            }
            else { Logging.LogEvent("connection challenge timed out!", ModuleName); }

            return default;
        }

        internal static (bool challenge_correct, Guid remote_guid) ChallengeRequest(TcpClient client, NetworkStream stream, byte[] localClientGuid = null, byte[] remoteClientGuidExpected = null, CancellationToken ct = default)
        {
            //#if DEBUG
            //            Logging.LogCall( ModuleName);
            //#endif

            //const bool logOkEvent = false;
            //const bool logErrEvent = false;

            // send challenge request
            if (ct.IsCancellationRequested) return default;

            //using var cts = new CancellationTokenSource();
            //var ct = cts.Token;

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);//, ct);
            var linkedCt = linkedCts.Token;

            var secretKey = nameof(TcpClientExtra) + Program.ProgramArgs.ServerGuid;
            var secret = Encoding.UTF8.GetBytes(Convert.ToBase64String(Encoding.UTF8.GetBytes(secretKey)));
            const int guidBytesLen = 128 / 8; //Guid.Empty.ToByteArray().Length;
            const int shaBytesLen = 256 / 8; //Sha.ComputeHash(new byte[1]).Length;


            if (linkedCt.IsCancellationRequested)
                //if (logErrEvent) Logging.LogEvent(, "challenge cancelled before it ran");

                return default;

            var challengeCorrect = false;
            var remoteClientGuidReceived = Guid.Empty;
            byte[] remoteClientGuidReceivedBytes = null;

            // 1. send local challenge code
            // 2. read remote challenge code
            // 3. both client and server have a challenge code to solve
            // 4. reply to remote challenge
            // 5. read challenge response


            try
            {
                // 1. send local challenge code
                var challengeSent = Guid.NewGuid().ToByteArray();

                WriteRawFixedLength(client, stream, challengeSent);

                //if (logOkEvent) Logging.LogEvent(, $"local challenge has been sent {string.Join("-", challengeSent.Select(a => $"{a:000}"))}");


                // 2. read remote challenge code
                var challengeReceived = ReadRawFixedLength(client, stream, guidBytesLen, linkedCt);
                if (challengeReceived == null || challengeReceived.Length != guidBytesLen)
                    //if (logErrEvent) Logging.LogEvent(, $"local and remote challenge lengths are different (local: {(challengeSent?.Length ?? 0)}; remote: {(challengeReceived?.Length ?? 0)}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");

                    return default;

                //if (logOkEvent) Logging.LogEvent(, $"remote challenge has been read {string.Join("-", challengeReceived.Select(a => $"{a:000}").ToArray())}");

                // 3. solve challenges

                // local_hash|remote_hash|secret
                using var sha = SHA256.Create();
                var challengeAnswerSent = sha.ComputeHash(new[] { challengeSent, challengeReceived, secret }.SelectMany(a => a).ToArray());
                var challengeAnswerExpected = sha.ComputeHash(new[] { challengeReceived, challengeSent, secret }.SelectMany(a => a).ToArray());
                if (challengeAnswerExpected.Length != shaBytesLen || challengeAnswerSent.Length != shaBytesLen)
                    //if (logErrEvent) Logging.LogEvent(, "challenge problem");

                    return default;

                //if (logOkEvent) Logging.LogEvent(, "solved challenges");

                // 4. reply to remote challenge
                WriteRawFixedLength(client, stream, challengeAnswerSent);
                //if (logOkEvent) Logging.LogEvent(, $"replied solution to challenge {string.Join("-", challengeAnswerSent.Select(a => $"{a:000}").ToArray())}");

                // 5. read challenge response
                var challengeAnswerReceived = ReadRawFixedLength(client, stream, shaBytesLen, linkedCt);
                if (challengeAnswerReceived == null || challengeAnswerReceived.Length != challengeAnswerExpected.Length)
                    //if (logErrEvent) Logging.LogEvent(, $"local challenge response was wrong length (expected: {(challengeAnswerExpected?.Length ?? 0)}; received: {(challengeAnswerReceived?.Length ?? 0)}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");
                    return default;

                //if (logOkEvent) Logging.LogEvent(, $"local challenge response received {string.Join("-", challengeAnswerReceived.Select(a => $"{a:000}").ToArray())} - {string.Join("-", challengeAnswerExpected.Select(a => $"{a:000}").ToArray())}");
                var challengeAnswerCorrect = challengeAnswerReceived.SequenceEqual(challengeAnswerExpected);
                // shaRemoteChallengeAnswer?

                if (!challengeAnswerCorrect)
                    //if (logErrEvent) Logging.LogEvent(, $"local challenge response was wrong; (ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");
                    return default;
                //if (logOkEvent) Logging.LogEvent(, "local challenge response was correct");

                // 6. send local guid to identify this connection
                if (localClientGuid != null) WriteRawFixedLength(client, stream, localClientGuid);

                // 7. read remote connection guid
                remoteClientGuidReceivedBytes = ReadRawFixedLength(client, stream, guidBytesLen, linkedCt);
                if (remoteClientGuidReceivedBytes == null || remoteClientGuidReceivedBytes.Length != guidBytesLen)
                    //if (logErrEvent) Logging.LogEvent(, $"remote guid length is incorrect ({(remoteClientGuidReceivedBytes?.Length ?? 0)}); ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");

                    return default;

                var remoteGuidCorrect = remoteClientGuidExpected == null || remoteClientGuidExpected.Length == 0 || remoteClientGuidExpected.SequenceEqual(remoteClientGuidReceivedBytes);
                if (!remoteGuidCorrect)
                    //if (logErrEvent) Logging.LogEvent(, $"remote connection guid received - {(remoteGuidCorrect ? "correct" : "wrong")}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested}");
                    return default;
                //if (logOkEvent) Logging.LogEvent(, $"remote connection guid received - {(remoteGuidCorrect ? "correct" : "wrong")}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested}");

                remoteClientGuidReceived = new Guid(remoteClientGuidReceivedBytes);

                challengeCorrect = true;

                //if (logOkEvent) Logging.LogEvent(, $"challenge successful");
            }
            catch (Exception e) { Logging.LogException(e); }

            return ct.IsCancellationRequested ? default : (challengeCorrect, remoteClientGuidReceived);


            //var task_delay = Task.Run(() => { if (!ct.IsCancellationRequested) try { Task.Delay(TimeSpan.FromMilliseconds(1), ct).Wait(ct); } catch (Exception) { } }, ct:ct);

            //Task.WhenAny(task, task_delay).Wait(externalCancellationToken);

            //cts.Cancel();
            //cts.Dispose();

            //if (task.IsCompletedSuccessfully)
            //{
            //    if (task.Result != default)
            //    {
            //        return ct.IsCancellationRequested ? default :task.Result;
            //    }
            //    else
            //    {
            //        Logging.LogEvent(, "connection challenge wrong");
            //    }
            //}
            //else
            //{
            //    Logging.LogEvent(, "connection challenge timed out!");
            //}

            //return default;
        }

        private static readonly byte[] _bytesFrameCode = new byte[] {0, 0, 0, 0,     1, 1, 1, 1,      2, 2, 2, 2,     3, 3, 3, 3,     4, 4, 4, 4,  5, 5 ,5 ,5 };
        
            internal static async Task<bool> WriteFrameAsync(ulong frameId, ulong frameType, string text, TcpClient client, NetworkStream stream, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            try
            {
                if (client != null && stream != null && client.Connected && stream.CanWrite)
                {

                    var bytesText = !string.IsNullOrEmpty(text)
                        ? Encoding.UTF8.GetBytes(text)
                        : Array.Empty<byte>();
                    var bytesFrameId = BitConverter.GetBytes(frameId);
                    var bytesFrameType = BitConverter.GetBytes(frameType);
                    var bytesFrameLength = BitConverter.GetBytes((ulong)bytesText.Length);
                    var frame = new[] { _bytesFrameCode, bytesFrameId, bytesFrameType, bytesFrameLength, bytesText/*, _bytesFrameCode*/ }.SelectMany(a => a).ToArray();

                    await stream.WriteAsync(frame, ct).ConfigureAwait(false);
                    await stream.FlushAsync(ct).ConfigureAwait(false);
                    return true;
                }

                return false;
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
                return false;
            }
        }


        internal static bool WriteFrame(ulong frameId, ulong frameType, string text, TcpClient client, NetworkStream stream, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            try
            {
                if (client != null && stream != null && client.Connected && stream.CanWrite)
                {
                    var bytesText = !string.IsNullOrEmpty(text)
                        ? Encoding.UTF8.GetBytes(text)
                        : Array.Empty<byte>();
                    var bytesFrameId = BitConverter.GetBytes(frameId);
                    var bytesFrameType = BitConverter.GetBytes(frameType);
                    var bytesFrameLength = BitConverter.GetBytes((ulong)bytesText.Length);
                    var frame = new[] { bytesFrameId, bytesFrameType, bytesFrameLength, bytesText }.SelectMany(a => a).ToArray();

                    stream.Write(frame);
                    stream.Flush();
                    return true;
                }

                return default;
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
                return default;
            }
        }

        internal static async Task<(bool readOk, ulong frameId, ulong frameType, int frameLength, byte[] bytesTextIn, string textIn, string[] textInLines)> ReadFrameAsync(TcpClient client, NetworkStream stream, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;
            if (client == null || stream == null || !client.Connected || !stream.CanRead) return default;

            var headerLen = _bytesFrameCode.Length + (sizeof(ulong) * 3);

            byte[] header = Array.Empty<byte>();

            while (!ct.IsCancellationRequested && header.Length != headerLen)
            {
                var headerExt = await ReadRawFixedLengthAsync(client, stream, headerLen - header.Length, ct).ConfigureAwait(false);
                if (headerExt != null && headerExt.Length > 0) header = header.Concat(headerExt).ToArray();

                if (ct.IsCancellationRequested) return default;

                if (header.Length != headerLen)
                {
                    var pollOk = TcpClientExtra.PollTcpClientConnection(client);

                    if (!pollOk) return default;
                } else break;

            }
            if (header == null || header.Length != headerLen) return default;


            
            var frameFrameCode = header.Take(_bytesFrameCode.Length).ToArray();
            if (!_bytesFrameCode.SequenceEqual(frameFrameCode))
            {
                return default;
            }
            var offset = _bytesFrameCode.Length;
            
            var frameId = BitConverter.ToUInt64(header,  offset);
            offset += sizeof(ulong);
            
            var frameType = BitConverter.ToUInt64(header, offset);
            offset += sizeof(ulong);
            
            var frameLength = (int) BitConverter.ToUInt64(header, offset);
            offset += sizeof(ulong);

            var bytesTextIn = Array.Empty<byte>();

            while (!ct.IsCancellationRequested && bytesTextIn.Length != frameLength)
            {
                var bytesTextInExt = frameLength > 0 ? await ReadRawFixedLengthAsync(client, stream, frameLength - bytesTextIn.Length, ct).ConfigureAwait(false) : default;

                if (bytesTextInExt != null && bytesTextInExt.Length > 0) bytesTextIn = bytesTextIn.Concat(bytesTextInExt).ToArray();

                if (ct.IsCancellationRequested) return default;

                if (bytesTextIn.Length != frameLength)
                {
                    var pollOk = TcpClientExtra.PollTcpClientConnection(client);

                    if (!pollOk) return default;
                }
                else break;
            }

            if (bytesTextIn == null || bytesTextIn.Length != frameLength) return default;
            var textIn = bytesTextIn != null && bytesTextIn.Length > 0 ? Encoding.UTF8.GetString(bytesTextIn) : null;
            var textInLines = textIn?.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);


            return ct.IsCancellationRequested
                ? default
                : (true, frameId, frameType, frameLength, bytesTextIn, textIn, textInLines);


        }

        //internal static (bool readOk, ulong frameId, ulong frameType, int frameLength, byte[] bytesTextIn, string textIn, string[] textInLines) ReadFrame(TcpClient client, NetworkStream stream, CancellationToken ct)
        //{
        //    if (ct.IsCancellationRequested) return default;

        //    if (client != null && stream != null && client.Connected && stream.CanRead)
        //    {
        //        var headerLen = sizeof(ulong) * 3;
        //        var header = ReadRawFixedLength(client, stream, headerLen, ct);
        //        //lastRead = DateTime.UtcNow;
        //        if (header == null || header.Length != headerLen) return default;

        //        var offset = 0;
        //        var frameId = BitConverter.ToUInt64(header, sizeof(ulong) * offset++);
        //        var frameType = BitConverter.ToUInt64(header, sizeof(ulong) * offset++);
        //        var frameLength = (int)BitConverter.ToUInt64(header, sizeof(ulong) * offset++);
        //        var bytesTextIn = ReadRawFixedLength(client, stream, frameLength, ct);

        //        if ((bytesTextIn?.Length ?? 0) != frameLength) return default;


        //        var textIn = bytesTextIn != null
        //            ? Encoding.UTF8.GetString(bytesTextIn)
        //            : null;
        //        var textInLines = textIn?.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
        //        if (bytesTextIn == null || bytesTextIn.Length != frameLength) return default;

        //        return ct.IsCancellationRequested ? default : (true, frameId, frameType, frameLength, bytesTextIn, textIn, textInLines);
        //    }

        //    return default;
        //}

        //internal static async Task CloseAsync(ulong frameId, TcpClient client, NetworkStream stream, CancellationToken ct)
        //{
        //    if (ct.IsCancellationRequested) return;

        //    if (stream != null)
        //        try { await WriteFrameAsync(frameId, (ulong) PayloadFrameTypes.FrameTypeClose, "close", client, stream, ct).ConfigureAwait(false); }
        //        catch (Exception e) { Logging.LogException(e, "", ModuleName); }

        //    try { stream?.Close(); }
        //    catch (Exception e) { Logging.LogException(e, "", ModuleName); }

        //    try { client?.Close(); }
        //    catch (Exception e) { Logging.LogException(e, "", ModuleName); }
        //}

        //internal static void Close(ulong frameId, TcpClient client, NetworkStream stream, CancellationToken ct)
        //{
        //    if (stream != null)
        //        try { WriteFrame(frameId, (ulong) PayloadFrameTypes.FrameTypeClose, "close", client, stream, ct); }
        //        catch (Exception e) { Logging.LogException(e, "", ModuleName); }

        //    try { stream?.Close(); }
        //    catch (Exception e) { Logging.LogException(e, "", ModuleName); }

        //    try { client?.Close(); }
        //    catch (Exception e) { Logging.LogException(e, "", ModuleName); }
        //}

        internal static (IPAddress localAddress, int localPort, IPAddress remoteAddress, int remotePort) ReadTcpClientRemoteAddress(TcpClient client)
        {
            return ReadTcpClientRemoteAddress(client.Client);
        }

        internal static (IPAddress localAddress, int localPort, IPAddress remoteAddress, int remotePort) ReadTcpClientRemoteAddress(TcpListener server)
        {
            return ReadTcpClientRemoteAddress(server.Server);
        }

        internal static (IPAddress localAddress, int localPort, IPAddress remoteAddress, int remotePort) ReadTcpClientRemoteAddress(Socket client)
        {
            try
            {
                var localAddress = ((IPEndPoint)client?.LocalEndPoint)?.Address;
                var localPort = ((IPEndPoint)client?.LocalEndPoint)?.Port ?? default;
                var remoteAddress = ((IPEndPoint)client?.RemoteEndPoint)?.Address;
                var remotePort = ((IPEndPoint)client?.RemoteEndPoint)?.Port ?? default;
                return (localAddress, localPort, remoteAddress, remotePort);
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
                return default;
            }
        }

        internal static bool PollTcpClientConnection(TcpClient client)
        {
            var connected = PollTcpClientConnection2(client);

            //if (!connected)
            //{
            //    try { stream?.Close(); } catch (Exception) { }
            //    try { client?.Close(); } catch (Exception) { }
            //}

            return connected;
        }

        private static bool PollTcpClientConnection2(TcpClient client)
        {
            if (client == null || client.Client == null || !client.Connected) return false;
            var tcpState = ReadTcpState(client);
            if (tcpState == TcpState.Established) return true;
            if (tcpState != TcpState.Unknown) return false;
            var socketConnected = PollSocketConnection(client.Client);


            return socketConnected;
        }

        internal static TcpState ReadTcpState(TcpClient tcpClient)
        {
            try
            {
                if (tcpClient == null || tcpClient.Client == null || !tcpClient.Connected) return TcpState.Closed;
                TcpConnectionInformation[] states = null;
                lock (tcpClient)
                {
                    try { states = IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpConnections().Where(x => x.LocalEndPoint.Equals(tcpClient.Client.LocalEndPoint) && x.RemoteEndPoint.Equals(tcpClient.Client.RemoteEndPoint)).ToArray(); }
                    catch (Exception e)
                    {
                        Logging.LogException(e, "", ModuleName);

                        return TcpState.Closed;
                    }
                }

                if (states != null && (states.Length == 1 || states.Select(a => a.State).Distinct().Count() == 1)) return states[0].State;

                return TcpState.Unknown;
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);

                return TcpState.Closed;
            }
        }


        internal static void KeepAlive(Socket socket)
        {
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 5); // The number of seconds a TCP connection will wait for a keepalive response before sending another keepalive probe.
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 5); // The number of seconds a TCP connection will wait for a keepalive response before sending another keepalive probe.
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, 16); // The number of TCP keep alive probes that will be sent before the connection is terminated.
        }

        internal static bool PollSocketConnection(Socket clientSocket)
        {
            if (clientSocket == null) return false;

            lock (clientSocket)
            {
                var blockingState = false;
                try
                {
                    blockingState = clientSocket.Blocking;
                    clientSocket.Blocking = false;
                    clientSocket.Send(new byte[1], 0, 0);


                    return true;
                }
                catch (SocketException e)
                {
                    Logging.LogException(e, "", ModuleName);
                    //const int ewouldblock = 3406;
                    //const int wsaewouldblock = 10035;
                    //if (e.NativeErrorCode.Equals(wsaewouldblock))
                    if (e.SocketErrorCode == SocketError.WouldBlock) return true;
                    else return false;
                }
                catch (ObjectDisposedException e)
                {
                    Logging.LogException(e, "", ModuleName);
                    return false;
                }
                catch (Exception e)
                {
                    Logging.LogException(e, "", ModuleName);
                    return false;
                }
                finally
                {
                    try { clientSocket.Blocking = blockingState; }
                    catch (Exception e) { Logging.LogException(e, "", ModuleName); }
                }
            }
        }

        internal enum PayloadFrameTypes : ulong
        {
            FrameTypePing, FrameTypePingAcknowledge, FrameTypeRpcRequest1,
            FrameTypeRpcAcknowledge1, FrameTypeBreak, FrameTypeBreakAcknowledge,
            FrameTypeDataRpcResponse1, FrameTypeDataRpcResponseAcknowledge1, FrameTypeClose,
            FrameTypeCloseAcknowledge
        }
    }
}