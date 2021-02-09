using System;
using System.Diagnostics;
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
            Logging.LogCall(ModuleName);

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

            Logging.LogExit(ModuleName);
        }

        internal static async Task WriteRawFixedLengthAsync(TcpClient client, NetworkStream stream, byte[] data, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            try
            {
                if (client != null && stream != null && client.Connected && stream.CanWrite)
                {
                    await stream.WriteAsync(data, ct).ConfigureAwait(false);
                    await stream.FlushAsync(ct).ConfigureAwait(false);
                }
            }
            catch (Exception e) { Logging.LogException(e, "", ModuleName); }

            Logging.LogExit(ModuleName);
        }


        internal static void WriteRawFixedLength(TcpClient client, NetworkStream stream, byte[] data)
        {
            Logging.LogCall(ModuleName);

            try
            {
                if (client != null && stream != null && client.Connected && stream.CanWrite)
                {
                    stream.Write(data);
                    stream.Flush();
                }
            }
            catch (Exception e) { Logging.LogException(e, "", ModuleName); }

            Logging.LogExit(ModuleName);
        }

        internal static async Task<byte[]> ReadRawFixedLengthAsync(TcpClient client, NetworkStream stream, int length, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            if (length <= 0) { Logging.LogExit(ModuleName);  return default; }

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
                //    Logging.LogExit(ModuleName); return null;
                //}
                catch (Exception e)
                {
                    Logging.LogException(e, "", ModuleName);
                    Logging.LogExit(ModuleName);
                    return default;
                }
            //}

            if (pos == 0) { Logging.LogExit(ModuleName);  return default; }
            if (pos != length) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default : bytes[..pos]; }

            Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default : bytes;
        }

        //internal static byte[] ReadRawFixedLength(TcpClient client, NetworkStream stream, int length, CancellationToken ct)
        //{
        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }
        //
        //    if (length <= 0) { Logging.LogExit(ModuleName);  return null; }
        //
        //    var bytes = new byte[length];
        //    var pos = 0;
        //
        //    //lock (stream)
        //    {
        //        while (!ct.IsCancellationRequested && pos < length && client.Connected && stream.CanRead)
        //            try
        //            {
        //                var bytesRead = stream.Read(bytes, pos, bytes.Length - pos);
        //                pos += bytesRead;
        //
        //                if (bytesRead == 0 && pos < length) Task.Delay(TimeSpan.FromMilliseconds(1), ct).Wait(ct);
        //            }
        //            //catch (OperationCanceledException)
        //            //{
        //            //    Logging.LogExit(ModuleName); return null;
        //            //}
        //            catch (Exception e)
        //            {
        //                Logging.LogException(e, "", ModuleName);
        //                Logging.LogExit(ModuleName); return null;
        //            }
        //    }
        //
        //    if (pos == 0) { Logging.LogExit(ModuleName);  return null; }
        //    if (pos != length) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default : bytes[..pos];
        //
        //    Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default : bytes;
        //}

        internal static async Task<(bool challenge_correct, Guid remote_guid)> ChallengeRequestAsync(bool isServer, TcpClient client, NetworkStream stream, byte[] localClientGuidBytes = null, byte[] remoteClientGuidBytesExpected = null, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            if (localClientGuidBytes == null || localClientGuidBytes.Length == 0) throw new ArgumentOutOfRangeException(nameof(localClientGuidBytes));
            if (!isServer && (remoteClientGuidBytesExpected == null || remoteClientGuidBytesExpected.Length == 0 || remoteClientGuidBytesExpected.All(a => a == 0))) throw new ArgumentOutOfRangeException(nameof(remoteClientGuidBytesExpected));

            TcpClientExtra.KeepAlive(client.Client);
            TcpClientExtra.SetTimeouts(client, stream);
            //try{await stream.WriteAsync(new byte[1], 0, 0, ct).ConfigureAwait(false);}catch(Exception){}

            var pollOk = TcpClientExtra.PollTcpClientConnection(client);
            
            if (ct.IsCancellationRequested || !pollOk)
            {
                try { stream?.Close(); } catch (Exception) { /*Logging.LogException( e, "", ModuleName); */ }
                try { client?.Close(); } catch (Exception) { /*Logging.LogException( e, "", ModuleName);*/ }

                Logging.LogExit(ModuleName);
                return default;
            }

            using var localCts = new CancellationTokenSource();
            var localCt = localCts.Token;

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, localCt);//, externalCancellationToken);
            var linkedCt = linkedCts.Token;

            var protocolName = "232e91fb-2804-4ef4-83a6-5ff45a6b0f95";
            var secretKey = string.Join("_", protocolName, Program.ProgramArgs.ServerGuid);
            var secret = Encoding.UTF8.GetBytes(Convert.ToBase64String(Encoding.UTF8.GetBytes(secretKey)));
            const int guidBytesLen = 128 / 8; //Guid.Empty.ToByteArray().Length;
            const int shaBytesLen = 256 / 8; //Sha.ComputeHash(new byte[1]).Length;

            var task = Task.Run(async () =>
                {
                    // 1. send local challenge code
                    // 2. read remote challenge code
                    // 3. both client and server have a challenge code to solve
                    // 4. reply to remote challenge
                    // 5. read challenge response

                    if (linkedCt.IsCancellationRequested) { return default; }
                    var challengeCorrect = false;
                    var remoteClientGuidReceived = Guid.Empty;
                    byte[] remoteClientGuidReceivedBytes = null;
                    
                    try
                    {
                        // 1. send local challenge code
                        var challengeSent = Guid.NewGuid().ToByteArray();
                        await WriteRawFixedLengthAsync(client, stream, challengeSent, linkedCt).ConfigureAwait(false);
                        //if (logOkEvent) Logging.LogEvent(, $"local challenge has been sent {string.Join("-", challengeSent.Select(a => $"{a:000}"))}");
                        
                        // 2. read remote challenge code
                        var challengeReceived = await ReadRawFixedLengthAsync(client, stream, guidBytesLen, linkedCt).ConfigureAwait(false);
                        if (challengeReceived == null || challengeReceived.Length != guidBytesLen) {  return default; }
                        //if (logErrEvent) Logging.LogEvent(, $"local and remote challenge lengths are different (local: {(challengeSent?.Length ?? 0)}; remote: {(challengeReceived?.Length ?? 0)}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");
                        //if (logOkEvent) Logging.LogEvent(, $"remote challenge has been read {string.Join("-", challengeReceived.Select(a => $"{a:000}").ToArray())}");

                        // 3. solve challenges
                        using var sha = SHA256.Create();
                        var challengeAnswerSent = sha.ComputeHash(new[] { challengeSent, challengeReceived, secret }.SelectMany(a => a).ToArray());
                        var challengeAnswerExpected = sha.ComputeHash(new[] { challengeReceived, challengeSent, secret }.SelectMany(a => a).ToArray());
                        if (challengeAnswerExpected.Length != shaBytesLen || challengeAnswerSent.Length != shaBytesLen) { return default; }
                        //if (logErrEvent) Logging.LogEvent(, "challenge problem");
                        //if (logOkEvent) Logging.LogEvent(, "solved challenges");

                        // 4. reply to remote challenge
                        await WriteRawFixedLengthAsync(client, stream, challengeAnswerSent, linkedCt).ConfigureAwait(false);
                        //if (logOkEvent) Logging.LogEvent(, $"replied solution to challenge {string.Join("-", challengeAnswerSent.Select(a => $"{a:000}").ToArray())}");

                        // 5. read challenge response
                        var challengeAnswerReceived = await ReadRawFixedLengthAsync(client, stream, shaBytesLen, linkedCt).ConfigureAwait(false);
                        if (challengeAnswerReceived == null || challengeAnswerReceived.Length != challengeAnswerExpected.Length) {  return default; }
                        //if (logErrEvent) Logging.LogEvent(, $"local challenge response was wrong length (expected: {(challengeAnswerExpected?.Length ?? 0)}; received: {(challengeAnswerReceived?.Length ?? 0)}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");
                        //if (logOkEvent) Logging.LogEvent(, $"local challenge response received {string.Join("-", challengeAnswerReceived.Select(a => $"{a:000}").ToArray())} - {string.Join("-", challengeAnswerExpected.Select(a => $"{a:000}").ToArray())}");
                        var challengeAnswerCorrect = challengeAnswerReceived.SequenceEqual(challengeAnswerExpected);
                        if (!challengeAnswerCorrect) { return default; }
                        //if (logErrEvent) Logging.LogEvent(, $"local challenge response was wrong; (ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");
                        //if (logOkEvent) Logging.LogEvent(, "local challenge response was correct");

                        // 6. send local guid to identify this connection
                        if (localClientGuidBytes != null) await WriteRawFixedLengthAsync(client, stream, localClientGuidBytes, linkedCt).ConfigureAwait(false);

                        // 7. read and compare remote connection guid
                        remoteClientGuidReceivedBytes = await ReadRawFixedLengthAsync(client, stream, guidBytesLen, linkedCt).ConfigureAwait(false);
                        if (remoteClientGuidReceivedBytes == null || remoteClientGuidReceivedBytes.Length != guidBytesLen || remoteClientGuidReceivedBytes.All(a=>a==0)) {  return default; }
                        //if (logErrEvent) Logging.LogEvent(, $"remote guid length is incorrect ({(remoteClientGuidReceivedBytes?.Length ?? 0)}); ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");
                        var remoteGuidCorrect = (remoteClientGuidBytesExpected == null || remoteClientGuidBytesExpected.Length == 0) ||  remoteClientGuidBytesExpected.SequenceEqual(remoteClientGuidReceivedBytes);
                        if (!remoteGuidCorrect) { return default; }
                        //if (logErrEvent) Logging.LogEvent(, $"remote connection guid received - {(remoteGuidCorrect ? "correct" : "wrong")}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested}");
                        //if (logOkEvent) Logging.LogEvent(, $"remote connection guid received - {(remoteGuidCorrect ? "correct" : "wrong")}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested}");

                        remoteClientGuidReceived = new Guid(remoteClientGuidReceivedBytes);

                        challengeCorrect = true;

                        //if (logOkEvent) Logging.LogEvent(, $"challenge successful");
                    }
                    catch (Exception e) { Logging.LogException(e); }

                    return linkedCt.IsCancellationRequested ? default : (challengeCorrect, remoteClientGuidReceived);
                },
                linkedCt);

            try{await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(10), linkedCt)).ConfigureAwait(false);}catch (Exception e) { Logging.LogException(e, "", ModuleName); }
            try{localCts.Cancel();}catch (Exception e){ Logging.LogException(e, "", ModuleName); }

            var ret = !ct.IsCancellationRequested && task != null && task.IsCompletedSuccessfully && task.Result != default
                ? task.Result
                : default;

            if (!ret.challengeCorrect)
            {
                try { stream?.Close(); } catch (Exception) { }
                try { client?.Close(); } catch (Exception) { }
                //Logging.LogEvent("Challenge failed 2.");
                Logging.LogExit(ModuleName);
                return default;
            }

            Logging.LogExit(ModuleName);
            return ret;
        }

        //////internal static (bool challenge_correct, Guid remote_guid) ChallengeRequest(TcpClient client, NetworkStream stream, byte[] localClientGuid = null, byte[] remoteClientGuidExpected = null, CancellationToken ct = default)
        //////{
        //////    //#if DEBUG
        //////    //            Logging.LogCall( ModuleName);
        //////    //#endif

        //////    //const bool logOkEvent = false;
        //////    //const bool logErrEvent = false;

        //////    // send challenge request
        //////    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

        //////    //using var cts = new CancellationTokenSource();
        //////    //var ct = cts.Token;

        //////    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);//, ct);
        //////    var linkedCt = linkedCts.Token;

        //////    var secretKey = nameof(TcpClientExtra) + Program.ProgramArgs.ServerGuid;
        //////    var secret = Encoding.UTF8.GetBytes(Convert.ToBase64String(Encoding.UTF8.GetBytes(secretKey)));
        //////    const int guidBytesLen = 128 / 8; //Guid.Empty.ToByteArray().Length;
        //////    const int shaBytesLen = 256 / 8; //Sha.ComputeHash(new byte[1]).Length;


        //////    if (linkedCt.IsCancellationRequested)
        //////        //if (logErrEvent) Logging.LogEvent(, "challenge cancelled before it ran");

        //////        Logging.LogExit(ModuleName); return default;

        //////    var challengeCorrect = false;
        //////    var remoteClientGuidReceived = Guid.Empty;
        //////    byte[] remoteClientGuidReceivedBytes = null;

        //////    // 1. send local challenge code
        //////    // 2. read remote challenge code
        //////    // 3. both client and server have a challenge code to solve
        //////    // 4. reply to remote challenge
        //////    // 5. read challenge response


        //////    try
        //////    {
        //////        // 1. send local challenge code
        //////        var challengeSent = Guid.NewGuid().ToByteArray();

        //////        WriteRawFixedLength(client, stream, challengeSent);

        //////        //if (logOkEvent) Logging.LogEvent(, $"local challenge has been sent {string.Join("-", challengeSent.Select(a => $"{a:000}"))}");


        //////        // 2. read remote challenge code
        //////        var challengeReceived = ReadRawFixedLength(client, stream, guidBytesLen, linkedCt);
        //////        if (challengeReceived == null || challengeReceived.Length != guidBytesLen)
        //////            //if (logErrEvent) Logging.LogEvent(, $"local and remote challenge lengths are different (local: {(challengeSent?.Length ?? 0)}; remote: {(challengeReceived?.Length ?? 0)}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");

        //////            Logging.LogExit(ModuleName); return default;

        //////        //if (logOkEvent) Logging.LogEvent(, $"remote challenge has been read {string.Join("-", challengeReceived.Select(a => $"{a:000}").ToArray())}");

        //////        // 3. solve challenges

        //////        // local_hash|remote_hash|secret
        //////        using var sha = SHA256.Create();
        //////        var challengeAnswerSent = sha.ComputeHash(new[] { challengeSent, challengeReceived, secret }.SelectMany(a => a).ToArray());
        //////        var challengeAnswerExpected = sha.ComputeHash(new[] { challengeReceived, challengeSent, secret }.SelectMany(a => a).ToArray());
        //////        if (challengeAnswerExpected.Length != shaBytesLen || challengeAnswerSent.Length != shaBytesLen)
        //////            //if (logErrEvent) Logging.LogEvent(, "challenge problem");

        //////            Logging.LogExit(ModuleName); return default;

        //////        //if (logOkEvent) Logging.LogEvent(, "solved challenges");

        //////        // 4. reply to remote challenge
        //////        WriteRawFixedLength(client, stream, challengeAnswerSent);
        //////        //if (logOkEvent) Logging.LogEvent(, $"replied solution to challenge {string.Join("-", challengeAnswerSent.Select(a => $"{a:000}").ToArray())}");

        //////        // 5. read challenge response
        //////        var challengeAnswerReceived = ReadRawFixedLength(client, stream, shaBytesLen, linkedCt);
        //////        if (challengeAnswerReceived == null || challengeAnswerReceived.Length != challengeAnswerExpected.Length)
        //////            //if (logErrEvent) Logging.LogEvent(, $"local challenge response was wrong length (expected: {(challengeAnswerExpected?.Length ?? 0)}; received: {(challengeAnswerReceived?.Length ?? 0)}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");
        //////            Logging.LogExit(ModuleName); return default;

        //////        //if (logOkEvent) Logging.LogEvent(, $"local challenge response received {string.Join("-", challengeAnswerReceived.Select(a => $"{a:000}").ToArray())} - {string.Join("-", challengeAnswerExpected.Select(a => $"{a:000}").ToArray())}");
        //////        var challengeAnswerCorrect = challengeAnswerReceived.SequenceEqual(challengeAnswerExpected);
        //////        // shaRemoteChallengeAnswer?

        //////        if (!challengeAnswerCorrect)
        //////            //if (logErrEvent) Logging.LogEvent(, $"local challenge response was wrong; (ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");
        //////            Logging.LogExit(ModuleName); return default;
        //////        //if (logOkEvent) Logging.LogEvent(, "local challenge response was correct");

        //////        // 6. send local guid to identify this connection
        //////        if (localClientGuid != null) WriteRawFixedLength(client, stream, localClientGuid);

        //////        // 7. read remote connection guid
        //////        remoteClientGuidReceivedBytes = ReadRawFixedLength(client, stream, guidBytesLen, linkedCt);
        //////        if (remoteClientGuidReceivedBytes == null || remoteClientGuidReceivedBytes.Length != guidBytesLen)
        //////            //if (logErrEvent) Logging.LogEvent(, $"remote guid length is incorrect ({(remoteClientGuidReceivedBytes?.Length ?? 0)}); ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested})");

        //////            Logging.LogExit(ModuleName); return default;

        //////        var remoteGuidCorrect = remoteClientGuidExpected == null || remoteClientGuidExpected.Length == 0 || remoteClientGuidExpected.SequenceEqual(remoteClientGuidReceivedBytes);
        //////        if (!remoteGuidCorrect)
        //////            //if (logErrEvent) Logging.LogEvent(, $"remote connection guid received - {(remoteGuidCorrect ? "correct" : "wrong")}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested}");
        //////            Logging.LogExit(ModuleName); return default;
        //////        //if (logOkEvent) Logging.LogEvent(, $"remote connection guid received - {(remoteGuidCorrect ? "correct" : "wrong")}; ct.IsCancellationRequested={ct.IsCancellationRequested}; ext_ct.IsCancellationRequested={externalCancellationToken.IsCancellationRequested}");

        //////        remoteClientGuidReceived = new Guid(remoteClientGuidReceivedBytes);

        //////        challengeCorrect = true;

        //////        //if (logOkEvent) Logging.LogEvent(, $"challenge successful");
        //////    }
        //////    catch (Exception e) { Logging.LogException(e); }

        //////    Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default : (challengeCorrect, remoteClientGuidReceived);


        //////    //var task_delay = Task.Run(() => { if (!ct.IsCancellationRequested) try { Task.Delay(TimeSpan.FromMilliseconds(1), ct).Wait(ct); } catch (Exception) { } }, ct:ct);

        //////    //Task.WhenAny(task, task_delay).Wait(externalCancellationToken);

        //////    //cts.Cancel();
        //////    //cts.Dispose();

        //////    //if (task.IsCompletedSuccessfully)
        //////    //{
        //////    //    if (task.Result != default)
        //////    //    {
        //////    //        Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :task.Result;
        //////    //    }
        //////    //    else
        //////    //    {
        //////    //        Logging.LogEvent(, "connection challenge wrong");
        //////    //    }
        //////    //}
        //////    //else
        //////    //{
        //////    //    Logging.LogEvent(, "connection challenge timed out!");
        //////    //}

        //////    //Logging.LogExit(ModuleName); return default;
        //////}

        //private static readonly byte[] _bytesFrameCode = new byte[] {0, 0, 0, 0,     1, 1, 1, 1,      2, 2, 2, 2,     3, 3, 3, 3,     4, 4, 4, 4,  5, 5 ,5 ,5 };
        
        //    internal static async Task<bool> WriteFrameAsync(ulong frameId, ulong frameType, string text, TcpClient client, NetworkStream stream, CancellationToken ct)
        //{
        //    Logging.LogCall(ModuleName);

        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

        //    try
        //    {
        //        if (client != null && stream != null && client.Connected && stream.CanWrite)
        //        {

        //            var bytesText = !string.IsNullOrEmpty(text)
        //                ? Encoding.UTF8.GetBytes(text)
        //                : Array.Empty<byte>();
        //            var bytesFrameId = BitConverter.GetBytes(frameId);
        //            var bytesFrameType = BitConverter.GetBytes(frameType);
        //            var bytesFrameLength = BitConverter.GetBytes((ulong)bytesText.Length);
        //            var frame = new[] { _bytesFrameCode, bytesFrameId, bytesFrameType, bytesFrameLength, bytesText/*, _bytesFrameCode*/ }.SelectMany(a => a).ToArray();

        //            await stream.WriteAsync(frame, ct).ConfigureAwait(false);
        //            await stream.FlushAsync(ct).ConfigureAwait(false);
        //            Logging.LogExit(ModuleName); return true;
        //        }

        //        Logging.LogExit(ModuleName); return false;
        //    }
        //    catch (Exception e)
        //    {
        //        Logging.LogException(e, "", ModuleName);
        //        Logging.LogExit(ModuleName); return false;
        //    }
        //}


        //internal static bool WriteFrame(ulong frameId, ulong frameType, string text, TcpClient client, NetworkStream stream, CancellationToken ct)
        //{
        //    Logging.LogCall(ModuleName);

        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

        //    try
        //    {
        //        if (client != null && stream != null && client.Connected && stream.CanWrite)
        //        {
        //            var bytesText = !string.IsNullOrEmpty(text)
        //                ? Encoding.UTF8.GetBytes(text)
        //                : Array.Empty<byte>();
        //            var bytesFrameId = BitConverter.GetBytes(frameId);
        //            var bytesFrameType = BitConverter.GetBytes(frameType);
        //            var bytesFrameLength = BitConverter.GetBytes((ulong)bytesText.Length);
        //            var frame = new[] { bytesFrameId, bytesFrameType, bytesFrameLength, bytesText }.SelectMany(a => a).ToArray();

        //            stream.Write(frame);
        //            stream.Flush();
        //            Logging.LogExit(ModuleName); return true;
        //        }

        //        Logging.LogExit(ModuleName); return default;
        //    }
        //    catch (Exception e)
        //    {
        //        Logging.LogException(e, "", ModuleName);
        //        Logging.LogExit(ModuleName); return default;
        //    }
        //}

        

        //internal static (bool readOk, ulong frameId, ulong frameType, int frameLength, byte[] bytesTextIn, string textIn, string[] textInLines) ReadFrame(TcpClient client, NetworkStream stream, CancellationToken ct)
        //{
        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

        //    if (client != null && stream != null && client.Connected && stream.CanRead)
        //    {
        //        var headerLen = sizeof(ulong) * 3;
        //        var header = ReadRawFixedLength(client, stream, headerLen, ct);
        //        //lastRead = DateTime.UtcNow;
        //        if (header == null || header.Length != headerLen) { Logging.LogExit(ModuleName);  return default; }

        //        var offset = 0;
        //        var frameId = BitConverter.ToUInt64(header, sizeof(ulong) * offset++);
        //        var frameType = BitConverter.ToUInt64(header, sizeof(ulong) * offset++);
        //        var frameLength = (int)BitConverter.ToUInt64(header, sizeof(ulong) * offset++);
        //        var bytesTextIn = ReadRawFixedLength(client, stream, frameLength, ct);

        //        if ((bytesTextIn?.Length ?? 0) != frameLength) { Logging.LogExit(ModuleName);  return default; }


        //        var textIn = bytesTextIn != null
        //            ? Encoding.UTF8.GetString(bytesTextIn)
        //            : null;
        //        var textInLines = textIn?.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
        //        if (bytesTextIn == null || bytesTextIn.Length != frameLength) { Logging.LogExit(ModuleName);  return default; }

        //        Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default : (true, frameId, frameType, frameLength, bytesTextIn, textIn, textInLines);
        //    }

        //    Logging.LogExit(ModuleName); return default;
        //}

        //internal static async Task CloseAsync(ulong frameId, TcpClient client, NetworkStream stream, CancellationToken ct)
        //{
        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

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
            Logging.LogCall(ModuleName);

            Logging.LogExit(ModuleName); return ReadTcpClientRemoteAddress(client.Client);
        }

        internal static (IPAddress localAddress, int localPort, IPAddress remoteAddress, int remotePort) ReadTcpClientRemoteAddress(TcpListener server)
        {
            Logging.LogCall(ModuleName);

            Logging.LogExit(ModuleName); return ReadTcpClientRemoteAddress(server.Server);
        }

        internal static (IPAddress localAddress, int localPort, IPAddress remoteAddress, int remotePort) ReadTcpClientRemoteAddress(Socket client)
        {
            Logging.LogCall(ModuleName);

            try
            {
                var localAddress = ((IPEndPoint)client?.LocalEndPoint)?.Address;
                var localPort = ((IPEndPoint)client?.LocalEndPoint)?.Port ?? default;
                var remoteAddress = ((IPEndPoint)client?.RemoteEndPoint)?.Address;
                var remotePort = ((IPEndPoint)client?.RemoteEndPoint)?.Port ?? default;
                Logging.LogExit(ModuleName); return (localAddress, localPort, remoteAddress, remotePort);
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
                Logging.LogExit(ModuleName); return default;
            }
        }


        internal static bool PollTcpClientConnection(TcpClient client)
        {
            Logging.LogCall(ModuleName);

            if (client == null || client.Client == null || !client.Connected)
            {
                Logging.LogExit(ModuleName); 
                return false;
            }

            // tcpState may be outdated if still Established
            var tcpState = ReadTcpState(client);
            if (tcpState != TcpState.Established && tcpState != TcpState.Unknown)
            {
                Logging.LogExit(ModuleName); 
                return false;
            }

            var socketConnected = PollSocketConnection(client.Client);
            Logging.LogExit(ModuleName); 
            return socketConnected;
        }

        internal static TcpState ReadTcpState(TcpClient tcpClient)
        {
            Logging.LogCall(ModuleName);

            try
            {
                if (tcpClient == null || !tcpClient.Connected || tcpClient.Client == null || tcpClient.Client.LocalEndPoint == null || tcpClient.Client.RemoteEndPoint == null) return TcpState.Closed;
                var clientLocalEndPoint = (IPEndPoint) tcpClient.Client.LocalEndPoint;
                var clientRemoteEndPoint = (IPEndPoint) tcpClient.Client.RemoteEndPoint;
                if (clientLocalEndPoint == null || clientRemoteEndPoint == null) return TcpState.Closed;
                var allStates = IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpConnections().ToArray();
                if (allStates == null || allStates.Length == 0) return TcpState.Closed;

                {
                    var statesUnmapped = allStates.Where(x => x.LocalEndPoint.Equals(clientLocalEndPoint) && x.RemoteEndPoint.Equals(clientRemoteEndPoint)).ToArray();
                    if (statesUnmapped.Length > 0) return statesUnmapped.First().State;
                }

                {
                    var socketEndPointIPv6 = (LocalEndPoint: new IPEndPoint(clientLocalEndPoint.Address.MapToIPv6(), clientLocalEndPoint.Port), RemoteEndPoint: new IPEndPoint(clientRemoteEndPoint.Address.MapToIPv6(), clientRemoteEndPoint.Port));
                    var allStatesIPv6 = allStates.Select(a => (LocalEndPoint: new IPEndPoint(a.LocalEndPoint.Address.MapToIPv6(), a.LocalEndPoint.Port), RemoteEndPoint: new IPEndPoint(a.RemoteEndPoint.Address.MapToIPv6(), a.RemoteEndPoint.Port), a.State)).ToArray();
                    var statesIPv6 = allStatesIPv6.Where(x => x.LocalEndPoint.Equals(socketEndPointIPv6.LocalEndPoint) && x.RemoteEndPoint.Equals(socketEndPointIPv6.RemoteEndPoint)).ToArray();
                    if (statesIPv6.Length > 0) return statesIPv6.First().State;
                }

                {
                    var socketEndPointIPv4 = (LocalEndPoint: new IPEndPoint(clientLocalEndPoint.Address.MapToIPv4(), clientLocalEndPoint.Port), RemoteEndPoint: new IPEndPoint(clientRemoteEndPoint.Address.MapToIPv4(), clientRemoteEndPoint.Port));
                    var allStatesIPv4 = allStates.Select(a => (LocalEndPoint: new IPEndPoint(a.LocalEndPoint.Address.MapToIPv4(), a.LocalEndPoint.Port), RemoteEndPoint: new IPEndPoint(a.RemoteEndPoint.Address.MapToIPv4(), a.RemoteEndPoint.Port), a.State)).ToArray();
                    var statesIPv4 = allStatesIPv4.Where(x => x.LocalEndPoint.Equals(socketEndPointIPv4.LocalEndPoint) && x.RemoteEndPoint.Equals(socketEndPointIPv4.RemoteEndPoint)).ToArray();
                    if (statesIPv4.Length > 0) return statesIPv4.First().State;
                }

                return TcpState.Closed;

            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
                Logging.LogExit(ModuleName);
                return TcpState.Closed;
            }
        }


        internal static void KeepAlive(Socket socket)
        {
            Logging.LogCall(ModuleName);

            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 5); // The number of seconds a TCP connection will wait for a keepalive response before sending another keepalive probe.
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 5); // The number of seconds a TCP connection will wait for a keepalive response before sending another keepalive probe.
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, 16); // The number of TCP keep alive probes that will be sent before the connection is terminated.

            Logging.LogExit(ModuleName);
        }

        internal static bool PollSocketConnection(Socket clientSocket)
        {
            Logging.LogCall(ModuleName);

            if (clientSocket == null) { Logging.LogExit(ModuleName); return false; }

            lock (clientSocket)
            {
                var blockingState = false;
                try
                {
                    if (!clientSocket.Connected) { Logging.LogExit(ModuleName); return false; }

                    var b = new byte[1];
                    blockingState = clientSocket.Blocking;
                    clientSocket.Blocking = false;
                    clientSocket.Send(b, 0, 0);
                    clientSocket.Receive(b, 0, 0);

                    return true;
                }
                catch (SocketException e)
                {
                    Logging.LogException(e, "", ModuleName);
                    //const int ewouldblock = 3406;
                    //const int wsaewouldblock = 10035;
                    //if (e.NativeErrorCode.Equals(wsaewouldblock))
                    if (e.SocketErrorCode == SocketError.WouldBlock) { return true;}
                    else { return false;}
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

                    Logging.LogExit(ModuleName);
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