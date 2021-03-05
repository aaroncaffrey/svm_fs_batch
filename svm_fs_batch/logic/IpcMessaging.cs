using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch.logic
{
    public static class IpcMessaging
    {
        public const string ModuleName = nameof(IpcMessaging);

        public static async Task<(IndexData id, ConfusionMatrix[] CmList)> IpcAsync(string localName, string remoteName, ConnectionPoolMember cpm, DataSet DataSet, bool asParallel = true, IndexData idRequest = null, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0, CancellationToken ct = default)
        {

            // note: this method is not fully implemented.

            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();
            Logging.LogCall(ModuleName, callChain: callChain, lvl: lvl + 1);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            (IndexData id, ConfusionMatrix[] CmList) result = default;

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, /*clientTaskCt,*/ cpm.Ct);
            var linkedCt = linkedCts.Token;

            
            try
            {
                // send request - index_data instance
                if (idRequest != null)
                {
                    //var requestLines = new [] { IndexData.CsvHeaderString, idRequest.CsvValuesString() };
                    var text = idRequest.CsvValuesString() + Environment.NewLine;

                    var writeOk0 = await cpm.WriteFrameAsync( new (ulong, string)[] { ((ulong)TcpClientExtra.PayloadFrameTypes.FrameTypeRpcRequest1, text) }, callChain: callChain, lvl: lvl + 1).ConfigureAwait(false);
                    if (!writeOk0)
                    {
                        Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");
                        
                        Logging.LogExit(ModuleName);
                        return linkedCt.IsCancellationRequested
                            ? default
                            : result;
                    }
                }

                ulong whileCnt1 = 0;
                // read response
                while (!linkedCt.IsCancellationRequested)
                {
                    whileCnt1++;

                    Logging.LogEvent($"!!!! {nameof(IpcAsync)} WHILE 1 !!!! {whileCnt1}");

                    var frame = await cpm.ReadFrameTimeoutAsync(60*60,callChain: callChain, lvl: lvl + 1).ConfigureAwait(false);
                    
                    if (linkedCt.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

                    if (!frame.readOk)
                    {
                        if (!cpm.IsActive(callChain: callChain, lvl: lvl + 1))
                        {
                            Logging.LogEvent($@"{localName}: Frame could not be read from {remoteName}.", ModuleName);
                            Logging.LogExit(ModuleName); return default;
                        }

                        continue;
                    }


                    switch ((TcpClientExtra.PayloadFrameTypes) frame.frameType)
                    {
                        case TcpClientExtra.PayloadFrameTypes.FrameTypePing:
                            Logging.LogEvent($@"{localName}: Received ping from {remoteName}.", ModuleName);

                            // send ack
                            var writeAckOk1 = await cpm.WriteFrameAsync(new[] { ((ulong)TcpClientExtra.PayloadFrameTypes.FrameTypePingAcknowledge, @"")}, callChain: callChain, lvl: lvl + 1).ConfigureAwait(false);
                            if (!writeAckOk1) Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");
                            break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypePingAcknowledge:
                            Logging.LogEvent($@"{localName}: Acknowledgement of ping received from {remoteName}.", ModuleName);
                            break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeRpcRequest1:
                            Logging.LogEvent($@"{localName}: Received compute request from {remoteName}.", ModuleName);

                            // send ack
                            var writeAckOk2 = await cpm.WriteFrameAsync(new[] { ((ulong)TcpClientExtra.PayloadFrameTypes.FrameTypeRpcAcknowledge1, @"") }, callChain: callChain, lvl: lvl + 1).ConfigureAwait(false);
                            if (!writeAckOk2)
                            {
                                Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");
                                break;
                            }

                            // read request into IndexData instance
                            var ipcIdRequest = new IndexData(frame.textInLines);

                            Logging.LogEvent($@"{localName}: Compute request parameters: {ipcIdRequest.CsvValuesString()}");
                            // perform compute
                            var text = "";// await FsClient.CrossValidatePerformanceRequestAsync(DataSet, asParallel, ipcIdRequest, lvl: lvl + 1, linkedCt).ConfigureAwait(false);

                            if (linkedCt.IsCancellationRequested)
                            {
                                Logging.LogEvent($@"{localName}: Cancellation requested - not sending response to {remoteName}.", ModuleName);
                                var writeOk1 = await cpm.WriteFrameAsync(new[] { ((ulong)TcpClientExtra.PayloadFrameTypes.FrameTypeDataRpcResponse1, "") }, callChain: callChain, lvl: lvl + 1).ConfigureAwait(false);
                                if (!writeOk1)
                                {
                                    Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");
                                    break;
                                }
                            }
                            else if (string.IsNullOrWhiteSpace(text))
                            {
                                Logging.LogEvent($@"{localName}: Response was empty - not sending response to {remoteName}.", ModuleName);
                                var writeOk2 = await cpm.WriteFrameAsync(new[] { ((ulong)TcpClientExtra.PayloadFrameTypes.FrameTypeDataRpcResponse1, "") }, callChain: callChain, lvl: lvl + 1).ConfigureAwait(false);
                                if (!writeOk2)
                                {
                                    Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");
                                    break;
                                }
                            }
                            else
                            {
                                // send compute results
                                Logging.LogEvent($@"{localName}: Sending compute response to {remoteName}.", ModuleName);
                                var writeOk3 = await cpm.WriteFrameAsync(new[] { ((ulong)TcpClientExtra.PayloadFrameTypes.FrameTypeDataRpcResponse1, text) }, callChain: callChain, lvl: lvl + 1).ConfigureAwait(false);
                                if (!writeOk3)
                                {
                                    Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");
                                    break;
                                }
                            }

                            Logging.LogExit(ModuleName);
                            return linkedCt.IsCancellationRequested
                                ? default
                                : result;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeRpcAcknowledge1:
                            Logging.LogEvent($@"{localName}: Acknowledgement of compute request received from {remoteName}.", ModuleName);
                            break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeBreak:
                            Logging.LogEvent($@"{localName}: Received break request from {remoteName}.", ModuleName);

                            //send ack
                            var writeOk4 = await cpm.WriteFrameAsync(new[] { ((ulong)TcpClientExtra.PayloadFrameTypes.FrameTypeBreakAcknowledge, @"") }, callChain: callChain, lvl: lvl + 1).ConfigureAwait(false);
                            if (!writeOk4)
                            {
                                Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");
                                break;
                            }

                            Logging.LogExit(ModuleName);

                            return linkedCt.IsCancellationRequested
                                ? default
                                : result;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeBreakAcknowledge:
                            Logging.LogEvent($@"{localName}: Acknowledgement of break request received from {remoteName}.", ModuleName);
                            break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeDataRpcResponse1:
                            Logging.LogEvent($@"{localName}: Received compute response from {remoteName}.", ModuleName);

                            if (frame.textInLines != null && frame.textInLines.Length > 0)
                            {
                                var ans = ConfusionMatrix.LoadLines(frame.textInLines, ct: linkedCt);

                                // send ack
                                // await TcpClientExtra.WriteFrameAsync(NextFrameId(), (ulong)TcpClientExtra.PayloadFrameTypes.frameTypeDataResponseAcknowledge, "", client, stream, clientTaskCt).ConfigureAwait(false);

                                if (ans != null && ans.Length > 0)
                                {
                                    result = (idRequest, ans);
                                }
                            }

                            Logging.LogExit(ModuleName);
                            return linkedCt.IsCancellationRequested
                                ? default
                                : result;
                        //break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeDataRpcResponseAcknowledge1:
                            Logging.LogEvent($@"{localName}: Acknowledgement of compute response received from {remoteName}.", ModuleName);
                            break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeClose:
                            Logging.LogEvent($@"{localName}: {remoteName} requests close.", ModuleName);

                            // send ack
                            var writeAckOk5 = await cpm.WriteFrameAsync(new[] { ((ulong)TcpClientExtra.PayloadFrameTypes.FrameTypeCloseAcknowledge, "") }, callChain: callChain, lvl: lvl + 1).ConfigureAwait(false);
                            if (!writeAckOk5) Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");

                            break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeCloseAcknowledge:
                            Logging.LogEvent($@"{localName}: {remoteName} received close request.", ModuleName);
                            break;

                        default:
                            Logging.LogEvent($@"{localName}: {remoteName} sent unrecognized frame type: {frame.frameType}.", ModuleName);
                            throw new Exception( $@"{localName}: {remoteName} sent unrecognized value: {nameof(frame.frameType)} = {frame.frameType}.");
                    }
                }

                Logging.LogExit(ModuleName); return linkedCt.IsCancellationRequested ? default :result;
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
                Logging.LogExit(ModuleName); return linkedCt.IsCancellationRequested ? result : default;
            }
        }
    }
}