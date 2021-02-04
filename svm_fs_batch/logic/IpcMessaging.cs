using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch.logic
{
    internal class IpcMessaging
    {
        internal const string ModuleName = nameof(IpcMessaging);

        internal static async Task<(IndexData id, ConfusionMatrix[] CmList)> IpcAsync(string localName, string remoteName, ConnectionPool.ConnectionPoolMember cpm, DataSet DataSet, bool asParallel = true, IndexData idRequest = null, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            (IndexData id, ConfusionMatrix[] CmList) result = default;

            ulong frameId = 0;
            var lockFrameId = new object();

            //using var clientTaskCts = new CancellationTokenSource();
            //var clientTaskCt = clientTaskCts.Token;

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, /*clientTaskCt,*/ cpm.ct);
            var linkedCt = linkedCts.Token;

            ulong NextFrameId()
            {
                lock (lockFrameId) {return frameId++; }
            }

            try
            {
                // send request - index_data instance
                if (idRequest != null)
                {
                    var writeOk0 = await TcpClientExtra.WriteFrameAsync(NextFrameId(), (ulong) TcpClientExtra.PayloadFrameTypes.FrameTypeDataRequest, idRequest.CsvValuesString(), cpm.client, cpm.stream, linkedCt).ConfigureAwait(false);
                    if (!writeOk0)
                    {
                        Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");
                        return !linkedCt.IsCancellationRequested ? result : default;
                    }
                }
                //UpdateWriteTime();

                // read response
                while (!linkedCt.IsCancellationRequested)
                {
                    var frame = await TcpClientExtra.ReadFrameAsync(cpm.client, cpm.stream, linkedCt).ConfigureAwait(false);
                    //UpdateReadTime();

                    if (!frame.readOk)
                    {
                        var pollOk = TcpClientExtra.PollTcpClientConnection(cpm.client);
                        Logging.LogEvent($@"{localName}: Frame could not be read from {remoteName} - poll {pollOk}.", ModuleName);

                        if (!pollOk)
                        {
                            return !linkedCt.IsCancellationRequested ? result : default;
                        }
                        
                        continue;
                    }

                    if (linkedCt.IsCancellationRequested) return default;

                    switch ((TcpClientExtra.PayloadFrameTypes) frame.frameType)
                    {
                        case TcpClientExtra.PayloadFrameTypes.FrameTypePing:
                            Logging.LogEvent($@"{localName}: Received ping from {remoteName}.", ModuleName);

                            // send ack
                            var writeAckOk1 = await TcpClientExtra.WriteFrameAsync(NextFrameId(), (ulong) TcpClientExtra.PayloadFrameTypes.FrameTypePingAcknowledge, @"", cpm.client, cpm.stream, linkedCt).ConfigureAwait(false);
                            if (!writeAckOk1) Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");
                            break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypePingAcknowledge:
                            Logging.LogEvent($@"{localName}: Acknowledgement of ping received from {remoteName}.", ModuleName);
                            break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeDataRequest:
                            Logging.LogEvent($@"{localName}: Received compute request from {remoteName}.", ModuleName);

                            // send ack
                            var writeAckOk2 = await TcpClientExtra.WriteFrameAsync(NextFrameId(), (ulong) TcpClientExtra.PayloadFrameTypes.FrameTypeDataRequestAcknowledge, @"", cpm.client, cpm.stream, linkedCt).ConfigureAwait(false);
                            if (!writeAckOk2)
                            {
                                Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");
                                break;
                            }

                            // read request into IndexData instance
                            var ipcIdRequest = new IndexData(frame.textInLines);

                            Logging.LogEvent($@"{localName}: Compute request parameters: {ipcIdRequest.CsvValuesString()}");
                            // perform compute
                            var text = await FsClient.CrossValidatePerformanceRequestAsync(DataSet, asParallel, ipcIdRequest, linkedCt).ConfigureAwait(false);

                            if (linkedCt.IsCancellationRequested)
                            {
                                Logging.LogEvent($@"{localName}: Cancellation requested - not sending response to {remoteName}.", ModuleName);
                                var writeOk1 = await TcpClientExtra.WriteFrameAsync(NextFrameId(), (ulong) TcpClientExtra.PayloadFrameTypes.FrameTypeDataResponse, "", cpm.client, cpm.stream, linkedCt).ConfigureAwait(false);
                                if (!writeOk1)
                                {
                                    Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");
                                    break;
                                }
                            }
                            else if (string.IsNullOrWhiteSpace(text))
                            {
                                Logging.LogEvent($@"{localName}: Response was empty - not sending response to {remoteName}.", ModuleName);
                                var writeOk2 = await TcpClientExtra.WriteFrameAsync(NextFrameId(), (ulong) TcpClientExtra.PayloadFrameTypes.FrameTypeDataResponse, "", cpm.client, cpm.stream, linkedCt).ConfigureAwait(false);
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
                                var writeOk3 = await TcpClientExtra.WriteFrameAsync(NextFrameId(), (ulong) TcpClientExtra.PayloadFrameTypes.FrameTypeDataResponse, text, cpm.client, cpm.stream, linkedCt).ConfigureAwait(false);
                                if (!writeOk3)
                                {
                                    Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");
                                    break;
                                }
                            }

                            return !linkedCt.IsCancellationRequested ? result : default;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeDataRequestAcknowledge:
                            Logging.LogEvent($@"{localName}: Acknowledgement of compute request received from {remoteName}.", ModuleName);
                            break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeBreak:
                            Logging.LogEvent($@"{localName}: Received break request from {remoteName}.", ModuleName);

                            //send ack
                            var writeOk4 = await TcpClientExtra.WriteFrameAsync(NextFrameId(), (ulong) TcpClientExtra.PayloadFrameTypes.FrameTypeBreakAcknowledge, @"", cpm.client, cpm.stream, linkedCt).ConfigureAwait(false);
                            if (!writeOk4)
                            {
                                Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");
                                break;
                            }

                            return !linkedCt.IsCancellationRequested ? result : default;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeBreakAcknowledge:
                            Logging.LogEvent($@"{localName}: Acknowledgement of break request received from {remoteName}.", ModuleName);
                            break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeDataResponse:
                            Logging.LogEvent($@"{localName}: Received compute response from {remoteName}.", ModuleName);

                            if (frame.textInLines != null && frame.textInLines.Length > 0)
                            {
                                var ans = ConfusionMatrix.Load(frame.textInLines, ct: linkedCt);

                                // send ack
                                // await TcpClientExtra.WriteFrameAsync(NextFrameId(), (ulong)TcpClientExtra.PayloadFrameTypes.frameTypeDataResponseAcknowledge, "", client, stream, clientTaskCt).ConfigureAwait(false);

                                if (ans != null && ans.Length > 0)
                                {
                                    result = (idRequest, ans);
                                }
                            }

                            return !linkedCt.IsCancellationRequested ? result : default;
                        //break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeDataResponseAcknowledge:
                            Logging.LogEvent($@"{localName}: Acknowledgement of compute response received from {remoteName}.", ModuleName);
                            break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeClose:
                            Logging.LogEvent($@"{localName}: {remoteName} requests close.", ModuleName);

                            // send ack
                            var writeAckOk5 = await TcpClientExtra.WriteFrameAsync(NextFrameId(), (ulong) TcpClientExtra.PayloadFrameTypes.FrameTypeCloseAcknowledge, "", cpm.client, cpm.stream, linkedCt).ConfigureAwait(false);
                            if (!writeAckOk5) Logging.LogEvent($@"{localName}: Could not write to {remoteName}.");

                            break;

                        case TcpClientExtra.PayloadFrameTypes.FrameTypeCloseAcknowledge:
                            Logging.LogEvent($@"{localName}: {remoteName} received close request.", ModuleName);
                            break;

                        default:
                            Logging.LogEvent($@"{localName}: {remoteName} sent unrecognized frame type: {frame.frameType}.", ModuleName);
                            throw new ArgumentOutOfRangeException(nameof(frame.frameType), $@"{localName}: {remoteName} sent unrecognized frame type: {frame.frameType}.");
                    }
                }

                return linkedCt.IsCancellationRequested ? default :result;
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
                return !linkedCt.IsCancellationRequested ? result : default;
            }
        }
    }
}