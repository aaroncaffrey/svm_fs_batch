using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch.logic
{
    public class RpcService
    {
        public const string ModuleName = nameof(RpcService);

        public static (string host, int port)[] GetServiceListCache;
        public static DateTime GetServiceListTimeLastRefresh = DateTime.MinValue;
        public static TimeSpan GetServiceListRefreshInterval = TimeSpan.FromSeconds(10);

        public RpcService()
        {

        }

        public static Task RpcConnectTask(ConnectionPool cp, CancellationToken ct)
        {
            var cpConnectTask = Task.Run(async () =>
            {
                var hostsQueue = new Queue<(string host, int port)>();
                var hostsQueueLock = new object();
                var i = -1;

                while (!ct.IsCancellationRequested)
                {
                    i++;
                    if (i > 10) i = 0;


                    if (i == 0)
                    {
                        // every 10 seconds, check for new hosts, connect to all new hosts...
                        var hosts = await RpcService.GetServiceList(ct).ConfigureAwait(false);

                        if (hosts != null && hosts.Length > 0)
                        {
                            hosts = hosts.Except(hostsQueue).ToArray();
                            if (hosts != null && hosts.Length > 0)
                            {
                                foreach (var host in hosts) { hostsQueue.Enqueue(host); }
                                var hostTasks = hosts.AsParallel().AsOrdered().Select(async a => { try { return await Task.Run(async () => { try { return await cp.ConnectAsync(a.host, a.port).ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e); return default; } }, ct).ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e); return default; } }).ToArray();
                                try { await Task.WhenAll(hostTasks).ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e, "", ModuleName); }
                                continue;
                            }
                        }
                    }

                    // otherwise, try to connect every second to new host in the queue... up until max connections per host.

                    for (var k = 0; k < hostsQueue.Count(); k++)
                    {
                        var h = hostsQueue.Dequeue();
                        hostsQueue.Enqueue(h);

                        var numConnectionsToHost = 0;

                        lock (cp.PoolLock)
                        {
                            numConnectionsToHost = cp.Pool.Count(a => a.ConnectHost == h.host && a.ConnectPort == h.port && (a.Client?.Connected??false));
                        }

                        const int maxConnectionsToHost = 5;

                        if (numConnectionsToHost < maxConnectionsToHost)
                        {
                            try { await Task.Run(async () => { try { return await cp.ConnectAsync(h.host, h.port).ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e); return default; } }, ct).ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e); }
                            break;
                        }
                    }
                    try { await Task.Delay(TimeSpan.FromSeconds(1), ct).ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e, "", ModuleName); }
                }

                Logging.LogEvent("Exiting task cpConnectTask in RpcConnectTask.");
            });

            return cpConnectTask;
        }

        public static string Base64Encode(string plainText)
        {
            var plainTextBytes = Encoding.UTF8.GetBytes(plainText);
            return Convert.ToBase64String(plainTextBytes);
        }

        public static string Base64Decode(string base64EncodedData)
        {
            var base64EncodedBytes = Convert.FromBase64String(base64EncodedData);
            return Encoding.UTF8.GetString(base64EncodedBytes);
        }

        public static async Task<(string host, int port)[]> GetServiceList(CancellationToken ct = default)
        {
            if (GetServiceListCache == null || GetServiceListCache.Length == 0 || DateTime.UtcNow - GetServiceListTimeLastRefresh >= GetServiceListRefreshInterval)
            {
                var folder = Path.Combine(Program.ProgramArgs.ResultsRootFolder, "rpc_service");
                var files = await IoProxy.GetFilesAsync(true, ct, folder, "*", SearchOption.AllDirectories);

                if (files != null && files.Length > 0)
                {
                    var base64encoded = files.Select(a => Path.GetFileName(a)).ToArray();
                    var base64decoded = base64encoded.Select(a => Base64Decode(a)).ToArray();

                    var hosts = base64decoded.Select(a =>
                    {
                        var splitIndex = a.LastIndexOf(':');
                        if (splitIndex < 1) return default;

                        var host = a.Substring(0, splitIndex);
                        if (host == default || string.IsNullOrWhiteSpace(host)) return default;

                        var port = int.TryParse(a.Substring(splitIndex + 1), NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_port) ? out_port : default;
                        if (port == default || port < 1 || port > 65535) return default;

                        return (host, port);
                    }).Where(a => a != default).Distinct().ToArray();

                    GetServiceListCache = hosts;
                }
            }

            return GetServiceListCache;
        }

        public static string GetAdvertFilename(string hostname, int port)
        {
            var fn = Base64Encode($"{hostname}:{port}");
            var fullFn = Path.Combine(Program.ProgramArgs.ResultsRootFolder, "rpc_service", fn);
            return fullFn;
        }

        public static string[] AdvertiseServiceFileContents = null;
        public static DateTime timeLastAdvert = DateTime.MinValue;

        public static async Task AdvertiseService(int port, CancellationToken ct = default)
        {
            var now = DateTime.UtcNow;
            var elapsed = now - timeLastAdvert;
            if (elapsed <= TimeSpan.FromSeconds(10)) return;
            timeLastAdvert = now; ;

            // write ip address, port and guid to file
            var hostname = Dns.GetHostName();
            var fullFn = GetAdvertFilename(hostname, port);
            var hostEntries = Dns.GetHostEntry(hostname).AddressList.Select(a => $"{a}:{port}").ToArray();


            //Logging.LogEvent(elapsed.ToString());
            if (
                ((hostEntries?.Length ?? 0) != (AdvertiseServiceFileContents?.Length ?? 0)) ||
                !hostEntries.SequenceEqual(AdvertiseServiceFileContents) ||
                !IoProxy.ExistsFile(false, fullFn)
                )
            {

                AdvertiseServiceFileContents = hostEntries;
                await IoProxy.WriteAllLinesAsync(true, ct, fullFn, hostEntries).ConfigureAwait(false);
            }
        }

        public static async Task UnadvertiseService(int port, CancellationToken ct = default)
        {
            // delete service ad file
            var hostname = Dns.GetHostName();
            var fullFn = GetAdvertFilename(hostname, port);
            await IoProxy.DeleteFileAsync(true, ct, fullFn).ConfigureAwait(false);
        }

        public static async Task AttendServiceRequest(ConnectionPool CP, ConnectionPoolMember cpm, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);


            //const int maxRequests = 0;
            //var requestIndex = 0;

            var supportedMethods = new[] { RpcProxyMethods.ProxyOuterCrossValidationAsync.ProxyMethod, RpcProxyMethods.ProxyOuterCrossValidationSingleAsync.ProxyMethod };

            while (cpm != default && !ct.IsCancellationRequested && !cpm.Ct.IsCancellationRequested)
            {
                //if (maxRequests > 0 && requestIndex++ > maxRequests) { cpm?.Close(); break; }

                //Task.Delay(TimeSpan.FromSeconds(30)) cpm.Cts.Cancel();

                // read request
                var rpcMethodFrame = await cpm.ReadFrameTimeoutAsync(60, ModuleName).ConfigureAwait(false);
                if (!rpcMethodFrame.readOk || (TcpClientExtra.RpcFrameTypes)rpcMethodFrame.frameType != TcpClientExtra.RpcFrameTypes.RpcMethodCall) { cpm?.Close(); break; }
                var rpcMethod = rpcMethodFrame.textIn;
                if (!supportedMethods.Contains(rpcMethod)) { cpm?.Close(); break; }

                // read request params
                var rpcParamsFrame = await cpm.ReadFrameTimeoutAsync(60 * 2, ModuleName).ConfigureAwait(false);
                if (!rpcParamsFrame.readOk || (TcpClientExtra.RpcFrameTypes)rpcParamsFrame.frameType != TcpClientExtra.RpcFrameTypes.RpcMethodParameters) { cpm?.Close(); break; }
                var rpcParams = rpcParamsFrame.textIn;
                if (string.IsNullOrWhiteSpace(rpcParams)) { cpm?.Close(); break; }

                // send acknowledgement
                var rpcMethodCallAcceptWriteOk = await cpm.WriteFrameAsync(new[] { ((ulong)TcpClientExtra.RpcFrameTypes.RpcMethodCallAccept, "") }, ModuleName).ConfigureAwait(false);
                if (!rpcMethodCallAcceptWriteOk) { cpm?.Close(); break; }

                // run RPC method
                var rpcResult = "";
                switch (rpcMethod)
                {

                    case RpcProxyMethods.ProxyOuterCrossValidationAsync.ProxyMethod:

                        rpcResult = await RpcProxyMethods.ProxyOuterCrossValidationAsync.RpcReceiveAsync(rpcParams, ct).ConfigureAwait(false);
                        break;

                    case RpcProxyMethods.ProxyOuterCrossValidationSingleAsync.ProxyMethod:

                        rpcResult = await RpcProxyMethods.ProxyOuterCrossValidationSingleAsync.RpcReceiveAsync(rpcParams, ct).ConfigureAwait(false);
                        break;

                    default:

                        cpm?.Close();
                        break;
                }

                // return RPC method result
                var rpcResultWriteOk = await cpm.WriteFrameAsync(new[] { ((ulong)TcpClientExtra.RpcFrameTypes.RpcMethodReturn, rpcResult) }, ModuleName).ConfigureAwait(false);
                if (!rpcResultWriteOk) { cpm?.Close(); break; }
            }


            Logging.LogExit(ModuleName);
        }

        public static async Task<TcpListener> StartListening(int port, CancellationToken ct = default)
        {
            try
            {
                var tcpListener = new TcpListener(IPAddress.Any, port);
                do
                {
                    //tcpListener.Start(Program.ProgramArgs.ServerBacklog);
                    tcpListener.Start(2);
                    if (!tcpListener.Server.IsBound) try { await Task.Delay(TimeSpan.FromSeconds(1), ct).ConfigureAwait(false); } catch (Exception) { }
                } while (!tcpListener.Server.IsBound && !ct.IsCancellationRequested);

                return tcpListener;
            }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName);
                return default;
            }
        }

        public static void StopListening(TcpListener tcpListener)
        {
            try { tcpListener?.Stop(); } catch (Exception e) { Logging.LogException(e, "", ModuleName); }
        }

        public static async Task ListenForRPC(CancellationToken ct, int port)
        {
            Logging.LogCall(ModuleName);

            //IPEndPoint ListenerIPEndPoint = null;
            ConnectionPool CP = new ConnectionPool();

            if (port == default)
            {
                port = Program.ProgramArgs.ServerPort;
            }

            var tcpListener = await StartListening(port, ct).ConfigureAwait(false);
            //try { ListenerIPEndPoint = ((IPEndPoint)tcpListener.Server.LocalEndPoint); } catch (Exception e) { Logging.LogException(e, "", ModuleName); Logging.LogExit(ModuleName); return; }
            //StopListening(tcpListener);

            await AdvertiseService(port, ct).ConfigureAwait(false);

            while (!ct.IsCancellationRequested)
            {

                while (!ct.IsCancellationRequested && (!Program.ProgramArgs.IsUnix || (Program.Mpstat.GetAverageIdle() >= 20 && Program.Mpstat.GetIdle() >= 20)))
                {
                    if (!(tcpListener?.Server?.IsBound ?? false))
                    {
                        StopListening(tcpListener);
                        tcpListener = await StartListening(port, ct).ConfigureAwait(false);
                    }

                    await AdvertiseService(port, ct).ConfigureAwait(false);

                    try
                    {
                        if (!(tcpListener?.Server?.IsBound ?? false)) break;

                        while (!ct.IsCancellationRequested && tcpListener.Pending())
                        {
                            var tcpClient = await Task.Run(async () => { try { return await tcpListener.AcceptTcpClientAsync().ConfigureAwait(false); } catch (Exception) { return default; } }, ct);//.ConfigureAwait(false);

                            if (tcpClient != default)
                            {
                                var cpm = await CP.AddClient(tcpClient, ct: ct).ConfigureAwait(false);// challenge

                                if (cpm != default)
                                {
                                    var task = Task.Run(async () => await AttendServiceRequest(CP, cpm, ct).ConfigureAwait(false), ct);//.ConfigureAwait(false);
                                    try { await Task.Delay(TimeSpan.FromSeconds(1), ct).ConfigureAwait(false); } catch (Exception) { continue; }
                                    break;
                                }
                            }
                        }
                    }
                    catch (Exception e) { Logging.LogException(e, "", ModuleName); }

                    //StopListening(tcpListener);
                }

                try { await Task.Delay(TimeSpan.FromSeconds(5), ct).ConfigureAwait(false); } catch (Exception) { continue; }
            }

            StopListening(tcpListener);
            await UnadvertiseService(port, ct).ConfigureAwait(false);
            Logging.LogExit(ModuleName);
        }

    }
}
