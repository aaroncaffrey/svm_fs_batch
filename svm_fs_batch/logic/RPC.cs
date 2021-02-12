using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch.logic
{
    internal class RPC
    {
        internal string ModuleName = nameof(RPC);
        internal ConnectionPool CP = new ConnectionPool();

        internal void AdvertiseService()
        {
            // write ip address, port and guid to file
        }

        internal void UnadvertiseService()
        {
            // delete service ad file
        }

        internal async Task ServiceRequest(TcpClient tcpClient, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);

            // challenge
            var cpm = await CP.AddClient(tcpClient, ct: ct);

            const int maxRequests = 0;
            var requestIndex = 0;

            while (cpm != default && !ct.IsCancellationRequested && !cpm.Ct.IsCancellationRequested)
            {
                if (maxRequests > 0 && requestIndex++ > maxRequests) break;
                

                // read request
                var rpcMethodFrame = await cpm.ReadFrameAsync(ModuleName);
                if (!rpcMethodFrame.readOk) { break; }
                var rpcMethod = rpcMethodFrame.textIn;

                // read request params
                var rpcParamsFrame = await cpm.ReadFrameAsync(ModuleName);
                if (!rpcParamsFrame.readOk) { break; }
                var rpcParams = rpcParamsFrame.textIn;

                var rpcResult = "";

                switch (rpcMethod)
                {

                    case RpcProxyMethods.ProxyOuterCrossValidationAsync.ProxyMethod:

                        rpcResult = await RpcProxyMethods.ProxyOuterCrossValidationAsync.RpcReceiveAsync(rpcParams, ct);
                        break;

                    case RpcProxyMethods.ProxyOuterCrossValidationSingleAsync.ProxyMethod:

                        rpcResult = await RpcProxyMethods.ProxyOuterCrossValidationSingleAsync.RpcReceiveAsync(rpcParams, ct);
                        break;

                    default:

                        break;
                }

                var writeOk = await cpm.WriteFrameAsync(0, rpcResult, ModuleName);
                if (!writeOk) { break; }
            }

            cpm?.Close();
            Logging.LogExit(ModuleName);
        }

        internal async void ListenForRPC(CancellationToken ct)
        {
            Logging.LogCall(ModuleName);


            TcpListener tcpListener = new TcpListener(IPAddress.Any, Program.ProgramArgs.ServerPort);
            do
            {
                tcpListener.Start(Program.ProgramArgs.ServerBacklog);
                if (!tcpListener.Server.IsBound) try { await Task.Delay(TimeSpan.FromSeconds(1), ct); } catch (Exception) { }
            } while (!tcpListener.Server.IsBound && !ct.IsCancellationRequested);

            AdvertiseService();

            while (!ct.IsCancellationRequested)
            {
                while (!ct.IsCancellationRequested && (Program.Mpstat.GetAverageIdle() >= 85 && Program.Mpstat.GetIdle() >= 85))
                {
                    try { await Task.Delay(TimeSpan.FromSeconds(1), ct); } catch (Exception) { continue; }

                    while (tcpListener.Pending())
                    {
                        try
                        {
                            var tcpClient = await tcpListener.AcceptTcpClientAsync();

                            var task = Task.Run(async () => await ServiceRequest(tcpClient, ct), ct);
                        }
                        catch (Exception e)
                        {
                            Logging.LogException(e, "", ModuleName);
                        }
                    }
                }
            }

            try { tcpListener.Stop(); }catch (Exception) { }
            UnadvertiseService();
            Logging.LogExit(ModuleName);
        }

    }
}
