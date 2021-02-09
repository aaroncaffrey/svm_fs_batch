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
    class RPC
    {
        internal string ModuleName = nameof(RPC);
        internal ConnectionPool CP = new ConnectionPool();

        internal void AdvertiseService()
        {

        }

        internal void UnadvertiseService()
        {

        }

        internal async Task ServiceRequest(TcpClient tcpClient)
        {
            // challenge
            var cpm = await CP.AddClient(tcpClient, ct:default);
            var request = await cpm.ReadFrameAsync(ModuleName);
            
            // read request

            // invoke

            // return result

        }

        internal async void ListenForRPC(CancellationToken ct)
        {
            TcpListener tcpListener = new TcpListener(IPAddress.Any, 77777);
            tcpListener.Start(2);

            AdvertiseService();

            while (!ct.IsCancellationRequested)
            {
                while (!ct.IsCancellationRequested && (Program.Mpstat.GetAverageIdle() >= 85 && Program.Mpstat.GetIdle() >= 85))
                {
                    try{await Task.Delay(TimeSpan.FromSeconds(1), ct);} catch (Exception) {continue;}

                    try
                    {
                        var tcpClient = await tcpListener.AcceptTcpClientAsync();

                        var task = Task.Run(() => ServiceRequest(tcpClient));
                    }
                    catch (Exception e)
                    {
                        Logging.LogException(e, "", ModuleName);
                    }
                }
            }

            UnadvertiseService();
        }

    }
}
