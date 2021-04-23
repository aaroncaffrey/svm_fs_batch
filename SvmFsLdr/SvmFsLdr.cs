using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SvmFsLib;

namespace SvmFsLdr
{
    public class SvmFsLdr
    {
        public static async Task Main(string[] args)
        {
            // listen for work, run msub for array with new work.
            var mainCts = new CancellationTokenSource();
            var mainCt = mainCts.Token;

            Init.CloseNotifications(mainCt); //, mainCts);
            Init.CheckX64();
            Init.SetGcMode();

            var cts = new CancellationTokenSource();
            var ct = cts.Token;

            while (!ct.IsCancellationRequested)
            {
                // run controller - controller will find work position by itself

                // wait for controller to either send work or write exit file

                // check for new work files

                var total_vcpus = 1000;
                var instance_vcpus = 64;
                var num_instance = 1;

                // run msub with array


                await Task.Delay(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
            }
        }
    }
}
