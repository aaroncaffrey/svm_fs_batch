using System;
using System.Runtime;
using System.Runtime.Loader;
using System.Threading;

namespace SvmFsBatch
{
    internal class Init
    {
        public const string ModuleName = nameof(Init);

        internal static void SetThreadCounts()
        {
            ThreadPool.SetMinThreads(Environment.ProcessorCount * 10, Environment.ProcessorCount * 10);
            ThreadPool.SetMaxThreads(Environment.ProcessorCount * 100, Environment.ProcessorCount * 100);

            //ThreadPool.SetMinThreads(1000, 1000);
            //ThreadPool.SetMaxThreads(10000, 10000);

            //runtimeconfig.template.json
            //{
            //    "configProperties": {
            //        "System.Globalization.Invariant": true,
            //        "System.GC.Server": true,
            //        "System.GC.Concurrent": true,
            //        "System.Threading.ThreadPool.MinThreads": "1000",
            //        "System.Threading.ThreadPool.MaxThreads": "10000"
            //    }
            //}
        }

        internal static void CloseNotifications(CancellationToken ct) //, CancellationTokenSource cts = null)
        {
            if (ct.IsCancellationRequested) return;

            const string methodName = nameof(CloseNotifications);
            //if (ct.IsCancellationRequested) return;

            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                Logging.WriteLine(@"Console.CancelKeyPress", ModuleName, methodName);
                //if (cts != null && !cts.IsCancellationRequested) cts?.Cancel();
            };
            AssemblyLoadContext.Default.Unloading += context =>
            {
                Logging.WriteLine(@"AssemblyLoadContext.Default.Unloading", ModuleName, methodName);
                //if (cts != null && !cts.IsCancellationRequested) cts?.Cancel();
            };
            AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) =>
            {
                Logging.WriteLine(@"AppDomain.CurrentDomain.ProcessExit", ModuleName, methodName);
                //if (cts != null && !cts.IsCancellationRequested) cts?.Cancel();
            };
        }

        internal static void CheckX64()
        {
            var isX64 = IntPtr.Size == 8;
            if (!isX64) throw new Exception("Must run in x64 mode");
        }

        internal static void SetGcMode()
        {
            GCSettings.LatencyMode = GCLatencyMode.Batch;
            //GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
        }
    }
}