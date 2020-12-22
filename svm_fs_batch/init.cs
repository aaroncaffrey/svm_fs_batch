using System;
using System.Runtime;
using System.Runtime.Loader;
using System.Threading;

namespace svm_fs_batch
{
    internal class init
    {
        public const string module_name = nameof(init);
        internal static void set_thread_counts()
        {
            //ThreadPool.SetMinThreads(Environment.ProcessorCount * 10, Environment.ProcessorCount * 10);
            //ThreadPool.SetMaxThreads(Environment.ProcessorCount * 100, Environment.ProcessorCount * 100);

            ThreadPool.SetMinThreads(1000, 1000);
            ThreadPool.SetMaxThreads(10000, 10000);
        }

        internal static void close_notifications(CancellationTokenSource cts = null)
        {
            if (cts.IsCancellationRequested) return;

            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                io_proxy.WriteLine($@"Console.CancelKeyPress", nameof(program), nameof(close_notifications));
                cts?.Cancel();
            };
            AssemblyLoadContext.Default.Unloading += context =>
            {
                io_proxy.WriteLine($@"AssemblyLoadContext.Default.Unloading", nameof(program), nameof(close_notifications));
                cts?.Cancel();
            };
            AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) =>
            {
                io_proxy.WriteLine($@"AppDomain.CurrentDomain.ProcessExit", nameof(program), nameof(close_notifications));
                cts?.Cancel();
            };
        }

        internal static void check_x64()
        {
            var is_x64 = IntPtr.Size == 8;
            if (!is_x64) { throw new Exception("Must run in x64 mode"); }
        }

        internal static void set_gc_mode()
        {
            GCSettings.LatencyMode = GCLatencyMode.Batch;
            //GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
        }
    }
}
