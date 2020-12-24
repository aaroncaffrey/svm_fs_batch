using System;
using System.Collections.Concurrent;
using System.Threading;

namespace svm_fs_batch
{
    internal static class non_blocking_console
    {
        private static readonly BlockingCollection<string> msg_queue = new BlockingCollection<string>();

        static non_blocking_console()
        {
            var thread = new Thread(() =>
            {
                while (true) Console.WriteLine(msg_queue.Take());
            })
            {
                IsBackground = true
            };
            thread.Start();
        }

        internal static void WriteLine(string value) { msg_queue.Add(value); }
    }
}