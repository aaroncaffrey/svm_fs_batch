using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch.logic
{
    internal class Mpstat
    {
        internal string[] Data;
        internal double[] DataDouble;
        internal Queue<double[]> DataDoubleHistory = new Queue<double[]>();
        internal string[] Header;
        internal int Interval = 5; // the number of seconds between queries in mpstat (must be an int of 1 or more)
        internal int IntervalHistory = 60; // the number of seconds to keep data records for (i.e. 65 / 5 = 12 records)
        internal Process Process;
        internal bool State;
        internal string[] SysInfo;
        internal Task Task;
        internal DateTime TimeUpdate;
        internal CancellationToken ct = default;

        internal double GetIdle()
        {
            if (Task == null || Task.IsCompleted) Start(this.ct);

            if (Header == null || Header.Length == 0 || Data == null || Data.Length == 0) return 0;

            var ix = Array.FindIndex(Header, a => string.Equals(a, "%idle", StringComparison.OrdinalIgnoreCase));

            if (ix > -1 && DataDouble.Any()) return ct.IsCancellationRequested ? default :DataDouble[ix];

            return default;
        }


        internal double GetAverageIdle()
        {
            if (Task == null || Task.IsCompleted) Start(this.ct);

            if (Header == null || Header.Length == 0 || Data == null || Data.Length == 0) return 0;

            var ix = Array.FindIndex(Header, a => string.Equals(a, "%idle", StringComparison.OrdinalIgnoreCase));

            if (ix > -1 && DataDoubleHistory.Any()) return ct.IsCancellationRequested ? default :DataDoubleHistory.Average(a => a[ix]);

            return default;
        }

        private void Run()
        {
            if (ct.IsCancellationRequested) return;

            Stop();

            var start = new ProcessStartInfo
            {
                FileName = "/usr/bin/mpstat",
                Arguments = $@"{Interval}",
                UseShellExecute = false,
                CreateNoWindow = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true
            };

            try { Process = Process.Start(start); }
            catch (Exception e)
            {
                Logging.LogException(e);
                throw;
            }
        }

        internal void Start(CancellationToken ct)
        {
            this.ct = ct;
            if (ct.IsCancellationRequested) return;
            
            Run();

            if (Process != null)
            {
                State = true;

                Task = Task.Run(async () =>
                {
                    while (Process != null && !Process.HasExited)
                    {
                        try
                        {
                            if (this.ct.IsCancellationRequested)
                            {
                                Stop();
                                return;
                            }

                            var line = await Process.StandardOutput.ReadLineAsync().ConfigureAwait(false);

                            if (string.IsNullOrWhiteSpace(line)) continue;
                            var lineSplit = line.Split(new[] {' '}, StringSplitOptions.RemoveEmptyEntries);
                            if (lineSplit == null || lineSplit.Length == 0) continue;

                            if (SysInfo == null || SysInfo.Length == 0)
                            {
                                SysInfo = lineSplit;
                                continue;
                            }

                            var isPct = lineSplit.Select(a => a.StartsWith('%')).ToArray();
                            var isHeader = isPct.Count(a => a) >= lineSplit.Length / 2;
                            if (isHeader)
                            {
                                Header = lineSplit;
                                continue;
                            }

                            if (Header.Length == lineSplit.Length)
                            {
                                Data = lineSplit;
                                DataDouble = lineSplit.Select(a => double.TryParse(a, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var outDouble)
                                    ? outDouble
                                    : 0.0).ToArray();
                                TimeUpdate = DateTime.UtcNow;

                                DataDoubleHistory.Enqueue(DataDouble);

                                var numHist = (int) Math.Ceiling(IntervalHistory / (double) Interval);

                                while (DataDoubleHistory.Count > numHist) DataDoubleHistory.Dequeue();
                            }
                        }
                        catch (Exception e) { Logging.LogException(e); }

                        if (State && Process.HasExited && !this.ct.IsCancellationRequested)
                            // rerun if killed externally
                            Run();
                    }
                });
            }
        }

        internal void Stop()
        {
            State = false;

            if (Process == null) return;

            try
            {
                if (!Process.HasExited) try{Process.Kill();}catch (Exception){}
                Process.Dispose();
                Process = null;
            }
            catch (Exception e) { Logging.LogException(e); }
        }
    }
}