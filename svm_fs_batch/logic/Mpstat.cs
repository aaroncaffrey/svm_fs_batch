﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch.logic
{
    internal class Mpstat
    {
        internal const string ModuleName = nameof(Mpstat);

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

        internal bool IsRunning()
        {
            Logging.LogCall(ModuleName);
            Logging.LogExit(ModuleName); return Process != null && !Process.HasExited;
        }

        internal bool IsResponsive()
        {
            Logging.LogCall(ModuleName);
            if (!IsRunning()) {Logging.LogExit(ModuleName); return false; }

            var secondsSinceUpdate = (DateTime.UtcNow - TimeUpdate).TotalSeconds;

            Logging.LogExit(ModuleName);
            return secondsSinceUpdate <= IntervalHistory;
        }

        internal double GetIdle()
        {
            Logging.LogCall(ModuleName);
            if (Task == null || Task.IsCompleted) Start(this.ct).Wait(this.ct);

            if (Header == null || Header.Length == 0 || Data == null || Data.Length == 0) {Logging.LogExit(ModuleName); return 0; }

            var ix = Array.FindIndex(Header, a => string.Equals(a, "%idle", StringComparison.OrdinalIgnoreCase));

            if (ix > -1 && DataDouble.Any())
            {
                Logging.LogExit(ModuleName); 
                return ct.IsCancellationRequested ? default :DataDouble[ix];
            }

            Logging.LogExit(ModuleName);
            return default;
        }


        internal double GetAverageIdle()
        {
            Logging.LogCall(ModuleName);
            if (Task == null || Task.IsCompleted) Start(this.ct).Wait(this.ct);

            if (Header == null || Header.Length == 0 || Data == null || Data.Length == 0) {Logging.LogExit(ModuleName); return 0; }

            var ix = Array.FindIndex(Header, a => string.Equals(a, "%idle", StringComparison.OrdinalIgnoreCase));

            if (ix > -1 && DataDoubleHistory.Any())
            {
                Logging.LogExit(ModuleName);
                return ct.IsCancellationRequested ? default :DataDoubleHistory.Average(a => a[ix]);
            }

            Logging.LogExit(ModuleName);
            return default;
        }

        private async Task RunAsync()
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            await Task.Run(async () =>
            {
                await StopAsync().ConfigureAwait(false);

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
                    //throw;
                }
            }, ct);

            Logging.LogExit(ModuleName);
        }

        internal async Task Start(CancellationToken ct)
        {
            Logging.LogCall(ModuleName);
            this.ct = ct;
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }
            
            await RunAsync();

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
                                await StopAsync().ConfigureAwait(false);
                                Logging.LogExit(ModuleName); return;
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
                        {
                            // rerun if killed externally
                            await RunAsync();
                        }
                    }
                });
            }

            Logging.LogExit(ModuleName);
        }

        internal async Task StopAsync()
        {
            Logging.LogCall(ModuleName);
            State = false;

            if (Process == null) { Logging.LogExit(ModuleName); return; }

            try
            {
                if (!Process.HasExited) try{Process.Kill(); await Process.WaitForExitAsync(ct).ConfigureAwait(false); } catch (Exception){}
                Process.Dispose();
                Process = null;
            }
            catch (Exception e) { Logging.LogException(e); }

            Logging.LogExit(ModuleName);
        }
    }
}