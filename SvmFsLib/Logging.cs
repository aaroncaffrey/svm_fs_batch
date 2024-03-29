﻿using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsLib
{
    public static class Logging
    {
        public const string ModuleName = nameof(Logging);
        public static readonly Random Random = new Random();

        public static void WriteLine(string text = "", string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0)
        {
            // [CallerFilePath] string callerFilePath = "",
            //await Task.Run(async ()=> Console.WriteLine($@"[{DateTime.UtcNow:G}] [{_CallerModuleName}.{_CallerMethodName}:{_CallerLineNumber}]: {text}")).ConfigureAwait(false);
            Console.WriteLine($@"[{DateTime.UtcNow:G}] [{Thread.CurrentThread.ManagedThreadId}] [{Task.CurrentId}] [{callerModuleName}.{callerMethodName}:{callerLineNumber}]: {text}");
        }

        //public static async Task DelayAsync(TimeSpan time, CancellationToken ct,
        //    string callerModuleName = "", [CallerMemberName] string callerMethodName = "",
        //    /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0,
        //    string callerMethodType = "", (string type, string name, string value)[] callerMethodArgs = null)
        //{
        //    const string MethodName = nameof(DelayAsync);
        //    LogEvent($@"{ModuleName}.{MethodName}:", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);//, _CallerMethodType, _CallerMethodArgs);
        //
        //    if (!ct.IsCancellationRequested)
        //        try
        //        {
        //            await Task.Delay(time, cancellationToken:ct).ConfigureAwait(false);
        //        }
        //        catch (OperationCanceledException e)
        //        {
        //            Logging.LogException(e, "", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);
        //        }
        //        catch (Exception e)
        //        {
        //            Logging.LogException(e, "", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);
        //        }
        //}

        public static async Task WaitAsync(int minSecs, int maxSecs, string message = "", string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested)
            {
                Logging.LogExit(ModuleName); 
                return;
            }

            //const string MethodName = nameof(WaitAsync);

            var ts = TimeSpan.FromSeconds(minSecs != maxSecs
                ? minSecs + Random.Next(0, Math.Abs(maxSecs + 1 - minSecs))
                : minSecs);

            LogEvent($"{callerModuleName}.{callerMethodName} -> Waiting for {ts.Seconds} seconds [{message}]", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);

            try { await Task.Delay(ts, ct).ConfigureAwait(false); }
            catch (Exception e) { LogException(e, "", ModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber); }

            Logging.LogExit(ModuleName);
        }

        public static async Task WaitAsync(TimeSpan delay, string message = "", string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0, CancellationToken ct = default)
        {
            try { await WaitAsync((int) Math.Round(delay.TotalSeconds), (int) Math.Round(delay.TotalSeconds), message, callerModuleName, callerMethodName, callerLineNumber, ct: ct).ConfigureAwait(false); }
            catch (Exception e) { LogException(e, "", ModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber); }
        }

        public static void Wait(int minSecs, int maxSecs, string message = "", string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0, CancellationToken ct = default)
        {
            try{WaitAsync(minSecs, maxSecs, message, callerModuleName, callerMethodName, callerLineNumber, ct: ct).Wait(ct);}
            catch (Exception e) { LogException(e, "", ModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber); }
        }

        public static void Wait(TimeSpan delay, string message = "", string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0, CancellationToken ct = default)
        {
            try{WaitAsync((int)Math.Round(delay.TotalSeconds), (int)Math.Round(delay.TotalSeconds), message, callerModuleName, callerMethodName, callerLineNumber, ct: ct).Wait(ct);}
            catch (Exception e) { LogException(e, "", ModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber); }
        }



        public static void LogEvent(string text = "", string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0)
        //string _CallerMethodType = "",
        //(string type, string name, string value)[] _CallerMethodArgs = null

        {
            //var text2 = $@"{text} --> {_CallerMethodType} <- {_CallerModuleName}.{_CallerMethodName}:{_CallerLineNumber}({string.Join(", ", _CallerMethodArgs?.Select(a => $@"{a.type} {a.name} = ""{a.value}""").ToArray() ?? Array.Empty<string>())})";

            WriteLine(text, callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);
        }


        public static void LogException(Exception e = null, string msg = "", string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0) //,
                                                                                                                                                                                                                                                            //string _CallerMethodType = "", (string type, string name, string value)[] _CallerMethodArgs = null)
        {
            do
            {
                LogEvent($@"[EXCEPTION] {(!string.IsNullOrWhiteSpace(msg) ? $@"[{nameof(msg)}=""{msg}""]" : "")} [{nameof(e.GetType)}=""{e?.GetType()}""] [{nameof(e.Source)}=""{e?.Source}""] [{nameof(e.Message)}={e?.Message}] [{nameof(e.TargetSite)}=""{e?.TargetSite}""] [{nameof(e.StackTrace)}=""{string.Join(", ", e?.StackTrace?.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(a => a.Trim()).ToArray() ?? Array.Empty<string>())}""]", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber); //, _CallerMethodType, _CallerMethodArgs);
                if (e != null && e != e.InnerException) e = e?.InnerException;
            } while (e != null);
        }

        public static void LogCall(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
#if DEBUG

            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();

            var callChainStr = string.Join(" -> ", callChain?.Select(a => $"{a.callerModuleName}.{a.callerMethodName}:{a.callerLineNumer}()").ToArray() ?? Array.Empty<string>());
            //LogEvent($"[CALL] {lvl} {callChainStr}", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);
            //,_CallerMethodType, _CallerMethodArgs);
#endif
        }

        public static void LogExit(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
#if DEBUG
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();

            var callChainStr = string.Join(" -> ", callChain?.Select(a => $"{a.callerModuleName}.{a.callerMethodName}:{a.callerLineNumer}()").ToArray() ?? Array.Empty<string>());
            //LogEvent($"[EXIT] {lvl} {callChainStr}", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);
            //,_CallerMethodType, _CallerMethodArgs);
#endif
        }

        public static void LogGap(int count)
        {
            for (var i=0; i < count; i++) Console.WriteLine();
        }

        public static void LogLockKnock(string lockName, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0)
        {
            LogEvent($"[LOCK:KNOCK] {lockName}", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);
            //, _CallerMethodType, _CallerMethodArgs);
        }

        public static void LogLockEnter(string lockName, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0)
        {
            LogEvent($"[LOCK:ENTER] {lockName}", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber); //, _CallerMethodType, _CallerMethodArgs);
        }

        public static void LogLockExit(string lockName, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0)
        {
            LogEvent($"[LOCK:EXIT] {lockName}", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber); //, _CallerMethodType, _CallerMethodArgs);
        }

        public static void LogCancellation(string ctName, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0)
        {
            LogEvent($"[CANCELLATION] {ctName}", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber); //, _CallerMethodType, _CallerMethodArgs);
        }
    }
}