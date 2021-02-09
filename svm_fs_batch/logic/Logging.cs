using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal static class Logging
    {
        internal const string ModuleName = nameof(Logging);
        private static readonly Random Random = new Random();

        internal static void WriteLine(string text = "", string callerModuleName = "", [CallerMemberName] string callerMethodName = "", [CallerLineNumber] int callerLineNumber = 0)
        {
            // [CallerFilePath] string callerFilePath = "",
            //await Task.Run(async ()=> Console.WriteLine($@"[{DateTime.UtcNow:G}] [{_CallerModuleName}.{_CallerMethodName}:{_CallerLineNumber}]: {text}")).ConfigureAwait(false);
            Console.WriteLine($@"[{DateTime.UtcNow:G}] [{Thread.CurrentThread.ManagedThreadId}] [{Task.CurrentId}] [{callerModuleName}.{callerMethodName}:{callerLineNumber}]: {text}");
        }

        //internal static async Task DelayAsync(TimeSpan time, CancellationToken ct,
        //    string callerModuleName = "", [CallerMemberName] string callerMethodName = "",
        //    /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0,
        //    string callerMethodType = "", (string type, string name, string value)[] callerMethodArgs = null)
        //{
        //    const string methodName = nameof(DelayAsync);
        //    LogEvent($@"{ModuleName}.{methodName}:", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);//, _CallerMethodType, _CallerMethodArgs);
        //
        //    if (!ct.IsCancellationRequested)
        //        try
        //        {
        //            await Task.Delay(time, cancellationToken:ct);
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

        internal static async Task WaitAsync(int minSecs, int maxSecs, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            //const string MethodName = nameof(WaitAsync);

            var ts = TimeSpan.FromSeconds(minSecs != maxSecs
                ? minSecs + Random.Next(0, Math.Abs(maxSecs + 1 - minSecs))
                : minSecs);
            WriteLine($"{callerModuleName}.{callerMethodName} -> Waiting for {ts.Seconds} seconds", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);

            try { await Task.Delay(ts, ct).ConfigureAwait(false); }
            catch (Exception e) { LogException(e, "", ModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber); }

            Logging.LogExit(ModuleName);
        }


        internal static void Wait(int minSecs, int maxSecs, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0, CancellationToken ct = default)
        {
            WaitAsync(minSecs, maxSecs, callerModuleName, callerMethodName, callerLineNumber, ct).Wait(ct);
        }



        internal static void LogEvent(string text = "", string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0)
        //string _CallerMethodType = "",
        //(string type, string name, string value)[] _CallerMethodArgs = null

        {
            //var text2 = $@"{text} --> {_CallerMethodType} <- {_CallerModuleName}.{_CallerMethodName}:{_CallerLineNumber}({string.Join(", ", _CallerMethodArgs?.Select(a => $@"{a.type} {a.name} = ""{a.value}""").ToArray() ?? Array.Empty<string>())})";

            WriteLine(text, callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);
        }

        internal static void LogException(Exception e = null, string msg = "", string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0) //,
                                                                                                                                                                                                                                                            //string _CallerMethodType = "", (string type, string name, string value)[] _CallerMethodArgs = null)
        {
            do
            {
                LogEvent($@"[EXCEPTION] {(!string.IsNullOrWhiteSpace(msg) ? $@"[{nameof(msg)}=""{msg}""]" : "")} [{nameof(e.GetType)}=""{e?.GetType()}""] [{nameof(e.Source)}=""{e?.Source}""] [{nameof(e.Message)}={e?.Message}] [{nameof(e.TargetSite)}=""{e?.TargetSite}""] [{nameof(e.StackTrace)}=""{string.Join(", ", e?.StackTrace?.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(a => a.Trim()).ToArray() ?? Array.Empty<string>())}""]", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber); //, _CallerMethodType, _CallerMethodArgs);
                if (e != null && e != e.InnerException) e = e?.InnerException;
            } while (e != null);
        }

        internal static void LogCall(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();

            var callChainStr = string.Join(" -> ", callChain?.Select(a => $"{a.callerModuleName}.{a.callerMethodName}:{a.callerLineNumer}()").ToArray() ?? Array.Empty<string>());
            LogEvent($"[CALL] {lvl} {callChainStr}", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);
            //,_CallerMethodType, _CallerMethodArgs);
        }

        internal static void LogExit(string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0, (string callerModuleName, string callerMethodName, int callerLineNumer)[] callChain = null, ulong lvl = 0)
        {
            callChain = (callChain ?? Array.Empty<(string, string, int)>()).Concat(new[] { (callerModuleName, callerMethodName, callerLineNumber) }).ToArray();

            var callChainStr = string.Join(" -> ", callChain?.Select(a => $"{a.callerModuleName}.{a.callerMethodName}:{a.callerLineNumer}()").ToArray() ?? Array.Empty<string>());
            LogEvent($"[EXIT] {lvl} {callChainStr}", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);
            //,_CallerMethodType, _CallerMethodArgs);
        }

        internal static void LogLockKnock(string lockName, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0)
        {
            LogEvent($"[LOCK:KNOCK] {lockName}", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber);
            //, _CallerMethodType, _CallerMethodArgs);
        }

        internal static void LogLockEnter(string lockName, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0)
        {
            LogEvent($"[LOCK:ENTER] {lockName}", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber); //, _CallerMethodType, _CallerMethodArgs);
        }

        internal static void LogLockExit(string lockName, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", /*[CallerFilePath] string callerFilePath = "",*/ [CallerLineNumber] int callerLineNumber = 0)
        {
            LogEvent($"[LOCK:EXIT] {lockName}", callerModuleName, callerMethodName, /*callerFilePath,*/ callerLineNumber); //, _CallerMethodType, _CallerMethodArgs);
        }
    }
}