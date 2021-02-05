using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal static class IoProxy
    {
        public const string ModuleName = nameof(IoProxy);

        private static readonly Encoding Ec = new UTF8Encoding(false, true);

        internal static async Task<bool> IsFileAvailableAsync(bool log, CancellationToken ct, string filename, bool getFileLock, int maxTries = 1, bool rethrow = false, string callerModuleName = "", [CallerMemberName] string callerMethodName = "")
        {
            if (ct.IsCancellationRequested) return default;

            // returns true if not cancelled, file exists, file length > 0, and when Getfile_lock parameter set, the file could also be opened... otherwise false.
            const string methodName = nameof(IsFileAvailableAsync);
            var tries = 0;
            if (log) Logging.WriteLine($@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
            while (true)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested) return false;
                    if (string.IsNullOrWhiteSpace(filename)) return false;
                    if (!ExistsFile(log, filename)) return false;
                    if (new FileInfo(filename).Length <= 0) return false;
                    if (getFileLock)
                    {
                        var fs = File.Open(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

                        try { fs.Close(); }
                        catch (Exception e) { Logging.LogException(e, "", ModuleName); }

                        try { await fs.DisposeAsync().ConfigureAwait(false); }
                        catch (Exception e) { Logging.LogException(e, "", ModuleName); }
                    }

                    return true;
                }
                catch (IOException e)
                {
                    Logging.LogException(e, $@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        return false;
                    }

                    try { await Logging.WaitAsync(15, 30, ct: ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }
                catch (Exception e)
                {
                    Logging.LogException(e, $@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        return false;
                    }

                    try { await Logging.WaitAsync(15, 30, ct: ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }
        }

        internal static bool ExistsFile(bool log, string filename, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(ExistsFile);
            if (log) Logging.WriteLine($@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" )", ModuleName, methodName);
            return ct.IsCancellationRequested ? default :File.Exists(filename);
        }

        internal static bool ExistsDirectory(bool log, string dirName, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(ExistsDirectory);
            if (log) Logging.WriteLine($@"{callerModuleName}.{callerMethodName} -> ( ""{dirName}"" )", ModuleName, methodName);
            return ct.IsCancellationRequested ? default :Directory.Exists(dirName);
        }

        internal static async Task<bool> DeleteFileAsync(bool log, CancellationToken ct, string filename, int maxTries = 10, bool rethrow = false, string callerModuleName = "", [CallerMemberName] string callerMethodName = "")
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(DeleteFileAsync);
            var tries = 0;
            if (log) Logging.WriteLine($@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
            while (true)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested) return false;
                    File.Delete(filename);
                    return true;
                }
                catch (Exception e)
                {
                    Logging.LogException(e, $@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        return false;
                    }

                    try { await Logging.WaitAsync(15, 30, ct: ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }
        }

        internal static async Task<bool> DeleteDirectoryAsync(bool log, CancellationToken ct, string dirName, bool recursive, int maxTries = 1, bool rethrow = false, string callerModuleName = "", [CallerMemberName] string callerMethodName = "")
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(DeleteDirectoryAsync);
            var tries = 0;
            if (log) Logging.WriteLine($@"{callerModuleName}.{callerMethodName} -> ( ""{dirName}"", ""{recursive}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
            while (true)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested) return false;
                    Directory.Delete(dirName, recursive);
                    return true;
                }
                catch (Exception e)
                {
                    Logging.LogException(e, $@"{callerModuleName}.{callerMethodName} -> ( ""{dirName}"", ""{recursive}""  ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        return false;
                    }

                    try { await Logging.WaitAsync(15, 30, ct: ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }
        }

        internal static async Task<bool> CopyAsync(bool log, CancellationToken ct, string gkSource, string dest, bool overwrite = true, bool rethrow = true, string callerModuleName = "", [CallerMemberName] string callerMethodName = "", int maxTries = 1_000_000)
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(CopyAsync);
            var tries = 0;
            if (log) Logging.WriteLine($@"{ModuleName}.{methodName} -> ( ""{gkSource}"", ""{dest}"", ""{overwrite}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
            while (true)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested) return false;
                    await CreateDirectoryAsync(log, ct, dest, 1, false, ModuleName, methodName).ConfigureAwait(false);
                    File.Copy(gkSource, dest, overwrite);
                    return true;
                }
                catch (Exception e1)
                {
                    Logging.LogException(e1, $@"{ModuleName}.{methodName} -> ( ""{gkSource}"", ""{dest}"", ""{overwrite}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        return false;
                    }

                    try { await Logging.WaitAsync(15, 30, ct: ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }
        }

        internal static long FileLength(string filename, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(FileLength);

            try { return ct.IsCancellationRequested ? default :new FileInfo(filename).Length; }
            catch (Exception e)
            {
                Logging.LogException(e, "", ModuleName, methodName);
                return ct.IsCancellationRequested ? default :-1;
            }
        }

        internal static async Task<string[]> GetFilesAsync(bool log, CancellationToken ct, string path, string searchPattern, SearchOption searchOption, int maxTries = 10, bool rethrow = false, string callerModuleName = "", [CallerMemberName] string callerMethodName = "")
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(GetFilesAsync);
            var tries = 0;
            if (log) Logging.WriteLine($@"{callerModuleName}.{callerMethodName} -> ( ""{path}"" , ""{searchPattern}"" , ""{searchOption}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
            while (true)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested) return null;
                    return ct.IsCancellationRequested ? default :Directory.GetFiles(path, searchPattern, searchOption);
                }
                catch (Exception e1)
                {
                    Logging.LogException(e1, $@"{callerModuleName}.{callerMethodName} -> ( ""{path}"" , ""{searchPattern}"" , ""{searchOption}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        return null;
                    }

                    try { await Logging.WaitAsync(15, 30, ct: ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }
        }

        internal static async Task<bool> CreateDirectoryAsync(bool log, CancellationToken ct, string filename, int maxTries = 1, bool rethrow = false, string callerModuleName = "", [CallerMemberName] string callerMethodName = "")
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(CreateDirectoryAsync);
            var tries = 0;
            if (log) Logging.WriteLine($@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
            while (true)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested) return false;
                    var dir = Path.GetDirectoryName(filename);
                    if (!string.IsNullOrWhiteSpace(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);
                    return true;
                }
                catch (Exception e1)
                {
                    Logging.LogException(e1, $@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" ) {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        return false;
                    }

                    try { await Logging.WaitAsync(15, 30, ct: ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }
        }

        internal static async Task<string[]> ReadAllLinesAsync(bool log, CancellationToken ct, string filename, int maxTries = 1_000_000, bool rethrow = true, string callerModuleName = "", [CallerMemberName] string callerMethodName = "")
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(ReadAllLinesAsync);
            var tries = 0;
            if (log) Logging.WriteLine($@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" ). {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
            while (true)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested) return null;
                    return ct.IsCancellationRequested ? default :await File.ReadAllLinesAsync(filename, Ec, ct).ConfigureAwait(false);
                }
                catch (Exception e1)
                {
                    Logging.LogException(e1, $@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" ). {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        return null;
                    }

                    try { await Logging.WaitAsync(15, 30, ct: ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }
        }

        internal static async Task<string> ReadAllTextAsync(bool log, CancellationToken ct, string filename, int maxTries = 1_000_000, bool rethrow = true, string callerModuleName = "", [CallerMemberName] string callerMethodName = "")
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(ReadAllTextAsync);
            var tries = 0;
            if (log) Logging.WriteLine($@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" ). {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
            while (true)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested) return null;
                    return ct.IsCancellationRequested ? default :await File.ReadAllTextAsync(filename, Ec, ct).ConfigureAwait(false);
                }
                catch (Exception e1)
                {
                    Logging.LogException(e1, $@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" ). {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        return null;
                    }

                    try { await Logging.WaitAsync(15, 30, ct: ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }
        }

        internal static async Task<bool> WriteAllLinesAsync(bool log, CancellationToken ct, string filename, IList<string> lines, int maxTries = 1_000_000, bool rethrow = true, string callerModuleName = "", [CallerMemberName] string callerMethodName = "")
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(WriteAllLinesAsync);
            var tries = 0;
            if (log) Logging.WriteLine($@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" , {lines?.Count ?? 0} ). {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
            while (true)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested) return false;
                    await CreateDirectoryAsync(log, ct, filename, 1, false, callerModuleName, callerMethodName).ConfigureAwait(false);
                    await File.WriteAllLinesAsync(filename, lines ?? new List<string>(), Ec, ct).ConfigureAwait(false);
                    return true;
                }
                catch (Exception e1)
                {
                    Logging.LogException(e1, $@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" , {lines?.Count ?? 0} ). {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        return false;
                    }

                    try { await Logging.WaitAsync(15, 30, ct: ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }
        }

        internal static async Task<bool> AppendAllLinesAsync(bool log, CancellationToken ct, string filename, string[] lines, int maxTries = 1_000_000, bool rethrow = true, string callerModuleName = "", [CallerMemberName] string callerMethodName = "")
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(AppendAllLinesAsync);
            var tries = 0;
            if (log) Logging.WriteLine($@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" , {lines?.Length ?? 0} ). {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
            while (true)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested) return false;
                    await CreateDirectoryAsync(log, ct, filename, 1, false, callerModuleName, callerMethodName).ConfigureAwait(false);
                    await File.AppendAllLinesAsync(filename, lines ?? Array.Empty<string>(), Ec, ct).ConfigureAwait(false);
                    return true;
                }
                catch (Exception e1)
                {
                    Logging.LogException(e1, $@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" , {lines?.Length ?? 0} ). {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        return false;
                    }

                    try { await Logging.WaitAsync(15, 30, ct: ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }
        }

        internal static async Task<bool> AppendAllTextAsync(bool log, CancellationToken ct, string filename, string text, int maxTries = 1_000_000, bool rethrow = true, string callerModuleName = "", [CallerMemberName] string callerMethodName = "")
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(AppendAllTextAsync);
            var tries = 0;
            if (log) Logging.WriteLine($@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" , {text?.Length ?? 0} ). {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
            while (true)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested) return false;
                    await CreateDirectoryAsync(log, ct, filename, 1, false, callerModuleName, callerMethodName).ConfigureAwait(false);
                    await File.AppendAllTextAsync(filename, text, Ec, ct).ConfigureAwait(false);
                    return true;
                }
                catch (Exception e1)
                {
                    Logging.LogException(e1, $@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" , {text?.Length ?? 0} ). {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        return false;
                    }

                    try { await Logging.WaitAsync(15, 30, ct: ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }
        }

        internal static async Task<bool> WriteAllTextAsync(bool log, CancellationToken ct, string filename, string text, int maxTries = 1_000_000, bool rethrow = true, string callerModuleName = "", [CallerMemberName] string callerMethodName = "")
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(WriteAllTextAsync);
            var tries = 0;
            if (log) Logging.WriteLine($@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" , {text?.Length ?? 0} ). {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
            while (true)
                try
                {
                    tries++;
                    if (ct.IsCancellationRequested) return false;
                    await CreateDirectoryAsync(log, ct, filename, 1, false, callerModuleName, callerMethodName).ConfigureAwait(false);
                    await File.WriteAllTextAsync(filename, text, Ec, ct).ConfigureAwait(false);
                    return true;
                }
                catch (Exception e1)
                {
                    Logging.LogException(e1, $@"{callerModuleName}.{callerMethodName} -> ( ""{filename}"" , {text?.Length ?? 0} ). {nameof(tries)} = {tries}/{maxTries}.", ModuleName, methodName);
                    if (tries >= maxTries)
                    {
                        if (rethrow) throw;
                        return false;
                    }

                    try { await Logging.WaitAsync(15, 30, ct: ct).ConfigureAwait(false); }
                    catch (Exception) { }
                }
        }
    }
}