using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal static class io_proxy
    {
        private static readonly Random random = new Random();

        internal static void log_exception(Exception e, string msg, string caller_module_name, string caller_function_name)
        {
            do
            {
                io_proxy.WriteLine(
                    $@"Error: ""{msg}"" ""{e.GetType()}"" ""{e.Source}"" ""{e.Message}"" ""{e.StackTrace}""", caller_module_name, caller_function_name);

#if DEBUG
                if (e.InnerException == null || e == e.InnerException)
                {
                    throw e;
                }
#endif
                e = e != e?.InnerException ? e?.InnerException : null;
            } while (e != null);
        }

        internal static bool is_file_available(string filename, string caller_module_name = "", string caller_function_name = "")
        {
            var module_name = nameof(io_proxy);
            var method_name = nameof(is_file_available);

            try
            {
                //filename = (filename);

                if (String.IsNullOrWhiteSpace(filename)) return false;

                if (!io_proxy.Exists(filename, module_name, method_name)) return false;

                if (new FileInfo(filename).Length <= 0) return false;

                using (var fs = File.Open(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
                {
                    try { fs.Close(); } catch (Exception) { }
                    try { fs.Dispose(); } catch (Exception) { }
                }

                return true;
            }
            catch (IOException e)
            {
                log_exception(e, $@"{caller_module_name}.{caller_function_name} -> ( {filename} )", module_name, method_name);

                return false;
            }
            catch (Exception e)
            {
                log_exception(e, $@"{caller_module_name}.{caller_function_name} -> ( {filename} )", module_name, method_name);

                return false;
            }
        }

        //internal static string convert_path(string path)//, bool temp_file = false)
        //{
        //  return path;
        /*
        if (string.IsNullOrWhiteSpace(path))
        {
            return path;
        }

        if (Environment.OSVersion.Platform == PlatformID.Unix || Environment.OSVersion.Platform == PlatformID.MacOSX)
        {
            // convert windows path to linux

            if (path.Length >= 2 && char.IsLetter(path[0]) && path[1] == ':')
            {
                path = '~' + path.Substring(2);
            }

            if (path.Length > 0 && path[0] == '~')
            {
                // convert ~ to home directory
            }
        }
        else if (Environment.OSVersion.Platform == PlatformID.Win32NT)
        {
            // convert linux path to windows

            if ((path.Length == 1 && path[0] == '~') || (path.Length > 1 && path[0] == '~' && (path[1] == '\\' || path[1] == '/')))
            {
                //var ad = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
                //var up = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
                var md = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

                var p = path.Substring(path.Length > 1 && (path[1] == '\\' || path[1] == '/') ? 2 : 1);

                if (!string.IsNullOrWhiteSpace(p))
                {
                    path = Path.Combine(md, p);
                }
                else
                {
                    path = md;
                }
            }
            else if (path.Length > 0 && (path[0] == '\\' || path[0] == '/') && (path.Length == 1 || (path[1] != '\\' && path[1] != '/')))
            {
                if (path.StartsWith("/home", StringComparison.InvariantCulture) || path.StartsWith("\\home", StringComparison.InvariantCulture)) path = path.Substring("/home".Length);

                if (path.FirstOrDefault() == '/' || path.FirstOrDefault() == '\\') path = path.Substring(1);

                var md = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

                path = Path.Combine(md, path);
            }
            //else if (path.StartsWith("/home"))

        }

        if (Path.DirectorySeparatorChar != '\\' && path.Contains('\\', StringComparison.InvariantCulture))
        {
            path = path.Replace('\\', Path.DirectorySeparatorChar);
        }

        if (Path.DirectorySeparatorChar != '/' && path.Contains('/', StringComparison.InvariantCulture))
        {
            path = path.Replace('/', Path.DirectorySeparatorChar);
        }

        // remove invalid chars
        //var invalid = $"?%*|¦<>\"" + string.Join("", Enumerable.Range(0, 32).Select(a => (char)a).ToList()); // includes \0 \b \t \r \n, leaves /\\: as it is full paths input
        const string valid = ":\\/~.qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM0123456789_-+()[]";
        path = string.Join("", path.Select(a => valid.Contains(a, StringComparison.InvariantCulture) ? a : '_').ToList());

        // make sure no part is more than 255 length

        var path_split = path.Split(new char[] { '\\', '/' },StringSplitOptions.RemoveEmptyEntries);
        if (path_split.Any(a=> a.Length > 255))
        {
            var end_slash = path.Last() == '\\' || path.Last() == '/' ? path.Last().ToString(CultureInfo.InvariantCulture) : "";

            for (var i = 0; i < path_split.Length; i++)
            {
                if (path_split[i].Length > 255)
                {
                    path_split[i] = path_split[i].Substring(0, 255);
                }
            }

            path = end_slash.Length == 0 ? Path.Combine(path_split) : Path.Combine(path_split) + end_slash;
        }

        return path;*/
        //}

        //private static readonly object _console_lock = new object();
        //private static readonly object _log_lock = new object();
        //private static string log_file = null;

        internal static void WriteLine(string text = "", string module_name = "", string function_name = "")//, bool use_lock = false)
        {
            //if (!program.verbose) return;

            //try
            //{
            //const bool e_pid = false;
            //const bool e_threadid = false;
            //const bool e_taskid = false;
            //const bool e_mem = false;

            //var pid = Process.GetCurrentProcess().Id;
            //var thread_id = Thread.CurrentThread.ManagedThreadId;
            //var task_id = Task.CurrentId ?? 0;
            //Memory usage: 
            //var s = $@"{DateTime.Now:G} {(e_mem ? $@"{Math.Ceiling(GC.GetTotalMemory(false) / 1_000_000_000d):00}gb" : "")} {pid:000000}.{thread_id:000000}.{task_id:000000} {module_name}.{function_name} -> {text ?? ""}";

            var s = $@"{DateTime.Now:G} {module_name}.{function_name} -> {text}";

            //if (use_lock)
            //{
            //    lock (_console_lock)
            //    {
            //        Console.WriteLine(s);
            //    }
            //}
            //else
            //{
            Console.WriteLine(s);
            //}

            //    if (!String.IsNullOrEmpty(log_file))
            //    {
            //        lock (_log_lock)
            //        {
            //            File.AppendAllLines(log_file, new string[] {s});
            //        }
            //    }
            //}
            //catch (Exception)// e)
            //{
            //    //svm_ldr.log_exception(e, "", nameof(io_proxy), nameof(is_file_available));
            //}
        }

        //internal static bool is_file_empty(string filename, string module_name = "", string function_name = "")
        //{
        //    var file_empty = (!File.Exists(filename) || new FileInfo(filename).Length <= 0);
        //  
        //    //io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} ) = {file_empty}", nameof(io_proxy), nameof(is_file_empty));
        //
        //    return file_empty;
        //}

        internal static bool Exists(string filename, string module_name = "", string function_name = "")
        {
            //filename = /*convert_path*/(filename);

            var exists = File.Exists(filename);

            //io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} ) = {exists}", nameof(io_proxy), nameof(Exists));

            return exists;
        }

        internal static void Delete(string filename, string module_name = "", string function_name = "")
        {
            //filename = /*convert_path*/(filename);

            //io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(Delete));

            try
            {
                File.Delete(filename);
                return;
            }
            catch (Exception e)
            {
                log_exception(e, $@"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(Delete));

                return;
            }
        }



        internal static void Copy(string source, string dest, bool overwrite = true, string module_name = "", string function_name = "", int max_tries = 1_000_000)
        {
            //source = /*convert_path*/(source);
            //dest = /*convert_path*/(dest);

            var tries = 0;

            while (true)
            {
                try
                {
                    //io_proxy.WriteLine($"{module_name}.{function_name} -> ( {source} , {dest} , {overwrite} ) {tries}", nameof(io_proxy), nameof(Copy));

                    tries++;

                    CreateDirectory(dest);
                    File.Copy(source, dest, overwrite);

                    return;
                }
                catch (Exception e1)
                {

                    log_exception(e1, $@"{module_name}.{function_name} -> ( {source}, {dest}, {overwrite} )", nameof(io_proxy), nameof(Copy));


                    if (tries >= max_tries) throw;

                    try
                    {
                        Task.Delay(new TimeSpan(0, 0, 15 + random.Next(0, 31))).Wait();
                    }
                    catch (Exception e2)
                    {
                        log_exception(e2, $@"{module_name}.{function_name} -> ( {source}, {dest}, {overwrite} )", nameof(io_proxy), nameof(Copy));

                    }
                }
            }
        }

        //private static object dirs_created_lock = new object();
        //private static List<string> dirs_created = new List<string>();

        internal static void CreateDirectory(string filename, string module_name = "", string function_name = "")
        {
            try
            {


                //filename = /*convert_path*/(filename);

                //io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(CreateDirectory));

                var dir = Path.GetDirectoryName(filename);

                if (!string.IsNullOrWhiteSpace(dir) && !Directory.Exists(dir))
                {
                    Directory.CreateDirectory(dir);
                }

                //if (!String.IsNullOrWhiteSpace(dir))
                //{
                //    bool create = false;

                //    lock (dirs_created_lock)
                //    {
                //        if (!dirs_created.Contains(dir))
                //        {
                //            dirs_created.Add(dir);
                //            create = true;
                //        }
                //    }

                //    if (create)
                //    {
                //        if (!Directory.Exists(dir))
                //        {
                //            Directory.CreateDirectory(dir);
                //        }
                //    }
                //}
                //else
                //{
                //    //throw new Exception();
                //}
            }
            catch (Exception e)
            {
                log_exception(e, $@"{module_name}.{function_name} -> ( {filename} )", nameof(io_proxy), nameof(CreateDirectory));
            }
        }

        internal static string[] ReadAllLines(string filename, string module_name = "", string function_name = "", int max_tries = 1_000_000)
        {
            //filename = /*convert_path*/(filename);


            int tries = 0;

            while (true)
            {
                try
                {
                    //io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(ReadAllLines));

                    tries++;

                    var ret = File.ReadAllLines(filename);

                    return ret;
                }
                catch (Exception e1)
                {
                    log_exception(e1, $@"{module_name}.{function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(ReadAllLines));

                    if (tries >= max_tries) throw;

                    try
                    {
                        Task.Delay(new TimeSpan(0, 0, 0, 15 + random.Next(0, 31))).Wait();
                    }
                    catch (Exception e2)
                    {
                        log_exception(e2, $@"{module_name}.{function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(ReadAllLines));
                    }
                }
            }
        }



        internal static string ReadAllText(string filename, string caller_module_name = "", string caller_function_name = "", int max_tries = 1_000_000)
        {
            //filename = /*convert_path*/(filename);

            int tries = 0;

            while (true)
            {
                try
                {
                    //io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(ReadAllText));

                    tries++;

                    var ret = File.ReadAllText(filename);

                    return ret;
                }
                catch (Exception e1)
                {
                    log_exception(e1, $@"{caller_module_name}.{caller_function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(ReadAllText));

                    if (tries >= max_tries) throw;

                    try
                    {
                        Task.Delay(new TimeSpan(0, 0, 0, 15 + random.Next(0, 31))).Wait();
                    }
                    catch (Exception e2)
                    {
                        log_exception(e2, $@"{caller_module_name}.{caller_function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(ReadAllText));

                    }
                }
            }
        }


        internal static void WriteAllLines(string filename, IEnumerable<string> lines, string caller_module_name = "", string caller_function_name = "", int max_tries = 1_000_000)
        {
            //filename = /*convert_path*/(filename);

            CreateDirectory(filename, caller_module_name, caller_function_name);

            var tries = 0;

            while (true)
            {
                try
                {
                    //io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} ) {tries}", nameof(io_proxy), nameof(WriteAllLines));

                    tries++;

                    File.WriteAllLines(filename, lines);

                    return;
                }
                catch (Exception e1)
                {
                    log_exception(e1, $@"{caller_module_name}.{caller_function_name} -> ( ""{Path.GetDirectoryName(filename)}"" > ""{filename}"" ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(WriteAllLines));

                    if (tries >= max_tries) throw;

                    try
                    {
                        Task.Delay(new TimeSpan(0, 0, 0, 15 + random.Next(0, 31))).Wait();
                    }
                    catch (Exception e2)
                    {
                        log_exception(e2, $@"{caller_module_name}.{caller_function_name} -> ( ""{Path.GetDirectoryName(filename)}"" > ""{filename}"" ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(WriteAllLines));
                    }
                }
            }
        }

        internal static void AppendAllLines(string filename, IEnumerable<string> lines, string caller_module_name = "", string caller_function_name = "", int max_tries = 1_000_000)
        {
            //filename = /*convert_path*/(filename);

            CreateDirectory(filename, caller_module_name, caller_function_name);

            var tries = 0;
            while (true)
            {
                try
                {
                    //io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(AppendAllLines));

                    tries++;
                    File.AppendAllLines(filename, lines);
                    return;
                }
                catch (Exception e1)
                {
                    log_exception(e1, $@"{caller_module_name}.{caller_function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(AppendAllLines));

                    if (tries >= max_tries) throw;

                    try
                    {
                        Task.Delay(new TimeSpan(0, 0, 0, 15 + random.Next(0, 31))).Wait();
                    }
                    catch (Exception e2)
                    {
                        log_exception(e2, $@"{caller_module_name}.{caller_function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(AppendAllLines));
                    }
                }
            }
        }

        internal static void AppendAllText(string filename, string text, string caller_module_name = "", string caller_function_name = "", int max_tries = 1_000_000)
        {
            //filename = /*convert_path*/(filename);

            CreateDirectory(filename, caller_module_name, caller_function_name);

            var tries = 0;
            while (true)
            {
                try
                {
                    //io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(AppendAllText));

                    tries++;
                    File.AppendAllText(filename, text);
                    return;
                }
                catch (Exception e1)
                {
                    log_exception(e1, $@"{caller_module_name}.{caller_function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(AppendAllText));

                    if (tries >= max_tries) throw;

                    try
                    {
                        Task.Delay(new TimeSpan(0, 0, 0, 15 + random.Next(0, 31))).Wait();
                    }
                    catch (Exception e2)
                    {
                        log_exception(e2, $@"{caller_module_name}.{caller_function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(AppendAllText));
                    }
                }
            }
        }

        internal static void WriteAllText(string filename, string text, string caller_module_name = "", string caller_function_name = "", int max_tries = 1_000_000)
        {
            //filename = /*convert_path*/(filename);

            CreateDirectory(filename, caller_module_name, caller_function_name);

            var tries = 0;
            while (true)
            {
                try
                {
                    //io_proxy.WriteLine($"{module_name}.{function_name} -> ( {filename} ) {tries}", nameof(io_proxy), nameof(WriteAllText));

                    tries++;
                    File.WriteAllText(filename, text);
                    return;
                }
                catch (Exception e1)
                {

                    log_exception(e1, $@"{caller_module_name}.{caller_function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(WriteAllText));


                    if (tries >= max_tries) throw;

                    try
                    {
                        Task.Delay(new TimeSpan(0, 0, 0, 15 + random.Next(0, 31))).Wait();
                    }
                    catch (Exception e2)
                    {
                        log_exception(e2, $@"{caller_module_name}.{caller_function_name} -> ( {filename} ). {nameof(tries)} = {tries}.", nameof(io_proxy), nameof(WriteAllText));
                    }
                }
            }
        }
    }
}
