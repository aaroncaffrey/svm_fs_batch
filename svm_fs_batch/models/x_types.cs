using System.Globalization;
using System.Linq;
using System.Threading;

namespace svm_fs_batch
{
    internal class x_types
    {
        internal string as_str;
        internal int? as_int;
        internal long? as_long;
        internal double? as_double;
        internal bool? as_bool;

        public x_types(string value)
        {
            as_str = value;
            as_double = double.TryParse(this.as_str, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var out_double) ? (double?)out_double : (double?)null;
            as_int = int.TryParse(this.as_str, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_int) ? (int?)out_int : (int?)null;
            as_long = long.TryParse(this.as_str, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var out_long) ? (long?)out_long : (long?)null;
            as_bool = as_int == 1 && as_double == 1 ? (bool?)true : (as_int == 0 && as_double == 0 ? (bool?)false : (bool?)null);
            if (as_bool == null && bool.TryParse(this.as_str, out var out_bool)) as_bool = (bool?)out_bool;
        }

        internal static x_types[] get_x_types(string[] values, CancellationTokenSource cts = null, bool as_parallel = false)
        {
            if (as_parallel && cts == null) cts = new CancellationTokenSource();

            var x_type = as_parallel ? values
                    .AsParallel()
                    .AsOrdered()
                    .WithCancellation(cts.Token)
                    .Select(as_str => new x_types(as_str))
                    .ToArray() :
                values
                    .Select(as_str => new x_types(as_str))
                    .ToArray();

            return x_type;
        }
    }
}
