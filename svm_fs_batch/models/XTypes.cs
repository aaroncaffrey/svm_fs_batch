using System.Globalization;
using System.Linq;
using System.Threading;

namespace SvmFsBatch
{
    internal class XTypes
    {
        internal bool? AsBool;
        internal double? AsDouble;
        internal int? AsInt;
        internal long? AsLong;
        internal string AsStr;

        public XTypes(string value)
        {
            AsStr = value;
            AsDouble = double.TryParse(AsStr, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var outDouble)
                ? outDouble
                : (double?) null;
            AsInt = int.TryParse(AsStr, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var outInt)
                ? outInt
                : (int?) null;
            AsLong = long.TryParse(AsStr, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var outLong)
                ? outLong
                : (long?) null;
            AsBool = AsInt == 1 && AsDouble == 1 ? true :
                AsInt == 0 && AsDouble == 0 ? false : (bool?) null;
            if (AsBool == null && bool.TryParse(AsStr, out var outBool)) AsBool = outBool;
        }

        internal static XTypes[] GetXTypes(string[] values, bool asParallel = false, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            var xType = asParallel
                ? values.AsParallel().AsOrdered().WithCancellation(ct).Select(asStr => new XTypes(asStr)).ToArray()
                : values.Select(asStr => new XTypes(asStr)).ToArray();

            return ct.IsCancellationRequested ? default :xType;
        }
    }
}