using System.Globalization;
using System.Linq;
using System.Threading;

namespace SvmFsBatch
{
    public class XTypes
    {
        public const string ModuleName = nameof(XTypes);

        public bool? AsBool;
        public double? AsDouble;
        public int? AsInt;
        public long? AsLong;
        public string AsStr;

        public XTypes()
        {

        }

        public XTypes(string value)
        {
            Logging.LogCall(ModuleName);

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

        public static XTypes[] GetXTypes(string[] values, bool asParallel = false, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            var xType = asParallel
                ? values.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select(asStr => new XTypes(asStr)).ToArray()
                : values.Select(asStr => new XTypes(asStr)).ToArray();

            Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :xType;
        }
    }
}