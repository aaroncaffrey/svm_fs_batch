using System;
using System.Linq;

namespace SvmFsBatch
{
    internal class DataSetGroupKey : IEquatable<DataSetGroupKey>
    {
        internal static readonly DataSetGroupKey Empty = new DataSetGroupKey(null, null, null, null, null, null, null, null, null);

        internal static readonly string[] CsvHeaderValuesArray =
        {
            nameof(DataSetGroupKey.Value.gkFileTag),
            nameof(DataSetGroupKey.Value.gkAlphabet),
            nameof(DataSetGroupKey.Value.gkStats),
            nameof(DataSetGroupKey.Value.gkDimension),
            nameof(DataSetGroupKey.Value.gkCategory),
            nameof(DataSetGroupKey.Value.gkSource),
            nameof(DataSetGroupKey.Value.gkGroup),
            nameof(DataSetGroupKey.Value.gkMember),
            nameof(DataSetGroupKey.Value.gkPerspective),
            nameof(GkGroupIndex),
            nameof(GkColumnIndex)
        };

        internal static readonly string CsvHeaderString = string.Join(",", CsvHeaderValuesArray);

        internal int GkColumnIndex = -1; // note: column_index not used for equality checking, because it can change if extra/alternative data is loaded... all lookups should be done by string values.

        internal int GkGroupIndex = -1; // note: for internal use only... value subject to change.

        internal (string gkFileTag, string gkAlphabet, string gkStats, string gkDimension, string gkCategory, string gkSource, string gkGroup, string gkMember, string gkPerspective) Value;

        public DataSetGroupKey((string gkFileTag, string gkAlphabet, string gkStats, string gkDimension, string gkCategory, string gkSource, string gkGroup, string gkMember, string gkPerspective) value, int gkGroupIndex = -1, int gkColumnIndex = -1)
        {
            Value = value;
            GkGroupIndex = gkGroupIndex;
            GkColumnIndex = gkColumnIndex;
        }

        public DataSetGroupKey(string gkFileTag, string gkAlphabet, string gkStats, string gkDimension, string gkCategory, string gkSource, string gkGroup, string gkMember, string gkPerspective, int gkGroupIndex = -1, int gkColumnIndex = -1)
        {
            if (!string.IsNullOrEmpty(gkFileTag)) Value.gkFileTag = gkFileTag;
            if (!string.IsNullOrEmpty(gkAlphabet)) Value.gkAlphabet = gkAlphabet;
            if (!string.IsNullOrEmpty(gkStats)) Value.gkStats = gkStats;
            if (!string.IsNullOrEmpty(gkDimension)) Value.gkDimension = gkDimension;
            if (!string.IsNullOrEmpty(gkCategory)) Value.gkCategory = gkCategory;
            if (!string.IsNullOrEmpty(gkSource)) Value.gkSource = gkSource;
            if (!string.IsNullOrEmpty(gkGroup)) Value.gkGroup = gkGroup;
            if (!string.IsNullOrEmpty(gkMember)) Value.gkMember = gkMember;
            if (!string.IsNullOrEmpty(gkPerspective)) Value.gkPerspective = gkPerspective;
            GkGroupIndex = gkGroupIndex;
            GkColumnIndex = gkColumnIndex;
        }

        public DataSetGroupKey(string[] lineHeader, XTypes[] line, int columnOffset = 0)
        {
            var headerIndexes = CsvHeaderValuesArray.Select((h, i) => (header: h, index: lineHeader.Length > 0
                ? Array.FindIndex(lineHeader, a => a.EndsWith(h))
                : columnOffset + i)).ToArray();

            int Hi(string name)
            {
                var a = headerIndexes.FirstOrDefault(a => a.header.EndsWith(name, StringComparison.OrdinalIgnoreCase));
                return a == default
                    ? -1
                    : a.index;
            }

            var hiGkFileTag = Hi(nameof(Value.gkFileTag));
            var hiGkAlphabet = Hi(nameof(Value.gkAlphabet));
            var hiGkStats = Hi(nameof(Value.gkStats));
            var hiGkDimension = Hi(nameof(Value.gkDimension));
            var hiGkCategory = Hi(nameof(Value.gkCategory));
            var hiGkSource = Hi(nameof(Value.gkSource));
            var hiGkGroup = Hi(nameof(Value.gkGroup));
            var hiGkMember = Hi(nameof(Value.gkMember));
            var hiGkPerspective = Hi(nameof(Value.gkPerspective));
            var hiGkGroupIndex = Hi(nameof(GkGroupIndex));
            var hiGkColumnIndex = Hi(nameof(GkColumnIndex));


            var gkFileTag = hiGkFileTag > -1
                ? line[hiGkFileTag].AsStr
                : default;
            var gkAlphabet = hiGkAlphabet > -1
                ? line[hiGkAlphabet].AsStr
                : default;
            var gkStats = hiGkStats > -1
                ? line[hiGkStats].AsStr
                : default;
            var gkDimension = hiGkDimension > -1
                ? line[hiGkDimension].AsStr
                : default;
            var gkCategory = hiGkCategory > -1
                ? line[hiGkCategory].AsStr
                : default;
            var gkSource = hiGkSource > -1
                ? line[hiGkSource].AsStr
                : default;
            var gkGroup = hiGkGroup > -1
                ? line[hiGkGroup].AsStr
                : default;
            var gkMember = hiGkMember > -1
                ? line[hiGkMember].AsStr
                : default;
            var gkPerspective = hiGkPerspective > -1
                ? line[hiGkPerspective].AsStr
                : default;
            var gkGroupIndex = hiGkGroupIndex > -1
                ? line[hiGkGroupIndex].AsInt
                : null;
            var gkColumnIndex = hiGkColumnIndex > -1
                ? line[hiGkColumnIndex].AsInt
                : null;


            if (!string.IsNullOrEmpty(gkFileTag)) Value.gkFileTag = gkFileTag;
            if (!string.IsNullOrEmpty(gkAlphabet)) Value.gkAlphabet = gkAlphabet;
            if (!string.IsNullOrEmpty(gkStats)) Value.gkStats = gkStats;
            if (!string.IsNullOrEmpty(gkDimension)) Value.gkDimension = gkDimension;
            if (!string.IsNullOrEmpty(gkCategory)) Value.gkCategory = gkCategory;
            if (!string.IsNullOrEmpty(gkSource)) Value.gkSource = gkSource;
            if (!string.IsNullOrEmpty(gkGroup)) Value.gkGroup = gkGroup;
            if (!string.IsNullOrEmpty(gkMember)) Value.gkMember = gkMember;
            if (!string.IsNullOrEmpty(gkPerspective)) Value.gkPerspective = gkPerspective;
            if (gkGroupIndex != null) GkGroupIndex = gkGroupIndex.Value;
            if (gkColumnIndex != null) GkColumnIndex = gkColumnIndex.Value;
        }


        public bool Equals(DataSetGroupKey other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            //return ct.IsCancellationRequested ? default :value.Equals(other.value);

            return (Value.gkFileTag ?? "") == (other.Value.gkFileTag ?? "") && (Value.gkAlphabet ?? "") == (other.Value.gkAlphabet ?? "") && (Value.gkStats ?? "") == (other.Value.gkStats ?? "") && (Value.gkDimension ?? "") == (other.Value.gkDimension ?? "") && (Value.gkCategory ?? "") == (other.Value.gkCategory ?? "") && (Value.gkSource ?? "") == (other.Value.gkSource ?? "") && (Value.gkGroup ?? "") == (other.Value.gkGroup ?? "") && (Value.gkMember ?? "") == (other.Value.gkMember ?? "") && (Value.gkPerspective ?? "") == (other.Value.gkPerspective ?? "");
        }

        internal static DataSetGroupKey find_reference(DataSetGroupKey[] list, DataSetGroupKey item)
        {
            return list.FirstOrDefault(a => a == item);
        }


        internal string[] CsvValuesArray()
        {
            return new[]
            {
                $@"{Value.gkFileTag}",
                $@"{Value.gkAlphabet}",
                $@"{Value.gkStats}",
                $@"{Value.gkDimension}",
                $@"{Value.gkCategory}",
                $@"{Value.gkSource}",
                $@"{Value.gkGroup}",
                $@"{Value.gkMember}",
                $@"{Value.gkPerspective}",
                $@"{GkGroupIndex}",
                $@"{GkColumnIndex}"
            };
        }

        internal string CsvValuesString()
        {
            return string.Join(",", CsvValuesArray());
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((DataSetGroupKey) obj);
        }

        public override int GetHashCode()
        {
            return Value.GetHashCode();
        }

        public static bool operator ==(DataSetGroupKey left, DataSetGroupKey right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(DataSetGroupKey left, DataSetGroupKey right)
        {
            return !Equals(left, right);
        }
    }
}