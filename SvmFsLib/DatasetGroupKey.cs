using System;
using System.Linq;

namespace SvmFsLib
{
    public class DataSetGroupKey : IEquatable<DataSetGroupKey>
    {
        public const string ModuleName = nameof(DataSetGroupKey);

        public static readonly DataSetGroupKey Empty = new DataSetGroupKey(null, null, null, null, null, null, null, null, null);

        public static readonly string[] CsvHeaderValuesArray =
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
            nameof(gkGroupIndex),
            nameof(gkColumnIndex)
        };

        public static readonly string CsvHeaderString = string.Join(",", CsvHeaderValuesArray);

        public int gkColumnIndex = -1; // note: column_index not used for equality checking, because it can change if extra/alternative data is loaded... all lookups should be done by string values.

        public int gkGroupIndex = -1; // note: for public use only... value subject to change.

        public (string gkFileTag, string gkAlphabet, string gkStats, string gkDimension, string gkCategory, string gkSource, string gkGroup, string gkMember, string gkPerspective) Value;


        public DataSetGroupKey()
        {

        }

        public DataSetGroupKey((string gkFileTag, string gkAlphabet, string gkStats, string gkDimension, string gkCategory, string gkSource, string gkGroup, string gkMember, string gkPerspective) value, int gkGroupIndex = -1, int gkColumnIndex = -1)
        {
            Logging.LogCall(ModuleName);

            Value = value;
            this.gkGroupIndex = gkGroupIndex;
            this.gkColumnIndex = gkColumnIndex;

            Logging.LogExit(ModuleName);
        }

        public DataSetGroupKey(string gkFileTag, string gkAlphabet, string gkStats, string gkDimension, string gkCategory, string gkSource, string gkGroup, string gkMember, string gkPerspective, int gkGroupIndex = -1, int gkColumnIndex = -1)
        {
            Logging.LogCall(ModuleName);

            /*if (!string.IsNullOrEmpty(gkFileTag))    */ Value.gkFileTag = gkFileTag;
            /*if (!string.IsNullOrEmpty(gkAlphabet))   */ Value.gkAlphabet = gkAlphabet;
            /*if (!string.IsNullOrEmpty(gkStats))      */ Value.gkStats = gkStats;
            /*if (!string.IsNullOrEmpty(gkDimension))  */ Value.gkDimension = gkDimension;
            /*if (!string.IsNullOrEmpty(gkCategory))   */ Value.gkCategory = gkCategory;
            /*if (!string.IsNullOrEmpty(gkSource))     */ Value.gkSource = gkSource;
            /*if (!string.IsNullOrEmpty(gkGroup))      */ Value.gkGroup = gkGroup;
            /*if (!string.IsNullOrEmpty(gkMember))     */ Value.gkMember = gkMember;
            /*if (!string.IsNullOrEmpty(gkPerspective))*/ Value.gkPerspective = gkPerspective;
            this.gkGroupIndex = gkGroupIndex;
            this.gkColumnIndex = gkColumnIndex;

            Logging.LogExit(ModuleName);
        }

        public DataSetGroupKey(string[] lineHeader, XTypes[] line, int columnOffset = 0)
        {
            Logging.LogCall(ModuleName);

            //var headerIndexes = CsvHeaderValuesArray.Select((h, i) => (header: h, index: lineHeader.Length > 0
            //    ? Array.FindIndex(lineHeader, a => a.EndsWith(h))
            //    : columnOffset + i)).ToArray();

            var hasHeader = lineHeader != null && lineHeader.Length > 0;

            var headerIndexes = CsvHeaderValuesArray.Select((h, i) =>
            {
                var index = columnOffset + i;
                if (hasHeader)
                {
                    var indexFirst = Array.FindIndex(lineHeader, columnOffset, a => a.EndsWith(h));
                    var indexLast = Array.FindLastIndex(lineHeader, a => a.EndsWith(h));

                    if (indexFirst != indexLast) throw new Exception();

                    index = indexFirst;
                }
                return (header: h, index: index);
            }).ToArray();

            int Hi(string name)
            {
                var matches = headerIndexes.Where(a => a.header.EndsWith(name, StringComparison.OrdinalIgnoreCase)).ToArray();
                if (matches.Length == 1) return matches[0].index;
                //if (matches.Length == 0) return -1;
                throw new Exception();
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
            var higkGroupIndex = Hi(nameof(this.gkGroupIndex));
            var higkColumnIndex = Hi(nameof(this.gkColumnIndex));


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
            var gkGroupIndex = higkGroupIndex > -1
                ? line[higkGroupIndex].AsInt
                : null;
            var gkColumnIndex = higkColumnIndex > -1
                ? line[higkColumnIndex].AsInt
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
            if (gkGroupIndex != null) this.gkGroupIndex = gkGroupIndex.Value;
            if (gkColumnIndex != null) this.gkColumnIndex = gkColumnIndex.Value;

            Logging.LogExit(ModuleName);
        }


        public bool Equals(DataSetGroupKey other)
        {
            //Logging.LogCall(ModuleName);

            if (ReferenceEquals(null, other)) {Logging.LogExit(ModuleName); return false; }
            if (ReferenceEquals(this, other)) {Logging.LogExit(ModuleName); return true;}
            //Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :value.Equals(other.value);
            var ret = (Value.gkFileTag ?? "") == (other.Value.gkFileTag ?? "") && (Value.gkAlphabet ?? "") == (other.Value.gkAlphabet ?? "") && (Value.gkStats ?? "") == (other.Value.gkStats ?? "") && (Value.gkDimension ?? "") == (other.Value.gkDimension ?? "") && (Value.gkCategory ?? "") == (other.Value.gkCategory ?? "") && (Value.gkSource ?? "") == (other.Value.gkSource ?? "") && (Value.gkGroup ?? "") == (other.Value.gkGroup ?? "") && (Value.gkMember ?? "") == (other.Value.gkMember ?? "") && (Value.gkPerspective ?? "") == (other.Value.gkPerspective ?? "");

            //Logging.LogExit(ModuleName);
            
            return ret;
        }

        public static DataSetGroupKey FindReference(DataSetGroupKey[] list, DataSetGroupKey item)
        {
            Logging.LogCall(ModuleName);

            var ret = list.FirstOrDefault(a => a == item);
            
            Logging.LogExit(ModuleName);
            return ret;
        }


        public string[] CsvValuesArray()
        {
            Logging.LogCall(ModuleName);

            var ret = new[]
            {
                $@"{this.Value.gkFileTag}",
                $@"{this.Value.gkAlphabet}",
                $@"{this.Value.gkStats}",
                $@"{this.Value.gkDimension}",
                $@"{this.Value.gkCategory}",
                $@"{this.Value.gkSource}",
                $@"{this.Value.gkGroup}",
                $@"{this.Value.gkMember}",
                $@"{this.Value.gkPerspective}",
                $@"{this.gkGroupIndex}",
                $@"{this.gkColumnIndex}"
            };

            Logging.LogExit(ModuleName);
            return ret;
        }

        public string CsvValuesString()
        {
            Logging.LogCall(ModuleName);

            var ret = string.Join(",", CsvValuesArray());

            Logging.LogExit(ModuleName);
            return ret;
        }

        public override bool Equals(object obj)
        {
            //Logging.LogCall(ModuleName);

            if (ReferenceEquals(null, obj)) {Logging.LogExit(ModuleName); return false; }
            if (ReferenceEquals(this, obj)) {Logging.LogExit(ModuleName); return true; }
            if (obj.GetType() != GetType()) {Logging.LogExit(ModuleName); return false; }
            
            //Logging.LogExit(ModuleName); 
            return Equals((DataSetGroupKey) obj);
        }

        public override int GetHashCode()
        {
            //Logging.LogCall(ModuleName);
            //Logging.LogExit(ModuleName); 
            
            return Value.GetHashCode();
        }

        public static bool operator ==(DataSetGroupKey left, DataSetGroupKey right)
        {
            //Logging.LogCall(ModuleName);
            //Logging.LogExit(ModuleName); 
            
            return Equals(left, right);
        }

        public static bool operator !=(DataSetGroupKey left, DataSetGroupKey right)
        {
            //Logging.LogCall(ModuleName);
            //Logging.LogExit(ModuleName); 
            
            return !Equals(left, right);
        }
    }
}