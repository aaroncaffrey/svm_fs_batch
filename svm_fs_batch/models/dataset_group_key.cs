using System;
using System.Globalization;

namespace svm_fs_batch
{
    internal class dataset_group_key : IEquatable<dataset_group_key>
    {
        internal int column_index = -1; // note: column_index not used for equality checking, because it can change if extra/alternative data is loaded... all lookups should be done by string values.

        internal (string file_tag, string alphabet, string stats, string dimension, string category, string source, string @group, string member, string perspective) value;

        public dataset_group_key(string[] values)
        {
            if (values == null || values.Length == 0) return;
            
            var k = 0;

            if (values.Length > k) value.file_tag = values[k++];
            if (values.Length > k) value.alphabet = values[k++];
            if (values.Length > k) value.stats = values[k++];
            if (values.Length > k) value.dimension = values[k++];
            if (values.Length > k) value.category = values[k++];
            if (values.Length > k) value.source = values[k++];
            if (values.Length > k) value.@group = values[k++];
            if (values.Length > k) value.member = values[k++];
            if (values.Length > k) value.perspective = values[k++];
            if (values.Length > k) column_index = int.Parse(values[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
        }

        public dataset_group_key(dataset_group_key group_key)
        {
            this.value = group_key.value;
            this.column_index = group_key.column_index;
        }

        public dataset_group_key((string file_tag, string alphabet, string stats, string dimension, string category, string source, string @group, string member, string perspective) value, int column_index = -1)
        {
            this.value = value;
            this.column_index = column_index;
        }

        public dataset_group_key(string file_tag, string alphabet, string stats, string dimension, string category, string source, string @group, string member, string perspective, int column_index = -1)
        {
            this.value.file_tag = file_tag;
            this.value.alphabet = alphabet;
            this.value.stats = stats;
            this.value.dimension = dimension;
            this.value.category = category;
            this.value.source = source;
            this.value.@group = @group;
            this.value.member = member;
            this.value.perspective = perspective;
            this.column_index = column_index;
        }

        internal static readonly string[] csv_header_values_array = new string[]
        {
            nameof(dataset_group_key.value.file_tag),
            nameof(dataset_group_key.value.alphabet),
            nameof(dataset_group_key.value.stats),
            nameof(dataset_group_key.value.dimension),
            nameof(dataset_group_key.value.category),
            nameof(dataset_group_key.value.source),
            nameof(dataset_group_key.value.@group),
            nameof(dataset_group_key.value.member),
            nameof(dataset_group_key.value.perspective),
            nameof(column_index),
        };

        internal static readonly string csv_header_string = string.Join(",", csv_header_values_array);


        internal string[] csv_values_array()
        {
            return new string[]
            {
               $@"{value.file_tag}",
               $@"{value.alphabet}",
               $@"{value.stats}",
               $@"{value.dimension}",
               $@"{value.category}",
               $@"{value.source}",
               $@"{value.@group}",
               $@"{value.member}",
               $@"{value.perspective}",
               $@"{column_index}",
            };
        }

        internal string csv_values_string()
        {
            return string.Join(",", csv_values_array());
        }


        public bool Equals(dataset_group_key other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return value.Equals(other.value);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((dataset_group_key) obj);
        }

        public override int GetHashCode()
        {
            return value.GetHashCode();
        }

        public static bool operator ==(dataset_group_key left, dataset_group_key right) { return Equals(left, right); }
        public static bool operator !=(dataset_group_key left, dataset_group_key right) { return !Equals(left, right); }
    }

    /*internal class dataset_group_key : IEquatable<dataset_group_key>
    {
        public const string module_name = nameof(dataset_group_key);

        internal string file_tag;
        internal string alphabet;
        internal string stats;
        internal string dimension;
        internal string category;
        internal string source;
        internal string @group;
        internal string member;
        internal string perspective;

        public dataset_group_key(string file_tag, string alphabet, string stats, string dimension, string category, string source, string @group, string member, string perspective)
        {
            this.file_tag = file_tag;
            this.alphabet = alphabet;
            this.stats = stats;
            this.dimension = dimension;
            this.category = category;
            this.source = source;
            this.@group = @group;
            this.member = member;
            this.perspective = perspective;
        }

        public dataset_group_key(dataset_group_key dataset_group_key)
        {
            this.file_tag = dataset_group_key.file_tag;
            this.alphabet = dataset_group_key.alphabet;
            this.stats = dataset_group_key.stats;
            this.dimension = dataset_group_key.dimension;
            this.category = dataset_group_key.category;
            this.source = dataset_group_key.source;
            this.@group = dataset_group_key.@group;
            this.member = dataset_group_key.member;
            this.perspective = dataset_group_key.perspective;
        }

        public bool Equals(dataset_group_key other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return file_tag == other.file_tag && alphabet == other.alphabet && stats == other.stats && dimension == other.dimension && category == other.category && source == other.source && @group == other.@group && member == other.member && perspective == other.perspective;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((dataset_group_key) obj);
        }

        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.Add(file_tag);
            hash.Add(alphabet);
            hash.Add(stats);
            hash.Add(dimension);
            hash.Add(category);
            hash.Add(source);
            hash.Add(@group);
            hash.Add(member);
            hash.Add(perspective);
            return hash.ToHashCode();
        }

        public static bool operator ==(dataset_group_key left, dataset_group_key right) { return Equals(left, right); }
        public static bool operator !=(dataset_group_key left, dataset_group_key right) { return !Equals(left, right); }

        private sealed class DatasetGroupKeyEqualityComparer : IEqualityComparer<dataset_group_key>
        {
            public bool Equals(dataset_group_key x, dataset_group_key y)
            {
                if (ReferenceEquals(x, y)) return true;
                if (ReferenceEquals(x, null)) return false;
                if (ReferenceEquals(y, null)) return false;
                if (x.GetType() != y.GetType()) return false;
                return string.Equals(x.file_tag, y.file_tag, StringComparison.OrdinalIgnoreCase) && string.Equals(x.alphabet, y.alphabet, StringComparison.OrdinalIgnoreCase) && string.Equals(x.stats, y.stats, StringComparison.OrdinalIgnoreCase) && string.Equals(x.dimension, y.dimension, StringComparison.OrdinalIgnoreCase) && string.Equals(x.category, y.category, StringComparison.OrdinalIgnoreCase) && string.Equals(x.source, y.source, StringComparison.OrdinalIgnoreCase) && string.Equals(x.@group, y.@group, StringComparison.OrdinalIgnoreCase) && string.Equals(x.member, y.member, StringComparison.OrdinalIgnoreCase) && string.Equals(x.perspective, y.perspective, StringComparison.OrdinalIgnoreCase);
            }

            public int GetHashCode(dataset_group_key obj)
            {
                var hashCode = new HashCode();
                hashCode.Add(obj.file_tag, StringComparer.OrdinalIgnoreCase);
                hashCode.Add(obj.alphabet, StringComparer.OrdinalIgnoreCase);
                hashCode.Add(obj.stats, StringComparer.OrdinalIgnoreCase);
                hashCode.Add(obj.dimension, StringComparer.OrdinalIgnoreCase);
                hashCode.Add(obj.category, StringComparer.OrdinalIgnoreCase);
                hashCode.Add(obj.source, StringComparer.OrdinalIgnoreCase);
                hashCode.Add(obj.@group, StringComparer.OrdinalIgnoreCase);
                hashCode.Add(obj.member, StringComparer.OrdinalIgnoreCase);
                hashCode.Add(obj.perspective, StringComparer.OrdinalIgnoreCase);
                return hashCode.ToHashCode();
            }
        }

        public static IEqualityComparer<dataset_group_key> DatasetGroupKeyComparer { get; } = new DatasetGroupKeyEqualityComparer();
    }*/
}
