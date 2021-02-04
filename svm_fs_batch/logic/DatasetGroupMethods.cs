using System.Linq;
using System.Threading;

namespace SvmFsBatch
{
    internal class DataSetGroupMethods
    {
        public const string ModuleName = nameof(DataSetGroupMethods);
        // note: group_index is usually the array group index, which may be different to the group number in file

        internal static (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] GetMainGroups(CancellationToken ct, DataSet DataSet, bool fileTag, bool alphabet, bool stats, bool dimension, bool category, bool source, bool group, bool member, bool perspective, bool asParallel = true)
        {
            const string methodName = nameof(GetMainGroups);

            if (ct.IsCancellationRequested) return default;

            Logging.WriteLine("", ModuleName, methodName);

            return ct.IsCancellationRequested ? default :GetMainGroups(ct, DataSet.ColumnHeaderList.Skip(1 /* skip class id column - i.e. don't make a group for class id */).ToArray(), fileTag, alphabet, stats, dimension, category, source, group, member, perspective, asParallel);
        }

        internal static DataSetGroupKey[] Ungroup(CancellationToken ct, (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[][] groups, bool asParallel = true)
        {
            const string methodName = nameof(Ungroup);

            if (ct.IsCancellationRequested) return default;

            Logging.WriteLine("", ModuleName, methodName);

            return ct.IsCancellationRequested ? default :asParallel
                ? groups.AsParallel().AsOrdered().WithCancellation(ct).SelectMany(a => a.SelectMany(b => b.GroupColumnHeaders).ToArray()).ToArray()
                : groups.SelectMany(a => a.SelectMany(b => b.GroupColumnHeaders).ToArray()).ToArray();
        }

        internal static DataSetGroupKey[] Ungroup(CancellationToken ct, (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] groups, bool asParallel = true)
        {
            const string methodName = nameof(Ungroup);

            if (ct.IsCancellationRequested) return default;

            Logging.WriteLine("", ModuleName, methodName);

            return ct.IsCancellationRequested ? default :asParallel
                ? groups.AsParallel().AsOrdered().WithCancellation(ct).SelectMany(a => a.GroupColumnHeaders).ToArray()
                : groups.SelectMany(a => a.GroupColumnHeaders).ToArray();
        }

        internal static (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] GetMainGroups(CancellationToken ct, DataSetGroupKey[] columnHeaderList, bool fileTag, bool alphabet, bool stats, bool dimension, bool category, bool source, bool group, bool member, bool perspective, bool asParallel = true)
        {
            const string methodName = nameof(GetMainGroups);

            if (ct.IsCancellationRequested) return default;

            Logging.WriteLine("", ModuleName, methodName);

            return ct.IsCancellationRequested ? default :asParallel
                ? columnHeaderList.AsParallel().AsOrdered().WithCancellation(ct).GroupBy(a => (file_tag: fileTag
                    ? a.Value.gkFileTag
                    : null, alphabet: alphabet
                    ? a.Value.gkAlphabet
                    : null, stats: stats
                    ? a.Value.gkStats
                    : null, dimension: dimension
                    ? a.Value.gkDimension
                    : null, category: category
                    ? a.Value.gkCategory
                    : null, source: source
                    ? a.Value.gkSource
                    : null, group: group
                    ? a.Value.gkGroup
                    : null, member: member
                    ? a.Value.gkMember
                    : null, perspective: perspective
                    ? a.Value.gkPerspective
                    : null)).Select((a, i) => (GroupKey: new DataSetGroupKey(a.Key, i), GroupColumnHeaders: a.ToArray(), columns: a.Select(b => b.GkColumnIndex).ToArray())).ToArray()
                : columnHeaderList.GroupBy(a => (file_tag: fileTag
                    ? a.Value.gkFileTag
                    : null, alphabet: alphabet
                    ? a.Value.gkAlphabet
                    : null, stats: stats
                    ? a.Value.gkStats
                    : null, dimension: dimension
                    ? a.Value.gkDimension
                    : null, category: category
                    ? a.Value.gkCategory
                    : null, source: source
                    ? a.Value.gkSource
                    : null, group: group
                    ? a.Value.gkGroup
                    : null, member: member
                    ? a.Value.gkMember
                    : null, perspective: perspective
                    ? a.Value.gkPerspective
                    : null)).Select((a, i) => (GroupKey: new DataSetGroupKey(a.Key, i), GroupColumnHeaders: a.ToArray(), columns: a.Select(b => b.GkColumnIndex).ToArray())).ToArray();
        }

        internal static (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[][] GetSubGroups(CancellationToken ct, (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] columnHeaderList, bool fileTag, bool alphabet, bool stats, bool dimension, bool category, bool source, bool group, bool member, bool perspective, bool asParallel = true)
        {
            const string methodName = nameof(GetSubGroups);

            if (ct.IsCancellationRequested) return default;

            Logging.WriteLine("", ModuleName, methodName);

            return ct.IsCancellationRequested ? default :asParallel
                ? columnHeaderList.AsParallel().AsOrdered().WithCancellation(ct).Select(a => GetMainGroups(ct, a.GroupColumnHeaders, fileTag, alphabet, stats, dimension, category, source, group, member, perspective)).ToArray()
                : columnHeaderList.Select(a => GetMainGroups(ct, a.GroupColumnHeaders, fileTag, alphabet, stats, dimension, category, source, group, member, perspective)).ToArray();
        }
    }
}