using System.Linq;
using System.Threading;

namespace SvmFsBatch
{
    internal class DataSetGroupMethods
    {
        public const string ModuleName = nameof(DataSetGroupMethods);
        // note: group_index is usually the array gkGroup index, which may be different to the gkGroup number in file

        internal static (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] GetMainGroups(DataSet DataSet, bool gkFileTag, bool gkAlphabet, bool gkStats, bool gkDimension, bool gkCategory, bool gkSource, bool gkGroup, bool gkMember, bool gkPerspective, bool asParallel = true, CancellationToken ct = default)
        {
            const string methodName = nameof(GetMainGroups);

            if (ct.IsCancellationRequested) return default;

            Logging.WriteLine("", ModuleName, methodName);

            return GetMainGroups(DataSet.ColumnHeaderList.Skip(1 /* skip class id column - i.e. don't make a gkGroup for class id */).ToArray(), gkFileTag, gkAlphabet, gkStats, gkDimension, gkCategory, gkSource, gkGroup, gkMember, gkPerspective, asParallel, ct);
        }

        internal static DataSetGroupKey[] Ungroup((DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[][] groups, bool asParallel = true, CancellationToken ct = default)
        {
            const string methodName = nameof(Ungroup);

            if (ct.IsCancellationRequested) return default;

            Logging.WriteLine("", ModuleName, methodName);

            return asParallel
                ? groups.AsParallel().AsOrdered().WithCancellation(ct).SelectMany(a => a.SelectMany(b => b.GroupColumnHeaders).ToArray()).ToArray()
                : groups.SelectMany(a => a.SelectMany(b => b.GroupColumnHeaders).ToArray()).ToArray();
        }

        internal static DataSetGroupKey[] Ungroup((DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] groups, bool asParallel = true, CancellationToken ct = default)
        {
            const string methodName = nameof(Ungroup);

            if (ct.IsCancellationRequested) return default;

            Logging.WriteLine("", ModuleName, methodName);

            return asParallel
                ? groups.AsParallel().AsOrdered().WithCancellation(ct).SelectMany(a => a.GroupColumnHeaders).ToArray()
                : groups.SelectMany(a => a.GroupColumnHeaders).ToArray();
        }

        internal static (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] GetMainGroups(DataSetGroupKey[] columnHeaderList, bool gkFileTag, bool gkAlphabet, bool gkStats, bool gkDimension, bool gkCategory, bool gkSource, bool gkGroup, bool gkMember, bool gkPerspective, bool asParallel = true, CancellationToken ct = default)
        {
            const string methodName = nameof(GetMainGroups);

            if (ct.IsCancellationRequested) return default;

            Logging.WriteLine("", ModuleName, methodName);

            return asParallel
                ? columnHeaderList.AsParallel().AsOrdered().WithCancellation(ct).GroupBy(a => (file_tag: gkFileTag
                    ? a.Value.gkFileTag
                    : null, gkAlphabet: gkAlphabet
                    ? a.Value.gkAlphabet
                    : null, gkStats: gkStats
                    ? a.Value.gkStats
                    : null, gkDimension: gkDimension
                    ? a.Value.gkDimension
                    : null, gkCategory: gkCategory
                    ? a.Value.gkCategory
                    : null, gkSource: gkSource
                    ? a.Value.gkSource
                    : null, gkGroup: gkGroup
                    ? a.Value.gkGroup
                    : null, gkMember: gkMember
                    ? a.Value.gkMember
                    : null, gkPerspective: gkPerspective
                    ? a.Value.gkPerspective
                    : null)).Select((a, i) => (GroupKey: new DataSetGroupKey(a.Key, i), GroupColumnHeaders: a.ToArray(), columns: a.Select(b => b.gkColumnIndex).ToArray())).ToArray()
                : columnHeaderList.GroupBy(a => (file_tag: gkFileTag
                    ? a.Value.gkFileTag
                    : null, gkAlphabet: gkAlphabet
                    ? a.Value.gkAlphabet
                    : null, gkStats: gkStats
                    ? a.Value.gkStats
                    : null, gkDimension: gkDimension
                    ? a.Value.gkDimension
                    : null, gkCategory: gkCategory
                    ? a.Value.gkCategory
                    : null, gkSource: gkSource
                    ? a.Value.gkSource
                    : null, gkGroup: gkGroup
                    ? a.Value.gkGroup
                    : null, gkMember: gkMember
                    ? a.Value.gkMember
                    : null, gkPerspective: gkPerspective
                    ? a.Value.gkPerspective
                    : null)).Select((a, i) => (GroupKey: new DataSetGroupKey(a.Key, i), GroupColumnHeaders: a.ToArray(), columns: a.Select(b => b.gkColumnIndex).ToArray())).ToArray();
        }

        internal static (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[][] GetSubGroups((DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] columnHeaderList, bool gkFileTag, bool gkAlphabet, bool gkStats, bool gkDimension, bool gkCategory, bool gkSource, bool gkGroup, bool gkMember, bool gkPerspective, bool asParallel = true, CancellationToken ct = default)
        {
            const string methodName = nameof(GetSubGroups);

            if (ct.IsCancellationRequested) return default;

            Logging.WriteLine("", ModuleName, methodName);

            return asParallel
                ? columnHeaderList.AsParallel().AsOrdered().WithCancellation(ct).Select(a => GetMainGroups(a.GroupColumnHeaders, gkFileTag, gkAlphabet, gkStats, gkDimension, gkCategory, gkSource, gkGroup, gkMember, gkPerspective,ct:ct)).ToArray()
                : columnHeaderList.Select(a => GetMainGroups(a.GroupColumnHeaders, gkFileTag, gkAlphabet, gkStats, gkDimension, gkCategory, gkSource, gkGroup, gkMember, gkPerspective,ct:ct)).ToArray();
        }
    }
}