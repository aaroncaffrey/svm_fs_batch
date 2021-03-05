using System.Linq;
using System.Threading;

namespace SvmFsBatch
{
    public static class DataSetGroupMethods
    {
        public const string ModuleName = nameof(DataSetGroupMethods);
        // note: group_index is usually the array gkGroup index, which may be different to the gkGroup number in file

        public static (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] GetMainGroups(DataSet DataSet, bool gkFileTag, bool gkAlphabet, bool gkStats, bool gkDimension, bool gkCategory, bool gkSource, bool gkGroup, bool gkMember, bool gkPerspective, bool asParallel = true, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            const string MethodName = nameof(GetMainGroups);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            Logging.WriteLine("", ModuleName, MethodName);

            Logging.LogExit(ModuleName); return GetMainGroups(DataSet.ColumnHeaderList.Skip(1 /* skip class id column - i.e. don't make a gkGroup for class id */).ToArray(), gkFileTag, gkAlphabet, gkStats, gkDimension, gkCategory, gkSource, gkGroup, gkMember, gkPerspective, asParallel, ct);
        }

        public static DataSetGroupKey[] Ungroup((DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[][] groups, bool asParallel = true, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            const string MethodName = nameof(Ungroup);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            Logging.WriteLine("", ModuleName, MethodName);

            Logging.LogExit(ModuleName); return asParallel
                ? groups.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.SelectMany(a => a.SelectMany(b => b.GroupColumnHeaders).ToArray()).ToArray()
                : groups.SelectMany(a => a.SelectMany(b => b.GroupColumnHeaders).ToArray()).ToArray();
        }

        public static DataSetGroupKey[] Ungroup((DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] groups, bool asParallel = true, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            const string MethodName = nameof(Ungroup);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            Logging.WriteLine("", ModuleName, MethodName);

            Logging.LogExit(ModuleName); return asParallel
                ? groups.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.SelectMany(a => a.GroupColumnHeaders).ToArray()
                : groups.SelectMany(a => a.GroupColumnHeaders).ToArray();
        }

        public static (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] GetMainGroups(DataSetGroupKey[] columnHeaderList, bool gkFileTag, bool gkAlphabet, bool gkStats, bool gkDimension, bool gkCategory, bool gkSource, bool gkGroup, bool gkMember, bool gkPerspective, bool asParallel = true, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            const string MethodName = nameof(GetMainGroups);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            Logging.WriteLine("", ModuleName, MethodName);

            Logging.LogExit(ModuleName); return asParallel
                ? columnHeaderList.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.GroupBy(a => (FileTag: gkFileTag
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
                : columnHeaderList.GroupBy(a => (FileTag: gkFileTag
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

        public static (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[][] GetSubGroups((DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] columnHeaderList, bool gkFileTag, bool gkAlphabet, bool gkStats, bool gkDimension, bool gkCategory, bool gkSource, bool gkGroup, bool gkMember, bool gkPerspective, bool asParallel = true, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            const string MethodName = nameof(GetSubGroups);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            Logging.WriteLine("", ModuleName, MethodName);

            Logging.LogExit(ModuleName); return asParallel
                ? columnHeaderList.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select(a => GetMainGroups(a.GroupColumnHeaders, gkFileTag, gkAlphabet, gkStats, gkDimension, gkCategory, gkSource, gkGroup, gkMember, gkPerspective,ct:ct)).ToArray()
                : columnHeaderList.Select(a => GetMainGroups(a.GroupColumnHeaders, gkFileTag, gkAlphabet, gkStats, gkDimension, gkCategory, gkSource, gkGroup, gkMember, gkPerspective,ct:ct)).ToArray();
        }
    }
}