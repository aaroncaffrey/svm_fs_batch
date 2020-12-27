using System.Linq;
using System.Threading;

namespace svm_fs_batch
{
    internal class dataset_group_methods
    {
        public const string module_name = nameof(dataset_group_methods);
        // note: group_index is usually the array group index, which may be different to the group number in file

        internal static (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] get_main_groups(CancellationTokenSource cts, dataset_loader dataset, bool file_tag, bool alphabet, bool stats, bool dimension, bool category, bool source, bool @group, bool member, bool perspective, bool as_parallel=true)
        {
            const string method_name = nameof(get_main_groups);

            if (cts.IsCancellationRequested) return default;

            io_proxy.WriteLine("", module_name, method_name);
            
            return get_main_groups(cts, dataset.column_header_list.Skip(1 /* skip class id column - i.e. don't make a group for class id */).ToArray(), file_tag, alphabet, stats, dimension, category, source, @group, member, perspective, as_parallel);
        }

        internal static dataset_group_key[] ungroup(CancellationTokenSource cts, (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[][] groups, bool as_parallel = true)
        {
            const string method_name = nameof(ungroup);

            if (cts.IsCancellationRequested) return default;

            io_proxy.WriteLine("", module_name, method_name);

            return 
                as_parallel
                    ?
                    groups.AsParallel().AsOrdered().WithCancellation(cts.Token).SelectMany(a => a.SelectMany(b => b.group_column_headers).ToArray()).ToArray()
                    :
                    groups.SelectMany(a => a.SelectMany(b => b.group_column_headers).ToArray()).ToArray();
        }

        internal static dataset_group_key[] ungroup(CancellationTokenSource cts, (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] groups, bool as_parallel = true)
        {
            const string method_name = nameof(ungroup);

            if (cts.IsCancellationRequested) return default;

            io_proxy.WriteLine("", module_name, method_name);

            return as_parallel 
                ? 
                groups.AsParallel().AsOrdered().WithCancellation(cts.Token).SelectMany(a => a.group_column_headers).ToArray()
                :
                groups.SelectMany(a => a.group_column_headers).ToArray();
        }

        internal static (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] get_main_groups(CancellationTokenSource cts, dataset_group_key[] column_header_list, bool file_tag, bool alphabet, bool stats, bool dimension, bool category, bool source, bool @group, bool member, bool perspective, bool as_parallel=true)
        {
            const string method_name = nameof(get_main_groups);

            if (cts.IsCancellationRequested) return default;

            io_proxy.WriteLine("", module_name, method_name);

            return as_parallel?
                column_header_list
                    .AsParallel()
                    .AsOrdered()
                    .WithCancellation(cts.Token)
                    .GroupBy
                    (a =>
                        (
                            file_tag: file_tag ? a.value.file_tag : null,
                            alphabet: alphabet ? a.value.alphabet : null,
                            stats: stats ? a.value.stats : null,
                            dimension: dimension ? a.value.dimension : null,
                            category: category ? a.value.category : null,
                            source: source ? a.value.source : null,
                            group: @group ? a.value.@group : null,
                            member: member ? a.value.member : null,
                            perspective: perspective ? a.value.perspective : null
                        )
                    )
                    .Select(
                        a =>
                        (
                            group_key: new dataset_group_key(a.Key),
                            group_column_headers: a.ToArray(),
                            columns: a.Select(b => b.column_index).ToArray()
                        )
                    ).ToArray()
                :
                column_header_list
                    .GroupBy
                    (a =>
                        (
                            file_tag: file_tag ? a.value.file_tag : null,
                            alphabet: alphabet ? a.value.alphabet : null,
                            stats: stats ? a.value.stats : null,
                            dimension: dimension ? a.value.dimension : null,
                            category: category ? a.value.category : null,
                            source: source ? a.value.source : null,
                            group: @group ? a.value.@group : null,
                            member: member ? a.value.member : null,
                            perspective: perspective ? a.value.perspective : null
                        )
                    )
                    .Select(
                        a =>
                        (
                            group_key: new dataset_group_key(a.Key),
                            group_column_headers: a.ToArray(),
                            columns: a.Select(b => b.column_index).ToArray()
                        )
                    ).ToArray();
        }

        internal static (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[][] get_sub_groups(CancellationTokenSource cts, (dataset_group_key group_key, dataset_group_key[] group_column_headers, int[] columns)[] column_header_list, bool file_tag, bool alphabet, bool stats, bool dimension, bool category, bool source, bool @group, bool member, bool perspective, bool as_parallel = true)
        {
            const string method_name = nameof(get_sub_groups);

            if (cts.IsCancellationRequested) return default;

            io_proxy.WriteLine("", module_name, method_name);

            return 
                as_parallel
                    ?
                    column_header_list
                        .AsParallel()
                        .AsOrdered()
                        .WithCancellation(cts.Token)
                        .Select(a => get_main_groups(cts, a.group_column_headers, file_tag, alphabet, stats, dimension, category, source, @group, member, perspective))
                        .ToArray()
                    :
                    column_header_list
                        .Select(a => get_main_groups(cts, a.group_column_headers, file_tag, alphabet, stats, dimension, category, source, @group, member, perspective))
                        .ToArray();
        }
    }
}
