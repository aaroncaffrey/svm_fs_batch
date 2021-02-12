using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal class DataSet
    {
        public const string ModuleName = nameof(DataSet);
        //internal 
        //(
        //    int ClassId,
        //    string ClassName,
        //    int ClassSize,
        //    (
        //        (
        //            int row_index,
        //            int col_index,
        //            string comment_key,
        //            string CommentValue
        //        )[] row_comment,
        //        (
        //            int row_index,
        //            int col_index,
        //            (
        //                int internal_column_index,
        //                int external_column_index,
        //                string file_tag,
        //                string gkAlphabet,
        //                string gkStats,
        //                string gkDimension,
        //                string gkCategory,
        //                string gkSource,
        //                string @gkGroup,
        //                string gkMember,
        //                string gkPerspective
        //            ) column_header,
        //            double row_column_val
        //        )[] row_columns
        //    )[] val_list
        //)[] value_list;

        internal (int ClassId, string ClassName, int ClassSize, int DownSampledClassSize)[] ClassSizes;

        //internal (int internal_column_index, int external_column_index, string file_tag, string gkAlphabet, string gkStats, string gkDimension, string gkCategory, string gkSource, string @gkGroup, string gkMember, string gkPerspective)[] column_header_list;
        internal DataSetGroupKey[] ColumnHeaderList;

        internal (int ClassId, string ClassName, (int row_index, int col_index, string comment_key, string CommentValue)[][] cl_comment_list)[] CommentList;

        // feature values, grouped by class id (with meta data class name and class size)
        // internal List<(int ClassId, string ClassName, int ClassSize, List<((int internal_column_index, int external_column_index, string file_tag, string gkAlphabet, string gkDimension, string gkCategory, string gkSource, string @gkGroup, string gkMember, string gkPerspective) column_header, double fv)[]> val_list)> value_list;

        internal ( int ClassId, string ClassName, int ClassSize, ( ( int row_index, int col_index, string comment_key, string CommentValue )[] row_comment, ( int row_index, int col_index, DataSetGroupKey column_header, double row_column_val )[] row_columns )[] val_list )[] ValueList;

        internal static int[] RemoveDuplicateColumns(DataSet DataSet, int[] queryCols, bool asParallel = false, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            //const string MethodName = nameof(remove_duplicate_columns);
            // remove duplicate columns (may exist in separate groups)
            //var query_col_dupe_check = idr.DataSet_instance_list_grouped.SelectMany(a => a.examples).SelectMany(a => query_cols.Select(b => (query_col: b, fv: a.feature_data[b].fv)).ToList()).GroupBy(b => b.query_col).Select(b => (query_col: b.Key, values: b.Select(c => c.fv).ToList())).ToList();

            if (queryCols == null || queryCols.Length <= 1) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :queryCols; } //throw new ArgumentOutOfRangeException(nameof(query_cols));
            //if (query_cols.Length <= 1) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :query_cols;}

            var queryColDupeCheck = asParallel
                ? queryCols.AsParallel().AsOrdered().WithCancellation(ct).Select(colIndex => DataSet.ValueList.SelectMany(classValues => classValues.val_list.Select((row, rowIndex) =>
                {
                    var rc = row.row_columns[colIndex];

                    if (rc.col_index != colIndex || rc.row_index != rowIndex) throw new Exception();

                     return ct.IsCancellationRequested ? default :rc.row_column_val;
                }).ToArray()).ToArray()).ToArray()
                : queryCols.Select(colIndex => DataSet.ValueList.SelectMany(classValues => classValues.val_list.Select((row, rowIndex) =>
                {
                    var rc = row.row_columns[colIndex];

                    if (rc.col_index != colIndex || rc.row_index != rowIndex) throw new Exception();

                     return ct.IsCancellationRequested ? default :rc.row_column_val;
                }).ToArray()).ToArray()).ToArray();


            var startIndex = queryCols[0] == 0
                ? 1
                : 0;

            var indexPairs = new (int x, int y)[queryColDupeCheck.Length * (queryColDupeCheck.Length - 1) / 2];
            var k = 0;
            for (var i = startIndex; i < queryColDupeCheck.Length; i++)
            for (var j = startIndex; j < queryColDupeCheck.Length; j++)
            {
                if (i <= j) continue;

                indexPairs[k++] = (i, j);
            }

            var seqEq = asParallel
                ? indexPairs.AsParallel().AsOrdered().WithCancellation(ct).Select(a => queryColDupeCheck[a.x].SequenceEqual(queryColDupeCheck[a.y])).ToArray()
                : indexPairs.Select(a => queryColDupeCheck[a.x].SequenceEqual(queryColDupeCheck[a.y])).ToArray();
            var dupeClusters = new List<List<int>>();

            for (k = 0; k < indexPairs.Length; k++)
                if (seqEq[k])
                {
                    var cluster = new List<int>
                    {
                        queryCols[indexPairs[k].x],
                        queryCols[indexPairs[k].y]
                    };
                    var existingClusters = dupeClusters.Where(a => a.Any(b => cluster.Any(c => b == c))).ToArray();

                    for (var e = 0; e < existingClusters.Length; e++)
                    {
                        cluster.AddRange(existingClusters[e]);
                        dupeClusters.Remove(existingClusters[e]);
                    }

                    cluster = cluster.OrderBy(a => a).Distinct().ToList();
                    dupeClusters.Add(cluster);
                }

            var indexesToRemove = asParallel
                ? dupeClusters.AsParallel().AsOrdered().WithCancellation(ct).Where(dc => dc != null && dc.Count > 1).SelectMany(dc => dc.Skip(1).ToArray()).ToArray()
                : dupeClusters.Where(dc => dc != null && dc.Count > 1).SelectMany(dc => dc.Skip(1).ToArray()).ToArray();


            if (indexesToRemove.Length > 0)
            {
                var ret = queryCols.Except(indexesToRemove).ToArray();
//#if DEBUG
//                Logging.WriteLine($"Removed duplicate columns: [{string.Join(", ", indexes_to_remove)}].", program.ModuleName, MethodName);
//                Logging.WriteLine($"Duplicate columns: [{string.Join(", ", dupe_clusters.Select(a => $"[{string.Join(", ", a)}]").ToArray())}].", program.ModuleName, MethodName);
//                Logging.WriteLine($"Preserved columns: [{string.Join(", ", ret)}].", program.ModuleName, MethodName);
//#endif
                Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :ret;
            }

            Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :queryCols;
        }

        internal double[][] GetRowFeatures((int ClassId, int[] row_indexes)[] classRowIndexes, int[] columnIndexes, bool asParallel = false, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            if (columnIndexes.First() != 0) throw new Exception(); // class id missing

            var classRows = asParallel
                ? classRowIndexes.AsParallel().AsOrdered().WithCancellation(ct).Select(classRowIndex => GetClassRowFeatures(classRowIndex.ClassId, classRowIndex.row_indexes, columnIndexes)).ToList()
                : classRowIndexes.Select(classRowIndex => GetClassRowFeatures(classRowIndex.ClassId, classRowIndex.row_indexes, columnIndexes)).ToList();

            var rows = classRows.SelectMany(a => a.as_rows).ToArray();

            Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :rows;
        }

        internal static Scaling[] GetScalingParams(double[][] rows, int[] columnIndexes)
        {
            Logging.LogCall(ModuleName);

            var cols = columnIndexes.Select((columnIndex, xIndex) => rows.Select(row => row[xIndex /* column_index -> x_index*/]).ToArray()).ToArray();
            var sp = cols.Select((col, xIndex) => xIndex == 0 /* do not scale class id */
                ? null
                : new Scaling(col)).ToArray();

            Logging.LogExit(ModuleName); return sp;
        }

        public static double[][] GetScaledRows(double[][] rows, /*List<int> column_indexes,*/ Scaling[] sp, Scaling.ScaleFunction sf)
        {
            Logging.LogCall(ModuleName);

            //var cols = column_indexes.Select((column_index, x_index) => rows.Select(row => row[x_index /* column_index -> x_index*/]).ToArray()).ToArray();
            //var cols_scaled = cols.Select((v, x_index) =)

            var rowsScaled = rows.Select((row, rowIndex) => row.Select((colVal, colIndex) => colIndex != 0
                ? sp[colIndex].Scale(colVal, sf)
                : colVal).ToArray()).ToArray();

            Logging.LogExit(ModuleName); return rowsScaled;
        }

        internal (double[ /*row*/][ /*col*/] as_rows, double[ /*col*/][ /*row*/] as_cols) GetClassRowFeatures(int classId, int[] rowIndexes, int[] columnIndexes)
        {
            Logging.LogCall(ModuleName);

            if (columnIndexes.First() != 0) throw new Exception(); // class id missing

            var asRows = new double[rowIndexes.Length][];
            var asCols = new double[columnIndexes.Length][];

            var v = ValueList.First(a => a.ClassId == classId).val_list;

            for (var yIndex = 0; yIndex < rowIndexes.Length; yIndex++)
            {
                var rowIndex = rowIndexes[yIndex];
                asRows[yIndex] = new double[columnIndexes.Length];

                for (var xIndex = 0; xIndex < columnIndexes.Length; xIndex++)
                {
                    var colIndex = columnIndexes[xIndex];
                    //as_rows[row_index][col_index] = v[row_index].row_columns[col_index].row_column_val;
                    asRows[yIndex][xIndex] = v[rowIndex].row_columns[colIndex].row_column_val;
                }
            }


            for (var xIndex = 0; xIndex < columnIndexes.Length; xIndex++)
            {
                var colIndex = columnIndexes[xIndex];
                asCols[xIndex] = new double[rowIndexes.Length];

                for (var yIndex = 0; yIndex < rowIndexes.Length; yIndex++)
                {
                    var rowIndex = rowIndexes[yIndex];
                    //as_cols[col_index][row_index] = v[row_index].row_columns[col_index].row_column_val;
                    asCols[xIndex][yIndex] = v[rowIndex].row_columns[colIndex].row_column_val;
                }
            }

            Logging.LogExit(ModuleName); return (asRows, asCols);
        }

        //internal async Task DataSet_loader(CancellationToken ct, string[] DataSet_names)// = "[1i.aaindex]")//"2i,2n")//, bool split_by_file_tag = true, bool split_by_groups = true)
        //{
        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }
        //    if (DataSet_names == null || DataSet_names.Length == 0 || DataSet_names.Any(string.IsNullOrWhiteSpace)) throw new ArgumentOutOfRangeException(nameof(DataSet_names));

        //    //var required_default = false;
        //    //var required_matches = new List<(bool required, string gkAlphabet, string gkStats, string gkDimension, string gkCategory, string gkSource, string @gkGroup, string gkMember, string gkPerspective)>();
        //    //required_matches.Add((required: true, gkAlphabet: null, gkDimension: null, gkCategory: null, gkSource: null, gkGroup: null, gkMember: null, gkPerspective: null));

        //    // file tags: 1i, 1n, 1p, 2i, 2n, 2p, 3i, 3n, 3p (1d - linear, 2d - predicted, 3d - actual, interface, neighborhood, protein)

        //    await LoadDataSet(
        //        ct,
        //        DataSet_folder: program.program_args.DataSet_dir,
        //        file_tags: DataSet_names,//.Split(',', StringSplitOptions.RemoveEmptyEntries), // "2i"
        //        ClassNames: program.program_args.ClassNames,
        //        perform_integrity_checks: false,
        //        required_default: required_default,
        //        required_matches: required_matches
        //    );

        //}

        //private async Task LoadDataSetHeadersAsync(List<(int ClassId, string ClassName, List<(string file_tag, int ClassId, string ClassName, string filename)> values_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> header_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> comment_csv_filenames)> dataFilenames, CancellationToken ct)
        //{
        //    Logging.LogCall(ModuleName);

        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

        //    // 1. headers
        //    Logging.WriteLine(@"Start: reading headers.", ModuleName);
        //    var swHeader = Stopwatch.StartNew();

        //    ColumnHeaderList = dataFilenames.First( /* headers are same for all classes, so only load first class headers */).header_csv_filenames.AsParallel().AsOrdered().WithCancellation(ct).SelectMany((fileInfo, fileIndex) =>
        //    {
        //        Logging.LogExit(ModuleName); return IoProxy.ReadAllLinesAsync(true, ct, fileInfo.filename, callerModuleName: ModuleName).Result.Skip(fileIndex == 0
        //            ? 1
        //            : 2 /*skip header line, and if not first file, class id line too */).AsParallel().AsOrdered().WithCancellation(ct).Select((line, lineIndex) =>
        //            {
        //                var row = line.Split(',');

        //                if (row.Length == 9)
        //                    Logging.LogExit(ModuleName); return new DataSetGroupKey(
        //                        //internal_column_index: -1, 
        //                        //external_column_index: line_index /*int.Parse(row[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo)*/,
        //                        fileIndex == 0 && lineIndex == 0 /* class id isn't associated with any particular file */
        //                            ? ""
        //                            : fileInfo.file_tag,
        //                        row[1],
        //                        row[2],
        //                        row[3],
        //                        row[4],
        //                        row[5],
        //                        row[6],
        //                        row[7],
        //                        row[8]);
        //                if (row.Length == 8)
        //                    Logging.LogExit(ModuleName); return new DataSetGroupKey(
        //                        //internal_column_index: -1, 
        //                        //external_column_index: line_index /*int.Parse(row[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo)*/,
        //                        fileIndex == 0 && lineIndex == 0 /* class id isn't associated with any particular file */
        //                            ? ""
        //                            : fileInfo.file_tag,
        //                        row[1],
        //                        "",
        //                        row[2],
        //                        row[3],
        //                        row[4],
        //                        row[5],
        //                        row[6],
        //                        row[7]);
        //                throw new Exception();
        //            }).ToArray();
        //    }).ToArray();

        //    Parallel.For(0,
        //        ColumnHeaderList.Length,
        //        i =>
        //        {
        //            ColumnHeaderList[i].gkGroupIndex = i;
        //            ColumnHeaderList[i].gkColumnIndex = i;
        //        });


        //    //header_list = header_list.AsParallel().AsOrdered().Select((a, internal_column_index) => (internal_column_index, a.external_column_index, a.file_tag, a.gkAlphabet, a.gkStats, a.gkDimension, a.gkCategory, a.gkSource, a.@gkGroup, a.gkMember, a.gkPerspective)).ToArray();
        //    swHeader.Stop();
        //    Logging.WriteLine($@"Finish: reading headers ({swHeader.Elapsed}).", ModuleName);
        //}


        private void LoadDataSetHeaders(List<(int ClassId, string ClassName, List<(string file_tag, int ClassId, string ClassName, string filename)> values_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> header_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> comment_csv_filenames)> dataFilenames, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            // 1. headers
            Logging.WriteLine(@"Start: reading headers.", ModuleName);
            var swHeader = Stopwatch.StartNew();

            ColumnHeaderList = dataFilenames.First( /* headers are same for all classes, so only load first class headers */).header_csv_filenames.AsParallel().AsOrdered().WithCancellation(ct).SelectMany((fileInfo, fileIndex) =>
            {
                return IoProxy.ReadAllLines(true, ct, fileInfo.filename, callerModuleName: ModuleName).Skip(fileIndex == 0
                    ? 1
                    : 2 /*skip header line, and if not first file, class id line too */).AsParallel().AsOrdered().WithCancellation(ct).Select((line, lineIndex) =>
                    {
                        var row = line.Split(',');

                        if (row.Length == 9)
                        { return new DataSetGroupKey(
                                //internal_column_index: -1, 
                                //external_column_index: line_index /*int.Parse(row[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo)*/,
                                fileIndex == 0 && lineIndex == 0 /* class id isn't associated with any particular file */
                                    ? ""
                                    : fileInfo.file_tag,
                                row[1],
                                row[2],
                                row[3],
                                row[4],
                                row[5],
                                row[6],
                                row[7],
                                row[8]);}
                        if (row.Length == 8)
                        { return new DataSetGroupKey(
                                //internal_column_index: -1, 
                                //external_column_index: line_index /*int.Parse(row[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo)*/,
                                fileIndex == 0 && lineIndex == 0 /* class id isn't associated with any particular file */
                                    ? ""
                                    : fileInfo.file_tag,
                                row[1],
                                "",
                                row[2],
                                row[3],
                                row[4],
                                row[5],
                                row[6],
                                row[7]);}
                        throw new Exception();
                    }).ToArray();
            }).ToArray();

            Parallel.For(0,
                ColumnHeaderList.Length,
                i =>
                {
                    ColumnHeaderList[i].gkGroupIndex = i;
                    ColumnHeaderList[i].gkColumnIndex = i;
                });


            //header_list = header_list.AsParallel().AsOrdered().Select((a, internal_column_index) => (internal_column_index, a.external_column_index, a.file_tag, a.gkAlphabet, a.gkStats, a.gkDimension, a.gkCategory, a.gkSource, a.@gkGroup, a.gkMember, a.gkPerspective)).ToArray();
            swHeader.Stop();
            Logging.WriteLine($@"Finish: reading headers ({swHeader.Elapsed}).", ModuleName);

            Logging.LogExit(ModuleName);
        }

        //private async Task LoadDataSetCommentsAsync(List<(int ClassId, string ClassName, List<(string file_tag, int ClassId, string ClassName, string filename)> values_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> header_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> comment_csv_filenames)> dataFilenames, CancellationToken ct)
        //{
        //    Logging.LogCall(ModuleName);

        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

        //    const string methodName = nameof(LoadDataSetCommentsAsync);

        //    // 2. comment files. (same class with same samples = same data)
        //    Logging.WriteLine(@"Start: reading comments.", ModuleName, methodName);
        //    var swComment = Stopwatch.StartNew();

        //    var commentList2 = dataFilenames.AsParallel().AsOrdered().WithCancellation(ct).Select(async cl =>
        //    {
        //        var commentLines = (await IoProxy.ReadAllLinesAsync(true, ct, cl.comment_csv_filenames.First().filename, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false)).AsParallel().AsOrdered().WithCancellation(ct).Select(line => line.Split(',')).ToArray();
        //        var commentHeader = commentLines.First();
        //        var clCommentList = commentLines.Skip(1 /*skip header*/).AsParallel().AsOrdered().WithCancellation(ct).Select((rowSplit, rowIndex) =>
        //        {
        //            var keyValueList = rowSplit.AsParallel().AsOrdered().WithCancellation(ct).Select((colData, colIndex) => (row_index: rowIndex, col_index: colIndex, comment_key: commentHeader[colIndex], CommentValue: colData)).ToArray();

        //            Logging.LogExit(ModuleName); return keyValueList;
        //        }).ToArray();
        //        Logging.LogExit(ModuleName); return (cl.ClassId, cl.ClassName, cl_comment_list: clCommentList);
        //    }).ToArray();

        //    CommentList = await Task.WhenAll(commentList2).ConfigureAwait(false);

        //    swComment.Stop();
        //    Logging.WriteLine($@"Finish: reading comments ({swComment.Elapsed}).", ModuleName, methodName);
        //}

        private void LoadDataSetComments(List<(int ClassId, string ClassName, List<(string file_tag, int ClassId, string ClassName, string filename)> values_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> header_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> comment_csv_filenames)> dataFilenames, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            const string methodName = nameof(LoadDataSetComments);

            // 2. comment files. (same class with same samples = same data)
            Logging.WriteLine(@"Start: reading comments.", ModuleName, methodName);
            var swComment = Stopwatch.StartNew();

            var commentList2 = dataFilenames.AsParallel().AsOrdered().WithCancellation(ct).Select(cl =>
            {
                var commentLines = (IoProxy.ReadAllLines(true, ct, cl.comment_csv_filenames.First().filename, callerModuleName: ModuleName, callerMethodName: methodName)).AsParallel().AsOrdered().WithCancellation(ct).Select(line => line.Split(',')).ToArray();
                var commentHeader = commentLines.First();
                var clCommentList = commentLines.Skip(1 /*skip header*/).AsParallel().AsOrdered().WithCancellation(ct).Select((rowSplit, rowIndex) =>
                {
                    var keyValueList = rowSplit.AsParallel().AsOrdered().WithCancellation(ct).Select((colData, colIndex) => (row_index: rowIndex, col_index: colIndex, comment_key: commentHeader[colIndex], CommentValue: colData)).ToArray();

                     return keyValueList;
                }).ToArray();
                 return (cl.ClassId, cl.ClassName, cl_comment_list: clCommentList);
            }).ToArray();

            CommentList = commentList2;

            swComment.Stop();
            Logging.WriteLine($@"Finish: reading comments ({swComment.Elapsed}).", ModuleName, methodName);

            Logging.LogExit(ModuleName);
        }

        //private async Task LoadDataSetValuesAsync(List<(int ClassId, string ClassName, List<(string file_tag, int ClassId, string ClassName, string filename)> values_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> header_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> comment_csv_filenames)> dataFilenames, CancellationToken ct)
        //{
        //    Logging.LogCall(ModuleName);

        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            


        //    var t1 = Task.Run(async () => await LoadDataSetHeadersAsync(dataFilenames, ct).ConfigureAwait(false), ct);
        //    var t2 = Task.Run(async () => await LoadDataSetCommentsAsync(dataFilenames, ct).ConfigureAwait(false), ct);
        //    await Task.WhenAll(t1, t2).ConfigureAwait(false);

        //    if (ColumnHeaderList == null || ColumnHeaderList.Length == 0) throw new Exception();
        //    if (CommentList == null || CommentList.Length == 0) throw new Exception();

        //    // 3. values
        //    Logging.WriteLine(@"Start: reading values.", ModuleName);
        //    var swValues = Stopwatch.StartNew();

        //    ValueList = dataFilenames.AsParallel().AsOrdered().WithCancellation(ct).Select((cl, clIndex) =>
        //    {
        //        // 3. experimental sample data
        //        var valsTag = cl.values_csv_filenames.AsParallel().AsOrdered().WithCancellation(ct).Select((fileInfo, fileInfoIndex) => IoProxy.ReadAllLinesAsync(true, ct, fileInfo.filename, callerModuleName: ModuleName).ConfigureAwait(false).GetAwaiter().GetResult() //.Result
        //            .Skip(1 /*skip header - col index only*/).AsParallel().AsOrdered().WithCancellation(ct).Select((row, rowIndex) => row.Split(',').Skip(fileInfoIndex == 0
        //                ? 0
        //                : 1 /*skip class id*/).AsParallel().AsOrdered().WithCancellation(ct).Select((col, colIndex) => double.Parse(col, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray()).ToArray()).ToArray();

        //        var vals = new double[valsTag.First().Length /* number of rows */][ /* columns */];

        //        for (var rowIndex = 0; rowIndex < vals.Length; rowIndex++)
        //            //vals[row_index] = new double[vals_tag.Sum(a=> a[row_index].Length)];
        //            vals[rowIndex] = valsTag.SelectMany((aCl, aClIndex) => aCl[rowIndex]).ToArray();

        //        var valList = vals.AsParallel().AsOrdered().WithCancellation(ct).Select((row, rowIndex) => (row_comment: CommentList[clIndex].cl_comment_list[rowIndex], row_columns: row.Select((colVal, colIndex) => (row_index: rowIndex, col_index: colIndex, column_header: ColumnHeaderList[colIndex], row_column_val: vals[rowIndex][colIndex])).ToArray())).ToArray();
        //        Logging.LogExit(ModuleName); return (cl.ClassId, cl.ClassName, ClassSize: valList.Length, /*comment_list[cl_index].cl_comment_list,*/ val_list: valList);
        //    }).ToArray();
        //    swValues.Stop();
        //    Logging.WriteLine($@"Finish: reading values ({swValues.Elapsed}).", ModuleName);
        //}

        private void LoadDataSetValues(List<(int ClassId, string ClassName, List<(string file_tag, int ClassId, string ClassName, string filename)> values_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> header_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> comment_csv_filenames)> dataFilenames, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            const string methodName = nameof(LoadDataSetValues);

            var t1 = Task.Run(() => LoadDataSetHeaders(dataFilenames, ct), ct);
            var t2 = Task.Run(() => LoadDataSetComments(dataFilenames, ct), ct);
            Task.WaitAll(new[] {t1, t2},ct);

            if (ColumnHeaderList == null || ColumnHeaderList.Length == 0) throw new Exception();
            if (CommentList == null || CommentList.Length == 0) throw new Exception();

            // 3. values
            Logging.WriteLine(@"Start: reading values.", ModuleName, methodName);
            var swValues = Stopwatch.StartNew();

            ValueList = dataFilenames.AsParallel().AsOrdered().WithCancellation(ct).Select((cl, clIndex) =>
            {
                // 3. experimental sample data
                var valsTag = cl.values_csv_filenames.AsParallel().AsOrdered().WithCancellation(ct).Select((fileInfo, fileInfoIndex) => IoProxy.ReadAllLines(true, ct, fileInfo.filename, callerModuleName: ModuleName)
                    .Skip(1 /*skip header - col index only*/).AsParallel().AsOrdered().WithCancellation(ct).Select((row, rowIndex) => row.Split(',').Skip(fileInfoIndex == 0
                        ? 0
                        : 1 /*skip class id*/).AsParallel().AsOrdered().WithCancellation(ct).Select((col, colIndex) => double.Parse(col, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray()).ToArray()).ToArray();

                var vals = new double[valsTag.First().Length /* number of rows */][ /* columns */];

                for (var rowIndex = 0; rowIndex < vals.Length; rowIndex++)
                    //vals[row_index] = new double[vals_tag.Sum(a=> a[row_index].Length)];
                    vals[rowIndex] = valsTag.SelectMany((aCl, aClIndex) => aCl[rowIndex]).ToArray();

                var valList = vals.AsParallel().AsOrdered().WithCancellation(ct).Select((row, rowIndex) => (row_comment: CommentList[clIndex].cl_comment_list[rowIndex], row_columns: row.Select((colVal, colIndex) => (row_index: rowIndex, col_index: colIndex, column_header: ColumnHeaderList[colIndex], row_column_val: vals[rowIndex][colIndex])).ToArray())).ToArray();
                 return (cl.ClassId, cl.ClassName, ClassSize: valList.Length, /*comment_list[cl_index].cl_comment_list,*/ val_list: valList);
            }).ToArray();
            swValues.Stop();
            Logging.WriteLine($@"Finish: reading values ({swValues.Elapsed}).", ModuleName, methodName);

            Logging.LogExit(ModuleName);
        }

        private List<(int ClassId, string ClassName, List<(string file_tag, int ClassId, string ClassName, string filename)> values_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> header_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> comment_csv_filenames)> GetDataFilenames(string DataSetFolder, string[] fileTags, IList<(int ClassId, string ClassName)> classNames)
        {
            Logging.LogCall(ModuleName);

            const string methodName = nameof(GetDataFilenames);

            var dataFilenames = classNames.Select(cl =>
            {
                // (string file_tag, int ClassId, string ClassName, string filename)
                var valuesCsvFilenames = fileTags.Select(gkFileTag => (file_tag: gkFileTag, cl.ClassId, cl.ClassName, filename: Path.Combine(DataSetFolder, $@"f_({gkFileTag})_({cl.ClassId:+#;-#;+0})_({cl.ClassName}).csv"))).ToList();
                var headerCsvFilenames = fileTags.Select(gkFileTag => (file_tag: gkFileTag, cl.ClassId, cl.ClassName, filename: Path.Combine(DataSetFolder, $@"h_({gkFileTag})_({cl.ClassId:+#;-#;+0})_({cl.ClassName}).csv"))).ToList();
                var commentCsvFilenames = fileTags.Select(gkFileTag => (file_tag: gkFileTag, cl.ClassId, cl.ClassName, filename: Path.Combine(DataSetFolder, $@"c_({gkFileTag})_({cl.ClassId:+#;-#;+0})_({cl.ClassName}).csv"))).ToList();

                 return (cl.ClassId, cl.ClassName, values_csv_filenames: valuesCsvFilenames, header_csv_filenames: headerCsvFilenames, comment_csv_filenames: commentCsvFilenames);
            }).ToList();

            foreach (var cl in dataFilenames)
            {
                Logging.WriteLine($@"{nameof(cl.values_csv_filenames)}: {string.Join(", ", cl.values_csv_filenames)}", ModuleName, methodName);
                Logging.WriteLine($@"{nameof(cl.header_csv_filenames)}: {string.Join(", ", cl.header_csv_filenames)}", ModuleName, methodName);
                Logging.WriteLine($@"{nameof(cl.comment_csv_filenames)}: {string.Join(", ", cl.comment_csv_filenames)}", ModuleName, methodName);
            }

            Logging.LogExit(ModuleName); return dataFilenames;
        }

        private void CheckDataFiles(List<(int ClassId, string ClassName, List<(string file_tag, int ClassId, string ClassName, string filename)> values_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> header_csv_filenames, List<(string file_tag, int ClassId, string ClassName, string filename)> comment_csv_filenames)> dataFilenames)
        {
            Logging.LogCall(ModuleName);

            const string methodName = nameof(CheckDataFiles);

            // don't try to read any data until checking all files exist...
            if (dataFilenames == null || dataFilenames.Count == 0) throw new Exception();
            foreach (var cl in dataFilenames)
            {
                if (cl.values_csv_filenames == null || cl.values_csv_filenames.Count == 0 || cl.values_csv_filenames.Any(a => string.IsNullOrWhiteSpace(a.filename))) throw new Exception($@"{ModuleName}.{methodName}: {nameof(cl.values_csv_filenames)} is empty");
                if (cl.header_csv_filenames == null || cl.header_csv_filenames.Count == 0 || cl.header_csv_filenames.Any(a => string.IsNullOrWhiteSpace(a.filename))) throw new Exception($@"{ModuleName}.{methodName}: {nameof(cl.header_csv_filenames)} is empty");
                if (cl.comment_csv_filenames == null || cl.comment_csv_filenames.Count == 0 || cl.comment_csv_filenames.Any(a => string.IsNullOrWhiteSpace(a.filename))) throw new Exception($@"{ModuleName}.{methodName}: {nameof(cl.comment_csv_filenames)} is empty");

                if (cl.values_csv_filenames.Any(b => !IoProxy.ExistsFile(false, b.filename, ModuleName, methodName))) throw new Exception($@"{ModuleName}.{methodName}: missing input files: {string.Join(@", ", cl.values_csv_filenames.Where(a => !File.Exists(a.filename) || new FileInfo(a.filename).Length == 0).Select(a => a.filename).ToArray())}");
                if (cl.header_csv_filenames.Any(b => !IoProxy.ExistsFile(false, b.filename, ModuleName, methodName))) throw new Exception($@"{ModuleName}.{methodName}: missing input files: {string.Join(@", ", cl.header_csv_filenames.Where(a => !File.Exists(a.filename) || new FileInfo(a.filename).Length == 0).Select(a => a.filename).ToArray())}");
                if (cl.comment_csv_filenames.Any(b => !IoProxy.ExistsFile(false, b.filename, ModuleName, methodName))) throw new Exception($@"{ModuleName}.{methodName}: missing input files: {string.Join(@", ", cl.comment_csv_filenames.Where(a => !File.Exists(a.filename) || new FileInfo(a.filename).Length == 0).Select(a => a.filename).ToArray())}");
            }

            Logging.LogExit(ModuleName);
        }


        //internal async Task LoadDataSetAsync(string DataSetFolder, string[] fileTags /*DataSet_names*/, IList<(int ClassId, string ClassName)> classNames //,
        //    //bool perform_integrity_checks = false,
        //    //bool required_default = true,
        //    //IList<(bool required, string gkAlphabet, string gkStats, string gkDimension, string gkCategory, string gkSource, string @gkGroup, string gkMember, string gkPerspective)> required_matches = null
        //    , CancellationToken ct)
        //{
        //    Logging.LogCall(ModuleName);

        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

        //    const string methodName = nameof(LoadDataSetAsync);


        //    if (fileTags == null || fileTags.Length == 0 || fileTags.Any(string.IsNullOrWhiteSpace)) throw new ArgumentOutOfRangeException(nameof(fileTags));


        //    classNames = classNames.OrderBy(a => a.ClassId).ToList();
        //    foreach (var cl in classNames) Logging.WriteLine($@"{cl.ClassId:+#;-#;+0} = {cl.ClassName}", ModuleName, methodName);

        //    fileTags = fileTags.OrderBy(a => a).ToArray();
        //    foreach (var gkFileTag in fileTags) Logging.WriteLine($@"{gkFileTag}: {gkFileTag}", ModuleName, methodName);

        //    var dataFilenames = GetDataFilenames(DataSetFolder, fileTags, classNames);
        //    CheckDataFiles(dataFilenames);


        //    await LoadDataSetValuesAsync(dataFilenames, ct).ConfigureAwait(false);

        //    ClassSizes = ValueList.Select(a => (a.ClassId, a.ClassSize)).ToArray();
        //}

        internal void LoadDataSet(string DataSetFolder, string[] fileTags /*DataSet_names*/, IList<(int ClassId, string ClassName)> classNames //,
            //bool perform_integrity_checks = false,
            //bool required_default = true,
            //IList<(bool required, string gkAlphabet, string gkStats, string gkDimension, string gkCategory, string gkSource, string @gkGroup, string gkMember, string gkPerspective)> required_matches = null
            , CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            const string methodName = nameof(LoadDataSet);


            if (fileTags == null || fileTags.Length == 0 || fileTags.Any(string.IsNullOrWhiteSpace)) throw new ArgumentOutOfRangeException(nameof(fileTags));


            classNames = classNames.OrderBy(a => a.ClassId).ToList();
            foreach (var cl in classNames) Logging.WriteLine($@"{cl.ClassId:+#;-#;+0} = {cl.ClassName}", ModuleName, methodName);

            fileTags = fileTags.OrderBy(a => a).ToArray();
            foreach (var gkFileTag in fileTags) Logging.WriteLine($@"{gkFileTag}: {gkFileTag}", ModuleName, methodName);

            var dataFilenames = GetDataFilenames(DataSetFolder, fileTags, classNames);
            CheckDataFiles(dataFilenames);

            LoadDataSetValues(dataFilenames, ct);

            ClassSizes = ValueList.Select(a => (a.ClassId, a.ClassName, a.ClassSize, DownSampledClassSize: ValueList.Where(a => a.ClassSize > 0).Min(a => a.ClassSize))).ToArray();

            Logging.LogExit(ModuleName);
        }
    }
}
//{
//    // all comment headers should be equal
//    var CommentHeaders = file_data.SelectMany(a => a.comment_csv_data.Select(b => b.header_row).ToList()).ToList();
//    for (var ci = 0; ci < CommentHeaders.Count; ci++)
//    {
//        for (var cj = 0; cj < CommentHeaders.Count; cj++)
//        {
//            if (cj <= ci) continue;

//            if (!CommentHeaders[ci].SequenceEqual(CommentHeaders[cj])) throw new Exception();
//        }
//    }
//}

//{
//    // all headers headers should be equal
//    var headers_headers = file_data.SelectMany(a => a.header_csv_data.Select(b => b.header_row).ToList()).ToList();
//    for (var ci = 0; ci < headers_headers.Count; ci++)
//    {
//        for (var cj = 0; cj < headers_headers.Count; cj++)
//        {
//            if (cj <= ci) continue;

//            if (!headers_headers[ci].SequenceEqual(headers_headers[cj])) throw new Exception();
//        }
//    }
//}


// READ HEADER CSV FILE - ALL CLASSES HAVE THE SAME HEADERS/FEATURES within the same file_tag (will change for each one, e.g. 1i, 1n, 1p, 2i, 2n, 2p, 3i, 3n, 3p)
// same file_tag -> same headers... different file_tag -> different headers
//file_tags.Select(a=>a.).Select(a => file_data.First(b => b.header_csv_data.First(c => c.file_tag == a));
//var all_headers = file_data.SelectMany(a => a.header_csv_data).ToList();
//var first_headers = file_tags.Select(a => all_headers.First(b => b.file_tag == a)).ToList();


//var feature_catalog = first_headers.AsParallel().AsOrdered().SelectMany((tag_headers, tag_headers_index) =>
//    tag_headers.data_rows.Select((row, row_index) =>
//    //.SelectMany((cl, cl_index) => 
//    //cl.header_csv_data.First(/* first file - all file_tag header files are the same */).data_rows.AsParallel().AsOrdered().Select((row, row_index) =>
//    {
//        var internal_fid = row_index;
//        for (var i = 0; i < tag_headers_index; i++)
//        {
//            internal_fid += first_headers[i].data_rows.Count;
//        }

//        //if (file_tag_index != 0 && row_index == 0)
//        //{
//        //}


//        var external_fid = int.Parse(row[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
//        //if (fid!=i) throw new Exception();


//        var gkAlphabet = row[1];
//        var gkDimension = row[2];
//        var gkCategory = row[3];
//        var gkSource = row[4];
//        var gkGroup = row[5];
//        var gkMember = row[6];
//        var gkPerspective = row[7];


//        const string def = "default";

//        if (string.IsNullOrWhiteSpace(gkAlphabet)) gkAlphabet = def;
//        if (string.IsNullOrWhiteSpace(gkDimension)) gkDimension = def;
//        if (string.IsNullOrWhiteSpace(gkCategory)) gkCategory = def;
//        if (string.IsNullOrWhiteSpace(gkSource)) gkSource = def;
//        if (string.IsNullOrWhiteSpace(gkGroup)) gkGroup = def;
//        if (string.IsNullOrWhiteSpace(gkMember)) gkMember = def;
//        if (string.IsNullOrWhiteSpace(gkPerspective)) gkPerspective = def;

//        lock (lock_table)
//        {
//            if (!table_alphabet.Contains(gkAlphabet)) { table_alphabet.Add(gkAlphabet); }
//            if (!table_dimension.Contains(gkDimension)) { table_dimension.Add(gkDimension); }
//            if (!table_category.Contains(gkCategory)) { table_category.Add(gkCategory); }
//            if (!table_source.Contains(gkSource)) { table_source.Add(gkSource); }
//            if (!table_group.Contains(gkGroup)) { table_group.Add(gkGroup); }
//            if (!table_member.Contains(gkMember)) { table_member.Add(gkMember); }
//            if (!table_perspective.Contains(gkPerspective)) { table_perspective.Add(gkPerspective); }
//        }

//        var alphabet_id = table_alphabet.LastIndexOf(gkAlphabet);
//        var dimension_id = table_dimension.LastIndexOf(gkDimension);
//        var category_id = table_category.LastIndexOf(gkCategory);
//        var source_id = table_source.LastIndexOf(gkSource);
//        var group_id = table_group.LastIndexOf(gkGroup);
//        var member_id = table_member.LastIndexOf(gkMember);
//        var perspective_id = table_perspective.LastIndexOf(gkPerspective);

//        gkAlphabet = table_alphabet[alphabet_id];
//        gkDimension = table_dimension[dimension_id];
//        gkCategory = table_category[category_id];
//        gkSource = table_source[source_id];
//        gkGroup = table_group[group_id];
//        gkMember = table_member[member_id];
//        gkPerspective = table_perspective[perspective_id];

//        Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :(
//            internal_fid: internal_fid,
//            external_fid: external_fid,
//            file_tag: tag_headers.file_tag,
//            gkAlphabet: gkAlphabet,
//            gkDimension: gkDimension,
//            gkCategory: gkCategory,
//            gkSource: gkSource,
//            gkGroup: gkGroup,
//            gkMember: gkMember,
//            gkPerspective: gkPerspective
//        );

//        //internal_fid: internal_fid,
//        //fid: fid, 
//        //, alphabet_id: alphabet_id, dimension_id: dimension_id, category_id: category_id, source_id: source_id, group_id: group_id, member_id: member_id, perspective_id: perspective_id
//    }).ToList()).ToList();

////var feature_catalog = feature_catalog_data.Select((a,i) => 
////    (internal_fid:i, a.external_fid, a.file_tag, a.gkAlphabet, a.gkDimension, a.gkCategory, a.gkSource, a.gkGroup, a.gkMember, a.gkPerspective)
////).ToList();

////if (cl.values_csv_data.Select(a => a.data_rows.Select(b => b.Length).ToList()).Distinct().Count() != 1) throw new Exception();

////var sample_data = file_data.Select(cl => cl.values_csv_data.SelectMany(tag => tag.data_key_value_list).ToList()).ToList();
//var sample_data = file_data.Select(cl =>
//{
//    // check same number of samples per tag
//    if (cl.values_csv_data.Select(a => a.data_key_value_list.Count).Distinct().Count() != 1) throw new Exception();

//    var header_row = new List<string>();
//    var data_rows = new List<List<double>>();
//    for (var i = 0; i < )
//    var data_row_key_value_list = new List<(string key, double value, List<(string key, string value)> header, List<(string key, string value)> row_comments)>();

//    for (var tag_index = 0; tag_index < cl.values_csv_data.Count; tag_index++)
//    {
//        header_row.AddRange(cl.values_csv_data[tag_index].header_row);

//        if (tag_index == 0) data_rows.AddRange(cl.values_csv_data[tag_index].data_rows);
//        else data_rows = data_rows.Select(a => a).ToList();

//        data_row_key_value_list.AddRange(cl.values_csv_data[tag_index].data_row_key_value_list);
//    }

//    // merge tags

//    Logging.LogExit(ModuleName); return 0;
//    //Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :cl.values_csv_data.SelectMany(tag => tag.data_key_value_list).ToList();
//}).ToList();

// class [ example id ] [ file_tag , fid ] = fv
// class [ ] 

// limit DataSet to required matches...
/*
bool[][] required = null;
if (required_matches != null && required_matches.Count > 0)
{
    required = new bool[header_data.Count][];
    for (var i = 0; i < header_data.Count; i++)
    {
        required[i] = new bool[header_data[i].headers.Count];
        if (required_default != default(bool)) Array.Fill(required[i], required_default);
    }

    for (var index = 0; index < required_matches.Count; index++)
    {
        var rm = required_matches[index];

        for (var i = 0; i < header_data.Count; i++)
        {

            var matching_fids = header_data[i].headers.AsParallel().AsOrdered().Where(a =>
                matches(a.gkAlphabet, rm.gkAlphabet) &&
                matches(a.gkCategory, rm.gkCategory) &&
                matches(a.gkDimension, rm.gkDimension) &&
                matches(a.gkSource, rm.gkSource) &&
                matches(a.@gkGroup, rm.@gkGroup) &&
                matches(a.gkMember, rm.gkMember) &&
                matches(a.gkPerspective, rm.gkPerspective)
                ).Select(a => a.fid).ToList();


            matching_fids.ForEach(a => required[i][a] = rm.required);
            required[i][0] = true;

            var x = header_data[i].headers.Where((a, i) => required[i][a.fid]).ToList();
            header_data[i] = (header_data[i].file_tag, x);
        }
    }
}*/

//bool[] required = null;
//if (required_matches != null && required_matches.Count > 0)
//{
//    required = new bool[feature_catalog.Count];
//    if (required_default != default(bool)) Array.Fill(required, required_default);

//    for (var index = 0; index < required_matches.Count; index++)
//    {
//        var rm = required_matches[index];

//        var matching_fids = feature_catalog.AsParallel().AsOrdered().Where(a =>
//            matches(a.gkAlphabet, rm.gkAlphabet) &&
//            matches(a.gkCategory, rm.gkCategory) &&
//            matches(a.gkDimension, rm.gkDimension) &&
//            matches(a.gkSource, rm.gkSource) &&
//            matches(a.@gkGroup, rm.@gkGroup) &&
//            matches(a.gkMember, rm.gkMember) &&
//            matches(a.gkPerspective, rm.gkPerspective)
//        ).Select(a => a.internal_fid).ToList();


//        matching_fids.ForEach(a => required[a] = rm.required);

//    }

//    required[0] = true;

//    feature_catalog = feature_catalog.Where((a, i) => required[a.internal_fid]).ToList();
//}


/*
if (perform_integrity_checks)
{
    Logging.WriteLine($@"Checking all DataSet columns are the same length...", _CallerModuleName: ModuleName, _CallerMethodName: MethodName);
    var DataSet_num_diferent_column_length = DataSet_instance_list.Select(a => a.feature_data.Count).Distinct().Count();
    if (DataSet_num_diferent_column_length != 1) throw new Exception();

    Logging.WriteLine($@"Checking DataSet headers and DataSet columns are the same length...", _CallerModuleName: ModuleName, _CallerMethodName: MethodName);
    var header_length = header_data.Count;
    var DataSet_column_length = DataSet_instance_list.First().feature_data.Count;
    if (DataSet_column_length != header_length) throw new Exception();

    Logging.WriteLine($@"Checking all DataSet comment columns are the same length...", _CallerModuleName: ModuleName, _CallerMethodName: MethodName);
    var comments_num_different_column_length = DataSet_instance_list.Select(a => a.comment_columns.Count).Distinct().Count();
    if (comments_num_different_column_length != 1)
    {
        var distinct_comment_counts = DataSet_instance_list.Select(a => a.comment_columns.Count).Distinct().ToList();

        var cc = distinct_comment_counts.Select(a => (a, DataSet_instance_list.Where(b => b.comment_columns.Count == a).Select(b => b.example_id).ToList())).ToList();

        cc.ForEach(a => Console.WriteLine(a.a + ": " + string.Join(", ", a.Item2)));

        throw new Exception();
    }
}
*/

/*
 if (fix_DataSet)

{
    var num_headers_before = DataSet.DataSet_headers.Count;

    remove_empty_features(DataSet, 2);
    remove_empty_features_by_class(DataSet, 2);//, 2, $@"e:\input\feature_stats.csv");
    remove_large_groups(DataSet, 100);
    remove_duplicate_groups(DataSet);
    // remove_duplicate_features(); will be done on the feature selection algorithm, otherwise potential feature groups may lack useful information

    var num_headers_after = DataSet.DataSet_headers.Count;

    if (num_headers_before != num_headers_after)
    {
        save_DataSet(DataSet, new List<(int ClassId, string header_filename, string data_filename)>()
    {
        (+1, Path.Combine(DataSet_folder, $"{Path.GetFileNameWithoutExtension(DataSet_header_csv_files[0])}_updated{Path.GetExtension(DataSet_header_csv_files[0])}"), Path.Combine(DataSet_folder, $"{Path.GetFileNameWithoutExtension(DataSet_csv_files[0])}_updated{Path.GetExtension(DataSet_csv_files[0])}")),
        (-1, Path.Combine(DataSet_folder, $"{Path.GetFileNameWithoutExtension(DataSet_header_csv_files[1])}_updated{Path.GetExtension(DataSet_header_csv_files[1])}"), Path.Combine(DataSet_folder, $"{Path.GetFileNameWithoutExtension(DataSet_csv_files[1])}_updated{Path.GetExtension(DataSet_csv_files[0])}"))
    }); // Path.Combine(DataSet_folder, "updated_headers.csv"), Path.Combine(DataSet_folder, "updated_DataSet.csv"));
    }
}
*/


/*private static bool matches(string text, string search_pattern)
{
    if (string.IsNullOrWhiteSpace(search_pattern) || search_pattern == "*")
    {
        Logging.LogExit(ModuleName); return true;
    }
    else if (search_pattern.StartsWith("*", StringComparison.Ordinal) && search_pattern.EndsWith("*", StringComparison.Ordinal))
    {
        search_pattern = search_pattern[1..^1];

        Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :text.Contains(search_pattern, StringComparison.OrdinalIgnoreCase);
    }
    else if (search_pattern.StartsWith("*", StringComparison.Ordinal))
    {
        search_pattern = search_pattern[1..];

        Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :text.EndsWith(search_pattern, StringComparison.OrdinalIgnoreCase);
    }
    else if (search_pattern.EndsWith("*", StringComparison.Ordinal))
    {
        search_pattern = search_pattern[0..^1];

        Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :text.StartsWith(search_pattern, StringComparison.OrdinalIgnoreCase);
    }
    else
    {
        Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :string.Equals(text, search_pattern, StringComparison.OrdinalIgnoreCase);
    }
}*/

/*
internal static double[][][] Getcolumn_data_by_class(DataSet DataSet) // [column][row]
{
    //Logging.WriteLine("...", ModuleName, nameof(Getcolumn_data_by_class));

    var total_headers = DataSet.DataSet_headers.Count;
    var total_classes = DataSet.DataSet_instance_list.Select(a => a.ClassId).Distinct().Count();

    var result = new double[total_headers][][];

    for (var i = 0; i < DataSet.DataSet_headers.Count; i++)
    {
        var x = DataSet.DataSet_instance_list.GroupBy(a => a.ClassId).Select(a => a.Select(b => b.feature_data[i].fv).ToArray()).ToArray();

        result[i] = x;
    }

    Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :result;
}
*/

/*
internal static double[][] Getcolumn_data(DataSet DataSet) // [column][row]
{
    //Logging.WriteLine("...", ModuleName, nameof(Getcolumn_data));

    var result = new double[DataSet.DataSet_headers.Count][];

    for (var i = 0; i < DataSet.DataSet_headers.Count; i++)
    {
        result[i] = DataSet.DataSet_instance_list.Select(a => a.feature_data[i].fv).ToArray();
    }

    Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :result;
}
*/

/*
internal static void remove_large_groups(DataSet DataSet, int max_group_size)
{
    
    const string MethodName = nameof(remove_large_groups);

    //Logging.WriteLine("...", ModuleName, nameof(remove_large_groups));

    var groups = DataSet.DataSet_headers.Skip(1).GroupBy(a => (a.alphabet_id, a.dimension_id, a.category_id, a.source_id, a.group_id)).OrderBy(a => a.Count()).ToList();

    //groups.ForEach(a => Console.WriteLine(a.Count() + ": " + a.First().gkAlphabet + ", " + a.First().gkDimension + ", " + a.First().gkCategory + ", " + a.First().gkGroup));

    //var sizes = groups.Select(a => a.Count()).Distinct().Select(b=> (size: b, count: groups.Count(c=> c.Count() == b))).OrderByDescending(a => a).ToList();

    //Console.WriteLine("sizes: " + string.Join(", ", sizes));

    var groups_too_large = groups.Where(a => a.Count() > max_group_size).ToList();

    var fids_to_remove = groups_too_large.SelectMany(a => a.Select(b => b.fid).ToList()).ToList();

    if (fids_to_remove != null && fids_to_remove.Count > 0)
    {
        remove_fids(DataSet, fids_to_remove);
    }

    groups_too_large.ForEach(a => Logging.WriteLine($@"Removed large gkGroup: {a.Key}", _CallerModuleName: ModuleName, _CallerMethodName: MethodName));
}
*/

/*
internal static void remove_duplicate_groups(DataSet DataSet)
{
    
    //const string MethodName = nameof(remove_duplicate_groups);

    //Logging.WriteLine("...", ModuleName, nameof(remove_duplicate_groups));


    var column_data = Getcolumn_data(DataSet);

    var grouped_by_groups = DataSet.DataSet_headers.Skip(1).GroupBy(a => (a.alphabet_id, a.dimension_id, a.category_id, a.source_id, a.group_id)).ToList();

    var groups_same_sizes = grouped_by_groups.GroupBy(a => a.Count()).ToList();

    var fids_to_remove = new List<int>();


    foreach (var groups_same_size in groups_same_sizes)
    {
        var group_size = groups_same_size.Key;

        var groups_this_size = groups_same_size.ToList();

        var groups_this_size_columns = groups_this_size.Select(a => a.Select(b => b.fid).ToList()).ToList();

        var groups_this_size_columns_data_flattened = groups_this_size_columns.Select(a => a.SelectMany(b => column_data[b]).OrderBy(c => c).ToList()).ToList();
        var groups_this_size_columns_data = groups_this_size_columns.Select(a => a.Select(b => column_data[b]).ToList()).ToList();

        var clusters = new List<List<int>>();

        for (var i = 0; i < groups_this_size_columns_data_flattened.Count; i++)
        {
            for (var j = 0; j < groups_this_size_columns_data_flattened.Count; j++)
            {
                if (i <= j) continue;

                var g_i = groups_this_size_columns_data_flattened[i];
                var g_j = groups_this_size_columns_data_flattened[j];

                // do simple check
                if (g_i.SequenceEqual(g_j))
                {
                    // do full check to confirm
                    var x_i = groups_this_size_columns_data[i];
                    var x_j = groups_this_size_columns_data[j];

                    if (x_i.All(a => x_j.Any(a.SequenceEqual)))
                    {

                        var c_i = clusters.FirstOrDefault(a => a.Contains(i));
                        var c_j = clusters.FirstOrDefault(a => a.Contains(j));

                        var cluster = new List<int>();
                        cluster.AddRange(c_i ?? new List<int>() { i });
                        cluster.AddRange(c_j ?? new List<int>() { j });
                        cluster = cluster.Distinct().OrderBy(a => a).ToList();
                        if (c_i != null) clusters.Remove(c_i);
                        if (c_j != null) clusters.Remove(c_j);
                        clusters.Add(cluster);
                    }
                }

            }
        }

        //Console.WriteLine();



        foreach (var cluster in clusters)
        {
            var cluster_groups = cluster.Select(a => groups_this_size[a]).ToList();

            var groups_ungrouped = cluster_groups.Select(a => a.ToList()).ToList();
            var group_to_keep = groups_ungrouped.First();
            var groups_to_remove = groups_ungrouped.Skip(1).ToList();

            var group_to_keep_key = new string[] { group_to_keep.First().gkAlphabet, group_to_keep.First().gkDimension, group_to_keep.First().gkCategory, group_to_keep.First().gkSource, group_to_keep.First().gkGroup };
            var groups_to_remove_keys = groups_to_remove.Select(a => new string[] { a.First().gkAlphabet, a.First().gkDimension, a.First().gkCategory, a.First().gkSource, a.First().gkGroup }).ToList();

            Console.WriteLine($"+    Keeping: {string.Join(".", group_to_keep_key)}");
            groups_to_remove_keys.ForEach(a => Console.WriteLine($"-   Removing: {string.Join(".", a)}"));


            //var cluster_fids_to_remove = groups_to_remove.SelectMany(a => a.Select(b => b.fid).ToList()).Distinct().OrderBy(a=>a).ToList();
            var cluster_fids_to_remove = groups_to_remove.SelectMany(a => a.Select(b => b.fid).ToList()).ToList();
            fids_to_remove.AddRange(cluster_fids_to_remove);

            //
            var alphabets = new List<string>();
            var dimensions = new List<string>();
            var categories = new List<string>();
            var sources = new List<string>();
            var groups = new List<string>();
            //var members = new List<string>();
            //var perspectives = new List<string>();

            foreach (var cg in cluster_groups)
            {
                foreach (var x in cg)
                {
                    alphabets.Add(x.gkAlphabet);
                    dimensions.Add(x.gkDimension);
                    categories.Add(x.gkCategory);
                    sources.Add(x.gkSource);
                    groups.Add(x.gkGroup);
                    //members.Add(x.gkMember);
                    //perspectives.Add(x.gkPerspective);
                }

                //Console.WriteLine("Duplicate gkGroup: " + string.Join(",", cg.Select(b => b.gkAlphabet + "," + b.gkDimension + "," + b.gkCategory + "," + b.gkSource + "," + b.gkGroup + "," + b.gkMember + "," + b.gkPerspective).ToList()));
            }

            var new_header = (
                gkAlphabet: string.Join("|", alphabets.Distinct().ToList()),
                gkDimension: string.Join("|", dimensions.Distinct().ToList()),
                gkCategory: string.Join("|", categories.Distinct().ToList()),
                gkSource: string.Join("|", sources.Distinct().ToList()),
                gkGroup: string.Join("|", groups.Distinct().ToList())
                //string.Join("|", members.Distinct().ToList()),
                //string.Join("|", perspectives.Distinct().ToList())
                );

            var new_header_str = new string[] { new_header.gkAlphabet, new_header.gkDimension, new_header.gkCategory, new_header.gkSource, new_header.gkGroup };


            Console.WriteLine($"~ new header: {string.Join(".", new_header_str)}");
            Console.WriteLine();

            for (var i = 0; i < group_to_keep.Count; i++)
            {
                DataSet.DataSet_headers[group_to_keep[i].fid] =
                (group_to_keep[i].fid,

                    new_header.gkAlphabet, new_header.gkDimension, new_header.gkCategory, new_header.gkSource, new_header.gkGroup, group_to_keep[i].gkMember, group_to_keep[i].gkPerspective,
                    group_to_keep[i].alphabet_id, group_to_keep[i].dimension_id, group_to_keep[i].category_id, group_to_keep[i].source_id, group_to_keep[i].group_id, group_to_keep[i].member_id, group_to_keep[i].perspective_id

                    );
            }
        }
    }

    if (fids_to_remove != null && fids_to_remove.Count > 0)
    {
        remove_fids(DataSet, fids_to_remove);
    }
}
*/

/*
internal static void save_DataSet(DataSet DataSet, List<(int ClassId, string header_filename, string data_filename)> filenames)
{
    
    const string MethodName = nameof(save_DataSet);

    Logging.WriteLine("started saving...", _CallerModuleName: ModuleName, _CallerMethodName: MethodName);

    //var ClassIds = DataSet.DataSet_instance_list.Select(a => a.ClassId).Distinct().OrderBy(a => a).ToList();
    var header_fids = Enumerable.Range(0, DataSet.DataSet_headers.Count);
    var header_fids_str = string.Join(",", header_fids);

    foreach (var ClassId_filenames in filenames)//ClassIds)
    {
        var data = new List<string>();
        data.Add(header_fids_str);

        DataSet.DataSet_instance_list.Where(a => a.ClassId == ClassId_filenames.ClassId).ToList().
            ForEach(a => data.Add(string.Join(",", a.feature_data.Select(b => b.fv.ToString("G17", NumberFormatInfo.InvariantInfo)).ToList())));

        var header = DataSet.DataSet_headers.Select(a => $@"{a.fid},{a.gkAlphabet},{a.gkDimension},{a.gkCategory},{a.gkSource},{a.gkGroup},{a.gkMember},{a.gkPerspective}").ToList();
        header.Insert(0, $@"fid,gkAlphabet,gkDimension,gkCategory,gkSource,gkGroup,gkMember,gkPerspective");

        await io_proxy.WriteAllLines(true, ClassId_filenames.header_filename, header);

        await io_proxy.WriteAllLines(true, ClassId_filenames.data_filename, data);
    }

    Logging.WriteLine("finished saving...", _CallerModuleName: ModuleName, _CallerMethodName: MethodName);
}
*/

/*
internal static void remove_empty_features(DataSet DataSet, double min_non_zero_pct = 0.25, int min_distinct_numbers = 2)
internal static void remove_empty_features(DataSet DataSet, int min_distinct_numbers = 2)
{
    
    const string MethodName = nameof(remove_empty_features);

    Logging.WriteLine("...", _CallerModuleName: ModuleName, _CallerMethodName: MethodName);

    var empty_fids = new List<int>();

    var column_data = Getcolumn_data(DataSet);


    for (var i = 1; i < DataSet.DataSet_headers.Count; i++)
    {
        var values = column_data[i];

        if (values == null || values.Length == 0)
        {
            continue;
        }

        var values_distinct = values.Distinct().OrderBy(a => a).ToList();
        //var values_count = values_distinct.Select(a => (value: a, count: values.Count(b => a == b))).ToList();

        //var zero = values_count.FirstOrDefault(a => a.value == 0).count;
        //var non_zero = values.Length - zero;
        //var non_zero_pct = (double)non_zero / (double)values.Length;

        if (values_distinct.Count < min_distinct_numbers)// || non_zero_pct < min_non_zero_pct)
        {
            empty_fids.Add(DataSet.DataSet_headers[i].fid);
            break;
        }
    }

    if (empty_fids != null && empty_fids.Count > 0)
    {
        remove_fids(DataSet, empty_fids);
    }

    Logging.WriteLine($@"Removed features ({empty_fids.Count}): {string.Join(",", empty_fids)}", _CallerModuleName: ModuleName, _CallerMethodName: MethodName);

}
*/
/*
//internal static void remove_empty_features_by_class(DataSet DataSet, double min_non_zero_pct = 0.25, int min_distinct_numbers = 2, string stats_filename = null)
internal static void remove_empty_features_by_class(DataSet DataSet, int min_distinct_numbers = 2, string stats_filename = null)
{
    const string MethodName = nameof(remove_empty_features_by_class);

    Logging.WriteLine("...", _CallerModuleName: ModuleName, _CallerMethodName: MethodName);

    var save_stats = !string.IsNullOrWhiteSpace(stats_filename);

    var empty_fids = new List<int>();

    var column_data = Getcolumn_data_by_class(DataSet);
    var class_stats = new List<(

        int cid,
        int fid,

        int num_distinct_values,

        int num_values_zero,
        double num_values_zero_pct,

        int num_values_non_zero,
        double num_values_non_zero_pct,

        int overlap,
        double overlap_pct,

        int non_overlap,
        double non_overlap_pct

        )>();

    for (var fid = 1; fid < DataSet.DataSet_headers.Count; fid++)
    {
        //var mins = column_data[fid].Select(a=> a.Min()).ToList();
        //var maxs = column_data[fid].Select(a=> a.Max()).ToList();
        //var avs = column_data[fid].Select(a=> a.Average()).ToList();

        for (var cid = 0; cid < column_data[fid].Length; cid++)
        {
            var values = column_data[fid][cid];

            if (values == null || values.Length == 0)
            {
                if (save_stats) { class_stats.Add((cid, fid, 0, 0, 0, 0, 0, 0, 0, 0, 0)); }
                continue;
            }

            var other_cids = Enumerable.Range(0, column_data[fid].Length).Where(a => a != cid).ToList();
            var other_values = other_cids.SelectMany(a => column_data[fid][a]).ToList();
            var other_values_min = other_values.Min();
            var other_values_max = other_values.Max();


            var values_no_overlap = values.Where(a => a < other_values_min || a > other_values_max).ToList();

            var overlap = values.Length - values_no_overlap.Count;
            var overlap_pct = (double)overlap / (double)values.Length;
            var non_overlap = values_no_overlap.Count;
            var non_overlap_pct = (double)values_no_overlap.Count / (double)values.Length;

            //var values_before_other_classes_min = values_no_overlap.Where(a => a < other_values_min).ToList();
            //var values_after_other_classes_max = values_no_overlap.Where(a => a > other_values_max).ToList();



            var values_distinct = values.Distinct().OrderBy(a => a).ToList();




            if (save_stats)
            {
                var values_count = values_distinct.Select(a => (value: a, count: values.Count(b => a == b))).ToList();
                var num_values_zero = values_count.FirstOrDefault(a => a.value == 0).count;
                var num_values_zero_pct = (double)num_values_zero / (double)values.Length;
                var num_values_non_zero = values.Length - num_values_zero;
                var num_values_non_zero_pct = (double)num_values_non_zero / (double)values.Length;
                var num_distinct_values = values_distinct.Count;

                class_stats.Add
                ((
                    cid,
                    fid,
                    num_distinct_values: num_distinct_values,
                    num_values_zero: num_values_zero,
                    num_values_zero_pct: num_values_zero_pct,
                    num_values_non_zero: num_values_non_zero,
                    num_values_non_zero_pct: num_values_non_zero_pct,
                    overlap: overlap,
                    overlap_pct: overlap_pct,
                    non_overlap: non_overlap,
                    non_overlap_pct: non_overlap_pct
                ));
            }

            if (values_distinct.Count < min_distinct_numbers)// || num_values_non_zero_pct < min_non_zero_pct)
            {
                empty_fids.Add(DataSet.DataSet_headers[fid].fid);
                break;
            }
        }
    }

    if (empty_fids != null && empty_fids.Count > 0)
    {
        remove_fids(DataSet, empty_fids);
    }

    Logging.WriteLine($@"Removed features ({empty_fids.Count}): {string.Join(",", empty_fids)}", _CallerModuleName: ModuleName, _CallerMethodName: MethodName);

    if (save_stats)
    {
        var data = new List<string>();
        data.Add("cid,fid,num_distinct_values,num_values_zero,num_values_zero_pct,num_values_non_zero,num_values_non_zero_pct,overlap,overlap_pct,non_overlap,non_overlap_pct");
        data.AddRange(class_stats.Select(a => $@"{a.cid},{a.fid},{a.num_distinct_values},{a.num_values_zero},{a.num_values_zero_pct},{a.num_values_non_zero},{a.num_values_non_zero_pct},{a.overlap},{a.overlap_pct},{a.non_overlap},{a.non_overlap_pct}").ToList());
        await io_proxy.WriteAllLines(true, stats_filename, data);
    }
}
*/

/*
internal static void remove_fids(DataSet DataSet, List<int> fids_to_remove)
{
    //Logging.WriteLine("...", ModuleName, nameof(remove_fids));

    // removed given fids and renumber the headers/features

    if (fids_to_remove == null || fids_to_remove.Count == 0) { Logging.LogExit(ModuleName); return; }

    var remove = new bool[DataSet.DataSet_headers.Count];

    fids_to_remove.ForEach(a => remove[a] = true);

    DataSet.DataSet_headers = DataSet.DataSet_headers.Where((a, i) => !remove[i]).Select((b, j) =>
        (j, b.gkAlphabet, b.gkDimension, b.gkCategory, b.gkSource, b.gkGroup, b.gkMember, b.gkPerspective,
            b.alphabet_id, b.dimension_id, b.category_id, b.source_id, b.group_id, b.member_id, b.perspective_id)
        ).ToList();

    for (var index = 0; index < DataSet.DataSet_instance_list.Count; index++)
    {

        DataSet.DataSet_instance_list[index] = (

            DataSet.DataSet_instance_list[index].ClassId,
            DataSet.DataSet_instance_list[index].example_id,
            DataSet.DataSet_instance_list[index].class_example_id,
            DataSet.DataSet_instance_list[index].comment_columns,
            DataSet.DataSet_instance_list[index].feature_data.Where((b, i) => !remove[i]).Select((c, j) => (j, c.fv)).ToList()

            );
    }
}
*/
/*
internal static List<(int fid, double value)> parse_csv_line_doubles(string line, bool[] required = null)
{
    var result = new List<(int fid, double value)>();

    if (string.IsNullOrWhiteSpace(line)) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :result;}

    var fid = 0;

    var start = 0;
    var len = 0;

    for (var i = 0; i <= line.Length; i++)
    {
        if (i == line.Length || line[i] == ',')
        {
            if ((required == null || required.Length == 0 || fid == 0) || (required != null && required.Length > fid && required[fid]))
            {
                result.Add((fid, len == 0 ? 0d : double.Parse(line.Substring(start, len), NumberStyles.Float, NumberFormatInfo.InvariantInfo)));
            }

            fid++;

            start = i + 1;
            len = 0;
            continue;

        }

        len++;
    }

    Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :result;
}
*/
/* internal static List<string> parse_csv_line_strings(string line)
 {
     var result = new List<string>();

     if (string.IsNullOrWhiteSpace(line)) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :result;}

     var id = 0;

     var start = 0;
     var len = 0;

     for (var i = 0; i <= line.Length; i++)
     {
         if (i == line.Length || line[i] == ',')
         {

             result.Add(len == 0 ? "" : line.Substring(start, len));

             id++;

             start = i + 1;
             len = 0;
             continue;

         }

         len++;
     }

     Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :result;
 }*/


/*internal static double fix_double(string double_value)
{
    const char infinity = '∞';
    const string neg_infinity = "-∞";
    const string pos_infinity = "+∞";
    const string NaN = "NaN";

    if (double.TryParse(double_value, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var value1)) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :fix_double(value1);}

    if (double_value.Length == 1 && double_value[0] == infinity) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :fix_double(double.PositiveInfinity);}
    else if (double_value.Contains(pos_infinity, StringComparison.Ordinal)) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :fix_double(double.PositiveInfinity);}
    else if (double_value.Contains(neg_infinity, StringComparison.Ordinal)) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :fix_double(double.NegativeInfinity);}
    else if (double_value.Contains(infinity, StringComparison.Ordinal)) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :fix_double(double.PositiveInfinity);}
    else if (double_value.Contains(NaN, StringComparison.Ordinal)) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :fix_double(double.NaN);}
    else Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :0d;
}*/

/*internal static double fix_double(double value)
{
    // the doubles must be compatible with libsvm which is written in C (C and CSharp have different min/max values for double)
    const double c_double_max = (double)1.79769e+308;
    const double c_double_min = (double)-c_double_max;
    const double double_zero = (double)0;

    if (value >= c_double_min && value <= c_double_max)
    {
        Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :value;
    }
    else if (double.IsPositiveInfinity(value) || value >= c_double_max || value >= double.MaxValue)
    {
        value = c_double_max;
    }
    else if (double.IsNegativeInfinity(value) || value <= c_double_min || value <= double.MinValue)
    {
        value = c_double_min;
    }
    else if (double.IsNaN(value))
    {
        value = double_zero;
    }

    Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :value;
}*/

//    }
//}