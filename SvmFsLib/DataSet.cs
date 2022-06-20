using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsLib
{
    public class DataSet
    {
        public const string ModuleName = nameof(DataSet);
        //public 
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
        //        )[] RowComment,
        //        (
        //            int row_index,
        //            int col_index,
        //            (
        //                int internal_column_index,
        //                int external_column_index,
        //                string gkFileTag,
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
        //        )[] RowColumns
        //    )[] val_list
        //)[] value_list;

        public string[] DataSetFileTags;

        public  (
                    int ClassId,
                    string ClassName,
                    int ClassSize,
                    int DownSampledClassSize,
                    int ClassFeatures
                )[] ClassSizes;

        //public (int internal_column_index, int external_column_index, string FileTag, string gkAlphabet, string gkStats, string gkDimension, string gkCategory, string gkSource, string @gkGroup, string gkMember, string gkPerspective)[] column_header_list;
        
        // column labels (feature names)
        public DataSetGroupKey[] ColumnHeaderList;

        // row labels (class ids) // feature 0
        
        // current problem definition to sovle: need to specify the columns or only load those specific columns, for the baseline dataset

        public  (
                    int ClassId,
                    string ClassName, 
                    (int CommentRowIndex, int CommentColumnIndex, string CommentKey, string CommentValue)[][] ClassCommentList
                )[] CommentList;

        // feature values, grouped by class id (with meta data class name and class size)
        // public List<(int ClassId, string ClassName, int ClassSize, List<((int internal_column_index, int external_column_index, string FileTag, string gkAlphabet, string gkDimension, string gkCategory, string gkSource, string @gkGroup, string gkMember, string gkPerspective) column_header, double fv)[]> val_list)> value_list;

        public  (
                    int ClassId, 
                    string ClassName, 
                    int ClassSize, 
                    (
                        (int CommentRowIndex, int CommentColumnIndex, string CommentKey, string CommentValue)[] RowComment, 
                        (int RowIndex, int ColumnIndex, DataSetGroupKey ColumnHeader, double RowColumnValue)[] RowColumns
                    )[] ClassValueList
                )[] ValueList;

        public DataSet()
        {

        }

        public void Filter(int[] columnIndexes)
        {
            ColumnHeaderList = ColumnHeaderList.Where(a => columnIndexes.Contains(a.gkColumnIndex)).ToArray();
        }

        //public DataSet(DataSet dataSet, int[] columnIndexes)
        //{
        //    if (dataSet == null) throw new ArgumentNullException(nameof(dataSet));
        //    if (columnIndexes == null) throw new ArgumentNullException(nameof(columnIndexes));
        //    var copy = new DataSet();
        //    copy.ClassSizes = this.ClassSizes.ToArray();
        //    copy.ColumnHeaderList = this.ColumnHeaderList.ToArray();
        //    copy.CommentList = this.CommentList.ToArray();
        //    copy.ValueList.ToArray();
        //    throw new NotImplementedException();
        //}

        public static List<double[]> ReadBinaryValueFile(string inputFile, bool asStream =true)
        {
            Logging.LogCall();

            Logging.LogEvent($@"Loading {inputFile}");

            var lines = new List<double[]>();

            if (asStream)
            {
                var f = File.Open(inputFile, FileMode.Open, FileAccess.Read, FileShare.Read);


                while (f.CanRead)
                {
                    var buffer1 = new byte[sizeof(int)];
                    var b1 = f.Read(buffer1);
                    if (b1 == 0) break;
                    var len = BitConverter.ToInt32(buffer1);

                    if (len > 0)
                    {
                        var buffer2 = new byte[sizeof(double) * len];
                        var b2 = f.Read(buffer2);
                        if (b2 == 0) break;

                        var values = new double[buffer2.Length / sizeof(double)];
                        for (int i = 0; i < values.Length; i++) { values[i] = BitConverter.ToDouble(buffer2, i * sizeof(double)); }

                        lines.Add(values);
                    }
                }

                f.Close();
                f.Dispose();
            }
            else
            {
                var bytes = File.ReadAllBytes(inputFile);
                var b = 0;
                
                while (b < bytes.Length)
                {
                    var lineLen = BitConverter.ToInt32(bytes, b);
                    b += sizeof(int);

                    if (lineLen > 0)
                    {
                        var values = new double[lineLen];
                        for (int i = 0; i < lineLen; i++)
                        {
                            values[i] = BitConverter.ToDouble(bytes, b);
                            b += sizeof(double);
                        }

                        lines.Add(values);
                    }
                }
            }

            Logging.LogEvent($@"Loaded {inputFile}");
            Logging.LogExit();
            return lines;
        }

        public static string[][] ReadCsv(string inputFile)
        {
            Logging.LogCall();
            //Logging.LogEvent($@"Loading {inputFile}");

            var result = new List<string[]>();

            foreach (var line in File.ReadLines(inputFile))
            {
                var lineSplit = line.Split(',');

                result.Add(lineSplit);
            }

            //Logging.LogEvent($@"Loaded {inputFile}");
            Logging.LogExit();

            return result.ToArray();
        }

        public static string[][] ReadBinaryCsv(string inputFile, bool asStream=false)
        {
            Logging.LogCall();
            Logging.LogEvent($@"Loading {inputFile}");

            if (asStream)
            {
                using var inputFileSteam = File.Open(inputFile, FileMode.Open, FileAccess.Read, FileShare.Read);
                //Logging.LogEvent($@"Loading {inputFile}");

                var result1 = new List<byte[][]>();
                //var result2 = new List<string[]>();

                while (inputFileSteam.CanRead)
                {
                    var lineSizeBuffer = new byte[sizeof(int)];
                    var b1 = inputFileSteam.Read(lineSizeBuffer);
                    if (b1 == 0) break;
                    var lineSize = BitConverter.ToInt32(lineSizeBuffer);

                    var lineSplit1 = new byte[lineSize][];
                    //var lineSplit2 = new string[lineSize];

                    if (lineSize > 0)
                    {
                        for (var i = 0; i < lineSize; i++)
                        {
                            var strlenBuffer = new byte[sizeof(int)];
                            var b2 = inputFileSteam.Read(strlenBuffer);
                            if (b2 == 0) break;
                            var strlen = BitConverter.ToInt32(strlenBuffer);
                            //if (strlen==0) break;

                            if (strlen > 0)
                            {
                                var buffer = new byte[strlen];
                                var b3 = inputFileSteam.Read(buffer);
                                if (b3 == 0) break;
                                lineSplit1[i] = buffer;
                                //lineSplit2[i] = Encoding.UTF8.GetString(buffer);
                            }
                            else { lineSplit1[i] = Array.Empty<byte>(); }
                        }
                    }

                    result1.Add(lineSplit1);
                    //result2.Add(lineSplit2);
                }

                //inputFileSteam.Close();
                //inputFileSteam.Dispose();

                //Logging.LogEvent($@"Loaded {inputFile}");
                Logging.LogExit();

                var result2 = result1.AsParallel().AsOrdered().Select(a => a /*.AsParallel().AsOrdered()*/.Select(b => Encoding.UTF8.GetString(b)).ToArray()).ToArray();

                Logging.LogEvent($@"Loaded {inputFile}");
                Logging.LogExit();
                return result2; //result.ToArray();
            }
            else
            {
                var bytes = File.ReadAllBytes(inputFile);

                //var lines = new List<string[]>();
                var lines = new List<byte[][]>();
                
                var i = 0;
                while (i < bytes.Length)
                {
                    var lineSize = BitConverter.ToInt32(bytes, i);
                    i += sizeof(int);

                    var line = new byte[lineSize][];
                    lines.Add(line);

                    for (var k = 0; k < lineSize; k++)
                    {
                        var bytesStrLen = BitConverter.ToInt32(bytes, i);
                        i += sizeof(int);

                        //line[k] = Encoding.UTF8.GetString(bytes, i, bytesStrLen);
                        line[k] = bytes.Skip(i).Take(bytesStrLen).ToArray();//bytes[i..(i+bytesStrLen)];
                        i += bytesStrLen;
                    }
                }

                var result2 = lines.AsParallel().AsOrdered().Select(a => a /*.AsParallel().AsOrdered()*/.Select(b => Encoding.UTF8.GetString(b)).ToArray()).ToArray();

                Logging.LogEvent($@"Loaded {inputFile}");
                Logging.LogExit();
                return result2;
            }
        }

        public static List<string[]> ConvertCsvTextFileToBinary(string inputFile, string outputFile)
        {
            Logging.LogCall();
            Logging.LogEvent($@"Converting {inputFile} to {outputFile}");

            var tempOutputFile = Path.Combine(Path.GetDirectoryName(outputFile), $"tmp_{Guid.NewGuid():N}.bin");

            var outputFileSteam = File.Open(tempOutputFile, FileMode.Create, FileAccess.Write, FileShare.None);

            var result = new List<string[]>();

            void sendInt(int value)
            {
                outputFileSteam.Write(BitConverter.GetBytes(value));
            }

            void sendStr(string str)
            {
                var strbytes = Encoding.UTF8.GetBytes(str);
                var strlen = strbytes.Length;
                sendInt(strlen);
                outputFileSteam.Write(strbytes);
            }

            foreach (var line in File.ReadLines(inputFile))
            {
                var lineSplit = line.Split(',');

                sendInt(lineSplit.Length);

                foreach (var s in lineSplit)
                {
                    sendStr(s);
                }

                result.Add(lineSplit);
            }

            outputFileSteam.Flush(true);
            outputFileSteam.Close();
            outputFileSteam.Dispose();


            while (!File.Exists(outputFile))
            {
                try { File.Move(tempOutputFile, outputFile); }
                catch (Exception e)
                {
                    Logging.LogException(e);

                    try { File.Delete(tempOutputFile); }
                    catch (Exception e2) { Logging.LogException(e2); }
                }
            }

            Logging.LogEvent($@"Converted {inputFile} to {outputFile}");
            Logging.LogExit();
            return result;
        }

        public static List<double[]> ConvertCsvValueFileToBinary(string inputFile, string outputFile)
        {
            Logging.LogCall();
            Logging.LogEvent($@"Converting {inputFile} to {outputFile}");

            if (File.Exists(outputFile))
            {
                Logging.LogExit();
                Logging.LogEvent($@"Converting {inputFile} to {outputFile} - output file already exists.");

                return null;
            }

            var lines = new List<double[]>();

            var tempOutputFile = Path.Combine(Path.GetDirectoryName(outputFile), $"tmp_{Guid.NewGuid():N}.bin");

            var outputFileSteam = File.Open(tempOutputFile, FileMode.Create, FileAccess.Write, FileShare.None);

            //void writeInts(int[] values)
            //{
            //    outputFileSteam.Write(BitConverter.GetBytes(values.Length));
            //    for (var index = 0; index < values.Length; index++)
            //    {
            //        outputFileSteam.Write(BitConverter.GetBytes(values[index]));
            //    }
            //}

            void writeDoubles(double[] values)
            {
                // write record length
                outputFileSteam.Write(BitConverter.GetBytes(values.Length));

                // write record contents
                for (var index = 0; index < values.Length; index++)
                {
                    outputFileSteam.Write(BitConverter.GetBytes(values[index]));
                }
            }


            foreach (var line in File.ReadLines(inputFile).Skip(1))
            {


                var lineSplit = line.Split(',');

                var lineDoubles = lineSplit.AsParallel().AsOrdered().Select(a => double.Parse(a, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray();
                writeDoubles(lineDoubles);
                lines.Add(lineDoubles);
            }

            outputFileSteam.Flush(true);
            outputFileSteam.Close();
            outputFileSteam.Dispose();

            while (!File.Exists(outputFile))
            {
                try { File.Move(tempOutputFile, outputFile); }
                catch (Exception e)
                {
                    Logging.LogException(e);

                    try { File.Delete(tempOutputFile); }
                    catch (Exception e2) { Logging.LogException(e2); }
                }
            }

            Logging.LogEvent($@"Converted {inputFile} to {outputFile}");
            Logging.LogExit();
            return lines;

        }

       

                
        public static int[] RemoveDuplicateColumns(DataSet dataSet, int[] queryCols, bool removeWhenEmptyForSingleClass = false, bool asParallel = false,  CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }

            //const string MethodName = nameof(remove_duplicate_columns);
            // remove duplicate columns (may exist in separate groups)
            //var query_col_dupe_check = idr.DataSet_instance_list_grouped.SelectMany(a => a.examples).SelectMany(a => query_cols.Select(b => (query_col: b, fv: a.feature_data[b].fv)).ToList()).GroupBy(b => b.query_col).Select(b => (query_col: b.Key, values: b.Select(c => c.fv).ToList())).ToList();

            if (queryCols == null || queryCols.Length <= 1) 
            { 
                Logging.LogExit(ModuleName); 
                return /*ct.IsCancellationRequested ? default :*/ queryCols; 
            } //throw new ArgumentOutOfRangeException(nameof(query_cols));

            //if (query_cols.Length <= 1) {Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :query_cols;}

            var queryColDupeCheck = asParallel
                ? queryCols.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select(colIndex => dataSet.ValueList.SelectMany(classValues => classValues.ClassValueList.Select((row, rowIndex) =>
                {
                    var rc = row.RowColumns[colIndex];

#if DEBUG
                    if (rc.ColumnIndex != colIndex || rc.RowIndex != rowIndex) throw new Exception();
#endif

                    return rc.RowColumnValue;
                }).ToArray()).ToArray()).ToArray()
                : queryCols.Select(colIndex => dataSet.ValueList.SelectMany(classValues => classValues.ClassValueList.Select((row, rowIndex) =>
                {
                    var rc = row.RowColumns[colIndex];

#if DEBUG
                    if (rc.ColumnIndex != colIndex || rc.RowIndex != rowIndex) throw new Exception();
#endif

                    return rc.RowColumnValue;
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
                ? indexPairs.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select(a => queryColDupeCheck[a.x].SequenceEqual(queryColDupeCheck[a.y])).ToArray()
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
                ? dupeClusters.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Where(dc => dc != null && dc.Count > 1).SelectMany(dc => dc.Skip(1).ToArray()).ToArray()
                : dupeClusters.Where(dc => dc != null && dc.Count > 1).SelectMany(dc => dc.Skip(1).ToArray()).ToArray();

            var expectedSize = dataSet.ValueList.Sum(a => a.ClassValueList.Length);

            // check data not empty...
            var noVarianceColumnIndexes = new List<int>();
            for (var colIndex = startIndex; colIndex < queryColDupeCheck.Length; colIndex++)
            {
#if DEBUG
                if (queryColDupeCheck.Any(a => a.Length != expectedSize)) throw new Exception();
                if (queryCols.Length != queryColDupeCheck.Length) throw new Exception();
#endif

                if (indexesToRemove.Contains(queryCols[colIndex])) continue;

                var allColumnRows = queryColDupeCheck[colIndex];
                if (allColumnRows.Distinct().Count() == 1)
                {
                    noVarianceColumnIndexes.Add(queryCols[colIndex]);
                    continue;
                }


                if (removeWhenEmptyForSingleClass)
                {
                    var rowsIndex = 0;
                    for (var classIndex = 0; classIndex < dataSet.ValueList.Length; classIndex++)
                    {
                        var classNumRows = dataSet.ValueList[classIndex].ClassValueList.Length;

                        if (queryColDupeCheck[colIndex].Skip(rowsIndex).Take(classNumRows).Distinct().Count() == 1)
                        {

                            noVarianceColumnIndexes.Add(queryCols[colIndex]);
                            break;
                        }

                        rowsIndex += classNumRows;
                    }
                }
            }

            if (noVarianceColumnIndexes.Count>0)
            {
                indexesToRemove = indexesToRemove.Concat(noVarianceColumnIndexes).ToArray();
            }

            if (indexesToRemove.Length > 0)
            {
                var ret = queryCols.Except(indexesToRemove).ToArray();
                //#if DEBUG
                //                Logging.WriteLine($"Removed duplicate columns: [{string.Join(", ", indexes_to_remove)}].", program.ModuleName, MethodName);
                //                Logging.WriteLine($"Duplicate columns: [{string.Join(", ", dupe_clusters.Select(a => $"[{string.Join(", ", a)}]").ToArray())}].", program.ModuleName, MethodName);
                //                Logging.WriteLine($"Preserved columns: [{string.Join(", ", ret)}].", program.ModuleName, MethodName);
                //#endif
                Logging.LogExit(ModuleName); 
                return /*ct.IsCancellationRequested ? default :*/ ret;
            }

            Logging.LogExit(ModuleName); 
            return /*ct.IsCancellationRequested ? default :*/ queryCols;
        }

        public double[/* row */][/* column */] GetRowFeatures((int ClassId, int[] row_indexes)[] classRowIndexes, int[] columnIndexes, bool asParallel = false, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested)
            {
                Logging.LogExit(ModuleName);
                return default;
            }

            //if (columnIndexes.First() != 0) throw new ArgumentOutOfRangeException(nameof(columnIndexes)); // class id missing

            var classRows = asParallel
                ? classRowIndexes.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select(classRowIndex => GetClassRowFeatures(classRowIndex.ClassId, classRowIndex.row_indexes, columnIndexes)).ToList()
                : classRowIndexes.Select(classRowIndex => GetClassRowFeatures(classRowIndex.ClassId, classRowIndex.row_indexes, columnIndexes)).ToList();

            var rows = classRows.SelectMany(a => a.AsRows).ToArray();

            Logging.LogExit(ModuleName);

            return ct.IsCancellationRequested ? default : rows;
        }

        public DataSet GetFeaturesAsReducedDataSet(int[] columnIndexes, bool asParallel = false, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested)
            {
                Logging.LogExit(ModuleName);
                return default;
            }

            if (columnIndexes.First() != 0) throw new ArgumentOutOfRangeException(nameof(columnIndexes)); // class id missing


            var filteredDataSet = new DataSet();
            filteredDataSet.ValueList = this.ValueList.Select(cl => (cl.ClassId, cl.ClassName, cl.ClassSize, ClassValueList: cl.ClassValueList.Select(c => (RowComment: c.RowComment, RowColumns: columnIndexes.Select(ci => c.RowColumns[ci]).ToArray())).ToArray())).ToArray();
            filteredDataSet.ColumnHeaderList = columnIndexes.Select(ci => this.ColumnHeaderList[ci]).ToArray();
            filteredDataSet.CommentList = this.CommentList;
            filteredDataSet.ClassSizes = this.ClassSizes;


            Logging.LogExit(ModuleName);
            return ct.IsCancellationRequested ? default : filteredDataSet;
        }

        public double[/*class*/][/*row*/][/*column*/] GetFeaturesAsValues(int[] columnIndexes, bool asParallel = false, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested)
            {
                Logging.LogExit(ModuleName);
                return default;
            }

            if (columnIndexes.First() != 0) throw new ArgumentOutOfRangeException(nameof(columnIndexes)); // class id missing


            var result = this.ValueList.Select(cl => cl.ClassValueList.Select(c => columnIndexes.Select(ci => c.RowColumns[ci].RowColumnValue).ToArray()).ToArray()).ToArray();



            Logging.LogExit(ModuleName);
            return ct.IsCancellationRequested ? default : result;
        }

        public static Scaling[] GetScalingParams(double[][] rows, int[] columnIndexes, CancellationToken ct = default)
        {

            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested)
            {
                Logging.LogExit(ModuleName);
                return default;
            }

            var hasClassId = columnIndexes != null && columnIndexes.Length > 0 && columnIndexes[0] == 0;

            if (columnIndexes != null && (rows.FirstOrDefault()?.Length??0) != columnIndexes.Length) throw new Exception();

            if (columnIndexes == null) columnIndexes = Enumerable.Range(0, rows.Length).ToArray();

            var cols = columnIndexes.Select((columnIndex, xIndex) => rows.Select(row => row[xIndex /* column_index -> x_index*/]).ToArray()).ToArray();
            //var cols = Enumerable.Range(0,columnIndexes.Length).Select(xIndex => rows.Select(row => row[xIndex /* column_index -> x_index*/]).ToArray()).ToArray();

            

            var sp = cols.Select((col, xIndex) => xIndex == 0 && hasClassId /* do not scale class id */
                ? null
                : new Scaling(col)).ToArray();

            Logging.LogExit(ModuleName); 
            return sp;
        }

        public static double[][] GetScaledRows(double[][] rows, /*List<int> column_indexes,*/ Scaling[] sp, Scaling.ScaleFunction sf, CancellationToken ct = default)
        {

            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested)
            {
                Logging.LogExit(ModuleName);
                return default;
            }
            //var cols = column_indexes.Select((column_index, x_index) => rows.Select(row => row[x_index /* column_index -> x_index*/]).ToArray()).ToArray();
            //var cols_scaled = cols.Select((v, x_index) =)

            var rowsScaled = rows.Select((row, rowIndex) => row.Select((colVal, colIndex) => colIndex != 0
                ? sp[colIndex].Scale(colVal, sf)
                : colVal).ToArray()).ToArray();

            Logging.LogExit(ModuleName); return rowsScaled;
        }

        public (double[ /*row*/][ /*col*/] AsRows, double[ /*col*/][ /*row*/] AsColumns) GetClassRowFeatures(int classId, int[] rowIndexes, int[] columnIndexes, CancellationToken ct = default)
        {

            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested)
            {
                Logging.LogExit(ModuleName);
                return default;
            }

            //if (columnIndexes == null)
            //{
            //    // select all columns...
            //    columnIndexes = ValueList.SelectMany(a => a.ClassValueList.SelectMany(b => b.RowColumns.Select(c => c.ColumnIndex).ToArray()).ToArray()).Distinct().OrderBy(a => a).Skip(1/* skip class id */).ToArray();
            //}

            //if (columnIndexes.First() != 0) throw new Exception(); // class id missing

            var asRows = new double[rowIndexes.Length][];
            var asCols = new double[columnIndexes.Length][];

            var v = ValueList.First(a => a.ClassId == classId).ClassValueList;

            for (var yIndex = 0; yIndex < rowIndexes.Length; yIndex++)
            {
                var rowIndex = rowIndexes[yIndex];
                asRows[yIndex] = new double[columnIndexes.Length];

                for (var xIndex = 0; xIndex < columnIndexes.Length; xIndex++)
                {
                    var colIndex = columnIndexes[xIndex];
                    //AsRows[row_index][col_index] = v[row_index].RowColumns[col_index].row_column_val;
                    asRows[yIndex][xIndex] = v[rowIndex].RowColumns[colIndex].RowColumnValue;
#if DEBUG
                    if (v[rowIndex].RowColumns[colIndex].ColumnIndex != colIndex) throw new Exception();
#endif
                }
            }


            for (var xIndex = 0; xIndex < columnIndexes.Length; xIndex++)
            {
                var colIndex = columnIndexes[xIndex];
                asCols[xIndex] = new double[rowIndexes.Length];

                for (var yIndex = 0; yIndex < rowIndexes.Length; yIndex++)
                {
                    var rowIndex = rowIndexes[yIndex];
                    //AsColumns[col_index][row_index] = v[row_index].RowColumns[col_index].row_column_val;
                    asCols[xIndex][yIndex] = v[rowIndex].RowColumns[colIndex].RowColumnValue;

#if DEBUG
                    if (v[rowIndex].RowColumns[colIndex].RowIndex != rowIndex) throw new Exception();
#endif
                }
            }

            Logging.LogExit(ModuleName); return (asRows, asCols);
        }

        //public async Task DataSet_loader(CancellationToken ct, string[] DataSet_names)// = "[1i.aaindex]")//"2i,2n")//, bool split_by_FileTag = true, bool split_by_groups = true)
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
        //        FileTags: DataSet_names,//.Split(',', StringSplitOptions.RemoveEmptyEntries), // "2i"
        //        ClassNames: program.program_args.ClassNames,
        //        perform_integrity_checks: false,
        //        required_default: required_default,
        //        required_matches: required_matches
        //    );

        //}

        //public async Task LoadDataSetHeadersAsync(List<(int ClassId, string ClassName, List<(string FileTag, int ClassId, string ClassName, string filename)> valuesCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> headerCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> commentCsvFilenames)> dataFilenames, CancellationToken ct)
        //{
        //    Logging.LogCall(ModuleName);

        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

        //    // 1. headers
        //    Logging.WriteLine(@"Start: reading headers.", ModuleName);
        //    var swHeader = Stopwatch.StartNew();

        //    ColumnHeaderList = dataFilenames.First( /* headers are same for all classes, so only load first class headers */).headerCsvFilenames.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.SelectMany((fileInfo, fileIndex) =>
        //    {
        //        Logging.LogExit(ModuleName); return IoProxy.ReadAllLinesAsync(true, ct, fileInfo.filename, callerModuleName: ModuleName).Result.Skip(fileIndex == 0
        //            ? 1
        //            : 2 /*skip header line, and if not first file, class id line too */).AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((line, lineIndex) =>
        //            {
        //                var row = line.Split(',');

        //                if (row.Length == 9)
        //                    Logging.LogExit(ModuleName); return new DataSetGroupKey(
        //                        //internal_column_index: -1, 
        //                        //external_column_index: line_index /*int.Parse(row[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo)*/,
        //                        fileIndex == 0 && lineIndex == 0 /* class id isn't associated with any particular file */
        //                            ? ""
        //                            : fileInfo.FileTag,
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
        //                            : fileInfo.FileTag,
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


        //    //header_list = header_list.AsParallel().AsOrdered().Select((a, internal_column_index) => (internal_column_index, a.external_column_index, a.FileTag, a.gkAlphabet, a.gkStats, a.gkDimension, a.gkCategory, a.gkSource, a.@gkGroup, a.gkMember, a.gkPerspective)).ToArray();
        //    swHeader.Stop();
        //    Logging.WriteLine($@"Finish: reading headers ({swHeader.Elapsed}).", ModuleName);
        //}


        public void LoadDataSetHeaders(List<(int ClassId, string ClassName, List<(string FileTag, int ClassId, string ClassName, string filename)> valuesCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> headerCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> commentCsvFilenames)> dataFilenames, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            // 1. headers
            Logging.WriteLine(@"Start: reading headers.", ModuleName);
            var swHeader = Stopwatch.StartNew();

            ColumnHeaderList = dataFilenames.First( /* headers are same for all classes, so only load first class headers */).headerCsvFilenames.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.SelectMany((fileInfo, fileIndex) =>
            {

                var fn = fileInfo.filename;

                var binName = $"{fn}.bin";

                var binExists = File.Exists(binName) && new FileInfo(binName).Length > 0;

                string[][] x;

                if (!binExists)
                {
                    //x = ConvertCsvTextFileToBinary(fn, binName).Skip(fileIndex == 0 ? 1 /* skip header line */ : 2 /* skip header line, and class id line too, when not first file*/).ToArray();
                    x = ConvertCsvTextFileToBinary(fn, binName).ToArray();
                }
                else
                {
                    //x = ReadBinaryCsv(binName).Skip(fileIndex == 0 ? 1 /* skip header line */ : 2 /* skip header line, and class id line too, when not first file*/).ToArray();
                    x = ReadBinaryCsv(binName);
                }

                //var x = IoProxy.ReadAllLines(true, ct, fn, callerModuleName: ModuleName).Skip(fileIndex == 0
                //    ? 1 /* skip header line */
                //    : 2 /* skip header line, and class id line too, when not first file*/).AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((line, lineIndex) =>
                //    {
                //        var row = line.Split(',');
                //        return row;
                //    }).ToArray();

                var rowLen = x[0].Length;

                var y = x.Skip(
                    fileIndex == 0 ? 
                    1 /* skip header line */ 
                    :
                    2 /* skip header line, and class id line too, when not first file*/
                    ).AsParallel().AsOrdered().Select(row =>
                {
                    if (rowLen == 9)
                    {
                        return new DataSetGroupKey(
                            //internal_column_index: -1, 
                            //external_column_index: line_index /*int.Parse(row[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo)*/,
                            fileInfo.FileTag,
                            row[1],
                            row[2],
                            row[3],
                            row[4],
                            row[5],
                            row[6],
                            row[7],
                            row[8]);
                    }

                    if (rowLen == 8)
                    {
                        return new DataSetGroupKey(
                            //internal_column_index: -1, 
                            //external_column_index: line_index /*int.Parse(row[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo)*/,
                            fileInfo.FileTag,
                            row[1],
                            "",
                            row[2],
                            row[3],
                            row[4],
                            row[5],
                            row[6],
                            row[7]);
                    }

                    return default;
                }).ToArray();

                y[0].Value.gkFileTag = "";/* class id isn't associated with any particular file */
                return y;
            }).ToArray();

            Parallel.For(0,
                ColumnHeaderList.Length,
                i =>
                {
                    ColumnHeaderList[i].gkGroupIndex = i;
                    ColumnHeaderList[i].gkColumnIndex = i;
                });


            //header_list = header_list.AsParallel().AsOrdered().Select((a, internal_column_index) => (internal_column_index, a.external_column_index, a.FileTag, a.gkAlphabet, a.gkStats, a.gkDimension, a.gkCategory, a.gkSource, a.@gkGroup, a.gkMember, a.gkPerspective)).ToArray();
            swHeader.Stop();
            Logging.WriteLine($@"Finish: reading headers ({swHeader.Elapsed}).", ModuleName);

            Logging.LogExit(ModuleName);
        }

        //public async Task LoadDataSetCommentsAsync(List<(int ClassId, string ClassName, List<(string FileTag, int ClassId, string ClassName, string filename)> valuesCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> headerCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> commentCsvFilenames)> dataFilenames, CancellationToken ct)
        //{
        //    Logging.LogCall(ModuleName);

        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

        //    const string MethodName = nameof(LoadDataSetCommentsAsync);

        //    // 2. comment files. (same class with same samples = same data)
        //    Logging.WriteLine(@"Start: reading comments.", ModuleName, MethodName);
        //    var swComment = Stopwatch.StartNew();

        //    var commentList2 = dataFilenames.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select(async cl =>
        //    {
        //        var commentLines = (await IoProxy.ReadAllLinesAsync(true, ct, cl.commentCsvFilenames.First().filename, callerModuleName: ModuleName, callerMethodName: MethodName).ConfigureAwait(false)).AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select(line => line.Split(',')).ToArray();
        //        var commentHeader = commentLines.First();
        //        var clCommentList = commentLines.Skip(1 /*skip header*/).AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((rowSplit, rowIndex) =>
        //        {
        //            var keyValueList = rowSplit.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((colData, colIndex) => (row_index: rowIndex, col_index: colIndex, comment_key: commentHeader[colIndex], CommentValue: colData)).ToArray();

        //            Logging.LogExit(ModuleName); return keyValueList;
        //        }).ToArray();
        //        Logging.LogExit(ModuleName); return (cl.ClassId, cl.ClassName, cl_comment_list: clCommentList);
        //    }).ToArray();

        //    CommentList = await Task.WhenAll(commentList2).ConfigureAwait(false);

        //    swComment.Stop();
        //    Logging.WriteLine($@"Finish: reading comments ({swComment.Elapsed}).", ModuleName, MethodName);
        //}

        public void LoadDataSetComments(List<(int ClassId, string ClassName, List<(string FileTag, int ClassId, string ClassName, string filename)> valuesCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> headerCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> commentCsvFilenames)> dataFilenames, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            const string MethodName = nameof(LoadDataSetComments);

            // 2. comment files. (same class with same samples = same data)
            Logging.WriteLine(@"Start: reading comments.", ModuleName, MethodName);
            var swComment = Stopwatch.StartNew();

            var commentList2 = dataFilenames.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select(cl =>
            {
                var fn = cl.commentCsvFilenames.First().filename;

                var binName = $"{fn}.bin";

                var binExists = File.Exists(binName) && new FileInfo(binName).Length > 0;

                string[][] commentLines;

                if (!binExists)
                {
                    commentLines= ConvertCsvTextFileToBinary(fn, binName).ToArray();
                }
                else
                {
                    commentLines = ReadBinaryCsv(binName);
                }

                //var commentLines = (IoProxy.ReadAllLines(true, ct, fn, callerModuleName: ModuleName, callerMethodName: MethodName)).AsParallel().AsOrdered().Select(line => line.Split(',')).ToArray();
                var commentHeader = commentLines.First();
                var clCommentList = commentLines.Skip(1 /*skip header*/).AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((rowSplit, commentRowIndex) =>
                {
                    var keyValueList = rowSplit.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((CommentValue, commentColumnIndex) => (CommentRowIndex: commentRowIndex, CommentColumnIndex: commentColumnIndex, CommentKey: commentHeader[commentColumnIndex], CommentValue: CommentValue)).ToArray();

                    return keyValueList;
                }).ToArray();
                return (cl.ClassId, cl.ClassName, cl_comment_list: clCommentList);
            }).ToArray();

            CommentList = commentList2;

            swComment.Stop();
            Logging.WriteLine($@"Finish: reading comments ({swComment.Elapsed}).", ModuleName, MethodName);

            Logging.LogExit(ModuleName);
        }

        //public async Task LoadDataSetValuesAsync(List<(int ClassId, string ClassName, List<(string FileTag, int ClassId, string ClassName, string filename)> valuesCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> headerCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> commentCsvFilenames)> dataFilenames, CancellationToken ct)
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

        //    ValueList = dataFilenames.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((cl, clIndex) =>
        //    {
        //        // 3. experimental sample data
        //        var valsTag = cl.valuesCsvFilenames.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((fileInfo, fileInfoIndex) => IoProxy.ReadAllLinesAsync(true, ct, fileInfo.filename, callerModuleName: ModuleName).ConfigureAwait(false).GetAwaiter().GetResult() //.Result
        //            .Skip(1 /*skip header - col index only*/).AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((row, rowIndex) => row.Split(',').Skip(fileInfoIndex == 0
        //                ? 0
        //                : 1 /*skip class id*/).AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((col, colIndex) => double.Parse(col, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray()).ToArray()).ToArray();

        //        var vals = new double[valsTag.First().Length /* number of rows */][ /* columns */];

        //        for (var rowIndex = 0; rowIndex < vals.Length; rowIndex++)
        //            //vals[row_index] = new double[vals_tag.Sum(a=> a[row_index].Length)];
        //            vals[rowIndex] = valsTag.SelectMany((aCl, aClIndex) => aCl[rowIndex]).ToArray();

        //        var valList = vals.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((row, rowIndex) => (RowComment: CommentList[clIndex].cl_comment_list[rowIndex], RowColumns: row.Select((colVal, colIndex) => (row_index: rowIndex, col_index: colIndex, column_header: ColumnHeaderList[colIndex], row_column_val: vals[rowIndex][colIndex])).ToArray())).ToArray();
        //        Logging.LogExit(ModuleName); return (cl.ClassId, cl.ClassName, ClassSize: valList.Length, /*comment_list[cl_index].cl_comment_list,*/ val_list: valList);
        //    }).ToArray();
        //    swValues.Stop();
        //    Logging.WriteLine($@"Finish: reading values ({swValues.Elapsed}).", ModuleName);
        //}

        public void LoadDataSetValues(List<(int ClassId, string ClassName, List<(string FileTag, int ClassId, string ClassName, string filename)> valuesCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> headerCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> commentCsvFilenames)> dataFilenames, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            double[][][][] LoadValues()
            {
                var swValues = Stopwatch.StartNew();

                Logging.WriteLine(@"Start: reading values.", ModuleName);

                var ret = dataFilenames.AsParallel().AsOrdered().Select((cl, clIndex) =>
                    /*{
                        var valsTagBin =*/ cl.valuesCsvFilenames.AsParallel().AsOrdered().Select((fileInfo, fileInfoIndex) =>
                    {
                        var binName = $"{fileInfo.filename}.bin";

                        var binExists = File.Exists(binName) && new FileInfo(binName).Length > 0;

                        if (!binExists)
                        {
                            return ConvertCsvValueFileToBinary(fileInfo.filename, binName).ToArray();
                        }

                        return ReadBinaryValueFile(binName).ToArray();
                    }).ToArray() /*;

                // code to load CSV instead of bin:
                //// 3. experimental sample data
                //var valsTag = cl.valuesCsvFilenames.AsParallel().AsOrdered().Select((fileInfo, fileInfoIndex) => IoProxy.ReadAllLines(true, ct, fileInfo.filename, callerModuleName: ModuleName)
                //    .Skip(1 /*skip header - col index only* /).AsParallel().AsOrdered().Select((row, rowIndex) => row.Split(',').Skip(fileInfoIndex == 0
                //        ? 0
                //        : 1 /*skip class id* /).AsParallel().AsOrdered().Select((col, colIndex) => double.Parse(col, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray()).ToArray()).ToArray();

                return valsTagBin;
            }*/).ToArray();
                
                swValues.Stop();
                Logging.WriteLine($@"Finish: reading values ({swValues.Elapsed}).", ModuleName);

                return ret;
            }

            
            // 3. values
            var taskLoadValues = Task.Run(() => LoadValues(), ct);
            var taskLoadHeaders = Task.Run(() => LoadDataSetHeaders(dataFilenames, ct), ct);
            var taskLoadComments = Task.Run(() => LoadDataSetComments(dataFilenames, ct), ct);
            Task.WaitAll(new[] { taskLoadValues, taskLoadHeaders, taskLoadComments }, ct);
            var values1 = taskLoadValues.Result;

            if (ColumnHeaderList == null || ColumnHeaderList.Length == 0) throw new Exception();
            if (CommentList == null || CommentList.Length == 0) throw new Exception();


            ValueList = dataFilenames.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((cl, clIndex) =>
            {
                // 3. experimental sample data
                //var valsTag = cl.valuesCsvFilenames.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((fileInfo, fileInfoIndex) => IoProxy.ReadAllLines(true, ct, fileInfo.filename, callerModuleName: ModuleName)
                //    .Skip(1 /*skip header - col index only*/).AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((row, rowIndex) => row.Split(',').Skip(fileInfoIndex == 0
                //        ? 0
                //        : 1 /*skip class id*/).AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((col, colIndex) => double.Parse(col, NumberStyles.Float, NumberFormatInfo.InvariantInfo)).ToArray()).ToArray()).ToArray();

                var valsTag = values1[clIndex];

                var vals = new double[valsTag.First().Length /* number of rows */][ /* columns */];

                for (var rowIndex = 0; rowIndex < vals.Length; rowIndex++)
                    //vals[row_index] = new double[vals_tag.Sum(a=> a[row_index].Length)];
                    vals[rowIndex] = valsTag.SelectMany((aCl, aClIndex) => aCl[rowIndex]).ToArray();

                var valList = vals.AsParallel().AsOrdered()/*.WithCancellation(ct)*/.Select((row, rowIndex) => (RowComment: CommentList[clIndex].ClassCommentList[rowIndex], RowColumns: row.Select((colVal, colIndex) => (rowIndex: rowIndex, colIndex: colIndex, ColumnHeader: ColumnHeaderList[colIndex], row_column_val: vals[rowIndex][colIndex])).ToArray())).ToArray();
                return (cl.ClassId, cl.ClassName, ClassSize: valList.Length, /*comment_list[cl_index].cl_comment_list,*/ val_list: valList);
            }).ToArray();

            
            // check same lengths...
            var lengths = new List<int>();
            lengths.Add(ColumnHeaderList.Length);
            lengths.AddRange(ValueList.SelectMany(a => a.ClassValueList.Select(b => b.RowColumns.Length).ToArray()).ToArray());
            if (lengths.Distinct().Count() != 1) throw new Exception("The feature value column lengths mismatch.");

            Logging.LogExit(ModuleName);
        }

        public List<(int ClassId, string ClassName, List<(string FileTag, int ClassId, string ClassName, string Filename)> valuesCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string Filename)> headerCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> CommentCsvFilenames)> GetDataFilenames(string DataSetFolder, string[] fileTags, IList<(int ClassId, string ClassName)> classNames)
        {
            Logging.LogCall(ModuleName);

            const string MethodName = nameof(GetDataFilenames);

            var dataFilenames = classNames.Select(cl =>
            {
                // (string FileTag, int ClassId, string ClassName, string filename)
                var valuesCsvFilenames = fileTags.Select(fileTag => (FileTag: fileTag, cl.ClassId, cl.ClassName, Filename: Path.Combine(DataSetFolder, $@"f_({fileTag})_({cl.ClassId:+#;-#;+0})_({cl.ClassName}).csv"))).ToList();
                var headerCsvFilenames = fileTags.Select(fileTag => (FileTag: fileTag, cl.ClassId, cl.ClassName, Filename: Path.Combine(DataSetFolder, $@"h_({fileTag})_({cl.ClassId:+#;-#;+0})_({cl.ClassName}).csv"))).ToList();
                var commentCsvFilenames = fileTags.Select(fileTag => (FileTag: fileTag, cl.ClassId, cl.ClassName, Filename: Path.Combine(DataSetFolder, $@"c_({fileTag})_({cl.ClassId:+#;-#;+0})_({cl.ClassName}).csv"))).ToList();

                return (cl.ClassId, cl.ClassName, valuesCsvFilenames: valuesCsvFilenames, headerCsvFilenames: headerCsvFilenames, commentCsvFilenames: commentCsvFilenames);
            }).ToList();

            foreach (var cl in dataFilenames)
            {
                Logging.WriteLine($@"{nameof(cl.valuesCsvFilenames)}: {string.Join(", ", cl.valuesCsvFilenames)}", ModuleName, MethodName);
                Logging.WriteLine($@"{nameof(cl.headerCsvFilenames)}: {string.Join(", ", cl.headerCsvFilenames)}", ModuleName, MethodName);
                Logging.WriteLine($@"{nameof(cl.commentCsvFilenames)}: {string.Join(", ", cl.commentCsvFilenames)}", ModuleName, MethodName);
            }

            Logging.LogExit(ModuleName);
            return dataFilenames;
        }

        public void CheckDataFiles(List<(int ClassId, string ClassName, List<(string FileTag, int ClassId, string ClassName, string filename)> valuesCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> headerCsvFilenames, List<(string FileTag, int ClassId, string ClassName, string filename)> commentCsvFilenames)> dataFilenames)
        {
            Logging.LogCall(ModuleName);

            const string MethodName = nameof(CheckDataFiles);

            // don't try to read any data until checking all files exist...
            if (dataFilenames == null || dataFilenames.Count == 0) throw new Exception();
            foreach (var cl in dataFilenames)
            {
                if (cl.valuesCsvFilenames == null || cl.valuesCsvFilenames.Count == 0 || cl.valuesCsvFilenames.Any(a => string.IsNullOrWhiteSpace(a.filename))) throw new Exception($@"{ModuleName}.{MethodName}: {nameof(cl.valuesCsvFilenames)} is empty");
                if (cl.headerCsvFilenames == null || cl.headerCsvFilenames.Count == 0 || cl.headerCsvFilenames.Any(a => string.IsNullOrWhiteSpace(a.filename))) throw new Exception($@"{ModuleName}.{MethodName}: {nameof(cl.headerCsvFilenames)} is empty");
                if (cl.commentCsvFilenames == null || cl.commentCsvFilenames.Count == 0 || cl.commentCsvFilenames.Any(a => string.IsNullOrWhiteSpace(a.filename))) throw new Exception($@"{ModuleName}.{MethodName}: {nameof(cl.commentCsvFilenames)} is empty");

                if (cl.valuesCsvFilenames.Any(b => !IoProxy.ExistsFile(false, b.filename, ModuleName, MethodName))) throw new Exception($@"{ModuleName}.{MethodName}: missing input files: {string.Join(@", ", cl.valuesCsvFilenames.Where(a => !IoProxy.ExistsFile(true, a.filename) || new FileInfo(a.filename).Length == 0).Select(a => a.filename).ToArray())}");
                if (cl.headerCsvFilenames.Any(b => !IoProxy.ExistsFile(false, b.filename, ModuleName, MethodName))) throw new Exception($@"{ModuleName}.{MethodName}: missing input files: {string.Join(@", ", cl.headerCsvFilenames.Where(a => !IoProxy.ExistsFile(true, a.filename) || new FileInfo(a.filename).Length == 0).Select(a => a.filename).ToArray())}");
                if (cl.commentCsvFilenames.Any(b => !IoProxy.ExistsFile(false, b.filename, ModuleName, MethodName))) throw new Exception($@"{ModuleName}.{MethodName}: missing input files: {string.Join(@", ", cl.commentCsvFilenames.Where(a => !IoProxy.ExistsFile(true, a.filename) || new FileInfo(a.filename).Length == 0).Select(a => a.filename).ToArray())}");
            }

            Logging.LogExit(ModuleName);
        }


        //public async Task LoadDataSetAsync(string DataSetFolder, string[] fileTags /*DataSet_names*/, IList<(int ClassId, string ClassName)> classNames //,
        //    //bool perform_integrity_checks = false,
        //    //bool required_default = true,
        //    //IList<(bool required, string gkAlphabet, string gkStats, string gkDimension, string gkCategory, string gkSource, string @gkGroup, string gkMember, string gkPerspective)> required_matches = null
        //    , CancellationToken ct)
        //{
        //    Logging.LogCall(ModuleName);

        //    if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

        //    const string MethodName = nameof(LoadDataSetAsync);


        //    if (fileTags == null || fileTags.Length == 0 || fileTags.Any(string.IsNullOrWhiteSpace)) throw new ArgumentOutOfRangeException(nameof(fileTags));


        //    classNames = classNames.OrderBy(a => a.ClassId).ToList();
        //    foreach (var cl in classNames) Logging.WriteLine($@"{cl.ClassId:+#;-#;+0} = {cl.ClassName}", ModuleName, MethodName);

        //    fileTags = fileTags.OrderBy(a => a).ToArray();
        //    foreach (var gkFileTag in fileTags) Logging.WriteLine($@"{gkFileTag}: {gkFileTag}", ModuleName, MethodName);

        //    var dataFilenames = GetDataFilenames(DataSetFolder, fileTags, classNames);
        //    CheckDataFiles(dataFilenames);


        //    await LoadDataSetValuesAsync(dataFilenames, ct).ConfigureAwait(false);

        //    ClassSizes = ValueList.Select(a => (a.ClassId, a.ClassSize)).ToArray();
        //}

        public void LoadDataSet(string dataSetFolder, string[] fileTags /*DataSet_names*/, IList<(int ClassId, string ClassName)> classNames 
                                //,
                                //bool perform_integrity_checks = false,
                                //bool required_default = true,
                                //IList<(bool required, string gkAlphabet, string gkStats, string gkDimension, string gkCategory, string gkSource, string @gkGroup, string gkMember, string gkPerspective)> required_matches = null
                                , CancellationToken ct)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) 
            {
                Logging.LogExit(ModuleName); 
                return; 
            }

            DataSetFileTags = fileTags;

            const string methodName = nameof(LoadDataSet);


            if (fileTags == null || fileTags.Length == 0 || fileTags.Any(string.IsNullOrWhiteSpace)) throw new ArgumentOutOfRangeException(nameof(fileTags));


            classNames = classNames.OrderBy(a => a.ClassId).ToList();
            foreach (var cl in classNames) Logging.WriteLine($@"{cl.ClassId:+#;-#;+0} = {cl.ClassName}", ModuleName, methodName);

            fileTags = fileTags.OrderBy(a => a).ToArray();
            foreach (var fileTag in fileTags) Logging.WriteLine($@"{fileTag}: {fileTag}", ModuleName, methodName);

            var dataFilenames = GetDataFilenames(dataSetFolder, fileTags, classNames);
            CheckDataFiles(dataFilenames);

            LoadDataSetValues(dataFilenames, ct);


            //var numfeatures = ColumnHeaderList.Length;

            ClassSizes = ValueList.Select(a => (a.ClassId, a.ClassName, a.ClassSize, DownSampledClassSize: ValueList.Where(a => a.ClassSize > 0).Min(a => a.ClassSize), ClassFeatures: a.ClassValueList.First().RowColumns.Length)).ToArray();

            Logging.LogExit(ModuleName);
        }

        /*public void SaveBinary()
        {
            var f = File.Open("test", FileMode.Create, FileAccess.Write, FileShare.None);

            void sendInt(int value)
            {
                f.Write(BitConverter.GetBytes(value));
            }

            void sendDouble(double value)
            {
                f.Write(BitConverter.GetBytes(value));
            }

            void sendStr(string str)
            {
                var strbytes = Encoding.UTF8.GetBytes(str);
                var strlen = strbytes.Length;
                sendInt(strlen);
                f.Write(strbytes);
            }

            foreach (var a in ValueList)
            {
                f.Write(BitConverter.GetBytes(a.ClassId));
                sendStr(a.ClassName);
                sendInt(a.ClassSize);

                foreach (var b in a.ClassValueList)
                {
                    foreach (var c in b.RowComment)
                    {
                        sendInt(c.CommentRowIndex);
                        sendInt(c.CommentColumnIndex);
                        sendStr(c.CommentKey);
                        sendStr(c.CommentValue);
                    }

                    foreach (var d in b.RowColumns)
                    {
                        sendStr(d.ColumnHeader.);
                        sendInt(d.RowIndex);
                        sendInt(d.ColumnIndex);
                        sendDouble(d.RowColumnValue);
                    }
                }
            }


            f.Close();
            f.Dispose();
        }*/
    }
}
//{
//    // all comment headers should be equal
//    var CommentHeaders = file_data.SelectMany(a => a.commentCsv_data.Select(b => b.header_row).ToList()).ToList();
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
//    var headers_headers = file_data.SelectMany(a => a.headerCsv_data.Select(b => b.header_row).ToList()).ToList();
//    for (var ci = 0; ci < headers_headers.Count; ci++)
//    {
//        for (var cj = 0; cj < headers_headers.Count; cj++)
//        {
//            if (cj <= ci) continue;

//            if (!headers_headers[ci].SequenceEqual(headers_headers[cj])) throw new Exception();
//        }
//    }
//}


// READ HEADER CSV FILE - ALL CLASSES HAVE THE SAME HEADERS/FEATURES within the same FileTag (will change for each one, e.g. 1i, 1n, 1p, 2i, 2n, 2p, 3i, 3n, 3p)
// same FileTag -> same headers... different FileTag -> different headers
//FileTags.Select(a=>a.).Select(a => file_data.First(b => b.headerCsv_data.First(c => c.FileTag == a));
//var all_headers = file_data.SelectMany(a => a.headerCsv_data).ToList();
//var first_headers = FileTags.Select(a => all_headers.First(b => b.FileTag == a)).ToList();


//var feature_catalog = first_headers.AsParallel().AsOrdered().SelectMany((tag_headers, tag_headers_index) =>
//    tag_headers.data_rows.Select((row, row_index) =>
//    //.SelectMany((cl, cl_index) => 
//    //cl.headerCsv_data.First(/* first file - all FileTag header files are the same */).data_rows.AsParallel().AsOrdered().Select((row, row_index) =>
//    {
//        var internal_fid = row_index;
//        for (var i = 0; i < tag_headers_index; i++)
//        {
//            internal_fid += first_headers[i].data_rows.Count;
//        }

//        //if (FileTag_index != 0 && row_index == 0)
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
//            FileTag: tag_headers.FileTag,
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
////    (internal_fid:i, a.external_fid, a.FileTag, a.gkAlphabet, a.gkDimension, a.gkCategory, a.gkSource, a.gkGroup, a.gkMember, a.gkPerspective)
////).ToList();

////if (cl.valuesCsv_data.Select(a => a.data_rows.Select(b => b.Length).ToList()).Distinct().Count() != 1) throw new Exception();

////var sample_data = file_data.Select(cl => cl.valuesCsv_data.SelectMany(tag => tag.data_key_value_list).ToList()).ToList();
//var sample_data = file_data.Select(cl =>
//{
//    // check same number of samples per tag
//    if (cl.valuesCsv_data.Select(a => a.data_key_value_list.Count).Distinct().Count() != 1) throw new Exception();

//    var header_row = new List<string>();
//    var data_rows = new List<List<double>>();
//    for (var i = 0; i < )
//    var data_row_key_value_list = new List<(string key, double value, List<(string key, string value)> header, List<(string key, string value)> RowComments)>();

//    for (var tag_index = 0; tag_index < cl.valuesCsv_data.Count; tag_index++)
//    {
//        header_row.AddRange(cl.valuesCsv_data[tag_index].header_row);

//        if (tag_index == 0) data_rows.AddRange(cl.valuesCsv_data[tag_index].data_rows);
//        else data_rows = data_rows.Select(a => a).ToList();

//        data_row_key_value_list.AddRange(cl.valuesCsv_data[tag_index].data_row_key_value_list);
//    }

//    // merge tags

//    Logging.LogExit(ModuleName); return 0;
//    //Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :cl.valuesCsv_data.SelectMany(tag => tag.data_key_value_list).ToList();
//}).ToList();

// class [ example id ] [ FileTag , fid ] = fv
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
            header_data[i] = (header_data[i].FileTag, x);
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
        (+1, Path.Combine(DataSet_folder, $"{Path.GetFileNameWithoutExtension(DataSet_headerCsvFiles[0])}_updated{Path.GetExtension(DataSet_headerCsvFiles[0])}"), Path.Combine(DataSet_folder, $"{Path.GetFileNameWithoutExtension(DataSetCsvFiles[0])}_updated{Path.GetExtension(DataSetCsvFiles[0])}")),
        (-1, Path.Combine(DataSet_folder, $"{Path.GetFileNameWithoutExtension(DataSet_headerCsvFiles[1])}_updated{Path.GetExtension(DataSet_headerCsvFiles[1])}"), Path.Combine(DataSet_folder, $"{Path.GetFileNameWithoutExtension(DataSetCsvFiles[1])}_updated{Path.GetExtension(DataSetCsvFiles[0])}"))
    }); // Path.Combine(DataSet_folder, "updated_headers.csv"), Path.Combine(DataSet_folder, "updated_DataSet.csv"));
    }
}
*/


/*public static bool matches(string text, string search_pattern)
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
public static double[][][] Getcolumn_data_by_class(DataSet DataSet) // [column][row]
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
public static double[][] Getcolumn_data(DataSet DataSet) // [column][row]
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
public static void remove_large_groups(DataSet DataSet, int max_group_size)
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
public static void remove_duplicate_groups(DataSet DataSet)
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
public static void save_DataSet(DataSet DataSet, List<(int ClassId, string header_filename, string data_filename)> filenames)
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

        await io_proxy.WriteAllLines(true, ClassId_filenames.header_filename, header).ConfigureAwait(false);

        await io_proxy.WriteAllLines(true, ClassId_filenames.data_filename, data).ConfigureAwait(false);
    }

    Logging.WriteLine("finished saving...", _CallerModuleName: ModuleName, _CallerMethodName: MethodName);
}
*/

/*
public static void remove_empty_features(DataSet DataSet, double min_non_zero_pct = 0.25, int min_distinct_numbers = 2)
public static void remove_empty_features(DataSet DataSet, int min_distinct_numbers = 2)
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
//public static void remove_empty_features_by_class(DataSet DataSet, double min_non_zero_pct = 0.25, int min_distinct_numbers = 2, string stats_filename = null)
public static void remove_empty_features_by_class(DataSet DataSet, int min_distinct_numbers = 2, string stats_filename = null)
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
        await io_proxy.WriteAllLines(true, stats_filename, data).ConfigureAwait(false);
    }
}
*/

/*
public static void remove_fids(DataSet DataSet, List<int> fids_to_remove)
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
public static List<(int fid, double value)> parseCsv_line_doubles(string line, bool[] required = null)
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
/* public static List<string> parseCsv_line_strings(string line)
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


/*public static double fix_double(string double_value)
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

/*public static double fix_double(double value)
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