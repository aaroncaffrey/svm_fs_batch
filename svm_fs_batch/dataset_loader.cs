using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace svm_fs_batch
{
    internal class dataset_loader
    {
       

        internal List<(int internal_column_index, int external_column_index, string file_tag, string alphabet, string dimension, string category, string source, string @group, string member, string perspective)> header_list;
        internal List<(int class_id, string class_name, List<(int row_index, int col_index, string comment_key, string comment_value)[]> cl_comment_list)> comment_list;

        // feature values, grouped by class id, with class name and class size
        //internal List<(int class_id, string class_name, int class_size, List<((int internal_column_index, int external_column_index, string file_tag, string alphabet, string dimension, string category, string source, string @group, string member, string perspective) column_header, double fv)[]> val_list)> value_list;
        internal List<(int class_id, string class_name, int class_size, ((int row_index, int col_index, string comment_key, string comment_value)[] row_comment, (int row_index, int col_index, (int internal_column_index, int external_column_index, string file_tag, string alphabet, string dimension, string category, string source, string @group, string member, string perspective) column_header, double row_column_val)[] row_columns)[] val_list)> value_list;
        internal List<(int class_id, int class_size)> class_sizes;


        internal double[][] get_row_features(List<(int class_id, List<int> row_indexes)> class_row_indexes, List<int> column_indexes)
        {
            if (column_indexes.First() != 0) throw new Exception(); // class id missing

            var class_rows = class_row_indexes.AsParallel().AsOrdered().Select(class_row_index => get_class_row_features(class_row_index.class_id, class_row_index.row_indexes, column_indexes)).ToList();
            var rows = class_rows.SelectMany(a => a.as_rows).ToArray();

            return rows;
        }

        internal static scaling[] get_scaling_params(double[][] rows, List<int> column_indexes)
        {
            var cols = column_indexes.Select((column_index, x_index) => rows.Select(row => row[x_index /* column_index -> x_index*/]).ToArray()).ToArray();
            var sp = cols.Select((col, x_index) => x_index == 0 /* do not scale class id */ ? null : new scaling(col)).ToArray();

            return sp;
        }

        public static double[][] get_scaled_rows(double[][] rows, /*List<int> column_indexes,*/ scaling[] sp, scaling.scale_function sf)
        {
            //var cols = column_indexes.Select((column_index, x_index) => rows.Select(row => row[x_index /* column_index -> x_index*/]).ToArray()).ToArray();
            //var cols_scaled = cols.Select((v, x_index) =)

            var rows_scaled = rows.Select((row, row_index) => row.Select((col_val, col_index) => col_index != 0 ? sp[col_index].scale(col_val, sf) : col_val).ToArray()).ToArray();

            return rows_scaled;
        }

        internal (double[/*row*/][/*col*/] as_rows, double[/*col*/][/*row*/] as_cols) get_class_row_features(int class_id, List<int> row_indexes, List<int> column_indexes)
        {
            if (column_indexes.First() != 0) throw new Exception(); // class id missing

            var as_rows = new double[row_indexes.Count][];
            var as_cols = new double[column_indexes.Count][];

            var v = value_list.First(a => a.class_id == class_id).val_list;

            for (var y_index = 0; y_index < row_indexes.Count; y_index++)
            {
                var row_index = row_indexes[y_index];
                as_rows[row_index] = new double[column_indexes.Count];

                for (var x_index = 0; x_index < column_indexes.Count; x_index++)
                {
                    var col_index = column_indexes[x_index];
                    as_rows[row_index][col_index] = v[row_index].row_columns[col_index].row_column_val;
                }
            }


            for (var x_index = 0; x_index < column_indexes.Count; x_index++)
            {
                var col_index = column_indexes[x_index];
                as_cols[col_index] = new double[row_indexes.Count];

                for (var y_index = 0; y_index < row_indexes.Count; y_index++)
                {
                    var row_index = row_indexes[y_index];
                    as_cols[col_index][row_index] = v[row_index].row_columns[col_index].row_column_val;
                }
            }

            return (as_rows, as_cols);
        }

        internal dataset_loader(string dataset_names = "2i,2n")//, bool split_by_file_tag = true, bool split_by_groups = true)
        {
            var required_default = false;
            var required_matches = new List<(bool required, string alphabet, string dimension, string category, string source, string group, string member, string perspective)>();
            //required_matches.Add((required: true, alphabet: null, dimension: null, category: null, source: null, group: null, member: null, perspective: null));

            // file tags: 1i, 1n, 1p, 2i, 2n, 2p, 3i, 3n, 3p (1d - linear, 2d - predicted, 3d - actual, interface, neighborhood, protein)

            load_dataset(
                dataset_folder: settings.dataset_dir,
                file_tags: dataset_names.Split(',', StringSplitOptions.RemoveEmptyEntries), // "2i"
                class_names: settings.class_names,
                perform_integrity_checks: false,
                required_default: required_default,
                required_matches: required_matches
            );

        }

        private void load_dataset
        (
            string dataset_folder, 
            string[] file_tags, 
            List<(int class_id, string class_name)> class_names, 
            bool perform_integrity_checks = false, 
            bool required_default = true, 
            List<(bool required, string alphabet, string dimension, string category, string source, string @group, string member, string perspective)> required_matches = null
        )
        {
            const string method_name = nameof(load_dataset);
            const string module_name = nameof(dataset_loader);

            class_names = class_names.OrderBy(a => a.class_id).ToList();
            file_tags = file_tags.OrderBy(a => a).ToArray();

             var data_filenames = class_names.Select(cl =>
                {
                    // (string file_tag, int class_id, string class_name, string filename)
                    var values_csv_filenames = file_tags.Select(file_tag => (file_tag, cl.class_id, cl.class_name, filename: Path.Combine(dataset_folder, $@"f_{file_tag}_({cl.class_id:+#;-#;+0})_({cl.class_name}).csv"))).ToList();
                    var header_csv_filenames = file_tags.Select(file_tag => (file_tag, cl.class_id, cl.class_name, filename: Path.Combine(dataset_folder, $@"h_{file_tag}_({cl.class_id:+#;-#;+0})_({cl.class_name}).csv"))).ToList();
                    var comment_csv_filenames = file_tags.Select(file_tag => (file_tag, cl.class_id, cl.class_name, filename: Path.Combine(dataset_folder, $@"c_{file_tag}_({cl.class_id:+#;-#;+0})_({cl.class_name}).csv"))).ToList();

                    return (cl.class_id, cl.class_name, values_csv_filenames, header_csv_filenames, comment_csv_filenames);

                })
                .ToList();

            foreach (var cl in class_names) { io_proxy.WriteLine($@"{cl.class_id:+#;-#;+0} = {cl.class_name}", module_name, method_name); }

            foreach (var cl in data_filenames)
            {
                io_proxy.WriteLine($@"{nameof(cl.values_csv_filenames)}: {string.Join(", ", cl.values_csv_filenames)}", module_name, method_name);
                io_proxy.WriteLine($@"{nameof(cl.header_csv_filenames)}: {string.Join(", ", cl.header_csv_filenames)}", module_name, method_name);
                io_proxy.WriteLine($@"{nameof(cl.comment_csv_filenames)}: {string.Join(", ", cl.comment_csv_filenames)}", module_name, method_name);
            }

            // don't try to read any data until checking all files exist...
            if (data_filenames == null || data_filenames.Count == 0) throw new Exception();
            foreach (var cl in data_filenames)
            {
                if (cl.values_csv_filenames == null || cl.values_csv_filenames.Count == 0 || cl.values_csv_filenames.Any(b => !io_proxy.Exists(b.filename))) throw new Exception();
                if (cl.header_csv_filenames == null || cl.header_csv_filenames.Count == 0 || cl.header_csv_filenames.Any(b => !io_proxy.Exists(b.filename))) throw new Exception();
                if (cl.comment_csv_filenames == null || cl.comment_csv_filenames.Count == 0 || cl.comment_csv_filenames.Any(b => !io_proxy.Exists(b.filename))) throw new Exception();
            }




            // 1. headers
            io_proxy.WriteLine($@"Start: reading headers.", module_name, method_name);

            var header_list = data_filenames.First(/* headers are same for all classes, so only load first class headers */)
                .header_csv_filenames.AsParallel()
                .AsOrdered()
                .SelectMany((file_info, file_index) =>
                {
                    return io_proxy.ReadAllLines(file_info.filename, module_name, method_name)
                        .Skip(file_index == 0 ? 1 : 2 /*skip header, and if not first file, class id rows*/)
                        .AsParallel()
                        .AsOrdered()
                        .Select((b, b_i) =>
                        {
                            var row = b.Split(',');
                            return (internal_column_index: -1, external_column_index: b_i /*int.Parse(row[0], NumberStyles.Integer, CultureInfo.InvariantCulture)*/, file_tag: (file_index == 0 && b_i == 0 ? "" : file_info.file_tag), alphabet: row[1], dimension: row[2], category: row[3], source: row[4], group: row[5], member: row[6], perspective: row[7]);
                        })
                        .ToArray();
                })
                .ToList();

            header_list = header_list.AsParallel().AsOrdered().Select((a, internal_column_index) => (internal_column_index, a.external_column_index, a.file_tag, a.alphabet, a.dimension, a.category, a.source, a.@group, a.member, a.perspective)).ToList();

            io_proxy.WriteLine($@"Finish: reading headers.", module_name, method_name);

            // 1.a compress headers
            //var header_str = header_list.AsParallel().AsOrdered().SelectMany(a => new string[] {a.alphabet, a.dimension, a.category, a.source, a.group, a.member, a.perspective}).Distinct().ToList();
            //header_list = header_list.AsParallel().AsOrdered().Select(a => (
            //    a.internal_fid,
            //    a.external_fid,
            //    alphabet:header_str.First(b => b == a.alphabet),
            //    dimension:header_str.First(b => b == a.dimension),
            //    category:header_str.First(b => b == a.category),
            //    source:header_str.First(b => b == a.source),
            //    group:header_str.First(b => b == a.group),
            //    member:header_str.First(b => b == a.member),
            //    perspective:header_str.First(b => b == a.perspective)
            //        )).ToList();


            // 2. comment files. (same class with same samples = same data)
            io_proxy.WriteLine($@"Start: reading comments.", module_name, method_name);
            var comment_list = data_filenames.AsParallel()
                .AsOrdered()
                .Select(cl =>
                {

                    var comment_lines = io_proxy.ReadAllLines(cl.comment_csv_filenames.First().filename, module_name, method_name).AsParallel().AsOrdered().Select(line => line.Split(',')).ToList();
                    var comment_header = comment_lines.First();
                    var cl_comment_list = comment_lines.Skip(1 /*skip header*/)
                        .AsParallel()
                        .AsOrdered()
                        .Select((row_split, row_index) =>
                        {
                            var key_value_list = row_split.AsParallel().AsOrdered().Select((col_data, col_index) => (
                                row_index: row_index,
                                col_index: col_index,
                                comment_key: comment_header[col_index], 
                                comment_value: col_data
                                )).ToArray();

                            return key_value_list;
                        })
                        .ToList();
                    return (cl.class_id, cl.class_name, cl_comment_list);
                })
                .ToList();
            io_proxy.WriteLine($@"Finish: reading comments.", module_name, method_name);


            // 3. values
            io_proxy.WriteLine($@"Start: reading values.", module_name, method_name);

            var value_list = data_filenames
                    .AsParallel()
                    .AsOrdered()
                    .Select((cl, cl_index) =>
                {
                    // 3. experimental sample data
                    var vals_tag = cl.values_csv_filenames.AsParallel().AsOrdered().Select((file_info, file_info_index) => io_proxy.ReadAllLines(file_info.filename, module_name, method_name).Skip(1 /*skip header - col index only*/)
                        .AsParallel().AsOrdered().Select((row, row_index) => row.Split(',').Skip(file_info_index == 0 ? 0 : 1 /*skip class id*/).AsParallel().AsOrdered().Select((col, col_index) => double.Parse(col, NumberStyles.Float, CultureInfo.InvariantCulture)).ToArray()).ToArray()).ToList();
                    var vals = new double[vals_tag.First().Length /* number of rows */][ /* columns */];
                    for (var row_index = 0; row_index < vals.Length; row_index++)
                    {
                        //vals[row_index] = new double[vals_tag.Sum(a=> a[row_index].Length)];
                        vals[row_index] = vals_tag.SelectMany((a_cl, a_cl_index) => a_cl[row_index]).ToArray();
                    }

                    var val_list = vals.AsParallel()
                        .AsOrdered()
                        .Select((row, row_index) =>
                        {
                            var comment = comment_list[cl_index].cl_comment_list[row_index];

                            return (row_comment:comment, row_columns: row.Select((col_val, col_index) =>
                                {
                                    var column_header = header_list[col_index];

                                    return (
                                        row_index: row_index, 
                                        col_index: col_index, 
                                        column_header: column_header, 
                                        row_column_val: vals[row_index][col_index]
                                        );
                                })
                                .ToArray());
                        })
                        .ToArray();


                    return (cl.class_id, cl.class_name, class_size: val_list.Length, /*comment_list[cl_index].cl_comment_list,*/ val_list);
                })
                .ToList();
            io_proxy.WriteLine($@"Finish: reading values.", module_name, method_name);


            this.header_list = header_list;
            this.comment_list = comment_list;
            this.value_list = value_list;
            this.class_sizes = value_list.Select(a => (a.class_id, a.class_size)).ToList();
        }
    }
}
//{
            //    // all comment headers should be equal
            //    var comment_headers = file_data.SelectMany(a => a.comment_csv_data.Select(b => b.header_row).ToList()).ToList();
            //    for (var ci = 0; ci < comment_headers.Count; ci++)
            //    {
            //        for (var cj = 0; cj < comment_headers.Count; cj++)
            //        {
            //            if (cj <= ci) continue;

            //            if (!comment_headers[ci].SequenceEqual(comment_headers[cj])) throw new Exception();
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


            //        var external_fid = int.Parse(row[0], NumberStyles.Integer, CultureInfo.InvariantCulture);
            //        //if (fid!=i) throw new Exception();


            //        var alphabet = row[1];
            //        var dimension = row[2];
            //        var category = row[3];
            //        var source = row[4];
            //        var group = row[5];
            //        var member = row[6];
            //        var perspective = row[7];


            //        const string def = "default";

            //        if (string.IsNullOrWhiteSpace(alphabet)) alphabet = def;
            //        if (string.IsNullOrWhiteSpace(dimension)) dimension = def;
            //        if (string.IsNullOrWhiteSpace(category)) category = def;
            //        if (string.IsNullOrWhiteSpace(source)) source = def;
            //        if (string.IsNullOrWhiteSpace(group)) group = def;
            //        if (string.IsNullOrWhiteSpace(member)) member = def;
            //        if (string.IsNullOrWhiteSpace(perspective)) perspective = def;

            //        lock (lock_table)
            //        {
            //            if (!table_alphabet.Contains(alphabet)) { table_alphabet.Add(alphabet); }
            //            if (!table_dimension.Contains(dimension)) { table_dimension.Add(dimension); }
            //            if (!table_category.Contains(category)) { table_category.Add(category); }
            //            if (!table_source.Contains(source)) { table_source.Add(source); }
            //            if (!table_group.Contains(group)) { table_group.Add(group); }
            //            if (!table_member.Contains(member)) { table_member.Add(member); }
            //            if (!table_perspective.Contains(perspective)) { table_perspective.Add(perspective); }
            //        }

            //        var alphabet_id = table_alphabet.LastIndexOf(alphabet);
            //        var dimension_id = table_dimension.LastIndexOf(dimension);
            //        var category_id = table_category.LastIndexOf(category);
            //        var source_id = table_source.LastIndexOf(source);
            //        var group_id = table_group.LastIndexOf(group);
            //        var member_id = table_member.LastIndexOf(member);
            //        var perspective_id = table_perspective.LastIndexOf(perspective);

            //        alphabet = table_alphabet[alphabet_id];
            //        dimension = table_dimension[dimension_id];
            //        category = table_category[category_id];
            //        source = table_source[source_id];
            //        group = table_group[group_id];
            //        member = table_member[member_id];
            //        perspective = table_perspective[perspective_id];

            //        return (
            //            internal_fid: internal_fid,
            //            external_fid: external_fid,
            //            file_tag: tag_headers.file_tag,
            //            alphabet: alphabet,
            //            dimension: dimension,
            //            category: category,
            //            source: source,
            //            group: group,
            //            member: member,
            //            perspective: perspective
            //        );

            //        //internal_fid: internal_fid,
            //        //fid: fid, 
            //        //, alphabet_id: alphabet_id, dimension_id: dimension_id, category_id: category_id, source_id: source_id, group_id: group_id, member_id: member_id, perspective_id: perspective_id
            //    }).ToList()).ToList();

            ////var feature_catalog = feature_catalog_data.Select((a,i) => 
            ////    (internal_fid:i, a.external_fid, a.file_tag, a.alphabet, a.dimension, a.category, a.source, a.group, a.member, a.perspective)
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

            //    return 0;
            //    //return cl.values_csv_data.SelectMany(tag => tag.data_key_value_list).ToList();
            //}).ToList();

            // class [ example id ] [ file_tag , fid ] = fv
            // class [ ] 

            // limit dataset to required matches...
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
                            matches(a.alphabet, rm.alphabet) &&
                            matches(a.category, rm.category) &&
                            matches(a.dimension, rm.dimension) &&
                            matches(a.source, rm.source) &&
                            matches(a.@group, rm.@group) &&
                            matches(a.member, rm.member) &&
                            matches(a.perspective, rm.perspective)
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
            //            matches(a.alphabet, rm.alphabet) &&
            //            matches(a.category, rm.category) &&
            //            matches(a.dimension, rm.dimension) &&
            //            matches(a.source, rm.source) &&
            //            matches(a.@group, rm.@group) &&
            //            matches(a.member, rm.member) &&
            //            matches(a.perspective, rm.perspective)
            //        ).Select(a => a.internal_fid).ToList();


            //        matching_fids.ForEach(a => required[a] = rm.required);

            //    }

            //    required[0] = true;

            //    feature_catalog = feature_catalog.Where((a, i) => required[a.internal_fid]).ToList();
            //}




            /*
            if (perform_integrity_checks)
            {
                io_proxy.WriteLine($@"Checking all dataset columns are the same length...", module_name, method_name);
                var dataset_num_diferent_column_length = dataset_instance_list.Select(a => a.feature_data.Count).Distinct().Count();
                if (dataset_num_diferent_column_length != 1) throw new Exception();

                io_proxy.WriteLine($@"Checking dataset headers and dataset columns are the same length...", module_name, method_name);
                var header_length = header_data.Count;
                var dataset_column_length = dataset_instance_list.First().feature_data.Count;
                if (dataset_column_length != header_length) throw new Exception();

                io_proxy.WriteLine($@"Checking all dataset comment columns are the same length...", module_name, method_name);
                var comments_num_different_column_length = dataset_instance_list.Select(a => a.comment_columns.Count).Distinct().Count();
                if (comments_num_different_column_length != 1)
                {
                    var distinct_comment_counts = dataset_instance_list.Select(a => a.comment_columns.Count).Distinct().ToList();

                    var cc = distinct_comment_counts.Select(a => (a, dataset_instance_list.Where(b => b.comment_columns.Count == a).Select(b => b.example_id).ToList())).ToList();

                    cc.ForEach(a => Console.WriteLine(a.a + ": " + string.Join(", ", a.Item2)));

                    throw new Exception();
                }
            }
            */

            /*
             if (fix_dataset)
             
            {
                var num_headers_before = dataset.dataset_headers.Count;

                remove_empty_features(dataset, 2);
                remove_empty_features_by_class(dataset, 2);//, 2, $@"e:\input\feature_stats.csv");
                remove_large_groups(dataset, 100);
                remove_duplicate_groups(dataset);
                // remove_duplicate_features(); will be done on the feature selection algorithm, otherwise potential feature groups may lack useful information

                var num_headers_after = dataset.dataset_headers.Count;

                if (num_headers_before != num_headers_after)
                {
                    save_dataset(dataset, new List<(int class_id, string header_filename, string data_filename)>()
                {
                    (+1, Path.Combine(dataset_folder, $"{Path.GetFileNameWithoutExtension(dataset_header_csv_files[0])}_updated{Path.GetExtension(dataset_header_csv_files[0])}"), Path.Combine(dataset_folder, $"{Path.GetFileNameWithoutExtension(dataset_csv_files[0])}_updated{Path.GetExtension(dataset_csv_files[0])}")),
                    (-1, Path.Combine(dataset_folder, $"{Path.GetFileNameWithoutExtension(dataset_header_csv_files[1])}_updated{Path.GetExtension(dataset_header_csv_files[1])}"), Path.Combine(dataset_folder, $"{Path.GetFileNameWithoutExtension(dataset_csv_files[1])}_updated{Path.GetExtension(dataset_csv_files[0])}"))
                }); // Path.Combine(dataset_folder, "updated_headers.csv"), Path.Combine(dataset_folder, "updated_dataset.csv"));
                }
            }
            */

        


        /*private static bool matches(string text, string search_pattern)
        {
            if (string.IsNullOrWhiteSpace(search_pattern) || search_pattern == "*")
            {
                return true;
            }
            else if (search_pattern.StartsWith("*", StringComparison.InvariantCulture) && search_pattern.EndsWith("*", StringComparison.InvariantCulture))
            {
                search_pattern = search_pattern[1..^1];

                return text.Contains(search_pattern, StringComparison.InvariantCultureIgnoreCase);
            }
            else if (search_pattern.StartsWith("*", StringComparison.InvariantCulture))
            {
                search_pattern = search_pattern[1..];

                return text.EndsWith(search_pattern, StringComparison.InvariantCultureIgnoreCase);
            }
            else if (search_pattern.EndsWith("*", StringComparison.InvariantCulture))
            {
                search_pattern = search_pattern[0..^1];

                return text.StartsWith(search_pattern, StringComparison.InvariantCultureIgnoreCase);
            }
            else
            {
                return string.Equals(text, search_pattern, StringComparison.InvariantCultureIgnoreCase);
            }
        }*/

        /*
        internal static double[][][] get_column_data_by_class(dataset dataset) // [column][row]
        {
            //io_proxy.WriteLine("...", module_name, nameof(get_column_data_by_class));

            var total_headers = dataset.dataset_headers.Count;
            var total_classes = dataset.dataset_instance_list.Select(a => a.class_id).Distinct().Count();

            var result = new double[total_headers][][];

            for (var i = 0; i < dataset.dataset_headers.Count; i++)
            {
                var x = dataset.dataset_instance_list.GroupBy(a => a.class_id).Select(a => a.Select(b => b.feature_data[i].fv).ToArray()).ToArray();

                result[i] = x;
            }

            return result;
        }
        */

        /*
        internal static double[][] get_column_data(dataset dataset) // [column][row]
        {
            //io_proxy.WriteLine("...", module_name, nameof(get_column_data));

            var result = new double[dataset.dataset_headers.Count][];

            for (var i = 0; i < dataset.dataset_headers.Count; i++)
            {
                result[i] = dataset.dataset_instance_list.Select(a => a.feature_data[i].fv).ToArray();
            }

            return result;
        }
        */

        /*
        internal static void remove_large_groups(dataset dataset, int max_group_size)
        {
            const string module_name = nameof(dataset_loader);
            const string method_name = nameof(remove_large_groups);

            //io_proxy.WriteLine("...", module_name, nameof(remove_large_groups));

            var groups = dataset.dataset_headers.Skip(1).GroupBy(a => (a.alphabet_id, a.dimension_id, a.category_id, a.source_id, a.group_id)).OrderBy(a => a.Count()).ToList();

            //groups.ForEach(a => Console.WriteLine(a.Count() + ": " + a.First().alphabet + ", " + a.First().dimension + ", " + a.First().category + ", " + a.First().group));

            //var sizes = groups.Select(a => a.Count()).Distinct().Select(b=> (size: b, count: groups.Count(c=> c.Count() == b))).OrderByDescending(a => a).ToList();

            //Console.WriteLine("sizes: " + string.Join(", ", sizes));

            var groups_too_large = groups.Where(a => a.Count() > max_group_size).ToList();

            var fids_to_remove = groups_too_large.SelectMany(a => a.Select(b => b.fid).ToList()).ToList();

            if (fids_to_remove != null && fids_to_remove.Count > 0)
            {
                remove_fids(dataset, fids_to_remove);
            }

            groups_too_large.ForEach(a => io_proxy.WriteLine($@"Removed large group: {a.Key}", module_name, method_name));
        }
        */

        /*
        internal static void remove_duplicate_groups(dataset dataset)
        {
            //const string module_name = nameof(dataset_loader);
            //const string method_name = nameof(remove_duplicate_groups);

            //io_proxy.WriteLine("...", module_name, nameof(remove_duplicate_groups));


            var column_data = get_column_data(dataset);

            var grouped_by_groups = dataset.dataset_headers.Skip(1).GroupBy(a => (a.alphabet_id, a.dimension_id, a.category_id, a.source_id, a.group_id)).ToList();

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

                    var group_to_keep_key = new string[] { group_to_keep.First().alphabet, group_to_keep.First().dimension, group_to_keep.First().category, group_to_keep.First().source, group_to_keep.First().group };
                    var groups_to_remove_keys = groups_to_remove.Select(a => new string[] { a.First().alphabet, a.First().dimension, a.First().category, a.First().source, a.First().group }).ToList();

                    Console.WriteLine($"+    Keeping: {string.Join(".", group_to_keep_key)}");
                    groups_to_remove_keys.ForEach(a => Console.WriteLine($"-   Removing: {string.Join(".", a)}"));


                    //var cluster_fids_to_remove = groups_to_remove.SelectMany(a => a.Select(b => b.fid).ToList()).Distinct().OrderBy(a=>a).ToList();
                    var cluster_fids_to_remove = groups_to_remove.SelectMany(a => a.Select(b => b.fid).ToList()).ToList();
                    fids_to_remove.AddRange(cluster_fids_to_remove);

                    // todo: loop through each column of group to keep, find sequence equal in the groups to remove, to get the correct new header names
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
                            alphabets.Add(x.alphabet);
                            dimensions.Add(x.dimension);
                            categories.Add(x.category);
                            sources.Add(x.source);
                            groups.Add(x.group);
                            //members.Add(x.member);
                            //perspectives.Add(x.perspective);
                        }

                        //Console.WriteLine("Duplicate group: " + string.Join(",", cg.Select(b => b.alphabet + "," + b.dimension + "," + b.category + "," + b.source + "," + b.group + "," + b.member + "," + b.perspective).ToList()));
                    }

                    var new_header = (
                        alphabet: string.Join("|", alphabets.Distinct().ToList()),
                        dimension: string.Join("|", dimensions.Distinct().ToList()),
                        category: string.Join("|", categories.Distinct().ToList()),
                        source: string.Join("|", sources.Distinct().ToList()),
                        group: string.Join("|", groups.Distinct().ToList())
                        //string.Join("|", members.Distinct().ToList()),
                        //string.Join("|", perspectives.Distinct().ToList())
                        );

                    var new_header_str = new string[] { new_header.alphabet, new_header.dimension, new_header.category, new_header.source, new_header.group };


                    Console.WriteLine($"~ new header: {string.Join(".", new_header_str)}");
                    Console.WriteLine();

                    for (var i = 0; i < group_to_keep.Count; i++)
                    {
                        dataset.dataset_headers[group_to_keep[i].fid] =
                        (group_to_keep[i].fid,

                            new_header.alphabet, new_header.dimension, new_header.category, new_header.source, new_header.group, group_to_keep[i].member, group_to_keep[i].perspective,
                            group_to_keep[i].alphabet_id, group_to_keep[i].dimension_id, group_to_keep[i].category_id, group_to_keep[i].source_id, group_to_keep[i].group_id, group_to_keep[i].member_id, group_to_keep[i].perspective_id

                            );
                    }
                }
            }

            if (fids_to_remove != null && fids_to_remove.Count > 0)
            {
                remove_fids(dataset, fids_to_remove);
            }
        }
        */

        /*
        internal static void save_dataset(dataset dataset, List<(int class_id, string header_filename, string data_filename)> filenames)
        {
            const string module_name = nameof(dataset_loader);
            const string method_name = nameof(save_dataset);

            io_proxy.WriteLine("started saving...", module_name, method_name);

            //var class_ids = dataset.dataset_instance_list.Select(a => a.class_id).Distinct().OrderBy(a => a).ToList();
            var header_fids = Enumerable.Range(0, dataset.dataset_headers.Count);
            var header_fids_str = string.Join(",", header_fids);

            foreach (var class_id_filenames in filenames)//class_ids)
            {
                var data = new List<string>();
                data.Add(header_fids_str);

                dataset.dataset_instance_list.Where(a => a.class_id == class_id_filenames.class_id).ToList().
                    ForEach(a => data.Add(string.Join(",", a.feature_data.Select(b => b.fv.ToString("G17", NumberFormatInfo.InvariantInfo)).ToList())));

                var header = dataset.dataset_headers.Select(a => $@"{a.fid},{a.alphabet},{a.dimension},{a.category},{a.source},{a.group},{a.member},{a.perspective}").ToList();
                header.Insert(0, $@"fid,alphabet,dimension,category,source,group,member,perspective");

                io_proxy.WriteAllLines(class_id_filenames.header_filename, header);

                io_proxy.WriteAllLines(class_id_filenames.data_filename, data);
            }

            io_proxy.WriteLine("finished saving...", module_name, method_name);
        }
        */

        /*
        internal static void remove_empty_features(dataset dataset, double min_non_zero_pct = 0.25, int min_distinct_numbers = 2)
        internal static void remove_empty_features(dataset dataset, int min_distinct_numbers = 2)
        {
            const string module_name = nameof(dataset_loader);
            const string method_name = nameof(remove_empty_features);

            io_proxy.WriteLine("...", module_name, method_name);

            var empty_fids = new List<int>();

            var column_data = get_column_data(dataset);


            for (var i = 1; i < dataset.dataset_headers.Count; i++)
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
                    empty_fids.Add(dataset.dataset_headers[i].fid);
                    break;
                }
            }

            if (empty_fids != null && empty_fids.Count > 0)
            {
                remove_fids(dataset, empty_fids);
            }

            io_proxy.WriteLine($@"Removed features ({empty_fids.Count}): {string.Join(",", empty_fids)}", module_name, method_name);

        }
        */
        /*
        //internal static void remove_empty_features_by_class(dataset dataset, double min_non_zero_pct = 0.25, int min_distinct_numbers = 2, string stats_filename = null)
        internal static void remove_empty_features_by_class(dataset dataset, int min_distinct_numbers = 2, string stats_filename = null)
        {
            const string module_name = nameof(dataset_loader);
            const string method_name = nameof(remove_empty_features_by_class);

            io_proxy.WriteLine("...", module_name, method_name);

            var save_stats = !string.IsNullOrWhiteSpace(stats_filename);

            var empty_fids = new List<int>();

            var column_data = get_column_data_by_class(dataset);
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

            for (var fid = 1; fid < dataset.dataset_headers.Count; fid++)
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
                        empty_fids.Add(dataset.dataset_headers[fid].fid);
                        break;
                    }
                }
            }

            if (empty_fids != null && empty_fids.Count > 0)
            {
                remove_fids(dataset, empty_fids);
            }

            io_proxy.WriteLine($@"Removed features ({empty_fids.Count}): {string.Join(",", empty_fids)}", module_name, method_name);

            if (save_stats)
            {
                var data = new List<string>();
                data.Add("cid,fid,num_distinct_values,num_values_zero,num_values_zero_pct,num_values_non_zero,num_values_non_zero_pct,overlap,overlap_pct,non_overlap,non_overlap_pct");
                data.AddRange(class_stats.Select(a => $@"{a.cid},{a.fid},{a.num_distinct_values},{a.num_values_zero},{a.num_values_zero_pct},{a.num_values_non_zero},{a.num_values_non_zero_pct},{a.overlap},{a.overlap_pct},{a.non_overlap},{a.non_overlap_pct}").ToList());
                io_proxy.WriteAllLines(stats_filename, data);
            }
        }
        */

        /*
        internal static void remove_fids(dataset dataset, List<int> fids_to_remove)
        {
            //io_proxy.WriteLine("...", module_name, nameof(remove_fids));

            // removed given fids and renumber the headers/features

            if (fids_to_remove == null || fids_to_remove.Count == 0) return;

            var remove = new bool[dataset.dataset_headers.Count];

            fids_to_remove.ForEach(a => remove[a] = true);

            dataset.dataset_headers = dataset.dataset_headers.Where((a, i) => !remove[i]).Select((b, j) =>
                (j, b.alphabet, b.dimension, b.category, b.source, b.group, b.member, b.perspective,
                    b.alphabet_id, b.dimension_id, b.category_id, b.source_id, b.group_id, b.member_id, b.perspective_id)
                ).ToList();

            for (var index = 0; index < dataset.dataset_instance_list.Count; index++)
            {

                dataset.dataset_instance_list[index] = (

                    dataset.dataset_instance_list[index].class_id,
                    dataset.dataset_instance_list[index].example_id,
                    dataset.dataset_instance_list[index].class_example_id,
                    dataset.dataset_instance_list[index].comment_columns,
                    dataset.dataset_instance_list[index].feature_data.Where((b, i) => !remove[i]).Select((c, j) => (j, c.fv)).ToList()

                    );
            }
        }
        */
        /*
        internal static List<(int fid, double value)> parse_csv_line_doubles(string line, bool[] required = null)
        {
            var result = new List<(int fid, double value)>();

            if (string.IsNullOrWhiteSpace(line)) return result;

            var fid = 0;

            var start = 0;
            var len = 0;

            for (var i = 0; i <= line.Length; i++)
            {
                if (i == line.Length || line[i] == ',')
                {
                    if ((required == null || required.Length == 0 || fid == 0) || (required != null && required.Length > fid && required[fid]))
                    {
                        result.Add((fid, len == 0 ? 0d : double.Parse(line.Substring(start, len), NumberStyles.Float, CultureInfo.InvariantCulture)));
                    }

                    fid++;

                    start = i + 1;
                    len = 0;
                    continue;

                }

                len++;
            }

            return result;
        }
        */
       /* internal static List<string> parse_csv_line_strings(string line)
        {
            var result = new List<string>();

            if (string.IsNullOrWhiteSpace(line)) return result;

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

            return result;
        }*/




        /*internal static double fix_double(string double_value)
        {
            const char infinity = '∞';
            const string neg_infinity = "-∞";
            const string pos_infinity = "+∞";
            const string NaN = "NaN";

            if (double.TryParse(double_value, NumberStyles.Float, CultureInfo.InvariantCulture, out var value1)) return fix_double(value1);

            if (double_value.Length == 1 && double_value[0] == infinity) return fix_double(double.PositiveInfinity);
            else if (double_value.Contains(pos_infinity, StringComparison.InvariantCulture)) return fix_double(double.PositiveInfinity);
            else if (double_value.Contains(neg_infinity, StringComparison.InvariantCulture)) return fix_double(double.NegativeInfinity);
            else if (double_value.Contains(infinity, StringComparison.InvariantCulture)) return fix_double(double.PositiveInfinity);
            else if (double_value.Contains(NaN, StringComparison.InvariantCulture)) return fix_double(double.NaN);
            else return 0d;
        }*/

        /*internal static double fix_double(double value)
        {
            // the doubles must be compatible with libsvm which is written in C (C and CSharp have different min/max values for double)
            const double c_double_max = (double)1.79769e+308;
            const double c_double_min = (double)-c_double_max;
            const double double_zero = (double)0;

            if (value >= c_double_min && value <= c_double_max)
            {
                return value;
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

            return value;
        }*/

//    }
//}
