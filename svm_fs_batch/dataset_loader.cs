using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace svm_fs_batch
{
    internal static class dataset_loader
    {
        //        [Serializable]



        internal class dataset
        {
            internal List<(int fid, string alphabet, string dimension, string category, string source, string group, string member, string perspective, int alphabet_id, int dimension_id, int category_id, int source_id, int group_id, int member_id, int perspective_id)> dataset_headers = null;
            internal List<(int filename_index, int line_index, List<(string comment_header, string comment_value)> comment_columns/*, string comment_columns_hash*/)> dataset_comment_row_values = null;
            internal List<(int class_id, int example_id, int class_example_id, List<(string comment_header, string comment_value)> comment_columns, /*string comment_columns_hash,*/ List<(int fid, double fv)> feature_data/*, string feature_data_hash*/)> dataset_instance_list = null;

            //internal static void serialise(datax datax, string filename)
            //{
            //    IFormatter formatter = new BinaryFormatter();
            //    Stream stream = new FileStream(filename, FileMode.Create, FileAccess.Write, FileShare.None);
            //    formatter.Serialize(stream, datax);
            //    stream.Close();
            //}

            //internal void serialise(string filename)
            //{
            //    datax.serialise(this, filename);
            //}

            //internal static datax deserialise(string filename)
            //{
            //    IFormatter formatter = new BinaryFormatter();
            //    Stream stream = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read);
            //    datax datax = (datax)formatter.Deserialize(stream);
            //    stream.Close();
            //    return datax;
            //}
        }

        internal static bool matches(string text, string search_pattern)
        {
            if (string.IsNullOrWhiteSpace(search_pattern) || search_pattern == "*")
            {
                return true;
            }
            else if (search_pattern.StartsWith("*", StringComparison.InvariantCulture) && search_pattern.EndsWith("*", StringComparison.InvariantCulture))
            {
                search_pattern = search_pattern.Substring(1, search_pattern.Length - 2);

                return text.Contains(search_pattern, StringComparison.InvariantCultureIgnoreCase);
            }
            else if (search_pattern.StartsWith("*", StringComparison.InvariantCulture))
            {
                search_pattern = search_pattern.Substring(1);

                return text.EndsWith(search_pattern, StringComparison.InvariantCultureIgnoreCase);
            }
            else if (search_pattern.EndsWith("*", StringComparison.InvariantCulture))
            {
                search_pattern = search_pattern.Substring(0, search_pattern.Length - 1);

                return text.StartsWith(search_pattern, StringComparison.InvariantCultureIgnoreCase);
            }
            else
            {
                return string.Equals(text, search_pattern, StringComparison.InvariantCultureIgnoreCase);
            }
        }

        internal static
            dataset
           read_binary_dataset(

           string dataset_folder,
           string file_tag,
           int negative_class_id,
           int positive_class_id,
           List<(int class_id, string class_name)> class_names,
           bool use_parallel = true,
           bool perform_integrity_checks = false,
           //bool fix_double = true,
           bool required_default = true,
           List<(bool required, string alphabet, string dimension, string category, string source, string group, string member, string perspective)> required_matches = null,
           bool fix_dataset = false,
           bool headers_only = false
           )
        {
            var method_name = nameof(read_binary_dataset);
            var module_name = nameof(dataset_loader);

            var lock_table = new object();

            var table_alphabet = new List<string>();
            var table_dimension = new List<string>();
            var table_category = new List<string>();
            var table_source = new List<string>();
            var table_group = new List<string>();
            var table_member = new List<string>();
            var table_perspective = new List<string>();

            //dataset_folder = (dataset_folder);

            // formatting to always show sign :+#;-#;+0

            var dataset_csv_files = new List<string>()
            {
                (Path.Combine(dataset_folder, $@"f_{file_tag}_({class_names.First(a=>a.class_id==positive_class_id).class_id:+#;-#;+0})_({class_names.First(a => a.class_id == positive_class_id).class_name}).csv")),
                (Path.Combine(dataset_folder, $@"f_{file_tag}_({class_names.First(a=>a.class_id==negative_class_id).class_id:+#;-#;+0})_({class_names.First(a => a.class_id == negative_class_id).class_name}).csv")),
            };

            var dataset_header_csv_files = new List<string>()
            {
                (Path.Combine(dataset_folder, $@"h_{file_tag}_({class_names.First(a=>a.class_id==positive_class_id).class_id:+#;-#;+0})_({class_names.First(a => a.class_id == positive_class_id).class_name}).csv")),
                (Path.Combine(dataset_folder, $@"h_{file_tag}_({class_names.First(a=>a.class_id==negative_class_id).class_id:+#;-#;+0})_({class_names.First(a => a.class_id == negative_class_id).class_name}).csv")),
            };

            var dataset_comment_csv_files = new List<string>
            {
                (Path.Combine(dataset_folder, $@"c_{file_tag}_({class_names.First(a=>a.class_id==positive_class_id).class_id:+#;-#;+0})_({class_names.First(a => a.class_id == positive_class_id).class_name}).csv")),
                (Path.Combine(dataset_folder, $@"c_{file_tag}_({class_names.First(a=>a.class_id==negative_class_id).class_id:+#;-#;+0})_({class_names.First(a => a.class_id == negative_class_id).class_name}).csv")),
            };



            io_proxy.WriteLine($@"{nameof(positive_class_id)} = {positive_class_id:+#;-#;+0}", module_name, method_name);
            io_proxy.WriteLine($@"{nameof(negative_class_id)} = {negative_class_id:+#;-#;+0}", module_name, method_name);
            io_proxy.WriteLine($@"{nameof(class_names)} = {string.Join(", ", class_names)}", module_name, method_name);
            io_proxy.WriteLine($@"{nameof(dataset_csv_files)}: {string.Join(", ", dataset_csv_files)}", module_name, method_name);
            io_proxy.WriteLine($@"{nameof(dataset_header_csv_files)}: {string.Join(", ", dataset_header_csv_files)}", module_name, method_name);
            io_proxy.WriteLine($@"{nameof(dataset_comment_csv_files)}: {string.Join(", ", dataset_comment_csv_files)}", module_name, method_name);
            io_proxy.WriteLine($@"Reading non-novel dataset headers...", module_name, method_name);


            // READ HEADER CSV FILE - ALL CLASSES HAVE THE SAME HEADERS/FEATURES

            //List<(int fid, string alphabet, string dimension, string category, string source, string group, string member, string perspective, int alphabet_id, int dimension_id, int category_id, int source_id, int group_id, int member_id, int perspective_id)> header_data = io_proxy.ReadAllLines(dataset_header_csv_files.First()).Skip(1).Select((a, i) =>



            var header_data = io_proxy.ReadAllLines(dataset_header_csv_files.First()).Skip(1)/*.AsParallel().AsOrdered()*/.Select((a, i) =>
            {
                var b = a.Split(',');
                var fid = int.Parse(b[0], NumberStyles.Integer, CultureInfo.InvariantCulture);
                //if (fid!=i) throw new Exception();

                var alphabet = b[1];
                var dimension = b[2];
                var category = b[3];
                var source = b[4];
                var group = b[5];
                var member = b[6];
                var perspective = b[7];

                const string def = "default";

                if (string.IsNullOrWhiteSpace(alphabet)) alphabet = def;
                if (string.IsNullOrWhiteSpace(dimension)) dimension = def;
                if (string.IsNullOrWhiteSpace(category)) category = def;
                if (string.IsNullOrWhiteSpace(source)) source = def;
                if (string.IsNullOrWhiteSpace(group)) group = def;
                if (string.IsNullOrWhiteSpace(member)) member = def;
                if (string.IsNullOrWhiteSpace(perspective)) perspective = def;

                lock (lock_table)
                {
                    //var duplicate = true;
                    if (!table_alphabet.Contains(alphabet)) { table_alphabet.Add(alphabet); /*duplicate = false;*/ }
                    if (!table_dimension.Contains(dimension)) { table_dimension.Add(dimension); /*duplicate = false;*/ }
                    if (!table_category.Contains(category)) { table_category.Add(category); /*duplicate = false;*/ }
                    if (!table_source.Contains(source)) { table_source.Add(source); /*duplicate = false;*/ }
                    if (!table_group.Contains(group)) { table_group.Add(group); /*duplicate = false;*/ }
                    if (!table_member.Contains(member)) { table_member.Add(member); /*duplicate = false;*/ }
                    if (!table_perspective.Contains(perspective)) { table_perspective.Add(perspective); /*duplicate = false;*/ }

                    //if (duplicate)
                    //{
                    //io_proxy.WriteLine("Duplicate: " + a);
                    //Console.ReadLine();
                    //}
                }


                var alphabet_id = table_alphabet.LastIndexOf(alphabet);
                var dimension_id = table_dimension.LastIndexOf(dimension);
                var category_id = table_category.LastIndexOf(category);
                var source_id = table_source.LastIndexOf(source);
                var group_id = table_group.LastIndexOf(group);
                var member_id = table_member.LastIndexOf(member);
                var perspective_id = table_perspective.LastIndexOf(perspective);

                alphabet = table_alphabet[alphabet_id];
                dimension = table_dimension[dimension_id];
                category = table_category[category_id];
                source = table_source[source_id];
                group = table_group[group_id];
                member = table_member[member_id];
                perspective = table_perspective[perspective_id];

                return (fid: fid, alphabet: alphabet, dimension: dimension, category: category, source: source, group: group, member: member, perspective: perspective,
                                    alphabet_id: alphabet_id, dimension_id: dimension_id, category_id: category_id, source_id: source_id, group_id: group_id, member_id: member_id, perspective_id: perspective_id);
            }).ToList();

            bool[] required = null;

            if (required_matches != null && required_matches.Count > 0)
            {
                required = new bool[header_data.Count];
                Array.Fill(required, required_default);

                for (var index = 0; index < required_matches.Count; index++)
                {
                    var rm = required_matches[index];

                    var matching_fids = header_data.Where(a =>
                        matches(a.alphabet, rm.alphabet) &&
                        matches(a.category, rm.category) &&
                        matches(a.dimension, rm.dimension) &&
                        matches(a.source, rm.source) &&
                        matches(a.@group, rm.@group) &&
                        matches(a.member, rm.member) &&
                        matches(a.perspective, rm.perspective)
                        ).Select(a => a.fid).ToList();


                    matching_fids.ForEach(a => required[a] = rm.required);

                }

                required[0] = true;

                header_data = header_data.Where((a, i) => required[a.fid]).ToList();
            }

            var dataset = new dataset();
            dataset.dataset_headers = header_data;


            if (!headers_only)
            {

                // READ (DATA) COMMENTS CSV FILE - THESE ARE CLASS AND INSTANCE SPECIFIC
                var data_comments_header = File.ReadLines(dataset_comment_csv_files.First()).First().Split(',');

                //var total_features = header_data.Count;
                //Program.WriteLine($@"{nameof(total_features)}: {total_features}");
                //Program.WriteLine($@"{nameof(table_alphabet)}: {table_alphabet.Count}: {string.Join(", ", table_alphabet)}");
                //Program.WriteLine($@"{nameof(table_dimension)}: {table_dimension.Count}: {string.Join(", ", table_dimension)}");
                //Program.WriteLine($@"{nameof(table_category)}: {table_category.Count}: {string.Join(", ", table_category)}");
                //Program.WriteLine($@"{nameof(table_source)}: {table_source.Count}: {string.Join(", ", table_source)}");
                //Program.WriteLine($@"{nameof(table_group)}: {table_group.Count}: {string.Join(", ", table_group)}");
                //Program.WriteLine($@"{nameof(table_member)}: {table_member.Count}: {string.Join(", ", table_member)}");
                //Program.WriteLine($@"{nameof(table_perspective)}: {table_perspective.Count}: {string.Join(", ", table_perspective)}");
                //Program.WriteLine($@"");

                List<(int filename_index, int line_index, List<(string comment_header, string comment_value)> comment_columns /*, string comment_columns_hash*/)> dataset_comment_row_values = null;
                List<(int class_id, int example_id, int class_example_id, List<(string comment_header, string comment_value)> comment_columns, /*string comment_columns_hash,*/ List<(int fid, double fv)> feature_data /*, string feature_data_hash*/)> dataset_instance_list = null;




                // data comments: load comment lines as key-value pairs.  note: these are variables associated with each example instance rather than specific features.
                io_proxy.WriteLine($@"Reading data comments...", module_name, method_name);

                if (use_parallel)
                {
                    dataset_comment_row_values = dataset_comment_csv_files.AsParallel()
                        .AsOrdered()
                        .SelectMany((filename, filename_index) => io_proxy.ReadAllLines(filename, module_name, method_name)
                            .Skip(1 /*header line*/)
                            .AsParallel()
                            .AsOrdered()
                            .Select((line, line_index) =>
                            {

                                var comment_columns = line.Split(',').Select((col, col_index) => (comment_header: data_comments_header[col_index], comment_value: col)).ToList();
                                comment_columns = comment_columns.Where(d => d.comment_header.FirstOrDefault() != '#').ToList();

                                //var comment_columns_hash = hash.calc_hash(string.Join(" ", comment_columns.Select(c => c.comment_header + ":" + c.comment_value).ToList()));
                                return (filename_index: filename_index, line_index: line_index, comment_columns: comment_columns /*, comment_columns_hash: comment_columns_hash*/);

                            })
                            .ToList())
                        .ToList();
                }
                else
                {
                    dataset_comment_row_values = dataset_comment_csv_files.SelectMany((filename, filename_index) => io_proxy.ReadAllLines(filename, module_name, method_name)
                            .Skip(1 /*header line*/)
                            .Select((line, line_index) =>
                            {

                                var comment_columns = line.Split(',').Select((col, col_index) => (comment_header: data_comments_header[col_index], comment_value: col)).ToList();
                                comment_columns = comment_columns.Where(d => d.comment_header.FirstOrDefault() != '#').ToList();

                                //var comment_columns_hash = hash.calc_hash(string.Join(" ", comment_columns.Select(c => c.comment_header + ":" + c.comment_value).ToList()));
                                return (filename_index: filename_index, line_index: line_index, comment_columns: comment_columns /*, comment_columns_hash: comment_columns_hash*/);

                            })
                            .ToList())
                        .ToList();
                }

                //// data comments: filter out any '#' commented out key-value pairs 
                //svm_manager.WriteLine($@"Removing data comments which are commented out...");
                //if (use_parallel)
                //{
                //    dataset_comment_row_values = dataset_comment_row_values.AsParallel().AsOrdered().Select(a => { a.comment_columns = a.comment_columns.Where(b => b.comment_header.FirstOrDefault() != '#').ToList(); return a; }).ToList();
                //}
                //else
                //{
                //    dataset_comment_row_values = dataset_comment_row_values.Select(a => { a.comment_columns = a.comment_columns.Where(b => b.comment_header.FirstOrDefault() != '#').ToList(); return a; }).ToList();
                //}


                // data set: load data
                io_proxy.WriteLine($@"Reading data...", module_name, method_name);
                if (use_parallel)
                {
                    dataset_instance_list = dataset_csv_files.AsParallel()
                        .AsOrdered()
                        .SelectMany((filename, filename_index) => io_proxy.ReadAllLines(filename, module_name, method_name)
                            .Skip(1 /*skip header*/)
                            .AsParallel()
                            .AsOrdered()
                            .Select((line, line_index) =>
                            {
                                var class_id = int.Parse(line.Substring(0, line.IndexOf(',', StringComparison.InvariantCulture)), CultureInfo.InvariantCulture);
                                var feature_data = parse_csv_line_doubles(line, required);
                                //var feature_data_hash = hash.calc_hash(string.Join(" ", feature_data.Select(d => $"{d.fid}:{d.value}").ToList()));

                                var comment_row = dataset_comment_row_values.First(b => b.filename_index == filename_index && b.line_index == line_index);
                                var comment_columns = comment_row.comment_columns;
                                //var comment_columns_hash = comment_row.comment_columns_hash;

                                return (class_id: class_id, example_id: 0, class_example_id: 0, comment_columns: comment_columns,
                                        //comment_columns_hash: comment_columns_hash,
                                        feature_data: feature_data //,
                                                                   //feature_data_hash: feature_data_hash
                                    );

                            })
                            .ToList())
                        .ToList();
                }
                else
                {
                    dataset_instance_list = dataset_csv_files.SelectMany((filename, filename_index) => io_proxy.ReadAllLines(filename, module_name, method_name)
                            .Skip(1 /*skip header*/)
                            . /*Take(20).*/Select((line, line_index) =>
                            {
                                var class_id = int.Parse(line.Substring(0, line.IndexOf(',', StringComparison.InvariantCulture)), CultureInfo.InvariantCulture);
                                var feature_data = parse_csv_line_doubles(line, required);
                                //var feature_data_hash = hash.calc_hash(string.Join(" ", feature_data.Select(d => $"{d.fid}:{d.value}").ToList()));

                                var comment_row = dataset_comment_row_values.First(b => b.filename_index == filename_index && b.line_index == line_index);
                                var comment_columns = comment_row.comment_columns;
                                //var comment_columns_hash = comment_row.comment_columns_hash;

                                return (class_id: class_id, example_id: 0, class_example_id: 0, comment_columns: comment_columns,
                                        //comment_columns_hash: comment_columns_hash,
                                        feature_data: feature_data //,
                                                                   //feature_data_hash: feature_data_hash
                                    );

                            })
                            .ToList())
                        .ToList();
                }

                if (dataset_comment_row_values.Count != dataset_instance_list.Count) { throw new Exception(); }


                dataset_instance_list = dataset_instance_list.Select((a, i) => (a.class_id, i, a.class_example_id, a.comment_columns, /*a.comment_columns_hash,*/ a.feature_data /*, a.feature_data_hash*/)).ToList();
                dataset_instance_list = dataset_instance_list.GroupBy(a => a.class_id).SelectMany(x => x.Select((a, i) => (a.class_id, a.example_id, i, a.comment_columns, /*a.comment_columns_hash,*/ a.feature_data /*, a.feature_data_hash*/)).ToList()).ToList();



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




                //var dataset_instance_list2 = dataset_instance_list.Select((a, i) => (class_id: a.class_id, comment_columns: a.comment_columns, feature_data: a.feature_data,
                //feature_data_hash: svm_manager.calc_hash(string.Join(" ", a.feature_data.SelectMany(b => new string[] { ""+b.fid, ""+b.fv }).ToList())), example_id: i)).ToList();
                //
                //var dataset_instance_list3 = dataset_instance_list2.GroupBy(a => a.class_id).Select(x => x.Select((a, i) => (class_id: a.class_id, comment_columns: a.comment_columns, feature_data: a.feature_data, 
                //comment_columns_hash: a.comment_columns_hash, feature_data_hash: a.feature_data_hash, example_id: a.example_id, class_example_id: i))).SelectMany(a => a).ToList();

                dataset.dataset_comment_row_values = dataset_comment_row_values;
                dataset.dataset_instance_list = dataset_instance_list;
            }




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

            return dataset;
        }


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


        internal static void remove_large_groups(dataset dataset, int max_group_size)
        {
            var module_name = nameof(dataset_loader);
            var method_name = nameof(remove_large_groups);

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

        internal static void remove_duplicate_groups(dataset dataset)
        {
            //var module_name = nameof(dataset_loader);
            //var method_name = nameof(remove_duplicate_groups);

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

                            if (x_i.All(a => x_j.Any(b => a.SequenceEqual(b))))
                            {

                                var c_i = clusters.FirstOrDefault(a => a.Contains(i));
                                var c_j = clusters.FirstOrDefault(a => a.Contains(j));

                                var cluster = new List<int>();
                                cluster.AddRange(c_i != null ? c_i : new List<int>() { i });
                                cluster.AddRange(c_j != null ? c_j : new List<int>() { j });
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


                    var cluster_fids_to_remove = groups_to_remove.SelectMany(a => a.Select(b => b.fid).ToList())/*.Distinct().OrderBy(a=>a)*/.ToList();
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

        internal static void save_dataset(dataset dataset, List<(int class_id, string header_filename, string data_filename)> filenames)
        {
            var module_name = nameof(dataset_loader);
            var method_name = nameof(save_dataset);

            io_proxy.WriteLine("started saving...", module_name, method_name);

            //var class_ids = dataset.dataset_instance_list.Select(a => a.class_id).Distinct().OrderBy(a => a).ToList();
            var header_fids = Enumerable.Range(0, dataset.dataset_headers.Count);
            var header_fids_str = string.Join(",", header_fids);

            foreach (var class_id_filenames in filenames)//class_ids)
            {
                var data = new List<string>();
                data.Add(header_fids_str);

                dataset.dataset_instance_list.Where(a => a.class_id == class_id_filenames.class_id).ToList().
                    ForEach(a => data.Add(string.Join(",", a.feature_data.Select(b => b.fv.ToString("G17", CultureInfo.InvariantCulture)).ToList())));

                var header = dataset.dataset_headers.Select(a => $@"{a.fid},{a.alphabet},{a.dimension},{a.category},{a.source},{a.group},{a.member},{a.perspective}").ToList();
                header.Insert(0, $@"fid,alphabet,dimension,category,source,group,member,perspective");

                io_proxy.WriteAllLines(class_id_filenames.header_filename, header);

                io_proxy.WriteAllLines(class_id_filenames.data_filename, data);
            }

            io_proxy.WriteLine("finished saving...", module_name, nameof(save_dataset));
        }

        internal static void remove_empty_features(dataset dataset, /*double min_non_zero_pct = 0.25,*/ int min_distinct_numbers = 2)
        {
            var module_name = nameof(dataset_loader);
            var method_name = nameof(remove_empty_features);

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

        internal static void remove_empty_features_by_class(dataset dataset, /*double min_non_zero_pct = 0.25,*/ int min_distinct_numbers = 2, string stats_filename = null)
        {
            var module_name = nameof(dataset_loader);
            var method_name = nameof(remove_empty_features_by_class);

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

        internal static List<string> parse_csv_line_strings(string line)
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
        }




        internal static double fix_double(string double_value)
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
        }

        internal static double fix_double(double value)
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
        }

    }
}
