using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal class prediction
    {
        public const string module_name = nameof(prediction);

        internal int prediction_index;
        internal int class_sample_id; /* composite key with real_class_id for unique id */
        internal int real_class_id;
        internal int predicted_class_id;
        internal List<(int class_id, double probability_estimate)> probability_estimates;
        internal List<(string comment_header, string comment_value)> comment;

        internal string[] test_row_vector; // test_row_vector is not saved/loaded

        public prediction()
        {

        }

        public prediction(prediction prediction)
        {
            this.prediction_index = prediction.prediction_index;
            this.class_sample_id = prediction.class_sample_id;
            this.real_class_id = prediction.real_class_id;
            this.predicted_class_id = prediction.predicted_class_id;
            this.probability_estimates = prediction.probability_estimates;
            this.comment = prediction.comment;
            this.test_row_vector = prediction.test_row_vector;
        }

        public prediction(string str)
        {
            var k = 0;
            var s = str.Split('|');
            prediction_index = s.Length > k ? int.Parse(s[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) : 0;
            class_sample_id = s.Length > k ? int.Parse(s[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) : 0;

            real_class_id = s.Length > k ? int.Parse(s[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) : 0;
            predicted_class_id = s.Length > k ? int.Parse(s[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) : 0;

            var total_prob_est = s.Length > k ? int.Parse(s[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) : 0;
            var total_comment = s.Length > k ? int.Parse(s[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) : 0;

            for (var i = 0; i < total_prob_est; i++)
            {
                var s2 = s.Length > k ? s[k++] : null;
                var s2s = s2?.Split(':');//,StringSplitOptions.RemoveEmptyEntries);
                if (s2s != null && s2s.Length == 2 && s2s[0][0] == 'p')
                {
                    var cid = int.TryParse(s2s[0][1..], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var cid_out) ? cid_out : (int?)null;
                    var pe = double.TryParse(s2s[1], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var pe_out) ? pe_out : (double?)null;

                    if (cid != null && pe != null)
                    {
                        if (probability_estimates == null) probability_estimates = new List<(int class_id, double probability_estimate)>();
                        probability_estimates.Add((cid.Value, pe.Value));
                    }
                }
            }


            for (var i = 0; i < total_comment; i++)
            {
                var s2 = s.Length > k ? s[k++] : null;
                var s2s = s2?.Split(':');//,StringSplitOptions.RemoveEmptyEntries);
                if (s2s != null && s2s.Length == 2 && s2s[0][0] == 'c')
                {
                    var ch = s2s[0][1..];
                    var cv = s2s[1];

                    if (ch.Length > 0 || cv.Length > 0)
                    {
                        if (comment == null) comment = new List<(string comment_header, string comment_value)>();
                        comment.Add((ch, cv));
                    }
                }
            }
        }

        internal string str()
        {
            // 0;-1;1;2;p1:0.6;p-1:0.4;cval=xyz|0;-1;1;2;p1:0.6;p-1:0.4;cval=abc

            return string.Join("|", new string[]
            {
                    $@"{prediction_index}",
                    $@"{class_sample_id}",
                    $@"{real_class_id}",
                    $@"{predicted_class_id}",
                    $@"{(probability_estimates?.Count ?? 0)}",
                    $@"{(comment?.Count ?? 0)}",
                    $@"{(probability_estimates != null &&probability_estimates.Count>0? string.Join("|", probability_estimates.Select(a => $@"p{a.class_id:+#;-#;+0}:{a.probability_estimate:G17}").ToList()) : $@"")}",
                    $@"{(comment != null && comment.Count>0? string.Join("|", comment.Where(a=>!string.IsNullOrWhiteSpace(a.comment_header) || !string.IsNullOrWhiteSpace(a.comment_value)).Select(a => $@"c{a.comment_header}:{a.comment_value}").ToList()) : $@"")}"
            }.Where(a => !string.IsNullOrWhiteSpace(a)).ToArray());
        }



        public static void save(string prediction_list_filename, /*string last_filename,*/ IList<confusion_matrix> cm_list)
        {
            const string method_name = nameof(save);

            var pred_list = cm_list.SelectMany(a => a.predictions).ToList();

            var total_lines = cm_list.Sum(a => a.predictions.Count) + 1;

            //var last_lines = Array.Empty<string>();
            //
            //if (!string.IsNullOrWhiteSpace(last_filename) && File.Exists(last_filename) && new FileInfo(last_filename).Length > 0)
            //{
            //    last_lines = io_proxy.ReadAllLines(last_filename, module_name, method_name);
            //
            //    if (last_lines.Length > 0)
            //    {
            //        total_lines += (last_lines.Length - 1);
            //    }
            //}

            var lines = new string[total_lines];

            //if (last_lines != null && last_lines.Length>0)
            //{
            //    Array.Copy(last_lines, lines, last_lines.Length);
            //}

            var header_csv_values = new List<string>()
            {
                "perf_"+nameof(confusion_matrix.x_iteration_index),
                "perf_"+nameof(confusion_matrix.x_group_array_index),
                "perf_"+nameof(confusion_matrix.x_class_id),
                "perf_"+nameof(confusion_matrix.x_class_name),
                nameof(prediction_index),
                nameof(class_sample_id),
                nameof(real_class_id),
                nameof(predicted_class_id),
                $@"_"
            };

            var prob_classes = pred_list.Where(a => a != null && a.probability_estimates != null && a.probability_estimates.Count > 0).SelectMany(a => a.probability_estimates.Select(b => b.class_id).ToList()).Distinct().OrderBy(a => a).ToArray();
            var prob_comments = pred_list.Where(a => a != null && a.comment != null && a.comment.Count > 0).SelectMany(a => a.comment.Select(b => b.comment_header).ToList()).Distinct().OrderBy(a => a).ToArray();

            header_csv_values.AddRange(prob_classes.Select(a => $@"prob_{a:+#;-#;+0}").ToArray());
            header_csv_values.Add($@"_");

            header_csv_values.AddRange(prob_comments);
            header_csv_values.Add($@"_");

            lines[0] = string.Join($@",", header_csv_values);

            Parallel.For(0,
                cm_list.Count,
                cm_list_index =>
                {
                    Parallel.For(0,
                        cm_list[cm_list_index].predictions.Count,
                        cm_pred_index =>
                        {
                            var k = 0;

                            var values = new string[header_csv_values.Count];

                            values[k++] = $@"{cm_list[cm_list_index].x_iteration_index}";
                            values[k++] = $@"{cm_list[cm_list_index].x_group_array_index}";
                            values[k++] = $@"{cm_list[cm_list_index].x_class_id}";
                            values[k++] = $@"{cm_list[cm_list_index].x_class_name}";

                            values[k++] = $@"{cm_list[cm_list_index].predictions[cm_pred_index].prediction_index}";
                            values[k++] = $@"{cm_list[cm_list_index].predictions[cm_pred_index].class_sample_id}";
                            values[k++] = $@"{cm_list[cm_list_index].predictions[cm_pred_index].real_class_id:+#;-#;+0}";
                            values[k++] = $@"{cm_list[cm_list_index].predictions[cm_pred_index].predicted_class_id:+#;-#;+0}";
                            values[k++] = $"_";

                            if (cm_list[cm_list_index].predictions[cm_pred_index].probability_estimates != null && cm_list[cm_list_index].predictions[cm_pred_index].probability_estimates.Count > 0)
                            {
                                for (var probability_estimates_index = 0; probability_estimates_index < cm_list[cm_list_index].predictions[cm_pred_index].probability_estimates.Count; probability_estimates_index++)
                                {
                                    var values_index = header_csv_values.IndexOf($@"prob_{cm_list[cm_list_index].predictions[cm_pred_index].probability_estimates[probability_estimates_index].class_id:+#;-#;+0}", k);
                                    values[values_index] = $"{cm_list[cm_list_index].predictions[cm_pred_index].probability_estimates[probability_estimates_index].probability_estimate:G17}";
                                }

                                k += cm_list[cm_list_index].predictions[cm_pred_index].probability_estimates.Count + 1;
                            }

                            if (cm_list[cm_list_index].predictions[cm_pred_index].comment != null && cm_list[cm_list_index].predictions[cm_pred_index].comment.Count > 0)
                            {
                                for (var comment_index = 0; comment_index < cm_list[cm_list_index].predictions[cm_pred_index].comment.Count; comment_index++)
                                {
                                    var values_index = header_csv_values.IndexOf(cm_list[cm_list_index].predictions[cm_pred_index].comment[comment_index].comment_header, k);
                                    values[values_index] = cm_list[cm_list_index].predictions[cm_pred_index].comment[comment_index].comment_value;
                                }
                            }

                            for (var header_csv_values_index = 0; header_csv_values_index < header_csv_values.Count; header_csv_values_index++)
                            {
                                if (string.IsNullOrEmpty(values[header_csv_values_index]) && header_csv_values[header_csv_values_index] == $@"_") values[header_csv_values_index] = $@"_";
                            }

                            lines[cm_pred_index + 1] = string.Join($@",", values);
                        });
                });
            io_proxy.WriteAllLines(prediction_list_filename, lines, module_name, method_name);
        }
    }
}
