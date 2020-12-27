using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal class prediction
    {
        public const string module_name = nameof(prediction);
        internal static readonly prediction empty = new prediction();

        internal int prediction_index;
        internal int class_sample_id; /* composite key with real_class_id for unique id */
        internal int real_class_id;
        internal int predicted_class_id;
        internal (int class_id, double probability_estimate)[] probability_estimates;
        internal (string comment_header, string comment_value)[] comment;

        //internal string[] test_row_vector; // test_row_vector is not saved/loaded

        internal static readonly string[] csv_header_values_array = new string[]
        {
            nameof(prediction_index),
            nameof(class_sample_id),
            nameof(real_class_id),
            nameof(predicted_class_id),
            nameof(probability_estimates),
            nameof(comment),
        };

        internal static readonly string csv_header_string = string.Join(",", csv_header_values_array);

        internal string[] csv_values_array()
        {
            return new string[]
            {
                $"{prediction_index}",
                $"{class_sample_id}",
                $"{real_class_id}",
                $"{predicted_class_id}",
                $"{string.Join("/",probability_estimates?.Select(a=>string.Join(":", $@"{a.class_id}", $@"{a.probability_estimate:G17}")).ToArray() ?? Array.Empty<string>())}",
                $"{string.Join("/",comment?.Select(a=>string.Join(":", $@"{a.comment_header?.Replace(":","_")}", $@"{a.comment_value?.Replace(":","_")}")).ToArray() ?? Array.Empty<string>())}",
            };
        }

        internal string csv_values_string()
        {
            return string.Join(",", csv_values_array());
        }

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
            // this.test_row_vector = prediction.test_row_vector;
        }

        public prediction(string[] values)
        {
            var k = 0;
            prediction_index = int.Parse(values[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            class_sample_id = int.Parse(values[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            real_class_id = int.Parse(values[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            predicted_class_id = int.Parse(values[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            probability_estimates = values[k++].Split('/', StringSplitOptions.RemoveEmptyEntries).Select(a => { var b = a.Split(':'); return (int.Parse(b[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo), double.Parse(b[1], NumberStyles.Float, NumberFormatInfo.InvariantInfo)); }).ToArray();
            comment = values[k++].Split('/', StringSplitOptions.RemoveEmptyEntries).Select(a => { var b = a.Split(':'); return (b[0], b[1]); }).ToArray();
        }

        /*public prediction(string str)
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
        }*/

       /*internal string str()
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
        }*/



        public static void save(CancellationTokenSource cts, string prediction_list_filename, (index_data id, confusion_matrix cm, rank_score rs)[] cm_list)
        {
            const string method_name = nameof(save);

            if (cts.IsCancellationRequested) return;

            var pred_list = cm_list.SelectMany(a => a.cm.predictions).ToList();

            var total_lines = cm_list.Sum(a => a.cm.predictions.Length) + 1;

            var lines = new string[total_lines];

            var prob_classes = pred_list.Where(a => a != null && a.probability_estimates != null && a.probability_estimates.Length > 0).SelectMany(a => a.probability_estimates.Select(b => b.class_id).ToList()).Distinct().OrderBy(a => a).ToArray();
            var prob_comments = pred_list.Where(a => a != null && a.comment != null && a.comment.Length > 0).SelectMany(a => a.comment.Select(b => b.comment_header).ToList()).Distinct().OrderBy(a => a).ToArray();


            var header_csv_values = new List<string>()
            {
                "perf_"+nameof(index_data.iteration_index),
                "perf_"+nameof(index_data.group_array_index),
                "perf_"+nameof(confusion_matrix.x_class_id),
                "perf_"+nameof(confusion_matrix.x_class_name),

                nameof(prediction_index),
                nameof(class_sample_id),
                nameof(real_class_id),
                nameof(predicted_class_id),
            };
            header_csv_values.AddRange(prob_classes.Select(a => $@"prob_{a:+#;-#;+0}").ToArray());
            header_csv_values.AddRange(prob_comments);
            

            lines[0] = string.Join($@",", header_csv_values);

            Parallel.For(0,
                cm_list.Length,
                cm_list_index =>
                {
                    Parallel.For(0,
                        cm_list[cm_list_index].cm.predictions.Length,
                        cm_pred_index =>
                        {
                            var k = 0;

                            var values = new string[header_csv_values.Count];

                            values[k++] = $@"{cm_list[cm_list_index].id.iteration_index}";
                            values[k++] = $@"{cm_list[cm_list_index].id.group_array_index}";
                            values[k++] = $@"{cm_list[cm_list_index].cm.x_class_id}";
                            values[k++] = $@"{cm_list[cm_list_index].cm.x_class_name}";

                            values[k++] = $@"{cm_list[cm_list_index].cm.predictions[cm_pred_index].prediction_index}";
                            values[k++] = $@"{cm_list[cm_list_index].cm.predictions[cm_pred_index].class_sample_id}";
                            values[k++] = $@"{cm_list[cm_list_index].cm.predictions[cm_pred_index].real_class_id:+#;-#;+0}";
                            values[k++] = $@"{cm_list[cm_list_index].cm.predictions[cm_pred_index].predicted_class_id:+#;-#;+0}";
                            

                            if (cm_list[cm_list_index].cm.predictions[cm_pred_index].probability_estimates != null && cm_list[cm_list_index].cm.predictions[cm_pred_index].probability_estimates.Length > 0)
                            {
                                for (var probability_estimates_index = 0; probability_estimates_index < cm_list[cm_list_index].cm.predictions[cm_pred_index].probability_estimates.Length; probability_estimates_index++)
                                {
                                    var values_index = header_csv_values.IndexOf($@"prob_{cm_list[cm_list_index].cm.predictions[cm_pred_index].probability_estimates[probability_estimates_index].class_id:+#;-#;+0}", k);
                                    values[values_index] = $"{cm_list[cm_list_index].cm.predictions[cm_pred_index].probability_estimates[probability_estimates_index].probability_estimate:G17}";
                                }

                                k += cm_list[cm_list_index].cm.predictions[cm_pred_index].probability_estimates.Length + 1;
                            }

                            if (cm_list[cm_list_index].cm.predictions[cm_pred_index].comment != null && cm_list[cm_list_index].cm.predictions[cm_pred_index].comment.Length > 0)
                            {
                                for (var comment_index = 0; comment_index < cm_list[cm_list_index].cm.predictions[cm_pred_index].comment.Length; comment_index++)
                                {
                                    var values_index = header_csv_values.IndexOf(cm_list[cm_list_index].cm.predictions[cm_pred_index].comment[comment_index].comment_header, k);
                                    values[values_index] = cm_list[cm_list_index].cm.predictions[cm_pred_index].comment[comment_index].comment_value;
                                }
                            }

                            for (var header_csv_values_index = 0; header_csv_values_index < header_csv_values.Count; header_csv_values_index++)
                            {
                                if (string.IsNullOrEmpty(values[header_csv_values_index]) && header_csv_values[header_csv_values_index] == $@"_") values[header_csv_values_index] = $@"_";
                            }

                            lines[cm_pred_index + 1] = string.Join($@",", values);
                        });
                });
            io_proxy.WriteAllLines(cts, prediction_list_filename, lines, module_name, method_name);
        }
    }
}
