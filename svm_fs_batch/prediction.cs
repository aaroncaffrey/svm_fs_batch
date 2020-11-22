using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace svm_fs_batch
{
    internal class prediction
    {
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
            var s = str.Split(';');
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

            return string.Join(";", new string[]
            {
                    $@"{prediction_index}",
                    $@"{class_sample_id}",
                    $@"{real_class_id}",
                    $@"{predicted_class_id}",
                    $@"{(probability_estimates?.Count ?? 0)}",
                    $@"{(comment?.Count ?? 0)}",
                    $@"{(probability_estimates != null &&probability_estimates.Count>0? string.Join(";", probability_estimates.Select(a => $@"p{a.class_id:+#;-#;+0}:{a.probability_estimate:G17}").ToList()) : $@"")}",
                    $@"{(comment != null && comment.Count>0? string.Join(";", comment.Where(a=>!string.IsNullOrWhiteSpace(a.comment_header) || !string.IsNullOrWhiteSpace(a.comment_value)).Select(a => $@"c{a.comment_header}:{a.comment_value}").ToList()) : $@"")}"
            }.Where(a => !string.IsNullOrWhiteSpace(a)).ToArray());
        }
    }
}
