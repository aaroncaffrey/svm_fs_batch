using System;
using System.Collections.Generic;
using System.Linq;

namespace svm_fs_batch
{
    internal class feature_selection_status
    {
        public const string module_name = nameof(feature_selection_status);

        internal (string file_tag, string alphabet, string stats, string dimension, string category, string source, string @group, string member, string perspective) winner_key;

        internal int total_groups;
        internal int iterations_not_higher_than_all;
        internal int iterations_not_higher_than_last;
        internal int deep_search_index;
        internal bool feature_selection_finished;
        internal string[] scoring_metrics;
        internal int[] scoring_class_ids;

        internal score_data this_winner_score_data;
        internal score_data last_winner_score_data;
        internal score_data best_winner_score_data;

        public feature_selection_status()
        {
            
        }

        internal static void save(string status_filename, IList<feature_selection_status> feature_selection_status_list)
        {
            const string method_name = nameof(save);

            var lines = new string[feature_selection_status_list.Count + 1];
            lines[0] = csv_header;
            for (var i = 0; i < feature_selection_status_list.Count; i++)
            {
                lines[i + 1] = feature_selection_status_list[i].csv_values();
            }

            io_proxy.WriteAllLines(status_filename, lines, module_name, method_name);
        }



        internal static readonly string[] csv_header_values = new string[]
            {
                $@"fs_{nameof(scoring_metrics)}",
                $@"fs_{nameof(scoring_class_ids)}",
                $@"fs_{nameof(total_groups)}",
                $@"fs_{nameof(iterations_not_higher_than_all)}",
                $@"fs_{nameof(iterations_not_higher_than_last)}",
                $@"fs_{nameof(feature_selection_finished)}",
                $@"fs_{nameof(deep_search_index)}",

                $@"winner_key_file_tag",
                $@"winner_key_alphabet",
                $@"winner_key_stats",
                $@"winner_key_dimension",
                $@"winner_key_category",
                $@"winner_key_source",
                $@"winner_key_group",
                $@"winner_key_member",
                $@"winner_key_perspective",
            }
            .Concat(score_data.csv_header_array.Select(a => $@"this_winner_{a}").ToArray())
            .Concat(score_data.csv_header_array.Select(a => $@"last_winner_{a}").ToArray())
            .Concat(score_data.csv_header_array.Select(a => $@"best_winner_{a}").ToArray())
            .ToArray();
        

        internal static readonly string csv_header = string.Join(",", csv_header_values);

        internal string csv_values()
        {
            return string.Join(",", csv_values_array());
        }

        internal string[] csv_values_array()
        {
            var array1 = new List<string>
            {
                $@"{string.Join(";",scoring_metrics ?? Array.Empty<string>())}",
                $@"{string.Join(";",scoring_class_ids ?? Array.Empty<int>())}",
                $@"{total_groups}",
                $@"{iterations_not_higher_than_all}",
                $@"{iterations_not_higher_than_last}",
                $@"{(feature_selection_finished?1:0)}",
                $@"{deep_search_index}",

                $@"{winner_key.file_tag}",
                $@"{winner_key.alphabet}",
                $@"{winner_key.stats}",
                $@"{winner_key.dimension}",
                $@"{winner_key.category}",
                $@"{winner_key.source}",
                $@"{winner_key.group}",
                $@"{winner_key.member}",
                $@"{winner_key.perspective}",
            };

            array1.AddRange(this_winner_score_data.csv_values_array());
            array1.AddRange(last_winner_score_data.csv_values_array());
            array1.AddRange(best_winner_score_data.csv_values_array());

            return array1.Select(a => a?.Replace(",", ";", StringComparison.InvariantCultureIgnoreCase) ?? "").ToArray();
        }
    }
}
