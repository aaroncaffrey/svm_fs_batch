using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal class score_data
    {
        public const string module_name = nameof(score_data);

        // each group has an associated score_data

        private static readonly score_data empty = new score_data();

        internal index_data index_data;
        internal int class_id;
        
        internal score_data same_group_last_score_data;
        internal average_history same_group_score;
        internal average_history same_group_score_ppf;

#if SD_EXTRA
        internal double last_winner_num_groups_added_pct;
        internal double last_winner_num_columns_added_pct;
        internal int last_winner_num_groups_added;
#endif
        internal int last_winner_num_columns_added;
        internal double last_winner_score_increase;
        internal double last_winner_score_increase_pct;
        internal double last_winner_score_ppf_increase;
        internal double last_winner_score_ppf_increase_pct;

#if SD_EXTRA
        internal double best_winner_num_groups_added_pct;
        internal double best_winner_num_columns_added_pct;
        internal int best_winner_num_groups_added;
#endif
        internal int best_winner_num_columns_added;
        internal double best_winner_score_increase;
        internal double best_winner_score_increase_pct;
        internal double best_winner_score_ppf_increase;
        internal double best_winner_score_ppf_increase_pct;

        internal bool is_score_higher_than_last_winner;
        internal bool is_score_higher_than_best_winner;


        public static readonly string[] csv_header_values_array = new string[]
            {
                nameof(class_id),
                nameof(is_score_higher_than_last_winner),
                nameof(is_score_higher_than_best_winner),
                $@"_",
            }
            .Concat(index_data.csv_header_values_array.Select(a => $@"index_data_{a}").ToArray())
            .Concat(new[] { $@"_" })
            .Concat(average_history.csv_header_values_array.Select(a => $@"group_score_{a}").ToArray())
            .Concat(new[] { $@"_" })
            .Concat(average_history.csv_header_values_array.Select(a => $@"group_score_ppf_{a}").ToArray())
            .Concat(new[] { $@"_" })
            .Concat(new string[]
            {
#if SD_EXTRA
                nameof(last_winner_num_groups_added_pct),
                nameof(last_winner_num_columns_added_pct),
                nameof(last_winner_num_groups_added),
#endif
                nameof(last_winner_num_columns_added),
                nameof(last_winner_score_increase),
                nameof(last_winner_score_increase_pct),
                nameof(last_winner_score_ppf_increase),
                nameof(last_winner_score_ppf_increase_pct),
                $@"_",

#if SD_EXTRA
                
                nameof(best_winner_num_groups_added_pct),
                nameof(best_winner_num_columns_added_pct),
                nameof(best_winner_num_groups_added),
#endif
                nameof(best_winner_num_columns_added),
                nameof(best_winner_score_increase),
                nameof(best_winner_score_increase_pct),
                nameof(best_winner_score_ppf_increase),
                nameof(best_winner_score_ppf_increase_pct),
                $@"_",
            }).ToArray();

        public static readonly string csv_header_string = string.Join(",", csv_header_values_array);

        public string[] csv_values_array()
        {
            var values = new List<string>();

            values.AddRange(new string[]
            {
                    $@"{class_id}",
                    $@"{(is_score_higher_than_last_winner?1:0)}",
                    $@"{(is_score_higher_than_best_winner?1:0)}",
                    $@"_",
            });
            values.AddRange(index_data?.csv_values_array() ?? index_data.empty.csv_values_array());
            values.Add($@"_");
            values.AddRange(same_group_score?.csv_values_array() ?? average_history.empty.csv_values_array());
            values.Add($@"_");
            values.AddRange(same_group_score_ppf?.csv_values_array() ?? average_history.empty.csv_values_array());
            values.Add($@"_");
            values.AddRange(new string[]{
#if SD_EXTRA
                $@"{(last_winner_num_groups_added_pct):G17}",
                $@"{(last_winner_num_columns_added_pct):G17}",
                $@"{(last_winner_num_groups_added)}",
#endif
                $@"{(last_winner_num_columns_added)}",
                $@"{(last_winner_score_increase):G17}",
                $@"{(last_winner_score_increase_pct):G17}",
                $@"{(last_winner_score_ppf_increase):G17}",
                $@"{(last_winner_score_ppf_increase_pct):G17}",
                $@"_",
#if SD_EXTRA
                $@"{(best_winner_num_groups_added_pct):G17}",
                $@"{(best_winner_num_columns_added_pct):G17}",
                $@"{(best_winner_num_groups_added)}",
#endif
                $@"{(best_winner_num_columns_added)}",
                $@"{(best_winner_score_increase):G17}",
                $@"{(best_winner_score_increase_pct):G17}",
                $@"{(best_winner_score_ppf_increase):G17}",
                $@"{(best_winner_score_ppf_increase_pct):G17}",
                $@"_",
            });

            var values_array = values
            .Select(a => a.Replace(",", ";", StringComparison.OrdinalIgnoreCase))
            .ToArray();

            return values_array;
        }

        public string csv_values_string()
        {
            return string.Join(",", csv_values_array());
        }

        internal static void save(CancellationTokenSource cts, string sd_list_filename, score_data[] sd_list)
        {
            const string method_name = nameof(save);

            if (cts.IsCancellationRequested) return;

            var lines = new string[sd_list.Length + 1];
            lines[0] = csv_header_string;

            Parallel.For(0,
                sd_list.Length,
                i =>
                //for (var i = 0; i < cm_list.Count; i++)
                {
                    lines[i + 1] = sd_list[i].csv_values_string();
                });

            io_proxy.WriteAllLines(cts, sd_list_filename, lines, module_name, method_name);
        }

        internal score_data()
        {

        }

        internal score_data(index_data id, confusion_matrix cm, score_data same_group, score_data last_winner, score_data best_winner)
        {
            this.same_group_last_score_data = same_group;

            this.class_id = cm.x_class_id ?? default;
            this.index_data = id;


            var score = scoring_args.scoring_metrics.Select(metric_name => cm.metrics.get_value_by_name(metric_name)).DefaultIfEmpty(0).Average();
            var score_ppf = score != 0 && id.num_columns != 0 ? score / id.num_columns : 0;

            this.same_group_score = new average_history(score, same_group?.same_group_score);
            this.same_group_score_ppf = new average_history(score_ppf, same_group?.same_group_score_ppf);

            calc_increase_from_last_winner(last_winner);
            calc_increase_from_best_winner(best_winner);
        }

        internal void calc_increase_from_last_winner(score_data last_winner)
        {
            if (last_winner == null) last_winner = empty;

#if SD_EXTRA
            this.last_winner_num_groups_added = this.num_groups - last_winner.num_groups;
            this.last_winner_num_groups_added_pct = last_winner.num_groups != 0 ? (double)this.num_groups / (double)last_winner.num_groups : 0;
            this.last_winner_num_columns_added_pct = last_winner.num_columns != 0 ? (double)this.num_columns / (double)last_winner.num_columns : 0;
#endif
            this.last_winner_num_columns_added = this.index_data.num_columns - last_winner.index_data.num_columns;
            this.last_winner_score_increase = this.same_group_score.value - (last_winner.same_group_score?.value ?? 0);
            this.last_winner_score_increase_pct = last_winner.same_group_score != null && last_winner.same_group_score.value != 0 ? (double)this.same_group_score.value / (double)last_winner.same_group_score.value : 0;
            this.last_winner_score_ppf_increase = this.same_group_score_ppf.value - (last_winner?.same_group_score_ppf?.value ?? 0);
            this.last_winner_score_ppf_increase_pct = last_winner.same_group_score_ppf != null && last_winner.same_group_score_ppf.value != 0 ? (double)this.same_group_score_ppf.value / (double)last_winner.same_group_score_ppf.value : 0;
            this.is_score_higher_than_last_winner = last_winner_score_increase > 0d;
        }

        internal void calc_increase_from_best_winner(score_data best_winner)
        {
            if (best_winner == null) best_winner = empty;

#if SD_EXTRA
            this.best_winner_num_groups_added = this.num_groups - best_winner.num_groups;
            this.best_winner_num_groups_added_pct = best_winner.num_groups != 0 ? (double)this.num_groups / (double)best_winner.num_groups : 0;
            this.best_winner_num_columns_added_pct = best_winner.num_columns != 0 ? (double)this.num_columns / (double)best_winner.num_columns : 0;
#endif
            this.best_winner_num_columns_added = this.index_data.num_columns - best_winner.index_data.num_columns;
            this.best_winner_score_increase = this.same_group_score.value - (best_winner?.same_group_score?.value ?? 0);
            this.best_winner_score_increase_pct = best_winner.same_group_score != null && best_winner.same_group_score.value != 0 ? (double)this.same_group_score.value / (double)best_winner.same_group_score.value : 0;
            this.best_winner_score_ppf_increase = this.same_group_score_ppf.value - (best_winner?.same_group_score_ppf?.value ?? 0);
            this.best_winner_score_ppf_increase_pct = best_winner.same_group_score_ppf != null && best_winner.same_group_score_ppf.value != 0 ? (double)this.same_group_score_ppf.value / (double)best_winner.same_group_score_ppf.value : 0;
            this.is_score_higher_than_best_winner = best_winner_score_increase > 0d;
        }
    }
}
