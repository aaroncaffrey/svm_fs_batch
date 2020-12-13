using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace svm_fs_batch
{
    internal class score_data
    {
        public const string module_name = nameof(score_data);
        private static readonly score_data empty = new score_data();


        internal int current_rank = -1;
        internal int last_rank = -1;
        internal int rank_increase;
        internal double rank_average;
        internal double rank_increase_average;

        internal int[] rank_history;
        internal int[] rank_increase_history;

        internal int class_id;
        internal int iteration_index;
        internal string iteration_name;
        internal int group_array_index;
        internal int num_groups;
        internal int num_columns;
        internal program.direction selection_direction;

        internal double score;
        internal double score_ppf;
        internal int[] selected_groups;
        internal int[] selected_columns;

        internal double same_group_score_increase;
        internal double same_group_score_increase_pct;
        internal double same_group_score_increase_ppf;
        internal double same_group_score_increase_ppf_pct;

        internal int last_winner_num_groups_added;
        internal double last_winner_num_groups_added_pct;
        internal int last_winner_num_columns_added;
        internal double last_winner_num_columns_added_pct;
        internal double last_winner_score_increase;
        internal double last_winner_score_increase_pct;
        internal double last_winner_score_increase_ppf;
        internal double last_winner_score_increase_ppf_pct;

        internal int best_winner_num_groups_added;
        internal double best_winner_num_groups_added_pct;
        internal int best_winner_num_columns_added;
        internal double best_winner_num_columns_added_pct;
        internal double best_winner_score_increase;
        internal double best_winner_score_increase_pct;
        internal double best_winner_score_increase_ppf;
        internal double best_winner_score_increase_ppf_pct;

        internal bool is_score_higher_than_same_group;
        internal bool is_score_higher_than_last_winner;
        internal bool is_score_higher_than_best_winner;


        public static readonly string[] csv_header_array = new string[]
        {
            nameof(current_rank),
            nameof(last_rank),
            nameof(rank_increase),
            nameof(rank_average),
            nameof(rank_increase_average),

            nameof(rank_history),
            nameof(rank_increase_history),

            nameof(class_id),
            nameof(iteration_index),
            nameof(iteration_name),
            nameof(group_array_index),
            nameof(num_groups),
            nameof(num_columns),
            nameof(selection_direction),

            nameof(score),
            nameof(score_ppf),
            nameof(selected_groups),
            nameof(selected_columns),

            nameof(is_score_higher_than_same_group),
            nameof(is_score_higher_than_last_winner),
            nameof(is_score_higher_than_best_winner),

            nameof(same_group_score_increase),
            nameof(same_group_score_increase_pct),
            nameof(same_group_score_increase_ppf),
            nameof(same_group_score_increase_ppf_pct),

            nameof(last_winner_num_groups_added),
            nameof(last_winner_num_groups_added_pct),
            nameof(last_winner_num_columns_added),
            nameof(last_winner_num_columns_added_pct),
            nameof(last_winner_score_increase),
            nameof(last_winner_score_increase_pct),
            nameof(last_winner_score_increase_ppf),
            nameof(last_winner_score_increase_ppf_pct),

            nameof(best_winner_num_groups_added),
            nameof(best_winner_num_groups_added_pct),
            nameof(best_winner_num_columns_added),
            nameof(best_winner_num_columns_added_pct),
            nameof(best_winner_score_increase),
            nameof(best_winner_score_increase_pct),
            nameof(best_winner_score_increase_ppf),
            nameof(best_winner_score_increase_ppf_pct),
        };

        public static readonly string csv_header = string.Join(",", csv_header_array);

        public string[] csv_values_array()
        {
            return new string[]
            {
                $@"{current_rank}",
                $@"{last_rank}",
                $@"{rank_increase}",
                $@"{rank_average:G17}",
                $@"{rank_increase_average:G17}",
                
                //$@"{string.Join($@";", previous_ranks.Select((a,i)=>$@"{i}:{a}").ToArray())}",
                $@"{string.Join($@";", rank_history ?? Array.Empty<int>())}",
                $@"{string.Join($@";", rank_increase_history ?? Array.Empty<int>())}",

                $@"{class_id}",
                $@"{iteration_index}",
                $@"{iteration_name}",
                $@"{group_array_index}",
                $@"{num_groups}",
                $@"{num_columns}",
                $@"{selection_direction}",
                $@"{score:G17}",
                $@"{score_ppf:G17}",
                $@"{string.Join($@";", selected_groups ?? Array.Empty<int>())}",
                $@"{string.Join($@";", selected_columns ?? Array.Empty<int>())}",

                $@"{(is_score_higher_than_same_group?1:0)}",
                $@"{(is_score_higher_than_last_winner?1:0)}",
                $@"{(is_score_higher_than_best_winner?1:0)}",

                $@"{(same_group_score_increase):G17}",
                $@"{(same_group_score_increase_pct):G17}",
                $@"{(same_group_score_increase_ppf):G17}",
                $@"{(same_group_score_increase_ppf_pct):G17}",

                $@"{(last_winner_num_groups_added)}",
                $@"{(last_winner_num_groups_added_pct):G17}",
                $@"{(last_winner_num_columns_added)}",
                $@"{(last_winner_num_columns_added_pct):G17}",
                $@"{(last_winner_score_increase):G17}",
                $@"{(last_winner_score_increase_pct):G17}",
                $@"{(last_winner_score_increase_ppf):G17}",
                $@"{(last_winner_score_increase_ppf_pct):G17}",

                $@"{(best_winner_num_groups_added)}",
                $@"{(best_winner_num_groups_added_pct):G17}",
                $@"{(best_winner_num_columns_added)}",
                $@"{(best_winner_num_columns_added_pct):G17}",
                $@"{(best_winner_score_increase):G17}",
                $@"{(best_winner_score_increase_pct):G17}",
                $@"{(best_winner_score_increase_ppf):G17}",
                $@"{(best_winner_score_increase_ppf_pct):G17}",
            }.Select(a => a.Replace(",", ";", StringComparison.InvariantCultureIgnoreCase)).ToArray();
        }

        public string csv_values()
        {
            return string.Join(",", csv_values_array());
        }

        internal static void save(string sd_list_filename, IList<score_data> sd_list)
        {
            const string method_name = nameof(save);

            var lines = new string[sd_list.Count + 1];
            lines[0] = csv_header;

            Parallel.For(0,
                sd_list.Count,
                i =>
                    //for (var i = 0; i < cm_list.Count; i++)
                {
                    lines[i + 1] = sd_list[i].csv_values();
                });

            io_proxy.WriteAllLines(sd_list_filename, lines, module_name, method_name);
        }

        internal score_data()
        {

        }


        internal score_data(
            int class_id,
            int iteration_index,
            string iteration_name,
            int group_array_index,
            int num_groups,
            int num_columns,
            program.direction selection_direction,
            double score,
            int[] selected_groups,
            int[] selected_columns,
            score_data same_group,
            score_data last_winner,
            score_data best_winner)
        {
            this.class_id = class_id;
            this.iteration_index = iteration_index;
            this.iteration_name = iteration_name;
            this.group_array_index = group_array_index;
            this.num_groups = num_groups;
            this.num_columns = num_columns;
            this.selection_direction = selection_direction;
            this.score = score;
            this.selected_groups = selected_groups;
            this.selected_columns = selected_columns;

            this.score_ppf = num_columns > 0 ? score / num_columns : 0;
            //var score_increase_from_previous_winner_ppf = num_columns_added > 0 ? score_increase_from_previous_winner / num_columns_added : 0;

            calc_increase_from_same_group(same_group);
            calc_increase_from_last_winner(last_winner);
            calc_increase_from_best_winner(best_winner);
        }

        internal static void set_ranks(ref List<(confusion_matrix cm, score_data sd)> score_data_list, bool order_by_ppf, score_data last_winner)
        {
            // ensure consistent reordering (i.e. for items with equal tied scores when processing may have been done out of order)
            score_data_list = score_data_list.OrderBy(a => a.sd.group_array_index).ThenBy(a => a.sd.class_id).ToList();

            // reorder by score, or score_ppf
            score_data_list = score_data_list
                .OrderByDescending((a => order_by_ppf ? a.sd.last_winner_score_increase_ppf : a.sd.last_winner_score_increase))
                .ThenByDescending(a => !order_by_ppf ? a.sd.last_winner_score_increase_ppf : a.sd.last_winner_score_increase)
                .ThenBy(a => a.sd.last_winner_num_columns_added)
                .ToList();


            if (score_data_list[0].sd.group_array_index == (last_winner?.group_array_index ?? -1) && score_data_list.Count > 1)
            {
                // if winner is the same as last time (due to random variance), then take the next group instead.
                var ix0 = score_data_list[0];
                var ix1 = score_data_list[1];

                score_data_list[0] = ix1;
                score_data_list[1] = ix0;
            }

            for (var index = 0; index < score_data_list.Count; index++)
            {
                var this_rank = index + 1;

                var sdl_item = score_data_list[index];
                sdl_item.sd.current_rank = this_rank;

                sdl_item.sd.last_rank = sdl_item.sd.rank_history?.DefaultIfEmpty(-1).Last() ?? -1;
                //var is_rank_higher = sd.last_rank != -1 && sd.current_rank < sd.last_rank; // '<' because ranks ascend
                //var is_rank_same   = sd.last_rank != -1 && sd.current_rank == sd.last_rank;

                sdl_item.sd.rank_history = (sdl_item.sd.rank_history ?? Array.Empty<int>()).Concat(new[] { sdl_item.sd.current_rank }).ToArray();
                sdl_item.sd.rank_increase = sdl_item.sd.last_rank == -1 ? 0 : sdl_item.sd.last_rank - sdl_item.sd.current_rank;
                sdl_item.sd.rank_average = sdl_item.sd.rank_history.Average();

                sdl_item.sd.rank_increase_history = (sdl_item.sd.rank_increase_history ?? Array.Empty<int>()).Concat(new[] { sdl_item.sd.rank_increase }).ToArray();
                sdl_item.sd.rank_increase_average = sdl_item.sd.rank_increase_history.Average();
            }
        }

        internal void calc_increase_from_same_group(score_data same_group)
        {
            // todo: this isn't called after ranking, only before,... make it called after ranking.
            // todo: ban consistently poor performing groups (i.e. groups whose ranks never improve)

            //if (same_group != null)
            //{
            //    io_proxy.WriteLine("");
            //}

            if (same_group == null) same_group = empty;

            this.rank_history = same_group.rank_history?.ToArray() ?? Array.Empty<int>();
            this.rank_increase_history = same_group.rank_increase_history?.ToArray() ?? Array.Empty<int>();

            this.same_group_score_increase = this.score - same_group.score;
            this.same_group_score_increase_pct = same_group.score != 0 ? this.score / same_group.score : 0;

            this.same_group_score_increase_ppf = this.score_ppf - same_group.score_ppf;
            this.same_group_score_increase_ppf_pct = same_group.score_ppf != 0 ? this.score_ppf / same_group.score_ppf : 0;

            this.is_score_higher_than_same_group = same_group_score_increase > 0d;
        }

        internal void calc_increase_from_last_winner(score_data last_winner)
        {
            if (last_winner == null) last_winner = empty;

            this.last_winner_num_groups_added = this.num_groups - last_winner.num_groups;
            this.last_winner_num_groups_added_pct = last_winner.num_groups != 0 ? (double)this.num_groups / (double)last_winner.num_groups : 0;

            this.last_winner_num_columns_added = this.num_columns - last_winner.num_columns;
            this.last_winner_num_columns_added_pct = last_winner.num_columns != 0 ? (double)this.num_columns / (double)last_winner.num_columns : 0;

            this.last_winner_score_increase = this.score - last_winner.score;
            this.last_winner_score_increase_pct = last_winner.score != 0 ? this.score / last_winner.score : 0;

            this.last_winner_score_increase_ppf = this.score_ppf - last_winner.score_ppf;
            this.last_winner_score_increase_ppf_pct = last_winner.score_ppf != 0 ? this.score_ppf / last_winner.score_ppf : 0;

            this.is_score_higher_than_last_winner = last_winner_score_increase > 0d;
        }

        internal void calc_increase_from_best_winner(score_data best_winner)
        {
            if (best_winner == null) best_winner = empty;

            this.best_winner_num_groups_added = this.num_groups - best_winner.num_groups;
            this.best_winner_num_groups_added_pct = best_winner.num_groups != 0 ? (double)this.num_groups / (double)best_winner.num_groups : 0;

            this.best_winner_num_columns_added = this.num_columns - best_winner.num_columns;
            this.best_winner_num_columns_added_pct = best_winner.num_columns != 0 ? (double)this.num_columns / (double)best_winner.num_columns : 0;

            this.best_winner_score_increase = this.score - best_winner.score;
            this.best_winner_score_increase_pct = best_winner.score != 0 ? this.score / best_winner.score : 0;

            this.best_winner_score_increase_ppf = this.score_ppf - best_winner.score_ppf;
            this.best_winner_score_increase_ppf_pct = best_winner.score_ppf != 0 ? this.score_ppf / best_winner.score_ppf : 0;

            this.is_score_higher_than_best_winner = best_winner_score_increase > 0d;
        }
    }
}
