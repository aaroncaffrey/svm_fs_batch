using System.Collections.Generic;

namespace svm_fs_batch
{
    internal class feature_selection_status
    {
        internal double all_time_highest_score;
        internal int all_time_highest_score_iteration_index;
        internal string all_time_highest_score_iteration_name;
        internal List<int> all_time_highest_score_selected_columns;
        internal List<int> all_time_highest_score_selected_groups;
        internal int deep_search_index;
        internal bool feature_selection_finished;
        internal int iteration_index;
        internal string iteration_name;
        internal int iterations_not_better_than_all;
        internal int iterations_not_better_than_last;
        internal List<int> previous_selected_columns;
        internal List<int> previous_selected_groups;
        internal int previous_winner_group_array_index;
        internal double previous_winner_score;
        internal List<int> selected_columns;
        internal List<int> selected_groups;
        internal int iteration_winner_group_array_index;
        internal double iteration_winner_score;
        internal program.direction iteration_winner_direction;
        internal double score_increase_from_last;
        internal double score_increase_from_all;
        internal double score_increase_from_last_pct;
        internal double score_increase_from_all_pct;
        internal bool score_better_than_last;
        internal bool score_better_than_all;
        internal int total_groups;
        internal var iteration_winner_group;
        internal var iteration_winner_group_key;

        internal static void save(string status_filename, IList<feature_selection_status> feature_selection_status_list)
        {
            const string module_name = nameof(feature_selection_status);
            const string method_name = nameof(save);

            var lines = new string[feature_selection_status_list.Count+1];
            lines[0] = status_csv_header;
            for (var i = 0; i < feature_selection_status_list.Count; i++)
            {
                lines[i + 1] = feature_selection_status_list[i].csv_values();
            }

            io_proxy.WriteAllLines(status_filename, lines, module_name, method_name);
        }

        internal static readonly string status_csv_header = string.Join(",", status_csv_header_array);

        internal static readonly string[] status_csv_header_array = new string[]
                        {
                            nameof(all_time_highest_score                  ),
                            nameof(all_time_highest_score_iteration_index  ),
                            nameof(all_time_highest_score_iteration_name   ),
                            nameof(all_time_highest_score_selected_columns ),
                            nameof(all_time_highest_score_selected_groups  ),
                            nameof(deep_search_index                       ),
                            nameof(feature_selection_finished              ),
                            nameof(iteration_index                         ),
                            nameof(iteration_name                          ),
                            nameof(iterations_not_better_than_all          ),
                            nameof(iterations_not_better_than_last         ),
                            nameof(previous_selected_columns               ),
                            nameof(previous_selected_groups                ),
                            nameof(previous_winner_group_array_index       ),
                            nameof(previous_winner_score                   ),
                            nameof(selected_columns                        ),
                            nameof(selected_groups                         ),
                            nameof(total_groups                            ),
                            nameof(iteration_winner_group_array_index      ),
                            nameof(iteration_winner_group                  ),
                            nameof(iteration_winner_group_key              ),
                            nameof(iteration_winner_score                  ),
                            nameof(iteration_winner_direction              ),
                            nameof(score_increase_from_last                ),
                            nameof(score_increase_from_all                 ),
                            nameof(score_increase_from_last_pct            ),
                            nameof(score_increase_from_all_pct             ),
                            nameof(score_better_than_last                  ),
                            nameof(score_better_than_all                   ),
                        };


        internal string csv_values()
        {
            return string.Join(",", csv_values_array);
        }

        internal string[] csv_values_array()
        {
            return new string[]
                          {
                            $"{all_time_highest_score:G17}",
                            $"{all_time_highest_score_iteration_index}",
                            $"{all_time_highest_score_iteration_name}",
                            $"{string.Join(";",all_time_highest_score_selected_columns)}",
                            $"{string.Join(";",all_time_highest_score_selected_groups)}",
                            $"{deep_search_index}",
                            $"{(feature_selection_finished?1:0)}",
                            $"{iteration_index}",
                            $"{iteration_name}",
                            $"{iterations_not_better_than_all}",
                            $"{iterations_not_better_than_last}",
                            $"{string.Join(";",previous_selected_columns)}",
                            $"{string.Join(";",previous_selected_groups)}",
                            $"{previous_winner_group_array_index}",
                            $"{previous_winner_score:G17}",
                            $"{string.Join(";",selected_columns)}",
                            $"{string.Join(";",selected_groups)}",
                            $"{total_groups}",
                            $"{iteration_winner_group_array_index}",
                            $"{iteration_winner_group}",
                            $"{iteration_winner_group_key}",
                            $"{iteration_winner_score:G17}",
                            $"{iteration_winner_direction}",
                            $"{score_increase_from_last:G17}",
                            $"{score_increase_from_all:G17}",
                            $"{score_increase_from_last_pct:G17}",
                            $"{score_increase_from_all_pct:G17}",
                            $"{(score_better_than_last?1:0)}",
                            $"{(score_better_than_all?1:0)}",
                          };
        }
    }
}
