using System;

namespace svm_fs_batch
{
    internal class selection_test_info
    {
        public const string module_name = nameof(selection_test_info);

        internal bool              y_is_group_selected;
        internal bool              y_is_only_selection;
        internal bool              y_is_last_winner;
        internal int               y_num_columns_added_from_last_iteration;
        internal int               y_num_groups_added_from_last_iteration;
        internal int               y_num_columns_added_from_highest_score_iteration;
        internal int               y_num_groups_added_from_highest_score_iteration;
        internal program.direction y_selection_direction;

        internal int y_test_groups_count;
        internal int y_test_columns_count;
        internal int[] y_test_groups;
        internal int[] y_test_columns;

        internal int y_previous_winner_groups_count;
        internal int y_previous_winner_columns_count;
        internal int[] y_previous_winner_groups;
        internal int[] y_previous_winner_columns;

        internal int y_best_winner_groups_count;
        internal int y_best_winner_columns_count;
        internal int[] y_best_winner_groups;
        internal int[] y_best_winner_columns;

        public selection_test_info()
        {
            
        }

        public static readonly string[] csv_header_values = new string[]
            {
                nameof(y_is_group_selected),
                nameof(y_is_only_selection),
                nameof(y_is_last_winner),
                nameof(y_num_columns_added_from_last_iteration),
                nameof(y_num_groups_added_from_last_iteration),
                nameof(y_num_columns_added_from_highest_score_iteration),
                nameof(y_num_groups_added_from_highest_score_iteration),
                nameof(y_selection_direction),

                nameof(y_test_groups_count),
                nameof(y_test_columns_count),
                nameof(y_test_groups),
                nameof(y_test_columns),

                nameof(y_previous_winner_groups_count),
                nameof(y_previous_winner_columns_count),
                nameof(y_previous_winner_groups),
                nameof(y_previous_winner_columns),

                nameof(y_best_winner_groups_count),
                nameof(y_best_winner_columns_count),
                nameof(y_best_winner_groups),
                nameof(y_best_winner_columns),


            };

        public static readonly string csv_header = string.Join(",", csv_header_values);


        public string[] csv_values_array()
        {
            return new string[]
            {
                $"{y_is_group_selected}",
                $"{y_is_only_selection}",
                $"{y_is_last_winner}",
                $"{y_num_columns_added_from_last_iteration}",
                $"{y_num_groups_added_from_last_iteration}",
                $"{y_num_columns_added_from_highest_score_iteration}",
                $"{y_num_groups_added_from_highest_score_iteration}",
                $"{y_selection_direction}",

                $"{y_test_groups_count}",
                $"{y_test_columns_count}",
                $"{string.Join(";",y_test_groups ?? Array.Empty<int>())}",
                $"{string.Join(";",y_test_columns ?? Array.Empty<int>())}",

                $"{y_previous_winner_groups_count}",
                $"{y_previous_winner_columns_count}",
                $"{string.Join(";",y_previous_winner_groups ?? Array.Empty<int>())}",
                $"{string.Join(";",y_previous_winner_columns ?? Array.Empty<int>())}",

                $"{y_best_winner_groups_count}",
                $"{y_best_winner_columns_count}",
                $"{string.Join(";",y_best_winner_groups ?? Array.Empty<int>())}",
                $"{string.Join(";",y_best_winner_columns ?? Array.Empty<int>())}",
            };
        }

        public string csv_values()
        {
            return string.Join(",", csv_values_array());
        }

    }
}
