using System.Collections.Generic;

namespace svm_fs_batch
{
    internal class selection_test_info
    {
        internal bool              y_is_group_selected;
        internal bool              y_is_only_selection;
        internal bool              y_is_last_winner;
        internal int               y_num_columns_added_from_last_iteration;
        internal int               y_num_groups_added_from_last_iteration;
        internal int               y_num_columns_added_from_highest_score_iteration;
        internal int               y_num_groups_added_from_highest_score_iteration;
        internal program.direction y_selection_direction;
        internal List<int>         y_previous_selected_groups;
        internal List<int>         y_previous_selected_columns;
        internal List<int>         y_selected_groups;
        internal List<int>         y_selected_columns;
        internal List<int>         y_test_selected_groups;
        internal List<int>         y_test_selected_columns;

        public static readonly string csv_header = string.Join(",", csv_header_values);

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
                nameof(y_previous_selected_groups),
                nameof(y_previous_selected_columns),
                nameof(y_selected_groups),
                nameof(y_selected_columns),
                nameof(y_test_selected_groups),
                nameof(y_test_selected_columns),
            };


        public string csv_values()
        {
            return string.Join(",", csv_values_array());
        }

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
                $"{string.Join(";",y_previous_selected_groups)}",
                $"{string.Join(";",y_previous_selected_columns)}",
                $"{string.Join(";",y_selected_groups)}",
                $"{string.Join(";",y_selected_columns)}",
                $"{string.Join(";",y_test_selected_groups)}",
                $"{string.Join(";",y_test_selected_columns)}",
            };
        }

    }
}
