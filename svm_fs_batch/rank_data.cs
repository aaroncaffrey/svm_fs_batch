using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace svm_fs_batch
{
    internal class rank_data
    {
        internal rank_data last_rank;
        internal double max_rank;
        internal average_history rank_number;
        internal average_history rank_score;

#if RD_EXTRA
        internal average_history rank_number_pct;
        
        internal average_history rank_score_ppf;
#endif
        public rank_data(int rank_number /* max_rank is best rank */, int max_rank /* total - 1 */, double rank_score /* 0 to 1 */ , double rank_score_ppf /* 0 to 1 */, rank_data last_rank)
        {
            this.max_rank = max_rank;
            this.last_rank = last_rank;
            this.rank_number = new average_history(rank_number, last_rank?.rank_number);
            this.rank_score = new average_history(rank_score, last_rank?.rank_score);
#if RD_EXTRA
            this.rank_number_pct = new average_history(max_rank != 0 ? (double)rank_number / (double)max_rank : 0d, last_rank?.rank_number_pct);
            
            this.rank_score_ppf = new average_history(rank_score_ppf, last_rank?.rank_score_ppf);
#endif
        }

        internal static (index_data id, confusion_matrix cm, score_data sd, rank_data rd)[] set_ranks
        (
            CancellationTokenSource cts,
            ref List<(index_data id, confusion_matrix cm, score_data sd)> cm_sd_list_ref,
            //bool order_by_ppf,
            score_data last_winner,
            (index_data id, confusion_matrix cm, score_data sd, rank_data rd)[] last_iteration_cm_sd_rd_list
        )
        {
            if (cts.IsCancellationRequested) return default;
            // ensure consistent reordering (i.e. for items with equal tied scores when processing may have been done out of order)
            var cm_sd_list = cm_sd_list_ref.OrderBy(a => a.sd.group_array_index).ThenBy(a => a.sd.class_id).ToList();

            // reorder by score, or score_ppf... descending so that highest score is first result.
            //cm_sd_list = cm_sd_list
            //    .OrderByDescending((a => order_by_ppf ? a.sd.last_winner_score_ppf_increase : a.sd.last_winner_score_increase))
            //    .ThenByDescending(a => !order_by_ppf ? a.sd.last_winner_score_ppf_increase : a.sd.last_winner_score_increase)
            //    .ThenBy(a => a.sd.last_winner_num_columns_added)
            //    .ToList();


            cm_sd_list = cm_sd_list
                .OrderByDescending((a => a.sd.same_group_score.value))
                .ThenBy(a => a.sd.num_columns)
                .ToList();

            if (cm_sd_list[0].sd.group_array_index == (last_winner?.group_array_index ?? -1) && cm_sd_list.Count > 1)
            {
                // edge case, if winner is the same as last time (due to random variance), then take the next group instead.
                var ix0 = cm_sd_list[0];
                var ix1 = cm_sd_list[1];

                cm_sd_list[0] = ix1;
                cm_sd_list[1] = ix0;
            }

            var scores = cm_sd_list.Select(a => a.sd?.same_group_score?.value ?? 0d).ToArray();
            var scores_scale = new scaling(scores) { rescale_scale_min = 0, rescale_scale_max = 1 };

            var scores_ppf = cm_sd_list.Select(a => a.sd?.same_group_score_ppf?.value ?? 0d).ToArray();
            var scores_ppf_scale = new scaling(scores_ppf) { rescale_scale_min = 0, rescale_scale_max = 1 };



            // make rank_data instances, which track the ranks (performance) of each group over time, to allow for optimisation decisions and detection of variant features

            var cm_sd_rd_list = cm_sd_list
                .AsParallel()
                .AsOrdered()
                .WithCancellation(cts.Token)
                .Select((a, index) =>
                {
                    var sd_last = last_iteration_cm_sd_rd_list?.FirstOrDefault
                    (a =>
                        a.sd.iteration_index == cm_sd_list[index].sd.iteration_index &&
                        a.sd.group_array_index == cm_sd_list[index].sd.group_array_index &&
                        a.sd.class_id == cm_sd_list[index].sd.class_id
                    ) ?? default;

                    var rd = new rank_data
                    (
                        (cm_sd_list.Count - 1) - index,
                        cm_sd_list.Count - 1,
                        scores_scale.scale(cm_sd_list[index].sd.same_group_score.value, scaling.scale_function.rescale),
                        scores_ppf_scale.scale(cm_sd_list[index].sd.same_group_score_ppf.value, scaling.scale_function.rescale),
                        sd_last.rd
                    );

                    return (a.id, a.cm, a.sd, rd);
                })
                .ToArray();

            cm_sd_list_ref = cm_sd_list;

            return cm_sd_rd_list;
        }

        public static readonly string[] csv_header_values = new string[]
        {
            nameof(max_rank),
            $@"_"
        }
        .Concat(average_history.csv_header_values.Select(a => $@"{nameof(rank_number)}_{a}").ToArray())
        .Concat(new[] { $@"_" })
        .Concat(average_history.csv_header_values.Select(a => $@"{nameof(rank_score)}_{a}").ToArray())
        .Concat(new[] { $@"_" })

#if RD_EXTRA
        .Concat(average_history.csv_header_values.Select(a => $@"{nameof(rank_number_pct)}_{a}").ToArray())
        .Concat(new[] { $@"_" })

        .Concat(average_history.csv_header_values.Select(a => $@"{nameof(rank_score_ppf)}_{a}").ToArray())
        .Concat(new[] { $@"_" })
#endif
        .ToArray();


        public static readonly string csv_header = string.Join(",", csv_header_values);



        public string[] csv_values_array()
        {
            var values = new List<string>();
            values.AddRange(new string[]
            {
                $@"{max_rank:G17}",
                $@"_"
            });

            values.AddRange(rank_number.csv_values_array());
            values.Add($@"_");
            values.AddRange(rank_score.csv_values_array());
            values.Add($@"_");
#if RD_EXTRA
            values.AddRange(rank_number_pct.csv_values_array());
            values.Add($@"_");
            
            values.AddRange(rank_score_ppf.csv_values_array());
            values.Add($@"_");
#endif

            var values_array = values.Select(a => a.Replace(",", ";", StringComparison.OrdinalIgnoreCase)).ToArray();
            return values_array;
        }

        public string csv_values()
        {
            return string.Join(",", csv_values_array());
        }
    }
}
