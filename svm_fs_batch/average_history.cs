using System;
using System.Linq;

namespace svm_fs_batch
{
    internal class average_history
    {
        private static readonly average_history empty = new average_history();

        internal double value;
        internal double value_average;
        internal double value_sd;
        internal double value_increase;
        internal double value_increase_pct;
        internal double value_increase_average;
        internal double value_increase_sd;

        internal bool is_value_higher;
        
        internal average_history[] value_history;

        private average_history()
        {

        }

        public average_history(double value, average_history prev_average)
        {
            if (prev_average == null) prev_average = empty;

            this.value = value;
            this.value_history = (prev_average?.value_history ?? Array.Empty<average_history>()).Concat(new[] {this}).ToArray();
            this.value_increase = this.value - prev_average.value; 
            this.value_increase_pct = prev_average.value != 0 ? this.value / prev_average.value : 0;

            this.value_average = value_history?.Select(a => a.value).DefaultIfEmpty(0).Average() ?? 0;
            this.value_sd = routines.standard_deviation_population(value_history?.Select(a => a.value).DefaultIfEmpty(0).ToArray());
            
            this.value_increase_average = value_history?.Select(a => a.value_increase).DefaultIfEmpty(0).Average() ?? 0;
            this.value_increase_sd = routines.standard_deviation_population(value_history?.Select(a => a.value_increase).DefaultIfEmpty(0).ToArray());

            this.is_value_higher = this.value > prev_average.value;
        }

        public static readonly string[] csv_header_values = new string[]
       {
            nameof(is_value_higher),
            nameof(value),
            nameof(value_average),
            nameof(value_sd),
            nameof(value_increase),
            nameof(value_increase_pct),
            nameof(value_increase_average),
            nameof(value_increase_sd),
       };

        public static readonly string csv_header = string.Join(",", csv_header_values);


        public string[] csv_values_array()
        {
            return new string[]
            {
                $@"{(is_value_higher?1:0)}",
                $@"{value:G17}",
                $@"{value_average:G17}",
                $@"{value_sd:G17}",
                $@"{value_increase:G17}",
                $@"{value_increase_pct:G17}",
                $@"{value_increase_average:G17}",
                $@"{value_increase_sd:G17}",

            }.Select(a => a.Replace(",", ";", StringComparison.InvariantCultureIgnoreCase)).ToArray();
        }
        public string csv_values()
        {
            return string.Join(",", csv_values_array());
        }
    }
}
