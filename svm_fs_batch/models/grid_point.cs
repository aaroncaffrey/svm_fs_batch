using System.Globalization;
using System.Linq;
using System.Threading;

namespace svm_fs_batch
{
    internal class grid_point
    {
        public const string module_name = nameof(grid_point);
        internal static readonly grid_point empty = new grid_point();

        internal double? cost;
        internal double? gamma;
        internal double? epsilon;
        internal double? coef0;
        internal double? degree;
        internal double? cv_rate;

        internal grid_point()
        {

        }

        internal grid_point(grid_point grid_point)
        {
            if (grid_point == null) return;

            this.cost = grid_point.cost;
            this.gamma = grid_point.gamma;
            this.epsilon = grid_point.epsilon;
            this.coef0 = grid_point.coef0;
            this.degree = grid_point.degree;
            this.cv_rate = grid_point.cv_rate;
        }

        internal grid_point(grid_point[] grid_points)
        {
            if (grid_points == null || grid_points.Length == 0) return;
            
            this.cost = grid_points.Where(a => a?.cost != null).Select(a => a.cost).DefaultIfEmpty(0).Average();
            this.gamma = grid_points.Where(a => a?.gamma != null).Select(a => a.gamma).DefaultIfEmpty(0).Average();
            this.epsilon = grid_points.Where(a => a?.epsilon != null).Select(a => a.epsilon).DefaultIfEmpty(0).Average();
            this.coef0 = grid_points.Where(a => a?.coef0 != null).Select(a => a.coef0).DefaultIfEmpty(0).Average();
            this.degree = grid_points.Where(a => a?.degree != null).Select(a => a.degree).DefaultIfEmpty(0).Average();
            this.cv_rate = grid_points.Where(a => a?.cv_rate != null).Select(a => a.cv_rate).DefaultIfEmpty(0).Average();
        }

        internal static readonly string[] csv_header_values_array = new string[]
        {
            nameof(cost),
            nameof(gamma),
            nameof(epsilon),
            nameof(coef0),
            nameof(degree),
            nameof(cv_rate),
        };

        internal static readonly string csv_header_string = string.Join(",", csv_header_values_array);

        internal string[] csv_values_array()
        {
            return new string[]
            {
                cost?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                gamma?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                epsilon?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                coef0?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                degree?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                cv_rate?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
            };
        }

        internal string csv_values_string()
        {
            return string.Join(",", csv_values_array());
        }
    }
}
