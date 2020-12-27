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

        public grid_point(double? cost, double? gamma, double? epsilon, double? coef0, double? degree, double? cv_rate)
        {
            this.cost = cost;
            this.gamma = gamma;
            this.epsilon = epsilon;
            this.coef0 = coef0;
            this.degree = degree;
            this.cv_rate = cv_rate;
        }

        //public grid_point((double? cost, double? gamma, double? epsilon, double? coef0, double? degree, double? cv_rate) point)
        //{
        //    this.cost = point.cost;
        //    this.gamma = point.gamma;
        //    this.epsilon = point.epsilon;
        //    this.coef0 = point.coef0;
        //    this.degree = point.degree;
        //    this.cv_rate = point.cv_rate;
        //}

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

            var cost2 = grid_points.Where(a => a?.cost != null).Select(a => a.cost).ToArray();
            this.cost = cost2.Length > 0 ? cost2.Average() : null;

            var gamma2 = grid_points.Where(a => a?.gamma != null).Select(a => a.gamma).ToArray();
            this.gamma = gamma2.Length > 0 ? gamma2.Average() : null;

            var epsilon2 = grid_points.Where(a => a?.epsilon != null).Select(a => a.epsilon).ToArray();
            this.epsilon = epsilon2.Length > 0 ? epsilon2.Average() : null;

            var coef02 = grid_points.Where(a => a?.coef0 != null).Select(a => a.coef0).ToArray();
            this.coef0 = coef02.Length > 0 ? coef02.Average() : null;

            var degree2 = grid_points.Where(a => a?.degree != null).Select(a => a.degree).ToArray();
            this.degree = degree2.Length > 0 ? degree2.Average() : null;

            var cv_rate2 = grid_points.Where(a => a?.cv_rate != null).Select(a => a.cv_rate).ToArray();
            this.cv_rate = cv_rate2.Length > 0 ? cv_rate2.Average() : null;
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
