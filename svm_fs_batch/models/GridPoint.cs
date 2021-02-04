using System.Globalization;
using System.Linq;

namespace SvmFsBatch
{
    internal class GridPoint
    {
        public const string ModuleName = nameof(GridPoint);
        internal static readonly GridPoint Empty = new GridPoint();

        internal static readonly string[] CsvHeaderValuesArray =
        {
            nameof(GpCost),
            nameof(GpGamma),
            nameof(GpEpsilon),
            nameof(GpCoef0),
            nameof(GpDegree),
            nameof(GpCvRate)
        };

        internal static readonly string CsvHeaderString = string.Join(",", CsvHeaderValuesArray);
        internal double? GpCoef0;

        internal double? GpCost;
        internal double? GpCvRate;
        internal double? GpDegree;
        internal double? GpEpsilon;
        internal double? GpGamma;

        internal GridPoint()
        {
        }

        public GridPoint(double? cost, double? gamma, double? epsilon, double? coef0, double? degree, double? cvRate)
        {
            GpCost = cost;
            GpGamma = gamma;
            GpEpsilon = epsilon;
            GpCoef0 = coef0;
            GpDegree = degree;
            GpCvRate = cvRate;
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

        internal GridPoint(GridPoint gridPoint)
        {
            if (gridPoint == null) return;

            GpCost = gridPoint.GpCost;
            GpGamma = gridPoint.GpGamma;
            GpEpsilon = gridPoint.GpEpsilon;
            GpCoef0 = gridPoint.GpCoef0;
            GpDegree = gridPoint.GpDegree;
            GpCvRate = gridPoint.GpCvRate;
        }

        internal GridPoint(GridPoint[] gridPoints)
        {
            if (gridPoints == null || gridPoints.Length == 0) return;

            var cost2 = gridPoints.Where(a => a?.GpCost != null).Select(a => a.GpCost).ToArray();
            GpCost = cost2.Length > 0
                ? cost2.Average()
                : null;

            var gamma2 = gridPoints.Where(a => a?.GpGamma != null).Select(a => a.GpGamma).ToArray();
            GpGamma = gamma2.Length > 0
                ? gamma2.Average()
                : null;

            var epsilon2 = gridPoints.Where(a => a?.GpEpsilon != null).Select(a => a.GpEpsilon).ToArray();
            GpEpsilon = epsilon2.Length > 0
                ? epsilon2.Average()
                : null;

            var coef02 = gridPoints.Where(a => a?.GpCoef0 != null).Select(a => a.GpCoef0).ToArray();
            GpCoef0 = coef02.Length > 0
                ? coef02.Average()
                : null;

            var degree2 = gridPoints.Where(a => a?.GpDegree != null).Select(a => a.GpDegree).ToArray();
            GpDegree = degree2.Length > 0
                ? degree2.Average()
                : null;

            var cvRate2 = gridPoints.Where(a => a?.GpCvRate != null).Select(a => a.GpCvRate).ToArray();
            GpCvRate = cvRate2.Length > 0
                ? cvRate2.Average()
                : null;
        }

        internal string[] CsvValuesArray()
        {
            return new[]
            {
                GpCost?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                GpGamma?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                GpEpsilon?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                GpCoef0?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                GpDegree?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? "",
                GpCvRate?.ToString("G17", NumberFormatInfo.InvariantInfo) ?? ""
            };
        }

        internal string CsvValuesString()
        {
            return string.Join(",", CsvValuesArray());
        }
    }
}