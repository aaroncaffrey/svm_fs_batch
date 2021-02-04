namespace SvmFsBatch
{
    internal class RankScore
    {
        internal static readonly RankScore Empty = new RankScore();

        internal static readonly string[] CsvHeaderValuesArray =
        {
            /*"rs_" +*/ nameof(IterationIndex),
            /*"rs_" +*/ nameof(GroupArrayIndex),

            /*"rs_" +*/ nameof(FsScoreChangeBest),
            /*"rs_" +*/ nameof(FsScoreChangeLast),
            /*"rs_" +*/ nameof(FsScoreChangeGroup),

            /*"rs_" +*/ nameof(FsScore),
            /*"rs_" +*/ nameof(FsScorePercentile),
            /*"rs_" +*/ nameof(FsRankIndex),
            /*"rs_" +*/ nameof(FsMaxRankIndex),
            /*"rs_" +*/ nameof(FsRankIndexPercentile)
        };

        internal int FsMaxRankIndex;

        internal int FsRankIndex;
        internal double FsRankIndexPercentile;

        internal double FsScore;

        internal double FsScoreChangeBest;
        internal double FsScoreChangeGroup;
        internal double FsScoreChangeLast;
        internal double FsScorePercentile;
        internal int GroupArrayIndex;

        internal int IterationIndex;

        internal string[] CsvValuesArray()
        {
            return new[]
            {
                $@"{IterationIndex}",
                $@"{GroupArrayIndex}",

                $@"{FsScoreChangeBest:G17}",
                $@"{FsScoreChangeLast:G17}",
                $@"{FsScoreChangeGroup:G17}",

                $@"{FsScore:G17}",
                $@"{FsScorePercentile:G17}",
                $@"{FsRankIndex}",
                $@"{FsMaxRankIndex}",
                $@"{FsRankIndexPercentile:G17}"
            };
        }
    }
}