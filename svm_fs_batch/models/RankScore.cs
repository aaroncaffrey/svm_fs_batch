namespace SvmFsBatch
{
    internal class RankScore
    {
        internal static readonly RankScore Empty = new RankScore();

        internal static readonly string[] CsvHeaderValuesArray =
        {
            nameof(RsIterationIndex),
            nameof(RsGroupArrayIndex),

            nameof(RsFsScoreChangeBest),
            nameof(RsFsScoreChangeLast),
            nameof(RsFsScoreChangeGroup),

            nameof(RsFsScore),
            nameof(RsFsScorePercentile),
            nameof(RsFsRankIndex),
            nameof(RsFsMaxRankIndex),
            nameof(RsFsRankIndexPercentile)
        };

        internal int RsFsMaxRankIndex;

        internal int RsFsRankIndex;
        internal double RsFsRankIndexPercentile;

        internal double RsFsScore;

        internal double RsFsScoreChangeBest;
        internal double RsFsScoreChangeGroup;
        internal double RsFsScoreChangeLast;
        internal double RsFsScorePercentile;

        internal int RsGroupArrayIndex;
        internal int RsIterationIndex;

        internal string[] CsvValuesArray()
        {
            return new[]
            {
                $@"{RsIterationIndex}",
                $@"{RsGroupArrayIndex}",

                $@"{RsFsScoreChangeBest:G17}",
                $@"{RsFsScoreChangeLast:G17}",
                $@"{RsFsScoreChangeGroup:G17}",

                $@"{RsFsScore:G17}",
                $@"{RsFsScorePercentile:G17}",
                $@"{RsFsRankIndex}",
                $@"{RsFsMaxRankIndex}",
                $@"{RsFsRankIndexPercentile:G17}"
            };
        }
    }
}