namespace SvmFsLib
{
    public class RankScore
    {
        public const string ModuleName = nameof(RankScore);

        public static readonly RankScore Empty = new RankScore();

        public static readonly string[] CsvHeaderValuesArray =
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

        public int RsFsMaxRankIndex;

        public int RsFsRankIndex;
        public double RsFsRankIndexPercentile;

        public double RsFsScore;

        public double RsFsScoreChangeBest;
        public double RsFsScoreChangeGroup;
        public double RsFsScoreChangeLast;
        public double RsFsScorePercentile;

        public int RsGroupArrayIndex;
        public int RsIterationIndex;

        public RankScore()
        {
            Logging.LogCall(ModuleName);
        }

        public string[] CsvValuesArray()
        {
            Logging.LogCall(ModuleName);

            var ret = new[]
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
            Logging.LogExit(ModuleName);
            return ret;
        }
    }
}