using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal class Prediction
    {
        public const string ModuleName = nameof(Prediction);
        internal static readonly Prediction Empty = new Prediction();

        //internal string[] test_row_vector; // test_row_vector is not saved/loaded

        internal static readonly string[] CsvHeaderValuesArray =
        {
            nameof(PredictionIndex),
            nameof(ClassSampleId),
            nameof(RealClassId),
            nameof(PredictedClassId),
            nameof(ProbabilityEstimates),
            nameof(Comment)
        };

        internal static readonly string CsvHeaderString = string.Join(",", CsvHeaderValuesArray);
        internal int ClassSampleId; /* composite key with real_ClassId for unique id */
        internal (string CommentHeader, string CommentValue)[] Comment;
        internal int PredictedClassId;

        internal int PredictionIndex;
        internal (int ClassId, double ProbabilityEstimate)[] ProbabilityEstimates;
        internal int RealClassId;

        public Prediction()
        {
            Logging.LogCall(ModuleName);

            Logging.LogExit(ModuleName);
        }

        public Prediction(Prediction prediction)
        {
            Logging.LogCall(ModuleName);

            PredictionIndex = prediction.PredictionIndex;
            ClassSampleId = prediction.ClassSampleId;
            RealClassId = prediction.RealClassId;
            PredictedClassId = prediction.PredictedClassId;
            ProbabilityEstimates = prediction.ProbabilityEstimates;
            Comment = prediction.Comment;
            // this.test_row_vector = prediction.test_row_vector;

            Logging.LogExit(ModuleName);
        }

        public Prediction(string[] values)
        {
            Logging.LogCall(ModuleName);

            var k = 0;
            PredictionIndex = int.Parse(values[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            ClassSampleId = int.Parse(values[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            RealClassId = int.Parse(values[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);
            PredictedClassId = int.Parse(values[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo);

            if (values.Length > k)
                ProbabilityEstimates = values[k++].Split('/', StringSplitOptions.RemoveEmptyEntries).Select(a =>
                {
                    var b = a.Split(':');
                    return (int.Parse(b[0], NumberStyles.Integer, NumberFormatInfo.InvariantInfo), double.Parse(b[1], NumberStyles.Float, NumberFormatInfo.InvariantInfo));
                }).ToArray();

            if (values.Length > k)
                Comment = values[k++].Split('/', StringSplitOptions.RemoveEmptyEntries).Select(a =>
                {
                    var b = a.Split(':');
                    return (b[0], b[1]);
                }).ToArray();

            Logging.LogExit(ModuleName);
        }

        internal string[] CsvValuesArray()
        {
            Logging.LogCall(ModuleName);

            var ret = new[]
            {
                $"{PredictionIndex}",
                $"{ClassSampleId}",
                $"{RealClassId}",
                $"{PredictedClassId}",
                $"{string.Join("/", ProbabilityEstimates?.Select(a => string.Join(":", $@"{a.ClassId}", $@"{a.ProbabilityEstimate:G17}")).ToArray() ?? Array.Empty<string>())}",
                $"{string.Join("/", Comment?.Select(a => string.Join(":", $@"{a.CommentHeader?.Replace(":", "_")}", $@"{a.CommentValue?.Replace(":", "_")}")).ToArray() ?? Array.Empty<string>())}"
            };

            Logging.LogExit(ModuleName);
            return ret;
        }

        internal string CsvValuesString()
        {
            Logging.LogCall(ModuleName);

            var ret = string.Join(",", CsvValuesArray());
            
            Logging.LogExit(ModuleName);
            return ret;
        }

        /*public prediction(string str)
        {
            var k = 0;
            var s = str.Split('|');
            prediction_index = s.Length > k ? int.Parse(s[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) : 0;
            class_sample_id = s.Length > k ? int.Parse(s[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) : 0;

            real_ClassId = s.Length > k ? int.Parse(s[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) : 0;
            predicted_ClassId = s.Length > k ? int.Parse(s[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) : 0;

            var total_prob_est = s.Length > k ? int.Parse(s[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) : 0;
            var total_comment = s.Length > k ? int.Parse(s[k++], NumberStyles.Integer, NumberFormatInfo.InvariantInfo) : 0;

            for (var i = 0; i < total_prob_est; i++)
            {
                var s2 = s.Length > k ? s[k++] : null;
                var s2s = s2?.Split(':');//,StringSplitOptions.RemoveEmptyEntries);
                if (s2s != null && s2s.Length == 2 && s2s[0][0] == 'p')
                {
                    var cid = int.TryParse(s2s[0][1..], NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out var cid_out) ? cid_out : (int?)null;
                    var pe = double.TryParse(s2s[1], NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var pe_out) ? pe_out : (double?)null;

                    if (cid != null && pe != null)
                    {
                        if (ProbabilityEstimates == null) ProbabilityEstimates = new List<(int ClassId, double ProbabilityEstimate)>();
                        ProbabilityEstimates.Add((cid.Value, pe.Value));
                    }
                }
            }


            for (var i = 0; i < total_comment; i++)
            {
                var s2 = s.Length > k ? s[k++] : null;
                var s2s = s2?.Split(':');//,StringSplitOptions.RemoveEmptyEntries);
                if (s2s != null && s2s.Length == 2 && s2s[0][0] == 'c')
                {
                    var ch = s2s[0][1..];
                    var cv = s2s[1];

                    if (ch.Length > 0 || cv.Length > 0)
                    {
                        if (comment == null) comment = new List<(string CommentHeader, string CommentValue)>();
                        comment.Add((ch, cv));
                    }
                }
            }
        }*/

        /*internal string str()
         {
             // 0;-1;1;2;p1:0.6;p-1:0.4;cval=xyz|0;-1;1;2;p1:0.6;p-1:0.4;cval=abc
 
             Logging.LogExit(ModuleName); return ct.IsCancellationRequested ? default :string.Join("|", new string[]
             {
                     $@"{prediction_index}",
                     $@"{class_sample_id}",
                     $@"{real_ClassId}",
                     $@"{predicted_ClassId}",
                     $@"{(ProbabilityEstimates?.Count ?? 0)}",
                     $@"{(comment?.Count ?? 0)}",
                     $@"{(ProbabilityEstimates != null &&ProbabilityEstimates.Count>0? string.Join("|", ProbabilityEstimates.Select(a => $@"p{a.ClassId:+#;-#;+0}:{a.ProbabilityEstimate:G17}").ToList()) : $@"")}",
                     $@"{(comment != null && comment.Count>0? string.Join("|", comment.Where(a=>!string.IsNullOrWhiteSpace(a.CommentHeader) || !string.IsNullOrWhiteSpace(a.CommentValue)).Select(a => $@"c{a.CommentHeader}:{a.CommentValue}").ToList()) : $@"")}"
             }.Where(a => !string.IsNullOrWhiteSpace(a)).ToArray());
         }*/


        public static async Task SaveAsync(CancellationToken ct, string predictionListFilename, (IndexData id, ConfusionMatrix cm, RankScore rs)[] cmList)
        {
            Logging.LogCall(ModuleName);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

            const string methodName = nameof(SaveAsync);
            
            var predList = cmList.SelectMany(a => a.cm.Predictions).ToList();

            var totalLines = cmList.Sum(a => a.cm.Predictions.Length) + 1;

            var lines = new string[totalLines];

            var probClasses = predList.Where(a => a != null && a.ProbabilityEstimates != null && a.ProbabilityEstimates.Length > 0).SelectMany(a => a.ProbabilityEstimates.Select(b => b.ClassId).ToList()).Distinct().OrderBy(a => a).ToArray();
            var probComments = predList.Where(a => a != null && a.Comment != null && a.Comment.Length > 0).SelectMany(a => a.Comment.Select(b => b.CommentHeader).ToList()).Distinct().OrderBy(a => a).ToArray();


            var headerCsvValues = new List<string>
            {
                "perf_" + nameof(IndexData.IdIterationIndex),
                "perf_" + nameof(IndexData.IdGroupArrayIndex),
                "perf_" + nameof(ConfusionMatrix.XClassId),
                "perf_" + nameof(ConfusionMatrix.XClassName),

                nameof(PredictionIndex),
                nameof(ClassSampleId),
                nameof(RealClassId),
                nameof(PredictedClassId)
            };
            headerCsvValues.AddRange(probClasses.Select(a => $@"prob_{a:+#;-#;+0}").ToArray());
            headerCsvValues.AddRange(probComments);


            lines[0] = string.Join(@",", headerCsvValues);

            Parallel.For(0,
                cmList.Length,
                cmListIndex =>
                {
                    Parallel.For(0,
                        cmList[cmListIndex].cm.Predictions.Length,
                        cmPredIndex =>
                        {
                            var k = 0;

                            var values = new string[headerCsvValues.Count];

                            values[k++] = $@"{cmList[cmListIndex].id.IdIterationIndex}";
                            values[k++] = $@"{cmList[cmListIndex].id.IdGroupArrayIndex}";
                            values[k++] = $@"{cmList[cmListIndex].cm.XClassId}";
                            values[k++] = $@"{cmList[cmListIndex].cm.XClassName}";

                            values[k++] = $@"{cmList[cmListIndex].cm.Predictions[cmPredIndex].PredictionIndex}";
                            values[k++] = $@"{cmList[cmListIndex].cm.Predictions[cmPredIndex].ClassSampleId}";
                            values[k++] = $@"{cmList[cmListIndex].cm.Predictions[cmPredIndex].RealClassId:+#;-#;+0}";
                            values[k++] = $@"{cmList[cmListIndex].cm.Predictions[cmPredIndex].PredictedClassId:+#;-#;+0}";


                            if (cmList[cmListIndex].cm.Predictions[cmPredIndex].ProbabilityEstimates != null && cmList[cmListIndex].cm.Predictions[cmPredIndex].ProbabilityEstimates.Length > 0)
                            {
                                for (var probabilityEstimatesIndex = 0; probabilityEstimatesIndex < cmList[cmListIndex].cm.Predictions[cmPredIndex].ProbabilityEstimates.Length; probabilityEstimatesIndex++)
                                {
                                    var valuesIndex = headerCsvValues.IndexOf($@"prob_{cmList[cmListIndex].cm.Predictions[cmPredIndex].ProbabilityEstimates[probabilityEstimatesIndex].ClassId:+#;-#;+0}", k);
                                    values[valuesIndex] = $"{cmList[cmListIndex].cm.Predictions[cmPredIndex].ProbabilityEstimates[probabilityEstimatesIndex].ProbabilityEstimate:G17}";
                                }

                                k += cmList[cmListIndex].cm.Predictions[cmPredIndex].ProbabilityEstimates.Length + 1;
                            }

                            if (cmList[cmListIndex].cm.Predictions[cmPredIndex].Comment != null && cmList[cmListIndex].cm.Predictions[cmPredIndex].Comment.Length > 0)
                                for (var commentIndex = 0; commentIndex < cmList[cmListIndex].cm.Predictions[cmPredIndex].Comment.Length; commentIndex++)
                                {
                                    var valuesIndex = headerCsvValues.IndexOf(cmList[cmListIndex].cm.Predictions[cmPredIndex].Comment[commentIndex].CommentHeader, k);
                                    values[valuesIndex] = cmList[cmListIndex].cm.Predictions[cmPredIndex].Comment[commentIndex].CommentValue;
                                }

                            for (var headerCsvValuesIndex = 0; headerCsvValuesIndex < headerCsvValues.Count; headerCsvValuesIndex++)
                                if (string.IsNullOrEmpty(values[headerCsvValuesIndex]) && headerCsvValues[headerCsvValuesIndex] == @"_")
                                    values[headerCsvValuesIndex] = @"_";

                            lines[cmPredIndex + 1] = string.Join(@",", values);
                        });
                });
            await IoProxy.WriteAllLinesAsync(true, ct, predictionListFilename, lines, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false);

            Logging.LogExit(ModuleName);
        }
    }
}