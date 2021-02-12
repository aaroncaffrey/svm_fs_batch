using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal partial class RpcProxyMethods
    {
        // OuterCrossValidationSingleAsync Proxy:

        internal class ProxyOuterCrossValidationSingleAsync
        {

            internal const string ProxyMethod = nameof(CrossValidate.OuterCrossValidationSingleAsync);

            internal class Params
            {
                internal IndexData unrolledIndexData;
                internal OuterCvInput outerCvInput;
                internal bool makeOuterCvConfusionMatrices = false;
                internal bool overwriteCache = false;
                internal bool saveGroupCache = false;

                internal (IndexData unrolledIndexData, OuterCvInput outerCvInput, bool makeOuterCvConfusionMatrices, bool overwriteCache, bool saveGroupCache) ToTuple()
                {
                    return (unrolledIndexData, outerCvInput, makeOuterCvConfusionMatrices, overwriteCache, saveGroupCache);
                }

                internal Params()
                {

                }

                internal Params(IndexData unrolledIndexData, OuterCvInput outerCvInput, bool makeOuterCvConfusionMatrices = false, bool overwriteCache = false, bool saveGroupCache = false)
                {
                    this.unrolledIndexData = unrolledIndexData;
                    this.outerCvInput = outerCvInput;
                    this.makeOuterCvConfusionMatrices = makeOuterCvConfusionMatrices;
                    this.overwriteCache = overwriteCache;
                    this.saveGroupCache = saveGroupCache;
                }

                internal string ToJson()
                {
                    var jsonSerializerOptions = new JsonSerializerOptions { IncludeFields = true };
                    var jsonSerialized = JsonSerializer.Serialize<Params>(this, jsonSerializerOptions);
                    return jsonSerialized;
                }

                internal static string ToJson(IndexData unrolledIndexData, OuterCvInput outerCvInput, bool makeOuterCvConfusionMatrices = false, bool overwriteCache = false, bool saveGroupCache = false)
                {
                    return new Params(unrolledIndexData, outerCvInput, makeOuterCvConfusionMatrices, overwriteCache, saveGroupCache).ToJson();
                }

                internal static Params FromJson(string jsonSerialized)
                {
                    var jsonSerializerOptions = new JsonSerializerOptions { IncludeFields = true };
                    var jsonDeserialized = JsonSerializer.Deserialize<Params>(jsonSerialized, jsonSerializerOptions);
                    return jsonDeserialized;
                }
            }

            internal class Result
            {
                TimeSpan? gridDur;
                TimeSpan? trainDur;
                TimeSpan? predictDur;
                GridPoint GridPoint;
                string[] PredictText;
                ConfusionMatrix[] OcvCm;

                internal (TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText, ConfusionMatrix[] OcvCm) ToTuple()
                {
                    return (gridDur, trainDur, predictDur, GridPoint, PredictText, OcvCm);
                }

                internal Result()
                {

                }

                internal Result(TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText, ConfusionMatrix[] OcvCm)
                {
                    this.gridDur = gridDur;
                    this.trainDur = trainDur;
                    this.predictDur = predictDur;
                    this.GridPoint = GridPoint;
                    this.PredictText = PredictText;
                    this.OcvCm = OcvCm;
                }

                internal string ToJson()
                {
                    var jsonSerializerOptions = new JsonSerializerOptions { IncludeFields = true };
                    var jsonSerialized = JsonSerializer.Serialize<Result>(this, jsonSerializerOptions);
                    return jsonSerialized;
                }

                internal static string ToJson((TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText, ConfusionMatrix[] OcvCm) result)
                {
                    return new Result(result.gridDur, result.trainDur, result.predictDur, result.GridPoint, result.PredictText, result.OcvCm).ToJson();
                }

                internal static string ToJson(TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText, ConfusionMatrix[] OcvCm)
                {
                    return new Result(gridDur, trainDur, predictDur, GridPoint, PredictText, OcvCm).ToJson();
                }

                internal static Result FromJson(string jsonSerialized)
                {
                    var jsonSerializerOptions = new JsonSerializerOptions { IncludeFields = true };
                    var jsonDeserialized = JsonSerializer.Deserialize<Result>(jsonSerialized, jsonSerializerOptions);
                    return jsonDeserialized;
                }

                
            }        
     
            internal static async Task<string> RpcReceiveAsync(string jsonTextParameters, CancellationToken ct = default)
            {
                Logging.LogCall(ModuleName);
                if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }

                // receive JSON parameters... decode... run OuterCrossValidationSingleAsync()... return result as json...
                // 1. convert json to Params
                var p = Params.FromJson(jsonTextParameters);

                // 2. run requested RPC call
                var result = await CrossValidate.OuterCrossValidationSingleAsync(unrolledIndexData: p.unrolledIndexData, outerCvInput: p.outerCvInput, makeOuterCvConfusionMatrices: p.makeOuterCvConfusionMatrices, overwriteCache: p.overwriteCache, saveGroupCache: p.saveGroupCache, ct: ct);

                // 3. encode result as json
                var jsonResult = Result.ToJson(result);

                // 4. return result
                Logging.LogExit(ModuleName);
                return jsonResult;
            }

            internal static async Task<(TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText, ConfusionMatrix[] OcvCm)> RpcSendAsync(IndexData unrolledIndexData, OuterCvInput outerCvInput, bool makeOuterCvConfusionMatrices = false, bool overwriteCache = false, bool saveGroupCache = false, CancellationToken ct = default)
            {
                // run OuterCrossValidationAsync on a remote RPC server
                Logging.LogCall(ModuleName);
                if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }
                if (unrolledIndexData == null) throw new ArgumentOutOfRangeException(nameof(unrolledIndexData));
                if (outerCvInput == null) throw new ArgumentOutOfRangeException(nameof(outerCvInput));

                // 1. encode method parameters as json
                var jsonParams = new Params(unrolledIndexData: unrolledIndexData, outerCvInput: outerCvInput, makeOuterCvConfusionMatrices: makeOuterCvConfusionMatrices, overwriteCache: overwriteCache, saveGroupCache: saveGroupCache).ToJson();

                // 2. send rpc method call with json parameters over tcp
                Console.WriteLine(jsonParams);

                // 3. receive resposne in json
                var jsonResult = "";

                // 4. decode response
                var result = Result.FromJson(jsonResult).ToTuple();

                // 5. return result
                Logging.LogExit(ModuleName);
                return result;
            }
        }


    }
}
