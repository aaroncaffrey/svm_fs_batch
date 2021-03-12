using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    public partial class RpcProxyMethods
    {
        // OuterCrossValidationSingleAsync Proxy:

        public class ProxyOuterCrossValidationSingleAsync
        {

            public const string ModuleName = nameof(ProxyOuterCrossValidationSingleAsync);
            public const string ProxyMethod = nameof(CrossValidate.OuterCrossValidationSingleRpcAsync);

            public ProxyOuterCrossValidationSingleAsync()
            {

            }

            public class Params
            {
                public IndexData unrolledIndexData;
                public OuterCvInput outerCvInput;
                public bool makeOuterCvConfusionMatrices = false;
                public bool overwriteCache = false;
                public bool saveGroupCache = false;

                public (IndexData unrolledIndexData, OuterCvInput outerCvInput, bool makeOuterCvConfusionMatrices, bool overwriteCache, bool saveGroupCache) ToTuple()
                {
                    return (unrolledIndexData, outerCvInput, makeOuterCvConfusionMatrices, overwriteCache, saveGroupCache);
                }

                public Params()
                {

                }

                public Params(IndexData unrolledIndexData, OuterCvInput outerCvInput, bool makeOuterCvConfusionMatrices = false, bool overwriteCache = false, bool saveGroupCache = false)
                {
                    this.unrolledIndexData = unrolledIndexData;
                    this.outerCvInput = outerCvInput;
                    this.makeOuterCvConfusionMatrices = makeOuterCvConfusionMatrices;
                    this.overwriteCache = overwriteCache;
                    this.saveGroupCache = saveGroupCache;
                }

                public string ToJson()
                {
                    var jsonSerializerOptions = new JsonSerializerOptions { IncludeFields = true };
                    var jsonSerialized = JsonSerializer.Serialize<Params>(this, jsonSerializerOptions);
                    return jsonSerialized;
                }

                public static string ToJson(IndexData unrolledIndexData, OuterCvInput outerCvInput, bool makeOuterCvConfusionMatrices = false, bool overwriteCache = false, bool saveGroupCache = false)
                {
                    return new Params(unrolledIndexData, outerCvInput, makeOuterCvConfusionMatrices, overwriteCache, saveGroupCache).ToJson();
                }

                public static Params FromJson(string jsonSerialized)
                {
                    var jsonSerializerOptions = new JsonSerializerOptions { IncludeFields = true };
                    var jsonDeserialized = JsonSerializer.Deserialize<Params>(jsonSerialized, jsonSerializerOptions);
                    return jsonDeserialized;
                }
            }

            public class Result
            {
                public TimeSpan? gridDur;
                public TimeSpan? trainDur;
                public TimeSpan? predictDur;
                public GridPoint GridPoint;
                public string[] PredictText;
                public ConfusionMatrix[] OcvCm;

                public (TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText, ConfusionMatrix[] OcvCm) ToTuple()
                {
                    return (gridDur, trainDur, predictDur, GridPoint, PredictText, OcvCm);
                }

                public Result()
                {

                }

                public Result(TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText, ConfusionMatrix[] OcvCm)
                {
                    this.gridDur = gridDur;
                    this.trainDur = trainDur;
                    this.predictDur = predictDur;
                    this.GridPoint = GridPoint;
                    this.PredictText = PredictText;
                    this.OcvCm = OcvCm;
                }

                public string ToJson()
                {
                    var jsonSerializerOptions = new JsonSerializerOptions { IncludeFields = true };
                    var jsonSerialized = JsonSerializer.Serialize<Result>(this, jsonSerializerOptions);
                    return jsonSerialized;
                }

                public static string ToJson((TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText, ConfusionMatrix[] OcvCm) result)
                {
                    return new Result(result.gridDur, result.trainDur, result.predictDur, result.GridPoint, result.PredictText, result.OcvCm).ToJson();
                }

                public static string ToJson(TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText, ConfusionMatrix[] OcvCm)
                {
                    return new Result(gridDur, trainDur, predictDur, GridPoint, PredictText, OcvCm).ToJson();
                }

                public static Result FromJson(string jsonSerialized)
                {
                    var jsonSerializerOptions = new JsonSerializerOptions { IncludeFields = true };
                    var jsonDeserialized = JsonSerializer.Deserialize<Result>(jsonSerialized, jsonSerializerOptions);
                    return jsonDeserialized;
                }

                
            }        
     
            public static async Task<string> RpcReceiveAsync(string jsonTextParameters, CancellationToken ct = default)
            {
                Logging.LogCall(ModuleName);
                if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }

                // receive JSON parameters... decode... run OuterCrossValidationSingleAsync()... return result as json...
                // 1. convert json to Params
                var p = Params.FromJson(jsonTextParameters);

                // 2. run requested RPC call
                var result = await CrossValidate.OuterCrossValidationSingleRpcAsync( unrolledIndexData: p.unrolledIndexData, outerCvInput: p.outerCvInput, makeOuterCvConfusionMatrices: p.makeOuterCvConfusionMatrices, overwriteCache: p.overwriteCache, saveGroupCache: p.saveGroupCache, ct: ct).ConfigureAwait(false);

                // 3. encode result as json
                var jsonResult = Result.ToJson(result);

                // 4. return result
                Logging.LogExit(ModuleName);
                return jsonResult;
            }

            public static async Task<(TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText, ConfusionMatrix[] OcvCm)> RpcSendAsync(ConnectionPool CP, IndexData unrolledIndexData, OuterCvInput outerCvInput, bool makeOuterCvConfusionMatrices = false, bool overwriteCache = false, bool saveGroupCache = false, CancellationToken ct = default)
            {
                // run OuterCrossValidationSingleAsync on a remote RPC server
                Logging.LogCall(ModuleName);
                if (CP == null || CP.IsDisposed) throw new ArgumentOutOfRangeException(nameof(CP));
                if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }
                if (unrolledIndexData == null) throw new ArgumentOutOfRangeException(nameof(unrolledIndexData));
                if (outerCvInput == null) throw new ArgumentOutOfRangeException(nameof(outerCvInput));

                // 1. encode method parameters as json
                var jsonParams = new Params(unrolledIndexData: unrolledIndexData, outerCvInput: outerCvInput, makeOuterCvConfusionMatrices: makeOuterCvConfusionMatrices, overwriteCache: overwriteCache, saveGroupCache: saveGroupCache).ToJson();

                // 2. send rpc method call with json parameters over tcp
                // 2a. get a RPC server connection
                // 2b. send RPC request over RPC server connection


                while (!ct.IsCancellationRequested)
                {
                    var cpm = CP.GetNextClient(ModuleName);
                    if (cpm == default) { try { await Task.Delay(TimeSpan.FromSeconds(5), ct).ConfigureAwait(false); } catch (Exception e) { Logging.LogException(e, "", ModuleName); } continue; }

                    var writeRpcCallOk = await cpm.WriteFrameAsync(new[] { ((ulong)TcpClientExtra.RpcFrameTypes.RpcMethodCall, ProxyMethod), ((ulong)TcpClientExtra.RpcFrameTypes.RpcMethodParameters, jsonParams) }, ModuleName).ConfigureAwait(false); ;
                    if (!writeRpcCallOk) { cpm?.Close(); continue; }

                    // 2c. read to see if request accepted.
                    var frameRpcCallAccepted = await cpm.ReadFrameTimeoutAsync(60,ModuleName).ConfigureAwait(false); 
                    if (!frameRpcCallAccepted.readOk || frameRpcCallAccepted.frameType != (ulong)TcpClientExtra.RpcFrameTypes.RpcMethodCallAccept) { cpm?.Close(); continue; }

                    // 2d. wait for read result
                    var frameRpcCallResult = await cpm.ReadFrameTimeoutAsync(60*60,ModuleName).ConfigureAwait(false); 
                    if (!frameRpcCallResult.readOk || frameRpcCallResult.frameType != (ulong)TcpClientExtra.RpcFrameTypes.RpcMethodReturn) { cpm?.Close(); continue; }
                    var result = Result.FromJson(frameRpcCallResult.textIn).ToTuple();

                    Logging.LogExit(ModuleName);
                    return result;
                }

                Logging.LogExit(ModuleName);
                return default;
            }
        }


    }
}
