using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    public partial class RpcProxyMethods
    {
        public class ProxyOuterCrossValidationAsync
        {
            public const string ModuleName = nameof(ProxyOuterCrossValidationAsync);
            public const string ProxyMethod = nameof(SvmFsBatch.CrossValidate.OuterCrossValidationRpcAsync);

            public ProxyOuterCrossValidationAsync()
            {

            }

            public class Params
            {
                
                public OuterCvInput[] outerCvInputs;
                public OuterCvInput mergedCvInput;
                
                public IndexData unrolledIndexData;
                public bool makeOuterCvConfusionMatrices;
                public bool overwriteCache = false;
                public bool saveGroupCache = false;
                public bool asParallel = true;

                public ( OuterCvInput[] outerCvInputs, OuterCvInput mergedCvInput, IndexData unrolledIndexData, bool makeOuterCvConfusionMatrices, bool overwriteCache, bool saveGroupCache, bool asParallel) ToTuple()
                {
                    return ( outerCvInputs, mergedCvInput, unrolledIndexData, makeOuterCvConfusionMatrices, overwriteCache, saveGroupCache, asParallel);
                }

                public Params()
                {

                }

                public Params(  OuterCvInput[] outerCvInputs, OuterCvInput mergedCvInput, IndexData unrolledIndexData, bool makeOuterCvConfusionMatrices, bool overwriteCache = false, bool saveGroupCache = false, bool asParallel = true)
                {
                    
                    this.outerCvInputs = outerCvInputs;
                    this.mergedCvInput = mergedCvInput;
                    this.unrolledIndexData = unrolledIndexData;
                    this.makeOuterCvConfusionMatrices = makeOuterCvConfusionMatrices;
                    this.overwriteCache = overwriteCache;
                    this.saveGroupCache = saveGroupCache;
                    this.asParallel = asParallel;
                }

                public string ToJson()
                {
                    var jsonSerializerOptions = new JsonSerializerOptions { IncludeFields = true };
                    var jsonSerialized = JsonSerializer.Serialize<Params>(this, jsonSerializerOptions);
                    return jsonSerialized;
                }

                public static string ToJson( OuterCvInput[] outerCvInputs, OuterCvInput mergedCvInput, IndexData unrolledIndexData, bool makeOuterCvConfusionMatrices, bool overwriteCache = false, bool saveGroupCache = false, bool asParallel = true)
                {
                    return new Params( outerCvInputs, mergedCvInput, unrolledIndexData, makeOuterCvConfusionMatrices, overwriteCache, saveGroupCache, asParallel).ToJson();
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
                public IndexData id;
                public ConfusionMatrix[] OcvCm;
                public ConfusionMatrix[] McvCm;

                public (IndexData id, ConfusionMatrix[] OcvCm, ConfusionMatrix[] McvCm) ToTuple()
                {
                    return (id, OcvCm, McvCm);
                }

                public Result()
                {

                }

                public Result(IndexData id, ConfusionMatrix[] OcvCm, ConfusionMatrix[] McvCm)
                {
                    this.id = id;
                    this.OcvCm = OcvCm;
                    this.McvCm = McvCm;
                }

                public string ToJson()
                {
                    var jsonSerializerOptions = new JsonSerializerOptions { IncludeFields = true };
                    var jsonSerialized = JsonSerializer.Serialize<Result>(this, jsonSerializerOptions);
                    return jsonSerialized;
                }

                public static string ToJson((IndexData id, ConfusionMatrix[] OcvCm, ConfusionMatrix[] McvCm) result)
                {
                    return new Result(result.id, result.OcvCm, result.McvCm).ToJson();
                }

                public static string ToJson(IndexData id, ConfusionMatrix[] OcvCm, ConfusionMatrix[] McvCm)
                {
                    return new Result(id, OcvCm, McvCm).ToJson();
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
                var result = await CrossValidate.OuterCrossValidationRpcAsync(null, rpcPoint: CrossValidate.RpcPoint.None, outerCvInputs: p.outerCvInputs, mergedCvInput: p.mergedCvInput, unrolledIndexData: p.unrolledIndexData, makeOuterCvConfusionMatrices: p.makeOuterCvConfusionMatrices, overwriteCache: p.overwriteCache, saveGroupCache: p.saveGroupCache, asParallel: p.asParallel, ct: ct).ConfigureAwait(false);
                // 3. encode result as json
                var jsonResult = Result.ToJson(result);

                // 4. return result
                Logging.LogExit(ModuleName);
                return jsonResult;
            }


            public static async Task<(IndexData id, ConfusionMatrix[] OcvCm, ConfusionMatrix[] McvCm)> RpcSendAsync(ConnectionPool CP, OuterCvInput[] outerCvInputs, OuterCvInput mergedCvInput, IndexData unrolledIndexData, bool makeOuterCvConfusionMatrices, bool overwriteCache = false, bool saveGroupCache = false, bool asParallel = true, CancellationToken ct = default)
            {
                // run OuterCrossValidationAsync on a remote RPC server
                Logging.LogCall(ModuleName);
                if (CP == null || CP.IsDisposed) throw new ArgumentOutOfRangeException(nameof(CP));
                if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }
                if (unrolledIndexData == null) throw new ArgumentOutOfRangeException(nameof(unrolledIndexData));
                if (outerCvInputs == null || outerCvInputs.Length == 0) throw new ArgumentOutOfRangeException(nameof(outerCvInputs));

                // 1. encode method parameters as json
                var jsonParams = new Params(outerCvInputs: outerCvInputs, mergedCvInput: mergedCvInput, unrolledIndexData: unrolledIndexData, makeOuterCvConfusionMatrices: makeOuterCvConfusionMatrices, overwriteCache: overwriteCache, saveGroupCache: saveGroupCache, asParallel: asParallel).ToJson();

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
                    var frameRpcCallAccepted = await cpm.ReadFrameTimeoutAsync(60, ModuleName).ConfigureAwait(false);
                    if (!frameRpcCallAccepted.readOk || frameRpcCallAccepted.frameType != (ulong)TcpClientExtra.RpcFrameTypes.RpcMethodCallAccept) { cpm?.Close(); continue; }

                    // 2d. wait for read result
                    var frameRpcCallResult = await cpm.ReadFrameTimeoutAsync(60 * 60, ModuleName).ConfigureAwait(false);
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
