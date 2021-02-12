using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal partial class RpcProxyMethods
    {
        internal class ProxyOuterCrossValidationAsync
        {
            internal const string ProxyMethod = nameof(SvmFsBatch.CrossValidate.OuterCrossValidationAsync);

            internal class Params
            {
                
                internal OuterCvInput[] outerCvInputs;
                internal IndexData unrolledIndexData;
                internal bool makeOuterCvConfusionMatrices;
                internal bool overwriteCache = false;
                internal bool saveGroupCache = false;
                internal bool asParallel = true;

                internal ( OuterCvInput[] outerCvInputs, IndexData unrolledIndexData, bool makeOuterCvConfusionMatrices, bool overwriteCache, bool saveGroupCache, bool asParallel) ToTuple()
                {
                    return ( outerCvInputs, unrolledIndexData, makeOuterCvConfusionMatrices, overwriteCache, saveGroupCache, asParallel);
                }

                internal Params()
                {

                }

                internal Params(  OuterCvInput[] outerCvInputs, IndexData unrolledIndexData, bool makeOuterCvConfusionMatrices, bool overwriteCache = false, bool saveGroupCache = false, bool asParallel = true)
                {
                    
                    this.outerCvInputs = outerCvInputs;
                    this.unrolledIndexData = unrolledIndexData;
                    this.makeOuterCvConfusionMatrices = makeOuterCvConfusionMatrices;
                    this.overwriteCache = overwriteCache;
                    this.saveGroupCache = saveGroupCache;
                    this.asParallel = asParallel;
                }

                internal string ToJson()
                {
                    var jsonSerializerOptions = new JsonSerializerOptions { IncludeFields = true };
                    var jsonSerialized = JsonSerializer.Serialize<Params>(this, jsonSerializerOptions);
                    return jsonSerialized;
                }

                internal static string ToJson( OuterCvInput[] outerCvInputs, IndexData unrolledIndexData, bool makeOuterCvConfusionMatrices, bool overwriteCache = false, bool saveGroupCache = false, bool asParallel = true)
                {
                    return new Params( outerCvInputs, unrolledIndexData, makeOuterCvConfusionMatrices, overwriteCache, saveGroupCache, asParallel).ToJson();
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
                internal IndexData id;
                internal ConfusionMatrix[] OcvCm;
                internal ConfusionMatrix[] McvCm;

                internal (IndexData id, ConfusionMatrix[] OcvCm, ConfusionMatrix[] McvCm) ToTuple()
                {
                    return (id, OcvCm, McvCm);
                }

                internal Result()
                {

                }

                internal Result(IndexData id, ConfusionMatrix[] OcvCm, ConfusionMatrix[] McvCm)
                {
                    this.id = id;
                    this.OcvCm = OcvCm;
                    this.McvCm = McvCm;
                }

                internal string ToJson()
                {
                    var jsonSerializerOptions = new JsonSerializerOptions { IncludeFields = true };
                    var jsonSerialized = JsonSerializer.Serialize<Result>(this, jsonSerializerOptions);
                    return jsonSerialized;
                }

                internal static string ToJson((IndexData id, ConfusionMatrix[] OcvCm, ConfusionMatrix[] McvCm) result)
                {
                    return new Result(result.id, result.OcvCm, result.McvCm).ToJson();
                }

                internal static string ToJson(IndexData id, ConfusionMatrix[] OcvCm, ConfusionMatrix[] McvCm)
                {
                    return new Result(id, OcvCm, McvCm).ToJson();
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
                var result = await CrossValidate.OuterCrossValidationAsync(isMethodCallRpc: true, rpcPoint: CrossValidate.RpcPoint.None, outerCvInputs: p.outerCvInputs, unrolledIndexData: p.unrolledIndexData, makeOuterCvConfusionMatrices: p.makeOuterCvConfusionMatrices, overwriteCache: p.overwriteCache, saveGroupCache: p.saveGroupCache, asParallel: p.asParallel, ct: ct);

                // 3. encode result as json
                var jsonResult = Result.ToJson(result);

                // 4. return result
                Logging.LogExit(ModuleName);
                return jsonResult;
            }


            internal static async Task<(IndexData id, ConfusionMatrix[] OcvCm, ConfusionMatrix[] McvCm)> RpcSendAsync( OuterCvInput[] outerCvInputs, IndexData unrolledIndexData, bool makeOuterCvConfusionMatrices, bool overwriteCache = false, bool saveGroupCache = false, bool asParallel = true, CancellationToken ct = default)
            {
                // run OuterCrossValidationAsync on a remote RPC server
                Logging.LogCall(ModuleName);
                if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return default; }
                if (unrolledIndexData == null) throw new ArgumentOutOfRangeException(nameof(unrolledIndexData));
                if (outerCvInputs == null || outerCvInputs.Length == 0) throw new ArgumentOutOfRangeException(nameof(outerCvInputs));

                // 1. encode method parameters as json
                var jsonParams = new Params(outerCvInputs: outerCvInputs, unrolledIndexData: unrolledIndexData, makeOuterCvConfusionMatrices: makeOuterCvConfusionMatrices, overwriteCache: overwriteCache, saveGroupCache: saveGroupCache, asParallel: asParallel).ToJson();

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
