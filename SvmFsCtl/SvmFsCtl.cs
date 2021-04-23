using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SvmFsLib;

namespace SvmFsCtl
{
    public static partial class SvmFsCtl
    {
        public const string ModuleName = nameof(SvmFsCtl);


        public static ProgramArgs ProgramArgs;
        //public static Mpstat Mpstat;
        

        public static async Task Main(string[] args)
        {
            //var xyz3 = DataSet.ConvertCsvTextFileToBinary($@"E:\dataset7\merged_files\test\h_([1i.aaindex])_(-1)_(standard_coil).csv", $@"E:\dataset7\merged_files\test\h_([1i.aaindex])_(-1)_(standard_coil).csv.bin");

            //var ts1 = Stopwatch.StartNew();
            //for (var i = 0; i < 1000; i++)
            //{
            //    var xyz4 = DataSet.ReadCsv($@"E:\dataset7\merged_files\test\h_([1i.aaindex])_(-1)_(standard_coil).csv");
            //}
            //ts1.Stop();
            //Console.WriteLine(ts1.Elapsed);

            //var ts2 = Stopwatch.StartNew();
            //for (var i = 0; i < 1000; i++)
            //{
            //    var xyz4 = DataSet.ReadBinaryCsv($@"E:\dataset7\merged_files\test\h_([1i.aaindex])_(-1)_(standard_coil).csv.bin");
            //}
            //ts2.Stop();
            //Console.WriteLine(ts2.Elapsed);
            //return;

            //var xyz1=DataSet.ConvertCsvValueFileToBinary($@"E:\caddy\input\New folder\f_2i_(+1)_(dimorphic_coil).csv", $@"E:\caddy\input\New folder\f_2i_(+1)_(dimorphic_coil).bin");

            //var sw1 = Stopwatch.StartNew();
            //for (var i = 0; i < 1000; i++)
            //{
            //    var xyz2 = DataSet.ReadBinaryValueFile($@"E:\caddy\input\New folder\f_2i_(+1)_(dimorphic_coil).bin", true);
            //    //var xyz2flat = xyz2.SelectMany(a => a).ToArray();
            //}

            //sw1.Stop();

            //var sw2 = Stopwatch.StartNew();
            //for (var i = 0; i < 1000; i++)
            //{
            //    var xyz3 = DataSet.ReadBinaryValueFile($@"E:\caddy\input\New folder\f_2i_(+1)_(dimorphic_coil).bin", false);
            //    //var xyz3flat = xyz3.SelectMany(a => a).ToArray();
            //}

            //sw2.Stop();

            //Console.WriteLine(sw1.Elapsed);
            //Console.WriteLine(sw2.Elapsed);

            //return;

            //for (var i = 0; i < xyz1.Count; i++)
            //{
            //    if (!xyz1[i].SequenceEqual(xyz2[i])) throw new Exception("!!!");
            //}

            //return;

            //var test1 = (new RpcProxyMethods.ProxyOuterCrossValidationAsync.Params() { saveGroupCache = true }).ToJson();
            //var test2 = new RpcProxyMethods.ProxyOuterCrossValidationAsync.Result() { }.ToJson();
            //var test3 = (new RpcProxyMethods.ProxyOuterCrossValidationSingleAsync.Params() { overwriteCache=true}).ToJson();
            //var test4 = new RpcProxyMethods.ProxyOuterCrossValidationSingleAsync.Result().ToJson();

            //var test1a = RpcProxyMethods.ProxyOuterCrossValidationAsync.Params.FromJson(test1);
            //var test2a = RpcProxyMethods.ProxyOuterCrossValidationAsync.Result.FromJson(test2);
            //var test3a = RpcProxyMethods.ProxyOuterCrossValidationSingleAsync.Params.FromJson(test3);
            //var test4a = RpcProxyMethods.ProxyOuterCrossValidationSingleAsync.Result.FromJson(test4);

            //var x1 = await RpcProxyMethods.ProxyOuterCrossValidationAsync.RpcSendAsync(default, new[] { new OuterCvInput() }, new IndexData(), default, default, default, default, default).ConfigureAwait(false);
            //var x2 = await RpcProxyMethods.ProxyOuterCrossValidationSingleAsync.RpcSendAsync(default, default, default, default, default, default, default).ConfigureAwait(false);

            //Console.WriteLine(x1);
            //Console.WriteLine(x2);

            ulong lvl = 0;

            Logging.LogCall(ModuleName, lvl: lvl + 1);





            //var id1 = new IndexData();
            //id1.IdExperimentName = "test";
            //id1.IdColumnArrayIndexes = new int[] { 1, 2, 3 };
            //id1.IdGroupArrayIndexes = new int[] { 7, 8, 9 };
            //id1.IdGroupKey = new DataSetGroupKey("file tag", "alpha", "stat", "dim", "cat", "src", "grp", "mem", "per", 10, 20);
            //var id2 = new IndexData(string.Join(",", id1.CsvValuesArray()));

            //var eq = IndexData.CompareReferenceData2(id1, id2);
            //Logging.LogExit(ModuleName); return;


            //var rnd = new metrics_box();
            //rnd.set_cm(null, null, 20, 11, 126, 30);
            //Console.WriteLine(string.Join("\r\n", rnd.CsvValuesArray().Select((a, i) => $"{metrics_box.CsvHeaderValuesArray[i]} = '{a}'").ToArray()));
            //Console.WriteLine();

            //rnd.apply_imbalance_correction1();
            //Console.WriteLine(string.Join("\r\n", rnd.CsvValuesArray().Select((a, i) => $"{metrics_box.CsvHeaderValuesArray[i]} = '{a}'").ToArray()));
            //Console.WriteLine();

            //rnd.set_random_perf();
            //Console.WriteLine(string.Join("\r\n", rnd.CsvValuesArray().Select((a, i) => $"{metrics_box.CsvHeaderValuesArray[i]} = '{a}'").ToArray()));
            //Console.WriteLine();

            //const string MethodName = nameof(Main);
            //-ExperimentName _20201028084510741 -job_id _ -job_name _ -instance_array_index_start 0 -array_instances 1 -array_start 0 -array_end 6929 -array_step 6930 -inner_folds 1 -outer_cv_folds 5 -outer_cv_folds_to_run 1 -repetitions 1
            //-ExperimentName test_20201025014739579 -job_id _ -job_name _ -array_index _ -array_instances _ -array_start 0 -array_end 6929 -array_step 385
            //var x=ConfusionMatrix.load($@"C:\mmfs1\data\scratch\k1040015\SvmFsBatch\results\test\it_5\x_it-5_gr-5_sv-1_kr-3_sc-2_rn-1_oc-10_ic-10_ix-1-5.cm.csv");
            // debug cmd line parameters: -ExperimentName test2 -array_start 0 -array_end 4 -array_index 0 -array_step 5 -array_instances 1 -array_last_index -1


            ThreadPool.GetMinThreads(out var minWorkerThreads, out var minCompletionPortThreads);
            ThreadPool.GetMaxThreads(out var maxWorkerThreads, out var maxCompletionPortThreads);

            Logging.WriteLine($@"Environment.CommandLine: {Environment.CommandLine}", ModuleName);
            Logging.WriteLine($@"Environment.ProcessorCount: {Environment.ProcessorCount}", ModuleName);
            Logging.WriteLine($@"GetMinThreads: minWorkerThreads = {minWorkerThreads}, minCompletionPortThreads = {minCompletionPortThreads}.", ModuleName);
            Logging.WriteLine($@"GetMaxThreads: maxWorkerThreads = {maxWorkerThreads}, maxCompletionPortThreads = {maxCompletionPortThreads}.", ModuleName);

            var mainCts = new CancellationTokenSource();
            var mainCt = mainCts.Token;

            Init.CloseNotifications(mainCt); //, mainCts);
            Init.CheckX64();
            Init.SetGcMode();
            //Init.SetThreadCounts();


            // - Folds                                     =   
            // - Repetitions                               =   1
            // - OuterCvFolds                              =   5
            // - OuterCvFoldsToRun                         =   1
            // - InnerFolds                                =   0
            // - ExperimentName                            =   test
            // - JobId                                     =   
            // - JobName                                   =   
            // - WholeArrayIndexFirst                      =   0
            // - WholeArrayIndexLast                       =   0
            // - WholeArrayStepSize                        =   1
            // - WholeArrayLength                          =   1
            // - PartitionArrayIndexFirst                  =   0
            // - PartitionArrayIndexLast                   =   0
            // - Setup                                     =   False
            // - SetupTotalVcpus                           =   -1
            // - SetupInstanceVcpus                        =   -1
            // - ScoringClassId                            =   1
            // - ScoringMetrics                            =   PF1S
            // - IsWin                                     =   True
            // - UserHome                                  =   C:\home\k1040015
            // - SvmFsBatchHome                            =   C:\mmfs1\data\scratch\k1040015\SvmFsBatch
            // - ResultsRootFolder                         =   C:\mmfs1\data\scratch\k1040015\SvmFsBatch\
            // - LibsvmPredictRuntime                      =   C:\libsvm\windows\svm-predict.exe
            // - LibsvmTrainRuntime                        =   C:\libsvm\windows\svm-train.exe
            // - ClassNames                                =   -1:standard_coil,1:dimorphic_coil
            // - ClassWeights                              =   
            // - SvmTypes                                  =   CSvc
            // - Kernels                                   =   Rbf
            // - Scales                                    =   Rescale
            // - CalcElevenPointThresholds                 =   0
            // - ClientGuid                                =   c0057c5a4c4b409283a1448c42c07b47
            // - ClientConnectionPoolSize                  =   10
            // - Server                                    =   1
            // - ServerIp                                  =   127.0.0.1
            // - ServerPort                                =   64727
            // - ServerBacklog                             =   1000
            // - ServerGuid                                =   d2d95e16d0864b58ba298b9b84e41be5
            // - DataSetDir                                =   E:\DataSet7\merged_files\
            // - DataSetNames                              =   [1i.aaindex]
            // - NegativeClassId                           =   -1
            // - PositiveClassId                           =   1
            // - NegativeClassName                         =   standard_coil
            // - PositiveClassName                         =   dimorphic_coil
            // - Folds                                     =   
            // - Repetitions                               =   1
            // - OuterCvFolds                              =   5
            // - OuterCvFoldsToRun                         =   1
            // - InnerFolds                                =   0
            // - Setup                                     =   False
            // - ExperimentName                            =   test
            // - JobId                                     =   
            // - JobName                                   =   
            // - PartitionArrayIndexFirst                  =   0
            // - PartitionArrayIndexLast                   =   0
            // - WholeArrayLength                          =   1
            // - WholeArrayStepSize                        =   1
            // - WholeArrayIndexFirst                      =   0
            // - WholeArrayIndexLast                       =   0
            // - SetupTotalVcpus                           =   -1
            // - SetupInstanceVcpus                        =   -1
            // - Option0                                   =   0
            // - Option1                                   =   1
            // - Option2                                   =   1
            // - Option3                                   =   1
            // - Option4                                   =   1

            // new parameters: SvmFsBatch -ExperimentName=Name -DataSetNames=[1i.aaindex] -InnerFolds=5 -OuterCvFolds=5 -Repetitions=1
            // -opt
            // -SvmFsBatchHome=C:\mmfs1\data\scratch\k1040015\SvmFsBatch  -DataSetDir=?????? 

            //var fake_args = $"-ExperimentName=test -whole_array_index_first=0 -whole_array_index_last=9 -whole_array_step_size=2 -whole_array_length=5 -partition_array_index_first=4 -partition_array_index_last=5";
            
            /*
            var fakeArgsList = new List<(string name, string value)>
            {
                (nameof(ProgramArgs.ExperimentName), "test"),
                (nameof(ProgramArgs.WholeArrayIndexFirst), "0"),
                (nameof(ProgramArgs.WholeArrayIndexLast), "0"),
                (nameof(ProgramArgs.WholeArrayStepSize), "1"),
                (nameof(ProgramArgs.WholeArrayLength), "1"),
                (nameof(ProgramArgs.PartitionArrayIndexFirst), "0"),
                (nameof(ProgramArgs.PartitionArrayIndexLast), "0"),

                //(nameof(ProgramArgs.Client), "0"),
                //(nameof(ProgramArgs.Server), "1"),

                (nameof(ProgramArgs.DataSetNames), "[1i.aaindex]"),
                (nameof(ProgramArgs.BaseLineDataSetNames), "[1i.aaindex]"),
                (nameof(ProgramArgs.BaseLineDataSetColumnIndexes), "1;2;3;4"),

                (nameof(ProgramArgs.InnerFolds), "0"),
                (nameof(ProgramArgs.OuterCvFolds), "5"),
                (nameof(ProgramArgs.OuterCvFoldsToRun), "1"),
                (nameof(ProgramArgs.Repetitions), "1")
            };

            var fakeArgs = string.Join(" ", fakeArgsList.Select(a => $"-{a.name}={a.value}").ToArray());
            //Console.WriteLine(); 
            //Console.WriteLine();
            //Console.WriteLine(string.Join("\r\n", fakeArgsList));
            //Console.WriteLine(); 
            //Console.WriteLine();
            args = fakeArgs.Split();
            */


            ProgramArgs = new ProgramArgs(args);


            if (ProgramArgs.Setup)
            {
                await Setup.SetupPbsJobAsync(ProgramArgs, mainCt).ConfigureAwait(false);
                Logging.LogExit(ModuleName, lvl: lvl + 1); return;
            }


            // check experiment name is valid
            if (string.IsNullOrWhiteSpace(ProgramArgs.ExperimentName)) throw new ArgumentOutOfRangeException(nameof(args), $"{nameof(ProgramArgs.ExperimentName)}: must specify experiment name");

            // check whole array indexes are valid
            if (ProgramArgs.WholeArrayIndexFirst <= -1) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.WholeArrayIndexFirst)} = {ProgramArgs.WholeArrayIndexFirst}");
            if (ProgramArgs.WholeArrayIndexLast <= -1) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.WholeArrayIndexLast)} = {ProgramArgs.WholeArrayIndexLast}");
            if (ProgramArgs.WholeArrayStepSize <= 0) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.WholeArrayStepSize)} = {ProgramArgs.WholeArrayStepSize}");
            if (ProgramArgs.WholeArrayLength <= 0) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.WholeArrayLength)} = {ProgramArgs.WholeArrayLength}");

            // check partition array indexes are valid
            if (!Routines.IsInRange(ProgramArgs.WholeArrayIndexFirst, ProgramArgs.WholeArrayIndexLast, ProgramArgs.PartitionArrayIndexFirst)) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.PartitionArrayIndexFirst)} = {ProgramArgs.PartitionArrayIndexFirst}");
            if (!Routines.IsInRange(ProgramArgs.WholeArrayIndexFirst, ProgramArgs.WholeArrayIndexLast, ProgramArgs.PartitionArrayIndexLast)) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.PartitionArrayIndexLast)} = {ProgramArgs.PartitionArrayIndexLast}");


            //var InstanceId = GetInstanceId(program_args.whole_array_index_first, program_args.whole_array_index_last, program_args.whole_array_step_size, program_args.partition_array_index_first, program_args.partition_array_index_last);

            var instanceId = Routines.ForIterations(ProgramArgs.WholeArrayIndexFirst, ProgramArgs.PartitionArrayIndexFirst, ProgramArgs.WholeArrayStepSize) - 1;
            var totalInstance = Routines.ForIterations(ProgramArgs.WholeArrayIndexFirst, ProgramArgs.WholeArrayIndexLast, ProgramArgs.WholeArrayStepSize);

            if (instanceId < 0) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(instanceId)} = {instanceId}");
            if (totalInstance != ProgramArgs.WholeArrayLength) throw new ArgumentOutOfRangeException(nameof(args), $@"{nameof(ProgramArgs.WholeArrayLength)} = {ProgramArgs.WholeArrayLength}, {nameof(totalInstance)} = {totalInstance}");


            Logging.WriteLine($"Array job index: {instanceId} / {totalInstance}. Partition array Indexes: {ProgramArgs.PartitionArrayIndexFirst}..{ProgramArgs.PartitionArrayIndexLast}.  Whole array Indexes: {ProgramArgs.WholeArrayIndexFirst}..{ProgramArgs.WholeArrayIndexLast}:{ProgramArgs.WholeArrayStepSize} (length: {ProgramArgs.WholeArrayLength}).");


            // Load DataSet


            //var ct = CancellationToken.None;
            //var cp = new ConnectionPool(callChain: null, lvl: lvl + 1);
            //var cpConnecTask = RpcService.RpcConnectTask(cp, ct);
            //await cpConnecTask;


            //if (Program.ProgramArgs.IsUnix)
            //{
            //    Mpstat = new Mpstat();
            //    await Mpstat.Start(mainCt).ConfigureAwait(false);
            //}


            //await DataSet.LoadDataSetAsync(ProgramArgs.DataSetDir, ProgramArgs.DataSetNames, ProgramArgs.ClassNames, mainCt).ConfigureAwait(false);

            var tasks = new List<Task>();
            //var threads = new List<Thread>();

            //if ( /*InstanceId == 0 ||*/ ProgramArgs.Server)
            //{
                DataSet baseLineDataSet = null;
                //int[] baseLineColumnIndexes = ProgramArgs.BaseLineDataSetColumnIndexes;

                if ((ProgramArgs.BaseLineDataSetNames?.Length ?? 0) > 0)
                {
                    baseLineDataSet = new DataSet();
                    baseLineDataSet.LoadDataSet(ProgramArgs.BaseLineDataSetDir, ProgramArgs.BaseLineDataSetNames, ProgramArgs.ClassNames, mainCt);
                }

                var dataSet = new DataSet();
                dataSet.LoadDataSet(ProgramArgs.DataSetDir, ProgramArgs.DataSetNames, ProgramArgs.ClassNames, mainCt);

                if (baseLineDataSet != null)
                {
                    if (!baseLineDataSet.ClassSizes.Select(a => (a.ClassId, a.ClassName, a.ClassSize, a.DownSampledClassSize)).SequenceEqual(dataSet.ClassSizes.Select(a => (a.ClassId, a.ClassName, a.ClassSize, a.DownSampledClassSize))))
                    {
                        throw new Exception();
                    }

                    var x = dataSet.ValueList.SelectMany(a => a.ClassValueList.Select(b => b.RowColumns[0].RowColumnValue).ToArray()).ToArray();
                    var y = baseLineDataSet.ValueList.SelectMany(a => a.ClassValueList.Select(b => b.RowColumns[0].RowColumnValue).ToArray()).ToArray();

                    if (!x.SequenceEqual(y)) throw new Exception();
                }

                //var fss = Task.Run(async () => await fs_server.feature_selection_initialization(
                var fsServerTask = Task.Run(async () => await FsController.FeatureSelectionInitializationAsync(
                        
                        baseLineDataSet, 
                        ProgramArgs.BaseLineDataSetColumnIndexes,
                        dataSet,
                        ProgramArgs.ScoringClassId,
                        ProgramArgs.ScoringMetrics,
                        ProgramArgs.ExperimentName,
                        instanceId,
                        ProgramArgs.WholeArrayLength,
                        //program_args.instance_array_index_start,
                        //program_args.array_step,
                        //program_args.instance_array_index_end,
                        ProgramArgs.Repetitions,
                        ProgramArgs.OuterCvFolds,
                        ProgramArgs.OuterCvFoldsToRun,
                        ProgramArgs.InnerFolds,
                        ProgramArgs.SvmTypes,
                        ProgramArgs.Kernels,
                        ProgramArgs.Scales,
                        ProgramArgs.ClassWeights,
                        ProgramArgs.CalcElevenPointThresholds,
                        lvl: lvl + 1,
                        ct: mainCt
                        
                        ).ConfigureAwait(false),
                    mainCt);

                tasks.Add(fsServerTask);
                //threads.Add(fss);
            //}

            //if ( /*InstanceId != 0 ||*/ ProgramArgs.Client)
            //{
            //    var DataSet = new DataSet();
            //    DataSet.LoadDataSet(ProgramArgs.DataSetDir, ProgramArgs.DataSetNames, ProgramArgs.ClassNames, mainCt);
            //
            //    //var fsc = Task.Run(async () => await fs_client.x0_feature_selection_client_initialization
            //    var fsClientTask = Task.Run(async () => await FsClient.FeatureSelectionClientInitializationAsync(DataSet, ProgramArgs.ExperimentName, instanceId, ProgramArgs.WholeArrayLength, lvl: lvl + 1, ct:mainCt).ConfigureAwait(false), mainCt);
            //
            //    tasks.Add(fsClientTask);
            //    //threads.Add(fsc);
            //}

            //if ( /*InstanceId != 0 ||*/ ProgramArgs.Client)
            //{
            //    var task = Task.Run(async () => await RpcService.ListenForRPC(mainCt, Program.ProgramArgs.ServerPort).ConfigureAwait(false), mainCt);
            //    tasks.Add(task);
            //}

            if (tasks.Count > 0)
            {
                try { await Task.WhenAny(tasks).ConfigureAwait(false); }
                catch (Exception e) { Logging.LogException(e, "", ModuleName); }

                try
                {
                    Logging.LogEvent($"Cancelling {nameof(mainCts)}", ModuleName);
                    mainCts.Cancel();
                }
                catch (Exception e) { Logging.LogException(e, "", ModuleName); }

                try { await Task.WhenAll(tasks).ConfigureAwait(false); }
                catch (Exception e) { Logging.LogException(e, "", ModuleName); }
            }

            //if (threads.Count > 0)
            //{
            //    for (var i = 0; i < threads.Count; i++)
            //    {
            //        try{threads[i].Join();} catch (Exception e){ Logging.LogException( e, ModuleName); }
            //    }
            //}

            Logging.LogEvent($"Reached end of {nameof(SvmFsCtl)}.{nameof(Main)}...", ModuleName);

            Logging.LogExit(ModuleName, lvl: lvl + 1);
#if DEBUG
            Console.ReadLine();
            Console.ReadLine();
            Console.ReadLine();
#endif
        }


       
    }
}