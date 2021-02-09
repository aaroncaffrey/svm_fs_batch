using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal class CrossValidate
    {
        public const string ModuleName = nameof(CrossValidate);

        internal static async Task<(TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText)> InnerCrossValidationAsync(IndexData unrolledIndex, OuterCvInput outerCvInput, bool libsvmTrainProbabilityEstimates = true, bool log = false, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }


            const string methodName = nameof(InnerCrossValidationAsync);

            var trainStdoutFilename = "";
            var trainStderrFilename = "";

            var predictStdoutFilename = "";
            var predictStderrFilename = "";

            var trainGridSearchResult = new GridPoint();

            var swGrid = new Stopwatch();

            // perform inner-cv
            if (unrolledIndex.IdInnerCvFolds >= 2)
            {
                swGrid.Start();
                if (!string.IsNullOrWhiteSpace(outerCvInput.GridFn))
                {
                    var trainGridStdoutFile = "";
                    var trainGridStderrFile = "";

                    trainGridSearchResult = await Grid.GridParameterSearchAsync(true, Program.ProgramArgs.LibsvmTrainRuntime, outerCvInput.GridFn, outerCvInput.TrainFn, trainGridStdoutFile, trainGridStderrFile, unrolledIndex.IdClassWeights, unrolledIndex.IdSvmType, unrolledIndex.IdSvmKernel, unrolledIndex.IdRepetitions, outerCvInput.RepetitionsIndex, unrolledIndex.IdOuterCvFolds, outerCvInput.OuterCvIndex, unrolledIndex.IdInnerCvFolds, libsvmTrainProbabilityEstimates, ct: ct).ConfigureAwait(false);
                }

                swGrid.Stop();
            }

            //var sw_gridDur = sw_grid.ElapsedMilliseconds;


            // train
            var swTrain = Stopwatch.StartNew();
            var trainResult = await Libsvm.TrainAsync(Program.ProgramArgs.LibsvmTrainRuntime, outerCvInput.TrainFn, outerCvInput.ModelFn, trainStdoutFilename, trainStderrFilename, trainGridSearchResult.GpCost, trainGridSearchResult.GpGamma, trainGridSearchResult.GpEpsilon, trainGridSearchResult.GpCoef0, trainGridSearchResult.GpDegree, null, unrolledIndex.IdSvmType, unrolledIndex.IdSvmKernel, null, libsvmTrainProbabilityEstimates, ct: ct).ConfigureAwait(false);
            swTrain.Stop();
            //var sw_trainDur = sw_train.ElapsedMilliseconds;

            if (log && !string.IsNullOrWhiteSpace(trainResult.CmdLine)) Logging.WriteLine(trainResult.CmdLine, ModuleName, methodName);
            if (log && !string.IsNullOrWhiteSpace(trainResult.stdout)) trainResult.stdout.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(line => Logging.WriteLine($@"{nameof(trainResult)}.{nameof(trainResult.stdout)}: {line}", ModuleName, methodName));
            if (log && !string.IsNullOrWhiteSpace(trainResult.stderr)) trainResult.stderr.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(line => Logging.WriteLine($@"{nameof(trainResult)}.{nameof(trainResult.stderr)}: {line}", ModuleName, methodName));


            // predict
            var swPredict = Stopwatch.StartNew();
            var predictResult = await Libsvm.PredictAsync(Program.ProgramArgs.LibsvmPredictRuntime, outerCvInput.TestFn, outerCvInput.ModelFn, outerCvInput.PredictFn, libsvmTrainProbabilityEstimates, predictStdoutFilename, predictStderrFilename, ct: ct).ConfigureAwait(false);

            swPredict.Stop();
            //var sw_predictDur = sw_train.ElapsedMilliseconds;

            if (log && !string.IsNullOrWhiteSpace(predictResult.CmdLine)) Logging.WriteLine(predictResult.CmdLine, ModuleName, methodName);
            if (log && !string.IsNullOrWhiteSpace(predictResult.stdout)) predictResult.stdout.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(line => Logging.WriteLine($@"{nameof(predictResult)}.{nameof(predictResult.stdout)}: {line}", ModuleName, methodName));
            if (log && !string.IsNullOrWhiteSpace(predictResult.stderr)) predictResult.stderr.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList().ForEach(line => Logging.WriteLine($@"{nameof(predictResult)}.{nameof(predictResult.stderr)}: {line}", ModuleName, methodName));

            var predictText = await IoProxy.ReadAllLinesAsync(true, ct, outerCvInput.PredictFn, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false);
            //Logging.WriteLine($@"Loaded {input.predict_fn}");

            Logging.LogExit(ModuleName); 
            
            return ct.IsCancellationRequested ? default : (swGrid != null && swGrid.Elapsed != TimeSpan.Zero
                ? (TimeSpan?)swGrid.Elapsed
                : (TimeSpan?)null, swTrain != null && swTrain.Elapsed != TimeSpan.Zero
                ? (TimeSpan?)swTrain.Elapsed
                : (TimeSpan?)null, swPredict != null && swPredict.Elapsed != TimeSpan.Zero
                ? (TimeSpan?)swPredict.Elapsed
                : (TimeSpan?)null, trainGridSearchResult, predictText);
        }

        internal static async Task<(IndexData id, ConfusionMatrix cm)[]> CrossValidatePerformanceAsync(DataSet DataSet, IndexData unrolledIndexData, bool makeOuterCvConfusionMatrices = false, bool overwriteCache = false, bool saveGroupCache = false, bool asParallel = true, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            if (DataSet == null) throw new ArgumentOutOfRangeException(nameof(DataSet));
            if (string.IsNullOrWhiteSpace(unrolledIndexData.IdExperimentName)) throw new ArgumentOutOfRangeException(nameof(unrolledIndexData));
            if (unrolledIndexData == null) throw new ArgumentOutOfRangeException(nameof(unrolledIndexData));

            // todo: try to reload gkGroup cache file
            //var groupCacheFiles = Directory.Exists(unrolledIndexData.IdGroupFolder)
            //    ? Directory.GetFiles(unrolledIndexData.IdGroupFolder, "m_*_full.csv")
            //    : null;

            //if (groupCacheFiles != null && groupCacheFiles.Length > 0)
            //{
            //    var results = groupCacheFiles.Select(a => ConfusionMatrix.LoadAsync(a, -1, asParallel, ct)).ToArray();
            //}

            //Logging.WriteLine(
            //    $@"{unrolledIndexData.IdExperimentName}: gkGroup cache: Unavailable for iteration {unrolledIndexData.IdIterationIndex} gkGroup {unrolledIndexData.IdGroupArrayIndex}/{unrolledIndexData.IdTotalGroups}");

            var ocvResult = await OuterCrossValidationAsync(DataSet, unrolledIndexData, makeOuterCvConfusionMatrices, overwriteCache, saveGroupCache, asParallel, ct).ConfigureAwait(false);

            var groupCmSdList = ocvResult.McvCm.Select(cm => (unrolled_index_data: unrolledIndexData, cm)).ToArray();
            
            Logging.LogExit(ModuleName); 
            return ct.IsCancellationRequested ? default :groupCmSdList;
        }

        private static async Task<(IndexData id, ConfusionMatrix[] OcvCm, ConfusionMatrix[] McvCm)> OuterCrossValidationAsync(DataSet DataSet, IndexData unrolledIndexData, bool makeOuterCvConfusionMatrices, bool overwriteCache = false, bool saveGroupCache = false, bool asParallel = true, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            const string methodName = nameof(OuterCrossValidationAsync);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            if (DataSet == null) throw new ArgumentOutOfRangeException(nameof(DataSet));
            //if (string.IsNullOrWhiteSpace(ExperimentName)) throw new ArgumentOutOfRangeException(nameof(ExperimentName));
            if (unrolledIndexData == null) throw new ArgumentOutOfRangeException(nameof(unrolledIndexData));
            if (unrolledIndexData.IdColumnArrayIndexes == null || unrolledIndexData.IdColumnArrayIndexes.Length == 0) throw new ArgumentOutOfRangeException(nameof(unrolledIndexData), $@"{ModuleName}.{methodName}.{nameof(unrolledIndexData)}.{nameof(unrolledIndexData.IdColumnArrayIndexes)}");
            //if (selection_test_info == null) throw new ArgumentOutOfRangeException(nameof(selection_test_info));
            //if (GroupKey == null) throw new ArgumentOutOfRangeException(nameof(GroupKey));

            // 1. make outer-cv files
            var outerCvInputs = await MakeOuterCvInputsAsync(DataSet,
                //column_indexes: test_selected_columns,
                //group_folder: group_folder,
                unrolledIndexData,
                ct: ct).ConfigureAwait(false);

            if (outerCvInputs == default) { Logging.LogExit(ModuleName);  return default; }

            // 2. run libsvm
            var outerCvInputsResultTasks = asParallel
                ? outerCvInputs.Where(a => a.OuterCvIndex != -1 && a.RepetitionsIndex != -1).AsParallel().AsOrdered().WithCancellation(ct).Select(async outerCvInput => await OuterCrossValidationSingleAsync(DataSet, unrolledIndexData, outerCvInput, makeOuterCvConfusionMatrices, overwriteCache, saveGroupCache, ct).ConfigureAwait(false)).ToArray()
                : outerCvInputs.Where(a => a.OuterCvIndex != -1 && a.RepetitionsIndex != -1).Select(async outerCvInput => await OuterCrossValidationSingleAsync(DataSet, unrolledIndexData, outerCvInput, makeOuterCvConfusionMatrices, overwriteCache, saveGroupCache, ct).ConfigureAwait(false)).ToArray();

            var outerCvInputsResult = await Task.WhenAll(outerCvInputsResultTasks).ConfigureAwait(false);

            if (outerCvInputsResult == null || outerCvInputsResult.Length == 0) { Logging.LogExit(ModuleName);  return default; }

            // 1a. the ocvi index -1 is merged data
            var mergedCvInput = outerCvInputs.First(a => a.OuterCvIndex == -1);

            if (mergedCvInput == null) { Logging.LogExit(ModuleName);  return default; }

            var ocvCm = makeOuterCvConfusionMatrices
                ? outerCvInputsResult.Where(a => a.OcvCm != null).SelectMany(a => a.OcvCm).ToArray()
                : null;
            var predictionDataList = outerCvInputsResult.Select(a => a.PredictionData).ToArray();

            // 3. make confusion matrix from the merged prediction results
            // note: repeated 'labels' lines will be ignored
            var mergedPredictionText = predictionDataList.SelectMany(a => a.PredictText).ToArray();

            var mergedTestClassSampleIdList = mergedCvInput.TestFoldIndexes.SelectMany(a => a.TestIndexes).ToArray();

            var predictionFileData = PerformanceMeasure.LoadPredictionFile(mergedCvInput.TestText, null, mergedPredictionText, unrolledIndexData.IdCalcElevenPointThresholds, mergedTestClassSampleIdList, ct);
            //for (var cm_index = 0; cm_index < prediction_file_data.CmList.Length; cm_index++) { prediction_file_data.CmList[cm_index].unrolled_index_data = unrolled_index_data; }

            if (predictionFileData == default || predictionFileData.prediction_list == null || predictionFileData.prediction_list.Length == 0 || predictionFileData.CmList == null || predictionFileData.CmList.Length == 0) { Logging.LogExit(ModuleName);  return default; }

            var mcvCm = predictionFileData.CmList;

            // add any missing details to the confusion-matrix
            Program.UpdateMergedCm(DataSet, predictionFileData, unrolledIndexData, mergedCvInput, predictionDataList, ct: ct);

            // save CM for gkGroup
            if (saveGroupCache)
            {
                await ConfusionMatrix.SaveAsync(mergedCvInput.CmFn1, /*merged_cv_input.cm_fn2,*/ overwriteCache, mcvCm, ct: ct).ConfigureAwait(false);
                Logging.WriteLine($@"{unrolledIndexData.IdExperimentName}: gkGroup MCV cache: Saved: {unrolledIndexData?.IdIndexStr()} {unrolledIndexData?.IdFoldStr()} {unrolledIndexData?.IdMlStr()}. Files: {mergedCvInput.CmFn1}, {mergedCvInput.CmFn2}.");
            }
            else
            {
                Logging.WriteLine($@"{unrolledIndexData.IdExperimentName}: gkGroup MCV cache: Save disabled: {unrolledIndexData?.IdIndexStr()} {unrolledIndexData?.IdFoldStr()} {unrolledIndexData?.IdMlStr()}. Files: {mergedCvInput.CmFn1}, {mergedCvInput.CmFn2}.");

                if (!saveGroupCache && !string.IsNullOrWhiteSpace(unrolledIndexData.IdGroupFolder)) await IoProxy.DeleteDirectoryAsync(true, ct, unrolledIndexData.IdGroupFolder, true).ConfigureAwait(false);
            }


            Logging.LogExit(ModuleName);

            return ct.IsCancellationRequested ? default
                : (unrolledIndexData, ocvCm, mcvCm);
        }


        internal static async Task<OuterCvInput[]> MakeOuterCvInputsAsync(DataSet DataSet, IndexData unrolledIndex, bool asParallel = false, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            const string methodName = nameof(MakeOuterCvInputsAsync);

            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            const bool preserveFid = false; // whether to keep the original FID in the libsvm training/testing files (note: if not, bear in mind that features with zero values are removed, so this must not distort the ordering...).

            if (unrolledIndex.IdColumnArrayIndexes == null || unrolledIndex.IdColumnArrayIndexes.Length == 0) throw new ArgumentOutOfRangeException(nameof(unrolledIndex), $@"{ModuleName}.{methodName}.{nameof(unrolledIndex)}.{nameof(unrolledIndex.IdColumnArrayIndexes)}");
            if (unrolledIndex.IdRepetitions <= 0) throw new ArgumentOutOfRangeException(nameof(unrolledIndex), $@"{ModuleName}.{methodName}.{nameof(unrolledIndex)}.{nameof(unrolledIndex.IdRepetitions)}");
            if (unrolledIndex.IdOuterCvFolds <= 0) throw new ArgumentOutOfRangeException(nameof(unrolledIndex), $@"{ModuleName}.{methodName}.{nameof(unrolledIndex)}.{nameof(unrolledIndex.IdOuterCvFolds)}");
            if (unrolledIndex.IdOuterCvFoldsToRun < 0 || unrolledIndex.IdOuterCvFoldsToRun > unrolledIndex.IdOuterCvFolds) throw new ArgumentOutOfRangeException(nameof(unrolledIndex), $@"{ModuleName}.{methodName}.{nameof(unrolledIndex)}.{nameof(unrolledIndex.IdOuterCvFoldsToRun)}");


            // ensure columns in correct order, and has class id
            unrolledIndex.IdColumnArrayIndexes = unrolledIndex.IdColumnArrayIndexes.OrderBy(a => a).ToArray();
            if (unrolledIndex.IdColumnArrayIndexes[0] != 0) unrolledIndex.IdColumnArrayIndexes = new[] { 0 }.Concat(unrolledIndex.IdColumnArrayIndexes).ToArray();

            var totalRepetitions = unrolledIndex.IdRepetitions == 0
                ? 1
                : unrolledIndex.IdRepetitions;
            var totalOuterFoldsToRun = unrolledIndex.IdOuterCvFoldsToRun == 0
                ? unrolledIndex.IdOuterCvFolds
                : unrolledIndex.IdOuterCvFoldsToRun;
            var pairIndexes = new (int RepetitionsIndex, int OuterCvIndex)[totalRepetitions * totalOuterFoldsToRun];
            var pairIndexesIndex = 0;
            for (var repetitionsCvIndex = 0; repetitionsCvIndex < totalRepetitions; repetitionsCvIndex++)
                for (var outerCvIndex = 0; outerCvIndex < totalOuterFoldsToRun; outerCvIndex++)
                    pairIndexes[pairIndexesIndex++] = (RepetitionsIndex: repetitionsCvIndex, OuterCvIndex: outerCvIndex);

            if (pairIndexesIndex < pairIndexes.Length) throw new Exception();

            var ocvData = asParallel
                ? pairIndexes.AsParallel().AsOrdered().WithCancellation(ct).Select(pairIndex => MakeOuterCvInputsSingle(DataSet, unrolledIndex, asParallel, pairIndex.RepetitionsIndex, pairIndex.OuterCvIndex, preserveFid, ct)).ToArray()
                : pairIndexes.Select(pairIndex => MakeOuterCvInputsSingle(DataSet, unrolledIndex, asParallel, pairIndex.RepetitionsIndex, pairIndex.OuterCvIndex, preserveFid, ct)).ToArray();

            if (asParallel)
            {
                Parallel.ForEach(ocvData,
                   async item =>
                   {
                       if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName); return; }

                       await IoProxy.WriteAllLinesAsync(true, ct, item.TrainFn, item.TrainText, callerModuleName: ModuleName).ConfigureAwait(false);
                       await IoProxy.WriteAllLinesAsync(true, ct, item.TestFn, item.TestText, callerModuleName: ModuleName).ConfigureAwait(false);
                   });

            }
            else
                for (var index = 0; index < ocvData.Length; index++)
                {
                    if (ct.IsCancellationRequested) break;

                    await IoProxy.WriteAllLinesAsync(true, ct, ocvData[index].TrainFn, ocvData[index].TrainText, callerModuleName: ModuleName).ConfigureAwait(false);
                    await IoProxy.WriteAllLinesAsync(true, ct, ocvData[index].TestFn, ocvData[index].TestText, callerModuleName: ModuleName).ConfigureAwait(false);
                }

            // filenames for merging all repetition indexes and outer cv indexes... as if it were a single test.
            var mergedFilenamePrefix = Path.Combine(unrolledIndex.IdGroupFolder, $@"m_{Program.GetIterationFilename(new[] { unrolledIndex },ct)}");

            var mergedCvInput = new OuterCvInput
            {
                RepetitionsIndex = -1,
                OuterCvIndex = -1,
                TrainFn = $@"{mergedFilenamePrefix}.train.libsvm",
                GridFn = $@"{mergedFilenamePrefix}.grid.libsvm",
                ModelFn = $@"{mergedFilenamePrefix}.model.libsvm",
                TestFn = $@"{mergedFilenamePrefix}.test.libsvm",
                PredictFn = $@"{mergedFilenamePrefix}.predict.libsvm",
                CmFn1 = $@"{mergedFilenamePrefix}_full.cm.csv",
                CmFn2 = "",
                TrainText = ocvData.SelectMany(a => a.TrainText).ToArray(),
                TestText = ocvData.SelectMany(a => a.TestText).ToArray(),
                TrainSizes = ocvData.SelectMany(a => a.TrainSizes).GroupBy(a => a.ClassId).Select(b => (ClassId: b.Key, train_size: b.Select(c => c.train_size).Sum())).ToArray(),
                TestSizes = ocvData.SelectMany(a => a.TestSizes).GroupBy(a => a.ClassId).Select(b => (ClassId: b.Key, test_size: b.Select(c => c.test_size).Sum())).ToArray(),
                TrainFoldIndexes = ocvData.SelectMany(a => a.TrainFoldIndexes).GroupBy(a => a.ClassId).Select(a => (ClassId: a.Key, TrainIndexes: a.SelectMany(b => b.TrainIndexes).ToArray())).ToArray(),
                TestFoldIndexes = ocvData.SelectMany(a => a.TestFoldIndexes).GroupBy(a => a.ClassId).Select(a => (ClassId: a.Key, TestIndexes: a.SelectMany(b => b.TestIndexes).ToArray())).ToArray()
            };
            ocvData = new[] { mergedCvInput }.Concat(ocvData).ToArray();

            //var test_class_sample_id_list = merged_cv_input.test_fold_indexes.SelectMany(a => a.TestIndexes).ToList();

            var saveMergedFiles = false;
            if (saveMergedFiles)
            {
                await IoProxy.WriteAllLinesAsync(true, ct, mergedCvInput.TrainFn, mergedCvInput.TrainText, callerModuleName: ModuleName).ConfigureAwait(false);
                await IoProxy.WriteAllLinesAsync(true, ct, mergedCvInput.TestFn, mergedCvInput.TestText, callerModuleName: ModuleName).ConfigureAwait(false);
            }

            Logging.LogExit(ModuleName); 
            
            return ct.IsCancellationRequested ?  default : ocvData;
        }

        private static OuterCvInput MakeOuterCvInputsSingle(DataSet DataSet, IndexData unrolledIndex, bool asParallel, int repetitionsIndex, int outerCvIndex, bool preserveFid, CancellationToken ct)
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            var filename = Path.Combine(unrolledIndex.IdGroupFolder, $@"o_{Program.GetItemFilename(unrolledIndex, repetitionsIndex, outerCvIndex,ct)}");

            var trainFn = $@"{filename}.train.libsvm";
            var gridFn = $@"{filename}.grid.libsvm";
            var modelFn = $@"{filename}.model.libsvm";
            var testFn = $@"{filename}.test.libsvm";
            var predictFn = $@"{filename}.predict.libsvm";
            var cmFn1 = $@"{filename}_full.cm.csv";
            var cmFn2 = "";

            var trainFoldIndexes = asParallel
                ? unrolledIndex.IdDownSampledTrainClassFolds /* down sample for training */
                    .AsParallel().AsOrdered().WithCancellation(ct).Select(a => (a.ClassId, TrainIndexes: a.folds.Where(b => b.RepetitionsIndex == repetitionsIndex && b.OuterCvIndex != outerCvIndex /* do not select test fold */).SelectMany(b => b.ClassSampleIndexes).OrderBy(b => b).ToArray())).ToArray()
                : unrolledIndex.IdDownSampledTrainClassFolds /* down sample for training */
                    .Select(a => (a.ClassId, TrainIndexes: a.folds.Where(b => b.RepetitionsIndex == repetitionsIndex && b.OuterCvIndex != outerCvIndex /* do not select test fold */).SelectMany(b => b.ClassSampleIndexes).OrderBy(b => b).ToArray())).ToArray();

            var trainSizes = trainFoldIndexes.Select(a => (a.ClassId, train_size: a.TrainIndexes?.Length ?? 0)).ToArray();
            var trainRowValues = DataSet.GetRowFeatures(trainFoldIndexes, unrolledIndex.IdColumnArrayIndexes, ct: ct);
            var trainScaling = DataSet.GetScalingParams(trainRowValues, unrolledIndex.IdColumnArrayIndexes);
            var trainRowScaledValues = DataSet.GetScaledRows(trainRowValues, /*column_indexes,*/
                trainScaling,
                unrolledIndex.IdScaleFunction);
            var trainText = asParallel
                ? trainRowScaledValues.AsParallel().AsOrdered().WithCancellation(ct).Select(row => $@"{(int)row[0]} {string.Join(" ", row.Skip(1 /* skip class id column */).Select((colVal, xIndex) => colVal != 0 ? $@"{(preserveFid ? unrolledIndex.IdColumnArrayIndexes[xIndex] : xIndex + 1)}:{colVal:G17}" : @"").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray())}").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray()
                : trainRowScaledValues.Select(row => $@"{(int)row[0]} {string.Join(" ", row.Skip(1 /* skip class id column */).Select((colVal, xIndex) => colVal != 0 ? $@"{(preserveFid ? unrolledIndex.IdColumnArrayIndexes[xIndex] : xIndex + 1)}:{colVal:G17}" : @"").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray())}").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray();

            //var v = train_fold_indexes.Select(a => a.indexes.Select(ix => DataSet.value_list.First(b => b.ClassId == a.ClassId).val_list[ix].row_comment).ToArray()).ToArray();


            var testFoldIndexes = asParallel
                ? unrolledIndex.IdClassFolds /* natural distribution for testing */
                    .AsParallel().AsOrdered().WithCancellation(ct).Select(a => (a.ClassId, TestIndexes: a.folds.Where(b => b.RepetitionsIndex == repetitionsIndex && b.OuterCvIndex == outerCvIndex /* select only test fold */).SelectMany(b => b.ClassSampleIndexes).OrderBy(b => b).ToArray())).ToArray()
                : unrolledIndex.IdClassFolds /* natural distribution for testing */
                    .Select(a => (a.ClassId, TestIndexes: a.folds.Where(b => b.RepetitionsIndex == repetitionsIndex && b.OuterCvIndex == outerCvIndex /* select only test fold */).SelectMany(b => b.ClassSampleIndexes).OrderBy(b => b).ToArray())).ToArray();

            var testSizes = testFoldIndexes.Select(a => (a.ClassId, test_size: a.TestIndexes?.Length ?? 0)).ToArray();
            var testRowValues = DataSet.GetRowFeatures(testFoldIndexes, unrolledIndex.IdColumnArrayIndexes, ct: ct);
            var testScaling = trainScaling; /* scale test data with training data */
            var testRowScaledValues = DataSet.GetScaledRows(testRowValues, /*column_indexes,*/
                testScaling,
                unrolledIndex.IdScaleFunction);
            var testText = asParallel
                ? testRowScaledValues.AsParallel().AsOrdered().WithCancellation(ct).Select(row => $@"{(int)row[0]} {string.Join(" ", row.Skip(1 /* skip class id column */).Select((colVal, xIndex) => colVal != 0 ? $@"{(preserveFid ? unrolledIndex.IdColumnArrayIndexes[xIndex] : xIndex + 1)}:{colVal:G17}" : @"").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray())}").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray()
                : testRowScaledValues.Select(row => $@"{(int)row[0]} {string.Join(" ", row.Skip(1 /* skip class id column */).Select((colVal, xIndex) => colVal != 0 ? $@"{(preserveFid ? unrolledIndex.IdColumnArrayIndexes[xIndex] : xIndex + 1)}:{colVal:G17}" : @"").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray())}").Where(c => !string.IsNullOrWhiteSpace(c)).ToArray();

            Logging.LogExit(ModuleName);

            var ret = new OuterCvInput
            {
                RepetitionsIndex = repetitionsIndex,
                OuterCvIndex = outerCvIndex,
                TrainFn = trainFn,
                GridFn = gridFn,
                ModelFn = modelFn,
                TestFn = testFn,
                PredictFn = predictFn,
                CmFn1 = cmFn1,
                CmFn2 = cmFn2,
                TrainText = trainText,
                TestText = testText,
                TrainSizes = trainSizes,
                TestSizes = testSizes,
                TrainFoldIndexes = trainFoldIndexes,
                TestFoldIndexes = testFoldIndexes
            };

            return ct.IsCancellationRequested
                ? default
                : ret;
        }


        internal static async Task<((TimeSpan? gridDur, TimeSpan? trainDur, TimeSpan? predictDur, GridPoint GridPoint, string[] PredictText) PredictionData, ConfusionMatrix[] OcvCm)> OuterCrossValidationSingleAsync(DataSet DataSet, IndexData unrolledIndexData, OuterCvInput outerCvInput, bool makeOuterCvConfusionMatrices = false, bool overwriteCache = false, bool saveGroupCache = false, CancellationToken ct = default)
        {
            Logging.LogCall(ModuleName);
            if (ct.IsCancellationRequested) { Logging.LogExit(ModuleName);  return default; }

            ConfusionMatrix[] ocvCm = null;

            // call libsvm... Logging.LogExit(ModuleName); returns raw prediction file data from doing: parameter search -> train (with best parameters) -> predict
            var predictionData = await InnerCrossValidationAsync(unrolledIndexData, outerCvInput, ct: ct).ConfigureAwait(false);

            // optional: make_outer_cv_confusion_matrices: this will output the individual outer-cross-validation confusion matrices (i.e. if outer-cv-folds = 5, then 5 respective confusion-matrices will be created, as well as the merged data confusion-matrix).
            if (makeOuterCvConfusionMatrices)
            {
                var ocvTestClassSampleIdList = (outerCvInput.TestFoldIndexes ?? Array.Empty<(int ClassId, int[] TestIndexes)>()).SelectMany(a => a.TestIndexes).ToArray();

                // convert text results to confusion matrix and performance metrics
                var ocvPredictionFileData = PerformanceMeasure.LoadPredictionFile(outerCvInput.TestText, null, predictionData.PredictText, unrolledIndexData.IdCalcElevenPointThresholds, ocvTestClassSampleIdList, ct);

                for (var cmIndex = 0; cmIndex < ocvPredictionFileData.CmList.Length; cmIndex++)
                {
                    //ocv_prediction_file_data.CmList[cm_index].unrolled_index_data = unrolled_index_data;
                    ocvPredictionFileData.CmList[cmIndex].XTimeGrid = predictionData.gridDur;
                    ocvPredictionFileData.CmList[cmIndex].XTimeTrain = predictionData.trainDur;
                    ocvPredictionFileData.CmList[cmIndex].XTimeTest = predictionData.predictDur;
                    ocvPredictionFileData.CmList[cmIndex].GridPoint = predictionData.GridPoint;

                    // add any missing meta details to the confusion-matrix
                    Program.UpdateMergedCmSingle(DataSet, unrolledIndexData, outerCvInput, ocvPredictionFileData.CmList[cmIndex], ct);
                }


                //OcvCm.AddRange(ocv_prediction_file_data.CmList);
                ocvCm = ocvPredictionFileData.CmList;

                if (saveGroupCache)
                {
                    // save outer-cross-validation confusion-matrix CM for gkGroup
                    await ConfusionMatrix.SaveAsync(outerCvInput.CmFn1, /*outer_cv_input.cm_fn2, */overwriteCache, ocvPredictionFileData.CmList, ct: ct).ConfigureAwait(false);
                    Logging.WriteLine($@"{unrolledIndexData.IdExperimentName}: gkGroup OCV cache: Saved: [R({outerCvInput.RepetitionsIndex}/{unrolledIndexData.IdRepetitions}) O({outerCvInput.OuterCvIndex}/{unrolledIndexData.IdOuterCvFolds})] {unrolledIndexData.IdIndexStr()} {unrolledIndexData.IdFoldStr()} {unrolledIndexData.IdMlStr()}. Files: {outerCvInput.CmFn1}, {outerCvInput.CmFn2}.");
                }
                else { Logging.WriteLine($@"{unrolledIndexData.IdExperimentName}: gkGroup OCV cache: Save disabled: [R({outerCvInput.RepetitionsIndex}/{unrolledIndexData.IdRepetitions}) O({outerCvInput.OuterCvIndex}/{unrolledIndexData.IdOuterCvFolds})] {unrolledIndexData.IdIndexStr()} {unrolledIndexData.IdFoldStr()} {unrolledIndexData.IdMlStr()}. Files: {outerCvInput.CmFn1}, {outerCvInput.CmFn2}."); }
            }

            // delete temporary files
            try { await Task.WhenAll(IoProxy.DeleteFileAsync(true, ct, outerCvInput.TrainFn), IoProxy.DeleteFileAsync(true, ct, outerCvInput.GridFn), IoProxy.DeleteFileAsync(true, ct, outerCvInput.ModelFn), IoProxy.DeleteFileAsync(true, ct, outerCvInput.TestFn), IoProxy.DeleteFileAsync(true, ct, outerCvInput.PredictFn)).ConfigureAwait(false); }
            catch (Exception e) { Logging.LogException(e, "", ModuleName); }
            // note: do not delete the confusion-matrix: await io_proxy.Delete(outer_cv_input.cm_fn).ConfigureAwait(false);

            Logging.LogExit(ModuleName); 
            
            return ct.IsCancellationRequested ? default : (predictionData, ocvCm);
        }
    }
}