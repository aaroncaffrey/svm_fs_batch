using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SvmFsBatch
{
    internal class CacheLoad
    {
        public const string ModuleName = nameof(CacheLoad);

        internal static IndexDataContainer GetFeatureSelectionInstructions(DataSet DataSet, (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] groups, GroupSeriesIndex[] jobGroupSeries, string ExperimentName, int iterationIndex, int totalGroups,
            //int InstanceId,
            //int TotalInstances,
            int repetitions, int outerCvFolds, int outerCvFoldsToRun, int innerFolds, Routines.LibsvmSvmType[] svmTypes, Routines.LibsvmKernelType[] kernels, Scaling.ScaleFunction[] scales,

            //(int ClassId, string ClassName)[] ClassNames,
            (int ClassId, double ClassWeight)[][] classWeightSets, bool calcElevenPointThresholds, int[] baseGroupIndexes, int[] groupIndexesToTest, int[] selectedGroupIndexes, int? previousWinnerGroupIndex, int[] selectionExcludedGroupIndexes, List<int[]> previousGroupTests, bool asParallel = true, CancellationToken ct = default)
        {
            const string methodName = nameof(GetFeatureSelectionInstructions);

            if (ct.IsCancellationRequested) return default;

            if (jobGroupSeries == null || jobGroupSeries.Length == 0) throw new ArgumentOutOfRangeException(nameof(jobGroupSeries), $"{ModuleName}.{methodName}.{nameof(jobGroupSeries)}");
            //var job_group_series = cache_load.job_group_series(ct, DataSet, groups, ExperimentName, IterationIndex, base_group_indexes, group_indexes_to_test, selected_group_indexes, previous_winner_group_index, selection_excluded_group_indexes, previous_group_tests, as_parallel);

            var uip = new UnrolledIndexesParameters
            {
                RepetitionCvSeriesStart = repetitions,
                RepetitionCvSeriesEnd = repetitions,
                RepetitionCvSeriesStep = 1,

                OuterCvSeriesStart = outerCvFolds,
                OuterCvSeriesEnd = outerCvFolds,
                OuterCvSeriesStep = 1,

                InnerCvSeriesStart = innerFolds,
                InnerCvSeriesEnd = innerFolds,
                InnerCvSeriesStep = 1,

                GroupSeries = jobGroupSeries,


                SvmTypes = svmTypes,
                LibsvmKernelTypes = kernels,
                Scales = scales,
                ClassWeightSets = classWeightSets,

                CalcElevenPointThresholds = calcElevenPointThresholds

                //group_series_start = TotalGroups < 0 ? TotalGroups : 0,
                //group_series_end = TotalGroups < 0 ? TotalGroups : (TotalGroups > 0 ? TotalGroups - 1 : 0)
            };

            var unrolledIndexes = GetUnrolledIndexes(DataSet, ExperimentName, iterationIndex, totalGroups, /*InstanceId, TotalInstances,*/ uip, outerCvFoldsToRun, ct);

            return ct.IsCancellationRequested
                ? default
                : unrolledIndexes;
        }

        internal static GroupSeriesIndex[] JobGroupSeries(DataSet DataSet, (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] groups, string ExperimentName, int iterationIndex, int[] baseGroupIndexes, int[] groupIndexesToTest, int[] selectedGroupIndexes, int? previousWinnerGroupIndex, int[] selectionExcludedGroupIndexes, List<int[]> previousGroupTests, bool asParallel, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            var jobGroupSeries = asParallel
                ? groupIndexesToTest.AsParallel().AsOrdered().WithCancellation(ct).Select(groupArrayIndex => JobGroupSeriesIndexSingle(DataSet, groups, ExperimentName, iterationIndex, baseGroupIndexes, selectedGroupIndexes, previousWinnerGroupIndex, selectionExcludedGroupIndexes, previousGroupTests, groupArrayIndex, ct)).ToArray()
                : groupIndexesToTest.Select(groupArrayIndex => JobGroupSeriesIndexSingle(DataSet, groups, ExperimentName, iterationIndex, baseGroupIndexes, selectedGroupIndexes, previousWinnerGroupIndex, selectionExcludedGroupIndexes, previousGroupTests, groupArrayIndex, ct)).ToArray();

            jobGroupSeries = jobGroupSeries.Where(a => a.SelectionDirection != Program.Direction.None).ToArray();
            return ct.IsCancellationRequested
                ? default
                : jobGroupSeries;
        }

        internal static GroupSeriesIndex JobGroupSeriesIndexSingle(DataSet DataSet, (DataSetGroupKey GroupKey, DataSetGroupKey[] GroupColumnHeaders, int[] columns)[] groups, string ExperimentName, int iterationIndex, int[] baseGroupIndexes, int[] selectedGroupIndexes, int? previousWinnerGroupIndex, int[] selectionExcludedGroupIndexes, List<int[]> previousGroupTests, int groupArrayIndex, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;
            //var test_selected_groups = selected_groups.OrderBy(group_index => group_index).ToArray();
            //var test_selected_columns = selected_columns.OrderBy(col_index => col_index).ToArray();
            var gsi = new GroupSeriesIndex();

            gsi.GroupArrayIndex = groupArrayIndex;

            gsi.GroupKey = gsi.GroupArrayIndex > -1 && groups != null && groups.Length - 1 >= gsi.GroupArrayIndex
                ? groups[gsi.GroupArrayIndex].GroupKey
                : default;
            gsi.GroupFolder = Program.GetIterationFolder(Program.ProgramArgs.ResultsRootFolder, ExperimentName, iterationIndex, gsi.GroupArrayIndex, ct);

            gsi.IsGroupIndexValid = gsi.GroupArrayIndex > -1 && Routines.IsInRange(0, (groups?.Length ?? 0) - 1, gsi.GroupArrayIndex);
            gsi.IsGroupSelected = selectedGroupIndexes.Contains(gsi.GroupArrayIndex);
            gsi.IsGroupOnlySelection = gsi.IsGroupSelected && selectedGroupIndexes.Length == 1;
            gsi.IsGroupLastWinner = gsi.GroupArrayIndex == previousWinnerGroupIndex;
            gsi.IsGroupBaseGroup = baseGroupIndexes?.Contains(gsi.GroupArrayIndex) ?? false;
            gsi.IsGroupBlacklisted = selectionExcludedGroupIndexes?.Contains(gsi.GroupArrayIndex) ?? false;


            // if selected, remove.  if not selected, add.  if only gkGroup, no action.  if just added, no action.

            gsi.SelectionDirection = Program.Direction.None;

            if (gsi.IsGroupBlacklisted || gsi.IsGroupBaseGroup && groups.Length > baseGroupIndexes.Length) gsi.SelectionDirection = Program.Direction.None;

            else if (!gsi.IsGroupIndexValid /* || is_group_base_group*/
            ) gsi.SelectionDirection = Program.Direction.Neutral; // calibration

            else if (gsi.IsGroupIndexValid && gsi.IsGroupSelected && !gsi.IsGroupBaseGroup && !gsi.IsGroupLastWinner && !gsi.IsGroupOnlySelection) gsi.SelectionDirection = Program.Direction.Backwards;

            else if (gsi.IsGroupIndexValid && !gsi.IsGroupSelected && !gsi.IsGroupBaseGroup && !gsi.IsGroupBlacklisted) gsi.SelectionDirection = Program.Direction.Forwards;

            else if (gsi.IsGroupOnlySelection) gsi.SelectionDirection = Program.Direction.Neutral;

            else throw new Exception();

            gsi.GroupIndexes = null;
            gsi.ColumnIndexes = null;

            if (gsi.SelectionDirection == Program.Direction.None)
            {
                // no action
            }
            else if (gsi.SelectionDirection == Program.Direction.Neutral)
            {
                gsi.GroupIndexes = selectedGroupIndexes;
                //gsi.column_indexes = selected_columns;
            }
            else if (gsi.SelectionDirection == Program.Direction.Forwards)
            {
                gsi.GroupIndexes = selectedGroupIndexes.Union(new[] {gsi.GroupArrayIndex}).OrderBy(groupIndex => groupIndex).ToArray();
                //gsi.column_indexes = gsi.group_indexes.SelectMany(group_index => groups[group_index].columns).Union(new[] { 0 }).OrderBy(col_index => col_index).Distinct().ToArray();
            }
            else if (gsi.SelectionDirection == Program.Direction.Backwards)
            {
                gsi.GroupIndexes = selectedGroupIndexes.Except(new[] {gsi.GroupArrayIndex}).OrderBy(groupIndex => groupIndex).ToArray();
                //gsi.column_indexes = gsi.group_indexes.SelectMany(group_index => groups[group_index].columns).Union(new[] { 0 }).OrderBy(col_index => col_index).Distinct().ToArray();
            }
            else { throw new Exception(); }

            if (previousGroupTests.Any(a => a.SequenceEqual(gsi.GroupIndexes)))
            {
                gsi.SelectionDirection = Program.Direction.None;
                gsi.GroupIndexes = null;
                gsi.ColumnIndexes = null;
            }

            gsi.ColumnIndexes = gsi.GroupIndexes?.SelectMany(groupIndex => groups[groupIndex].columns).Union(new[] {0}).OrderBy(colIndex => colIndex).Distinct().ToArray();
            gsi.ColumnIndexes = DataSet.RemoveDuplicateColumns(DataSet, gsi.ColumnIndexes, ct: ct);

            if (gsi.GroupIndexes != null && gsi.GroupIndexes.Length > 0 && (gsi.ColumnIndexes == null || gsi.ColumnIndexes.Length <= 1)) throw new Exception();

            return ct.IsCancellationRequested
                ? default
                : gsi;
        }


        internal static IndexDataContainer GetUnrolledIndexes(DataSet DataSet, string ExperimentName, int iterationIndex, int totalGroups, UnrolledIndexesParameters uip, int outerCvFoldsToRun = 0, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(GetUnrolledIndexes);

            if (uip.SvmTypes == null || uip.SvmTypes.Length == 0) uip.SvmTypes = new[] {Routines.LibsvmSvmType.CSvc};
            if (uip.LibsvmKernelTypes == null || uip.LibsvmKernelTypes.Length == 0) uip.LibsvmKernelTypes = new[] {Routines.LibsvmKernelType.Rbf};
            if (uip.Scales == null || uip.Scales.Length == 0) uip.Scales = new[] {Scaling.ScaleFunction.Rescale};

            if (uip.RepetitionCvSeries == null || uip.RepetitionCvSeries.Length == 0) uip.RepetitionCvSeries = Routines.Range(uip.RepetitionCvSeriesStart, uip.RepetitionCvSeriesEnd, uip.RepetitionCvSeriesStep);
            if (uip.OuterCvSeries == null || uip.OuterCvSeries.Length == 0) uip.OuterCvSeries = Routines.Range(uip.OuterCvSeriesStart, uip.OuterCvSeriesEnd, uip.OuterCvSeriesStep);
            if (uip.InnerCvSeries == null || uip.InnerCvSeries.Length == 0) uip.InnerCvSeries = Routines.Range(uip.InnerCvSeriesStart, uip.InnerCvSeriesEnd, uip.InnerCvSeriesStep);
            //if (p.group_series == null || p.group_series.Length == 0) p.group_series = routines.range(p.group_series_start, p.group_series_end, 1);

            //if (selection_excluded_groups != null && selection_excluded_groups.Length > 0)
            //{
            //    p.group_series = p.group_series.Except(selection_excluded_groups).ToArray();
            //}

            if (uip.RepetitionCvSeries == null || uip.RepetitionCvSeries.Length == 0) throw new ArgumentOutOfRangeException(nameof(uip), $@"{ModuleName}.{methodName}");
            if (uip.OuterCvSeries == null || uip.OuterCvSeries.Length == 0) throw new ArgumentOutOfRangeException(nameof(uip), $@"{ModuleName}.{methodName}");
            if (uip.InnerCvSeries == null || uip.InnerCvSeries.Length == 0) throw new ArgumentOutOfRangeException(nameof(uip), $@"{ModuleName}.{methodName}");
            if (uip.GroupSeries == null || uip.GroupSeries.Length == 0) throw new ArgumentOutOfRangeException(nameof(uip), $@"{ModuleName}.{methodName}");

            var unrolledWholeIndex = 0;
            //var unrolled_partition_indexes = new int[TotalInstances];
            //var unrolled_InstanceId = 0;

            var rCvSeriesLen = uip.RepetitionCvSeries.Length;
            var oCvSeriesLen = uip.OuterCvSeries.Length;
            var groupSeriesLen = uip.GroupSeries.Length;
            var pSvmTypesLen = uip.SvmTypes.Length;
            var pKernelsLen = uip.LibsvmKernelTypes.Length;
            var pScalesLen = uip.Scales.Length;
            var iCvSeriesLen = uip.InnerCvSeries.Length;
            var pClassWeightSetsLen = (uip.ClassWeightSets?.Length ?? 1) > 0
                ? uip.ClassWeightSets?.Length ?? 1
                : 1;

            var lenProduct = rCvSeriesLen * oCvSeriesLen * groupSeriesLen * pSvmTypesLen * pKernelsLen * pScalesLen * iCvSeriesLen * pClassWeightSetsLen;
            var indexesWhole = new IndexData[lenProduct];

            for (var zRCvSeriesIndex = 0; zRCvSeriesIndex < uip.RepetitionCvSeries.Length; zRCvSeriesIndex++) //1
            for (var zOCvSeriesIndex = 0; zOCvSeriesIndex < uip.OuterCvSeries.Length; zOCvSeriesIndex++) //2
            {
                // for the current number of R and O, set the folds (which vary according to the values of R and O).
                var (classFolds, downSampledTrainClassFolds) = Routines.Folds(DataSet.ClassSizes, uip.RepetitionCvSeries[zRCvSeriesIndex], uip.OuterCvSeries[zOCvSeriesIndex], ct: ct);

                for (var zSvmTypesIndex = 0; zSvmTypesIndex < uip.SvmTypes.Length; zSvmTypesIndex++) //4
                for (var zKernelsIndex = 0; zKernelsIndex < uip.LibsvmKernelTypes.Length; zKernelsIndex++) //5
                for (var zScalesIndex = 0; zScalesIndex < uip.Scales.Length; zScalesIndex++) //6
                for (var zICvSeriesIndex = 0; zICvSeriesIndex < uip.InnerCvSeries.Length; zICvSeriesIndex++) //7
                for (var zClassWeightSetsIndex = 0; zClassWeightSetsIndex <= (uip.ClassWeightSets?.Length ?? 0); zClassWeightSetsIndex++) //8
                {
                    if (uip.ClassWeightSets != null && uip.ClassWeightSets.Length > 0 && zClassWeightSetsIndex >= uip.ClassWeightSets.Length) continue;
                    var classWeights = uip.ClassWeightSets == null || zClassWeightSetsIndex >= uip.ClassWeightSets.Length
                        ? null
                        : uip.ClassWeightSets[zClassWeightSetsIndex];

                    for (var zGroupSeriesIndex = 0; zGroupSeriesIndex < uip.GroupSeries.Length; zGroupSeriesIndex++) //3
                    {
                        var indexData = new IndexData
                        {
                            //unrolled_partition_index = unrolled_partition_indexes[unrolled_InstanceId],
                            //unrolled_InstanceId = unrolled_InstanceId,

                            IdGroupArrayIndex = uip.GroupSeries[zGroupSeriesIndex].GroupArrayIndex,
                            IdSelectionDirection = uip.GroupSeries[zGroupSeriesIndex].SelectionDirection,
                            IdGroupArrayIndexes = uip.GroupSeries[zGroupSeriesIndex].GroupIndexes,
                            IdColumnArrayIndexes = uip.GroupSeries[zGroupSeriesIndex].ColumnIndexes,
                            IdNumGroups = uip.GroupSeries[zGroupSeriesIndex].GroupIndexes.Length,
                            IdNumColumns = uip.GroupSeries[zGroupSeriesIndex].ColumnIndexes.Length,
                            IdGroupFolder = uip.GroupSeries[zGroupSeriesIndex].GroupFolder,
                            IdGroupKey = uip.GroupSeries[zGroupSeriesIndex].GroupKey,

                            IdExperimentName = ExperimentName,
                            //unrolled_whole_index = unrolled_whole_index,
                            IdIterationIndex = iterationIndex,
                            IdCalcElevenPointThresholds = uip.CalcElevenPointThresholds,
                            IdRepetitions = uip.RepetitionCvSeries[zRCvSeriesIndex],
                            IdOuterCvFolds = uip.OuterCvSeries[zOCvSeriesIndex],
                            IdOuterCvFoldsToRun = outerCvFoldsToRun,
                            IdSvmType = uip.SvmTypes[zSvmTypesIndex],
                            IdSvmKernel = uip.LibsvmKernelTypes[zKernelsIndex],
                            IdScaleFunction = uip.Scales[zScalesIndex],
                            IdInnerCvFolds = uip.InnerCvSeries[zICvSeriesIndex],
                            //TotalInstances = TotalInstances,
                            //total_whole_indexes = len_product,

                            IdClassWeights = classWeights,
                            IdClassFolds = classFolds,
                            IdDownSampledTrainClassFolds = downSampledTrainClassFolds,
                            IdTotalGroups = totalGroups
                            //is_job_completed = false,

                            //unrolled_InstanceId = -1,
                            //unrolled_partition_index = -1,
                            //total_partition_indexes = -1,
                        };

                        indexesWhole[unrolledWholeIndex++] = indexData;

                        //unrolled_partition_indexes[unrolled_InstanceId++]++;

                        //if (unrolled_InstanceId >= TotalInstances) unrolled_InstanceId = 0;
                    }
                }
            }

            if (unrolledWholeIndex != lenProduct) throw new Exception();

            var indexDataContainer = new IndexDataContainer
            {
                IndexesWhole = indexesWhole
            }; //redistribute_work(ct, IndexesWhole/*, InstanceId, TotalInstances*/);

            return !ct.IsCancellationRequested ? indexDataContainer : default;
        }

        //internal static IndexDataContainer redistribute_work(CancellationToken ct, IndexDataContainer IndexDataContainer/*, int InstanceId, int TotalInstances*/)
        //{
        //    return ct.IsCancellationRequested ? default :redistribute_work(ct, IndexDataContainer.IndexesWhole/*, InstanceId, TotalInstances*/);
        //}

        /*internal static IndexDataContainer redistribute_work(CancellationToken ct, index_data[] IndexesWhole)//, int InstanceId, int TotalInstances, bool as_parallel = true)
        {
            var IndexDataContainer = new IndexDataContainer();
            IndexDataContainer.IndexesWhole = IndexesWhole;

            
            const string MethodName = nameof(redistribute_work);

            if (ct.IsCancellationRequested) return default;

            if (IndexesWhole == null || IndexesWhole.Length == 0) throw new ArgumentOutOfRangeException(nameof(IndexesWhole), $@"{ModuleName}.{MethodName}.{nameof(IndexesWhole)}");

            var IndexDataContainer = new IndexDataContainer();
            IndexDataContainer.IndexesWhole = IndexesWhole;

            var unrolled_whole_index = 0;
            var unrolled_partition_indexes = new int[TotalInstances];
            var unrolled_InstanceId = 0;

            for (var x = 0; x < IndexDataContainer.IndexesWhole.Length; x++)
            {
                // unique id for index_data
                IndexDataContainer.IndexesWhole[x].unrolled_whole_index = unrolled_whole_index++;
                IndexDataContainer.IndexesWhole[x].unrolled_partition_index = unrolled_partition_indexes[unrolled_InstanceId];
                IndexDataContainer.IndexesWhole[x].total_whole_indexes = IndexDataContainer.IndexesWhole.Length;

                // instance id
                IndexDataContainer.IndexesWhole[x].TotalInstances = TotalInstances;
                IndexDataContainer.IndexesWhole[x].unrolled_InstanceId = unrolled_InstanceId;

                unrolled_partition_indexes[unrolled_InstanceId++]++;

                if (unrolled_InstanceId >= TotalInstances) unrolled_InstanceId = 0;
            }

            if (as_parallel)
            {
                Parallel.For(0, IndexDataContainer.IndexesWhole.Length, index =>
                {
                    IndexDataContainer.IndexesWhole[index].total_partition_indexes = unrolled_partition_indexes[IndexDataContainer.IndexesWhole[index].unrolled_InstanceId];
                });
            }
            else
            {
                for (var index = 0; index < IndexDataContainer.IndexesWhole.Length; index++)
                {
                    IndexDataContainer.IndexesWhole[index].total_partition_indexes = unrolled_partition_indexes[IndexDataContainer.IndexesWhole[index].unrolled_InstanceId];
                }
            }

            IndexDataContainer.indexes_partitions = as_parallel
                ?
                IndexDataContainer.IndexesWhole.AsParallel().AsOrdered().WithCancellation(ct).GroupBy(a => a.unrolled_InstanceId).OrderBy(a => a.Key).Select(a => a.ToArray()).ToArray()
                :
                IndexDataContainer.IndexesWhole.GroupBy(a => a.unrolled_InstanceId).OrderBy(a => a.Key).Select(a => a.ToArray()).ToArray();

            IndexDataContainer.indexes_partition = IndexDataContainer.indexes_partitions.FirstOrDefault(a => (a.FirstOrDefault()?.unrolled_InstanceId ?? null) == InstanceId);

            if (IndexDataContainer.indexes_partitions.Any(a => a.FirstOrDefault().total_partition_indexes != unrolled_partition_indexes[IndexDataContainer.IndexesWhole[a.FirstOrDefault().unrolled_InstanceId].unrolled_InstanceId])) throw new Exception($@"{ModuleName}.{MethodName}");

            //IndexDataContainer.indexes_partition = IndexDataContainer.IndexesWhole.AsParallel().AsOrdered().WithCancellation(ct).Where(a => a.unrolled_InstanceId == InstanceId).ToArray();

            // shuffle with actual random seed, otherwise same 'random' work would be selected
            // if finished own work, do others  and save  gkGroup file

            
            return ct.IsCancellationRequested ? default :IndexDataContainer;
        }*/


        internal static async Task LoadCacheAsync(int iterationIndex, string ExperimentName, bool waitForCache, List<string> cacheFilesAlreadyLoaded, List<(IndexData id, ConfusionMatrix cm)> iterationCmSdList, IndexDataContainer indexDataContainer, (IndexData id, ConfusionMatrix cm, RankScore rs)[] lastIterationIdCmRs, (IndexData id, ConfusionMatrix cm, RankScore rs) lastWinnerIdCmRs, (IndexData id, ConfusionMatrix cm, RankScore rs) bestWinnerIdCmRs, bool asParallel = true, CancellationToken ct = default)
        {
            if (ct.IsCancellationRequested) return;

            const string methodName = nameof(LoadCacheAsync);
            
            const string fullTag = @"_full.cm.csv";

            // a single gkGroup may have multiple tests... e.g. different number of inner-cv, outer-cv, ClassWeights, etc...
            // therefore, group_index existing isn't enough, must also have the various other parameters

            if (iterationIndex < 0) throw new ArgumentOutOfRangeException(nameof(iterationIndex), $@"{ModuleName}.{methodName}");
            if (string.IsNullOrWhiteSpace(ExperimentName)) throw new ArgumentOutOfRangeException(nameof(ExperimentName), $@"{ModuleName}.{methodName}");
            //if (cache_files_already_loaded == null) throw new ArgumentOutOfRangeException(nameof(cache_files_already_loaded), $@"{ModuleName}.{MethodName}");
            if (iterationCmSdList == null) throw new ArgumentOutOfRangeException(nameof(iterationCmSdList), $@"{ModuleName}.{methodName}");
            if (lastIterationIdCmRs == null && iterationIndex != 0) throw new ArgumentOutOfRangeException(nameof(lastIterationIdCmRs), $@"{ModuleName}.{methodName}");
            if (lastWinnerIdCmRs == default && iterationIndex != 0) throw new ArgumentOutOfRangeException(nameof(lastWinnerIdCmRs), $@"{ModuleName}.{methodName}");
            if (bestWinnerIdCmRs == default && iterationIndex != 0) throw new ArgumentOutOfRangeException(nameof(bestWinnerIdCmRs), $@"{ModuleName}.{methodName}");

            // check which indexes are missing.
            UpdateMissing(iterationCmSdList, indexDataContainer, ct: ct);

            // if all indexes loaded, return
            if (!indexDataContainer.IndexesMissingWhole.Any()) return;

            var iterationFolder = Program.GetIterationFolder(Program.ProgramArgs.ResultsRootFolder, ExperimentName, iterationIndex, ct:ct);

            var cacheLevelWhole = (name: "whole", marker: 'z');
            var cacheLevelPartition = (name: "partition", marker: 'x');
            var cacheLevelGroup = (name: "group", marker: 'm');
            var cacheLevels = new[] {cacheLevelWhole, cacheLevelPartition, cacheLevelGroup};

            do
            {
                if (!IoProxy.ExistsDirectory(false, iterationFolder, ModuleName, methodName, ct))
                {
                    if (waitForCache && indexDataContainer.IndexesMissingWhole.Any()) await Logging.WaitAsync(10, 20, ModuleName, methodName, ct: ct).ConfigureAwait(false);
                    continue;
                }

                foreach (var cacheLevel in cacheLevels)
                {
                    // don't load if already loaded..
                    if (!indexDataContainer.IndexesMissingWhole.Any()) break;

                    // only load m* for the partition... pt1.
                    //if (cache_level == cache_level_group && !IndexDataContainer.indexes_missing_partition.Any()) continue;

                    // load cache, if exists (z, x, m)
                    var cacheFiles1 = Directory.GetFiles(iterationFolder,
                        $"{cacheLevel.marker}*{fullTag}",
                        cacheLevel == cacheLevelGroup
                            ? SearchOption.AllDirectories
                            : SearchOption.TopDirectoryOnly);

                    // don't load any files already previously loaded...
                    if (cacheFilesAlreadyLoaded != null) cacheFiles1 = cacheFiles1.Except(cacheFilesAlreadyLoaded).Distinct().ToArray();

                    // don't load if 'm' and already loaded or not expected
                    if (cacheLevel == cacheLevelGroup && cacheFiles1.Length > 0)
                    {
                        // ensure files are expected
                        // group_folder = program.GetIterationFolder(program.program_args.results_root_folder, ExperimentName, a.id_IterationIndex, a.id_group_array_index)
                        var groupCacheFilenames = asParallel
                            ? indexDataContainer.IndexesMissingWhole.AsParallel().AsOrdered().WithCancellation(ct).Select(a => $@"{Path.Combine(a.IdGroupFolder, $@"{cacheLevelGroup.marker}_{Program.GetIterationFilename(new[] {a}, ct)}")}{fullTag}").ToList()
                            : indexDataContainer.IndexesMissingWhole.Select(a => $@"{Path.Combine(a.IdGroupFolder, $@"{cacheLevelGroup.marker}_{Program.GetIterationFilename(new[] {a}, ct)}")}{fullTag}").ToList();

                        cacheFiles1 = cacheFiles1.Intersect(groupCacheFilenames, StringComparer.OrdinalIgnoreCase).ToArray();
                    }

                    if (cacheFiles1.Length == 0) continue;

                    await LoadCacheFileListWithUpdateAsync(cacheFilesAlreadyLoaded, iterationCmSdList, indexDataContainer, asParallel, cacheFiles1, ct).ConfigureAwait(false);
                }

                if (waitForCache && indexDataContainer.IndexesMissingWhole.Any()) await Logging.WaitAsync(10, 20, ModuleName, methodName, ct: ct).ConfigureAwait(false);
            } while (waitForCache && indexDataContainer.IndexesMissingWhole.Any());
        }

        internal static async Task LoadCacheFileListWithUpdateAsync(List<string> cacheFilesAlreadyLoaded, List<(IndexData id, ConfusionMatrix cm)> iterationCmSdList, IndexDataContainer indexDataContainer, bool asParallel, string[] cacheFiles1, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return;

            var loadResult = await LoadCacheFileListAsync(indexDataContainer, cacheFiles1, asParallel, ct).ConfigureAwait(false);

            cacheFilesAlreadyLoaded?.AddRange(loadResult.FilesLoaded);
            iterationCmSdList?.AddRange(loadResult.IdCmSd);
            if (iterationCmSdList != null && indexDataContainer != null) UpdateMissing(iterationCmSdList, indexDataContainer, asParallel, ct);
        }

        internal static async Task<(string[] FilesLoaded, (IndexData id, ConfusionMatrix cm)[] IdCmSd)> LoadCacheFileListAsync(
            //List<string> cache_files_already_loaded,
            //List<(index_data id, ConfusionMatrix cm)> iteration_cm_sd_list,
            IndexDataContainer indexDataContainer, string[] cacheFiles, bool asParallel = true, CancellationToken ct = default)
        {
            //const string MethodName = nameof(load_cache_file_list);

            if (ct.IsCancellationRequested) return default;
            if (cacheFiles == null || cacheFiles.Length == 0) return default;

            // load and parse cm files
            var loadedDataTasks = asParallel
                ? cacheFiles.AsParallel().AsOrdered().WithCancellation(ct).Select(async cmFn =>
                {
                    if (ct.IsCancellationRequested) return null;
                    return ct.IsCancellationRequested ? default :await LoadCacheFileAsync(cmFn, indexDataContainer, ct).ConfigureAwait(false);
                }).ToArray()
                : cacheFiles.Select(async cmFn =>
                {
                    if (ct.IsCancellationRequested) return null;
                    return ct.IsCancellationRequested ? default :await LoadCacheFileAsync(cmFn, indexDataContainer, ct).ConfigureAwait(false);
                }).ToArray();

            var loadedData = await Task.WhenAll(loadedDataTasks).ConfigureAwait(false);

            var loadedDataFlat = loadedData.SelectMany(a => a).ToArray();

            // record filenames to list of files already loaded
            var filesLoaded = cacheFiles.Where((a, i) => loadedData[i] != null && loadedData[i].Length > 0).ToArray();

            //cache_files_already_loaded?.AddRange(FilesLoaded);
            //iteration_cm_sd_list?.AddRange(loaded_data_flat);
            //if (iteration_cm_sd_list != null && IndexDataContainer != null) update_missing(ct, iteration_cm_sd_list, IndexDataContainer, as_parallel);

            return !ct.IsCancellationRequested ? (filesLoaded, loadedDataFlat) : default;
        }

        internal static async Task<(IndexData id, ConfusionMatrix cm)[]> LoadCacheFileAsync(string cmFn, IndexDataContainer indexDataContainer, CancellationToken ct)
        {
            if (ct.IsCancellationRequested) return default;

            const string methodName = nameof(LoadCacheFileAsync);

            if (await IoProxy.IsFileAvailableAsync(true, ct, cmFn, false, callerModuleName: ModuleName, callerMethodName: methodName).ConfigureAwait(false))
            {
                var cmList = await ConfusionMatrix.LoadAsync(cmFn, ct: ct).ConfigureAwait(false);
                if (cmList != null && cmList.Length > 0)
                {
                    //for (var cmIndex = 0; cmIndex < cmList.Length; cmIndex++)
                    //    if (cmList[cmIndex].UnrolledIndexData != null)
                    //    {
                    //        var id = IndexData.FindFirstReference(indexDataContainer.IndexesWhole, cmList[cmIndex].UnrolledIndexData);
                    //        cmList[cmIndex].UnrolledIndexData = id ?? throw new Exception();
                    //    }

                    Parallel.For(0, cmList.Length,
                        cmIndex =>
                        {
                            if (cmList[cmIndex].UnrolledIndexData != null)
                            {
                                var id = IndexData.FindFirstReference(indexDataContainer.IndexesWhole, cmList[cmIndex].UnrolledIndexData);
                                cmList[cmIndex].UnrolledIndexData = id ?? throw new Exception();
                            }
                        });

                    var idCmList = cmList.AsParallel().AsOrdered().Select(cm => (id: cm?.UnrolledIndexData, cm)).Where(a => a.id != null && a.cm != null).ToArray();

                    return ct.IsCancellationRequested
                        ? default
                        : idCmList;
                }
            }

            return null;
        }

        internal static void UpdateMissing(
            //int InstanceId,
            List<(IndexData id, ConfusionMatrix cm)> iterationCmAll, IndexDataContainer indexDataContainer, bool asParallel = true, CancellationToken ct = default)
        {
            // this method checks 'iteration_cm_all' to see which results are already loaded and which results are missing
            // note: if necessary, call redistribute_work().
            if (ct.IsCancellationRequested) return;

            var iterationCmAllId = iterationCmAll.Select(a => a.id).ToArray();
            //var iteration_cm_all_cm = iteration_cm_all.Select(a => a.cm).ToArray();

            // find loaded indexes
            indexDataContainer.IndexesLoadedWhole = 
                
                iterationCmAllId == null || iterationCmAllId.Length == 0 ? Array.Empty<IndexData>() 
                :
                asParallel ? 
                    indexDataContainer.IndexesWhole.AsParallel().AsOrdered().WithCancellation(ct).Where(id => IndexData.FindFirstReference(iterationCmAllId, id) != null).ToArray() :
                    indexDataContainer.IndexesWhole.Where(id => IndexData.FindFirstReference(iterationCmAllId, id) != null).ToArray();


            indexDataContainer.IndexesMissingWhole = indexDataContainer.IndexesWhole.Except(indexDataContainer.IndexesLoadedWhole).ToArray();

            //IndexDataContainer.indexes_loaded_partitions = as_parallel ?
            //    IndexDataContainer.indexes_partitions.AsParallel().AsOrdered().WithCancellation(ct).Select(ip => ip.Intersect(IndexDataContainer.indexes_loaded_whole).ToArray()).ToArray() :
            //    IndexDataContainer.indexes_partitions.Select(ip => ip.Intersect(IndexDataContainer.indexes_loaded_whole).ToArray()).ToArray();

            //IndexDataContainer.indexes_loaded_partition = IndexDataContainer.indexes_loaded_partitions.Length > InstanceId ? IndexDataContainer.indexes_loaded_partitions[InstanceId] : null;

            //IndexDataContainer.indexes_missing_partitions = as_parallel ?
            //    IndexDataContainer.indexes_partitions.AsParallel().AsOrdered().WithCancellation(ct).Select((ip, i) => ip.Except(IndexDataContainer.indexes_loaded_partitions[i]).ToArray()).ToArray() :
            //    IndexDataContainer.indexes_partitions.Select((ip, i) => ip.Except(IndexDataContainer.indexes_loaded_partitions[i]).ToArray()).ToArray();

            //IndexDataContainer.indexes_missing_partition = IndexDataContainer.indexes_missing_partitions.Length > InstanceId ? IndexDataContainer.indexes_missing_partitions[InstanceId] : null;
        }
    }
}
/*internal static (index_data[] IndexesWhole, index_data[] indexes_partition) GetUnrolledIndexes_check_bias(CancellationToken ct, int search_type, DataSet_loader DataSet, string ExperimentName, int IterationIndex, int TotalGroups, int InstanceId, int TotalInstances)
        {
            if (ct.IsCancellationRequested) return default;


            // for a specific set of features (i.e. the final result from feature selection):

            // what happens to performance if we vary the scale & kernel (is it stable, does it increase, decrease, or randomise?)

            if (search_type == 0)
            {
                var p1 = new unrolled_indexes_parameters
                {
                    group_series_start = -1,
                    group_series_end = -1,

                    calc_ElevenPoint_thresholds = true,

                    scales = new[]
                    {
                        scaling.scale_function.none,
                        scaling.scale_function.normalisation,
                        scaling.scale_function.rescale,
                        scaling.scale_function.standardisation,
                        scaling.scale_function.L0_norm,
                        scaling.scale_function.L1_norm,
                        scaling.scale_function.L2_norm,
                    },

                    kernels = new[]
                    {
                        routines.libsvm_kernel_type.linear,
                        routines.libsvm_kernel_type.polynomial,
                        routines.libsvm_kernel_type.sigmoid,
                        routines.libsvm_kernel_type.rbf
                    }
                };

                var variations_1 = GetUnrolledIndexes(ct, DataSet, ExperimentName, IterationIndex, TotalGroups, InstanceId, TotalInstances, p1);
                return ct.IsCancellationRequested ? default :variations_1;
            }
            else if (search_type == 1)
            {

                // what happens if we vary the repetitions, outer-cv, inner-cv
                var p2 = new unrolled_indexes_parameters
                {
                    group_series_start = -1,
                    group_series_end = -1,

                    calc_ElevenPoint_thresholds = true,

                    // repetitions: (10 - (1 - 1)) / 1 = 10

                    r_cv_series_start = 1,
                    r_cv_series_end = 10,
                    r_cv_series_step = 1,

                    // outer-cv: (10 - (2 - 1)) / 1 = 9

                    o_cv_series_start = 2,
                    o_cv_series_end = 10,
                    o_cv_series_step = 1,

                    // inner-cv: (10 - (2 - 1)) / 1 = 9

                    i_cv_series_start = 1, // start at 1 to test skipping inner-cv, to show what performance increase is obtained through inner-cv
                    i_cv_series_end = 10,
                    i_cv_series_step = 1
                };

                var variations_2 = GetUnrolledIndexes(ct, DataSet, ExperimentName, IterationIndex,TotalGroups, InstanceId, TotalInstances, p2);

                return ct.IsCancellationRequested ? default :variations_2;
            }
            else if (search_type == 2)
            {

                // what happens if we vary the weights 
                var weight_values = new double[]
                {
                    200,
                    100,
                    50,
                    10,
                    5,
                    2,
                    1,
                    0.75,
                    0.5,
                    0.25,
                    0,
                    -0.25,
                    -0.5,
                    -0.75,
                    -1,
                    -2,
                    -5,
                    -10,
                    -50,
                    -100,
                    -200
                }; // default: 1 // 21 values

                var p3 = new unrolled_indexes_parameters
                {
                    group_series_start = -1,
                    group_series_end = -1,

                    calc_ElevenPoint_thresholds = true,
                    ClassWeight_sets = new (int ClassId, double ClassWeight)[weight_values.Length * weight_values.Length][]
                };

                var k = 0;
                for (var wi_index = 0; wi_index < weight_values.Length; wi_index++)
                {
                    for (var wj_index = 0; wj_index < weight_values.Length; wj_index++)
                    {
                        p3.ClassWeight_sets[k++] = new (int ClassId, double ClassWeight)[2] { (+1, weight_values[wi_index]), (-1, weight_values[wj_index]) };
                    }
                }

                var variations_3 = GetUnrolledIndexes(ct, DataSet, ExperimentName, IterationIndex, TotalGroups, InstanceId, TotalInstances, p3);

                return ct.IsCancellationRequested ? default :variations_3;
            }
            else
            {
                // return ct.IsCancellationRequested ? default :null when no tests are left
                return ct.IsCancellationRequested ? default :(null, null);
            }
        }*/