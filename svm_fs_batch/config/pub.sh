# nohup ~/SvmFsBatch/SvmFsBatch/bin/Release/netcoreapp3.1/linux-x64/publish/SvmFsBatch -cm ldr 1> /mmfs1/data/scratch/k1040015/SvmFsBatch/pbs_ldr_sub/svm_ldr.stdout 2> /mmfs1/data/scratch/k1040015/SvmFsBatch/pbs_ldr_sub/svm_ldr.stderr &

msub -K compile.pbs
cd ~/SvmFsBatch/SvmFsBatch/bin/Release/netcoreapp3.1/linux-x64/publish/

echo Running...
echo ..........
echo ~/SvmFsBatch/SvmFsBatch/bin/Release/netcoreapp3.1/linux-x64/publish/SvmFsBatch -cm ldr -en vcpu_1_1056 -pc 1 -pt 1056 1> /mmfs1/data/scratch/k1040015/SvmFsBatch/pbs_ldr_sub/svm_ldr.stdout 2> /mmfs1/data/scratch/k1040015/SvmFsBatch/pbs_ldr_sub/svm_ldr.stderr
echo ...........
echo Finished...
