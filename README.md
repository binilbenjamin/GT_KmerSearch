GT_KmerSearch
=============

K-mer search using Hadoop/GenomeTools

Example command:
```
hadoop jar GT_Search.jar edu.arizona.cs.gt.search.Main -D mapred.task.timeout=1800000 -D gt.loc=/home/binilbenjamin/gt-1.5.1-Linux_x86_64-64bit/bin/gt -D gt.search.query=/home/binilbenjamin/run/query/* /home/binilbenjamin/run/index/* /home/binilbenjamin/run/output
```
