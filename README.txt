BD-AE1
------------------------------------------------------------------------------------
Fotis Savva - 1200032
Angelos Constantinides - 1200048
Zoe Gerolemou - 2023949
------------------------------------------------------------------------------------
====================================================================================
HOW TO RUN THE QUERIES
====================================================================================

Import the necessarry jars provided in "bd4-hadoop". Import the project and then generate the jar file.

The following instructions reflect the way that we have run the queries on the cluster.

After executing the command - "export HADOOP_CLASSPATH = /path/to/jar"

TASK 1:
	./java-run.sh task1.Task1Driver <input_file>  <output_file> <starting_date> <ending_date> [optional argument number of reducers]

	eg 
	./java-run.sh task1.Task1Driver small_sample.txt output 2005-08-01T13:45:23Z 2006-09-13T09:18:52Z 32

TASK 2:
	./java-run.sh task2.Task2Driver <input_file>  <output_file> <starting_date> <ending_date> <K> [optional argument number of reducers]

	eg 
	./java-run.sh task2.Task2Driver small_sample.txt output 2005-08-01T13:45:23Z 2006-09-13T09:18:52Z 10 32

TASK 3:
	./java-run.sh task3.Task3Driver <input_file>  <output_file> <date> [optional argument number of reducers]

	eg 
	./java-run.sh task3.Task3Driver small_sample.txt output 2005-08-01T13:45:23Z 32

TASK 3_f ("faster" version that uses lineCount, instead of checking every line for "REVISION "):
	./java-run.sh task3_f.Task3Driver <input_file>  <output_file> <date> [optional argument number of reducers]

	eg 
	./java-run.sh task3_f.Task3Driver small_sample.txt output 2005-08-01T13:45:23Z 32

====================================================================================
HOW TO EXPORT JOB OUTPUT TO A FILE (in local file system)
====================================================================================

The following command was used in order to export the output of a job into a text file:

hadoop fs -getmerge /output/dir/on/hdfs/ /desired/local/output/file.txt

(instead of hadoop fs, we used the corresponding script provided)
====================================================================================