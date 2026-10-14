@echo off
echo =======================================================
echo EXECUTANDO JOB HADOOP MAPREDUCE - DATASET NETFLIX
echo =======================================================
set PATH=C:\Java\jdk-11\bin;C:\hadoop\bin;%PATH%
set HADOOP_HOME=C:\hadoop
set JAVA_HOME=C:\Java\jdk-11

cd /d C:\netflix-hadoop
hadoop jar C:\netflix-hadoop\netflix-mapreduce.jar input/netflix_titles.csv output

echo.
echo =======================================================
echo RESULTADO GERADO:
echo =======================================================
type C:\netflix-hadoop\output\part-r-00000
pause