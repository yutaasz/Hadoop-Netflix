@echo off
echo =======================================================
echo EXECUTANDO JOB HADOOP MAPREDUCE - DATASET NETFLIX
echo =======================================================
set PATH=C:\Java\jdk-11\bin;C:\hadoop\bin;C:\maven\bin;%PATH%
set HADOOP_HOME=C:\hadoop
set JAVA_HOME=C:\Java\jdk-11

cd /d C:\netflix-hadoop

echo [1/3] Compilando projeto com Maven...
call mvn package -DskipTests
if errorlevel 1 (
    echo [ERRO] Falha ao compilar com Maven!
    pause
    exit /b 1
)

echo [2/3] Atualizando netflix-mapreduce.jar...
copy /y target\netflix-mapreduce-1.0-SNAPSHOT.jar netflix-mapreduce.jar >nul

echo [3/3] Executando Job Hadoop...
hadoop jar C:\netflix-hadoop\netflix-mapreduce.jar input/netflix_titles.csv output

echo.
echo =======================================================
echo RESULTADO GERADO:
echo =======================================================
type C:\netflix-hadoop\output\part-r-00000
pause