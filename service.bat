
@echo off
set "lj=%~p0"
set "lj=%lj:\= %"
for %%a in (%lj%) do set wjj=%%a
set APPLICATION_NAME=%wjj%

if "%1"=="" goto run
if "%1"=="runtest" goto runtest
if "%1"=="runscala" goto runscala

:run
set JAVA_OPTS="-Dapplication.name=%APPLICATION_NAME%" 
mkdir temp\classes 2>>nul >>nul 
scala -encoding UTF-8 -cp "lib\*;temp\classes;."  jvmdbbroker.core.Main
goto end

:runtest
set JAVA_OPTS="-Dapplication.name=%APPLICATION_NAME%test" 
scala -encoding UTF-8 -cp "lib\*;classes;temp\classes;." jvmdbbroker.core.TestCaseRunner %2
goto end

:runscala
scala -encoding UTF-8 -cp ".\lib\*;.\classes;.\temp\classes;."
goto end

:end


