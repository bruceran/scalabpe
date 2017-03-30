
@echo off
setlocal enableextensions enabledelayedexpansion

set "lj=%~p0"
set "lj=%lj:\= %"
for %%a in (%lj%) do set wjj=%%a
set APPLICATION_NAME=%wjj%

set CLASSPATH=lib\*
for /D %%c in (lib/*) do set CLASSPATH=!CLASSPATH!;lib\%%c\*
set CLASSPATH=%CLASSPATH%;temp\classes;.

if "%1"=="" goto run
if "%1"=="runtest" goto runtest
if "%1"=="runscala" goto runscala

:run
echo projectname: %APPLICATION_NAME%
echo classpath: %CLASSPATH%
mkdir temp\classes 2>>nul >>nul 
java -Dscalabpe.profile=%SCALABPE_PROFILE% -Dapplication.name=%APPLICATION_NAME% -cp "%CLASSPATH%"  jvmdbbroker.core.Main
goto end

:runtest
java -Dscalabpe.profile=%SCALABPE_PROFILE% -Dapplication.name=%APPLICATION_NAME% -cp "lib\*;classes;temp\classes;." jvmdbbroker.core.TestCaseRunner %2 %3 %4 %5 %6 %7 
goto end

:runscala
scala -encoding UTF-8 -cp ".\lib\*;.\classes;.\temp\classes;."
goto end

:end

