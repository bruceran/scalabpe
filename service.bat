
@echo off
setlocal enableextensions enabledelayedexpansion

set "lj=%~p0"
set "lj=%lj:\= %"
for %%a in (%lj%) do set wjj=%%a
set APPLICATION_NAME=%wjj%
set tempdir=temp
if "%SCALABPE_TEMPDIRROOT%" NEQ "" set tempdir=%SCALABPE_TEMPDIRROOT%\%APPLICATION_NAME%

set CLASSPATH=lib\*
for /D %%c in (lib/*) do set CLASSPATH=!CLASSPATH!;lib\%%c\*
set CLASSPATH=%CLASSPATH%;%tempdir%\classes;.

if "%1"=="" goto run
if "%1"=="runtest" goto runtest
if "%1"=="runscala" goto runscala

:run
echo projectname: %APPLICATION_NAME%
echo classpath: %CLASSPATH%
mkdir %tempdir%\classes 2>>nul >>nul 
java -Dapplication.name=%APPLICATION_NAME% -Dscalabpe.profile=%SCALABPE_PROFILE%  -Dscalabpe.tempdirroot=%SCALABPE_TEMPDIRROOT% -cp "%CLASSPATH%"  scalabpe.core.Main
goto end

:runtest
mkdir %tempdir%\classes 2>>nul >>nul 
java -Dapplication.name=%APPLICATION_NAME% -Dscalabpe.profile=%SCALABPE_PROFILE%  -Dscalabpe.tempdirroot=%SCALABPE_TEMPDIRROOT%  -cp "lib\*;classes;%tempdir%\classes;." scalabpe.core.TestCaseRunner %2 %3 %4 %5 %6 %7 
goto end

:runscala
scala -encoding UTF-8 -cp ".\lib\*;.\classes;.\%tempdir%\classes;."
goto end

:end

