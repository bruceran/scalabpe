@echo off

set version=1.1.15
set pluginversion=1.1.15

if "%1"=="" goto build
if "%1"=="clean" goto clean

:build
mkdir classes\jvmdbbroker\plugin\  2>>nul 1>>nul
call scalac -deprecation -encoding UTF-8 -cp ".\lib\*;.\lib\log\*;." -d ".\classes"   src\*.scala src\plugin\*.scala src\plugin\http\*.scala src\plugin\cache\*.scala
rem call scalac -deprecation -encoding UTF-8 -cp ".\lib\*;." -d ".\classes"   src\*.scala  
copy /Y src\plugin\jvmdbbroker.plugins.conf .\classes\  2>>nul 1>>nul 
copy /Y src\release_notes.txt .\classes\  2>>nul 1>>nul
jar cf .\lib\scalabpe-core-%version%.jar -C .\classes\  .\jvmdbbroker\core -C .\classes\ .\release_notes.txt
copy /Y src\plugin\release_notes.txt .\classes\  2>>nul 1>>nul
jar cf .\lib\scalabpe-plugins-%pluginversion%.jar -C .\classes\  .\jvmdbbroker\plugin -C .\classes\ .\jvmdbbroker.plugins.conf -C .\classes\ .\release_notes.txt
goto end

:clean
del /Q /S classes\*  2>>nul 1>>nul
del /Q /S temp\* 2>>nul 1>>nul
del /Q lib\scalabpe-core-*.jar  2>>nul 1>>nul
del /Q lib\scalabpe-plugins-*.jar  2>>nul 1>>nul
goto end

:end

