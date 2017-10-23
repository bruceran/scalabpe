@echo off

set version=1.2.6
set pluginversion=1.2.6

if "%1"=="" goto build
if "%1"=="clean" goto clean

:build
mkdir classes\scalabpe\plugin\  2>>nul 1>>nul
call scalac -deprecation -encoding UTF-8 -cp ".\lib\*;.\lib\log\*;." -d ".\classes"   src\scalabpe\core\*.scala src\scalabpe\plugin\*.scala src\scalabpe\plugin\http\*.scala src\scalabpe\plugin\cache\*.scala
rem call scalac -deprecation -encoding UTF-8 -cp ".\lib\*;." -d ".\classes"   src\scalabpe\plugin\*db*.scala
copy /Y src\scalabpe\plugin\scalabpe.plugins.conf .\classes\  2>>nul 1>>nul 
copy /Y src\scalabpe\core\release_notes.txt .\classes\  2>>nul 1>>nul
jar cf .\lib\scalabpe-core-%version%.jar -C .\classes\  .\scalabpe\core -C .\classes\ .\release_notes.txt
copy /Y src\scalabpe\plugin\release_notes.txt .\classes\  2>>nul 1>>nul
jar cf .\lib\scalabpe-plugins-%pluginversion%.jar -C .\classes\  .\scalabpe\plugin -C .\classes\ .\scalabpe.plugins.conf -C .\classes\ .\release_notes.txt
goto end

:clean
del /Q /S classes\*  2>>nul 1>>nul
del /Q /S temp\* 2>>nul 1>>nul
del /Q lib\scalabpe-core-*.jar  2>>nul 1>>nul
del /Q lib\scalabpe-plugins-*.jar  2>>nul 1>>nul
goto end

:end

